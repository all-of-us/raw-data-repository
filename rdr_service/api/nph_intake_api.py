from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import List, Optional

from flask import request
from werkzeug.exceptions import BadRequest, NotFound

from rdr_service import clock
from rdr_service.ancillary_study_resources.nph.enums import ConsentOptInTypes, ParticipantOpsElementTypes
from rdr_service.api.base_api import BaseApi, log_api_request
from rdr_service.api_util import RTI, RDR
from rdr_service.app_util import auth_required
from rdr_service.cloud_utils.gcp_cloud_tasks import GCPCloudTask
from rdr_service.config import GAE_PROJECT
from rdr_service.dao.study_nph_dao import NphIntakeDao, NphParticipantEventActivityDao, NphActivityDao, \
    NphPairingEventDao, NphSiteDao, NphDefaultBaseDao, NphEnrollmentEventTypeDao, NphConsentEventTypeDao, \
    NphParticipantDao
from rdr_service.model.study_nph import WithdrawalEvent, DeactivationEvent, ConsentEvent, EnrollmentEvent, \
    ParticipantOpsDataElement

MAX_PAYLOAD_LENGTH = 50


@dataclass
class ActivityData:
    id: int
    name: str
    source: str


@dataclass
class EntryObjData:
    participant_event_objs: List
    all_event_objs: List
    all_participant_ops_data: List
    summary_updates: List


class PostIntakePayload(ABC):

    def __init__(self, intake_payload):
        self.current_activities = NphActivityDao().get_all()
        self.nph_participant_dao = NphParticipantDao()
        self.nph_participant_activity_dao = NphParticipantEventActivityDao()
        self.nph_site_dao = NphSiteDao()

        self.nph_consent_type_dao = NphConsentEventTypeDao()
        self.nph_enrollment_type_dao = NphEnrollmentEventTypeDao()

        self.nph_pairing_event_dao = NphPairingEventDao()
        self.nph_consent_event_dao = NphDefaultBaseDao(model_type=ConsentEvent)
        self.nph_enrollment_event_dao = NphDefaultBaseDao(model_type=EnrollmentEvent)
        self.nph_withdrawal_event_dao = NphDefaultBaseDao(model_type=WithdrawalEvent)
        self.nph_deactivation_event_dao = NphDefaultBaseDao(model_type=DeactivationEvent)

        self.participant_op_data = NphDefaultBaseDao(model_type=ParticipantOpsDataElement)
        self.bundle_identifier = None
        self.event_dao_map = {}

        self.participant_response = []
        self.intake_payload = intake_payload
        # fhir + json
        self.applicable_entries = [
            'consent',
            'encounter',
            'pairing',
            'enrollmentstatus'
        ]

    @classmethod
    def create_post_intake(cls, *, intake_payload):
        return cls(intake_payload)

    def build_event_dao_map(self) -> dict:
        event_dao_map = {}
        event_dao_instance_items = {k: v for k, v in self.__dict__.items() if 'event_dao' in k}
        for activity in self.current_activities:
            event_dao_map[f'{activity.name.lower()}'] = event_dao_instance_items[
                f'nph_{activity.name.lower()}_event_dao']
        return event_dao_map

    def validate_payload_length(self) -> None:
        self.intake_payload = [self.intake_payload] if type(self.intake_payload) is not list else self.intake_payload
        if len(self.intake_payload) > MAX_PAYLOAD_LENGTH:
            raise BadRequest(f'Payload bundle(s) length is limited to {MAX_PAYLOAD_LENGTH}')

    @abstractmethod
    def create_ops_data_elements(self, *, participant_id: str, participant_obj: dict) -> List[dict]:
        ...

    @abstractmethod
    def get_site_id(self, entry: dict) -> Optional[int]:
        ...

    @abstractmethod
    def extract_participant_id(self, participant_obj) -> Optional[str]:
        ...

    @abstractmethod
    def extract_activity_data(self, entry: dict, activity_name: str = None) -> ActivityData:
        ...

    @abstractmethod
    def extract_authored_time(self, entry: dict) -> str:
        ...

    @abstractmethod
    def get_consent_events(self, entry: dict) -> List[dict]:
        ...

    @abstractmethod
    def get_bundle_identifier(self, entry: dict) -> int:
        ...

    @abstractmethod
    def iterate_entries(self) -> EntryObjData:
        ...

    def get_event_type_id(self, *, activity_name: str, activity_source: str) -> Optional[int]:
        event_type_dao_instance_items = {k: v for k, v in self.__dict__.items() if 'type_dao' in k}

        if not any(activity_name in key for key in event_type_dao_instance_items):
            return 1

        event_type_dao = event_type_dao_instance_items[f'nph_{activity_name}_type_dao']
        event_activity = event_type_dao.get_event_by_source_name(source_name=activity_source)

        if not event_activity:
            raise BadRequest(f'Cannot find event type: bundle_id: {self.bundle_identifier}')

        return event_activity.id

    def create_event_objs(self, *, participant_id: str, entry: dict, activity_data: ActivityData) -> List:

        nph_event_dao = self.event_dao_map[activity_data.name]
        current_entry_consent_events = self.get_consent_events(entry)
        current_entry_events = current_entry_consent_events if current_entry_consent_events else [entry]

        current_event_objs = []
        for entry_event in current_entry_events:
            # base event obj
            event_obj = {
                'created': clock.CLOCK.now(),
                'modified': clock.CLOCK.now(),
                'event_authored_time': self.extract_authored_time(entry),
                'participant_id': participant_id,
            }

            # handle consent events based on provisions in payload
            if current_entry_consent_events:
                event_obj['provision'] = entry_event

            # handle pairing event based on model
            if hasattr(nph_event_dao.model_type.__table__.columns, 'site_id'):
                event_obj['site_id'] = self.get_site_id(entry)

            # handle models with event type relationships
            if hasattr(nph_event_dao.model_type.__table__.columns, 'event_type_id'):
                # source for consent activity data should be null add opt_in value
                if activity_data.name == 'consent' and event_obj.get('provision'):
                    activity_data.source = event_obj['provision']['code']
                    event_obj['opt_in'] = event_obj['provision']['opt_in']

                event_obj['event_type_id'] = self.get_event_type_id(
                    activity_name=activity_data.name,
                    activity_source=activity_data.source
                )

            event_obj.pop('provision', None)

            # handle additional keys for later processing
            event_obj['additional'] = {
                'nph_event_dao': nph_event_dao,
                'activity_id': activity_data.id,
                'bundle_identifier': self.bundle_identifier
            }
            current_event_objs.append(event_obj)

        return current_event_objs

    def handle_data_inserts(self, **kwargs) -> None:

        # insert participant ops data elements
        if kwargs.get('all_participant_ops_data'):
            self.participant_op_data.insert_bulk(kwargs.get('all_participant_ops_data'))

        # insert participant activity events
        if kwargs.get('participant_event_objs'):
            self.nph_participant_activity_dao.insert_bulk(kwargs.get('participant_event_objs'))

        # insert activity events
        if kwargs.get('all_event_objs'):
            for dao_key, dao in self.event_dao_map.items():
                dao_event_objs = list(filter(
                    lambda x: x.get('additional') and dao_key in x['additional'][
                        'nph_event_dao'].model_type.__name__.lower(), kwargs.get('all_event_objs')
                ))
                for dao_obj in dao_event_objs:
                    participant_event_obj = self.nph_participant_activity_dao.get_activity_event_intake(
                        participant_id=dao_obj['participant_id'],
                        resource_identifier=dao_obj['additional']['bundle_identifier'],
                        activity_id=dao_obj['additional']['activity_id']
                    )
                    dao_obj['event_id'] = participant_event_obj.id
                    dao_obj.pop('additional')

                if dao_event_objs:
                    dao.insert_bulk(dao_event_objs)

    def handle_data_from_payload(self):

        self.validate_payload_length()
        self.event_dao_map = self.build_event_dao_map()
        entry_obj = self.iterate_entries()

        self.handle_data_inserts(
            participant_event_objs=entry_obj.participant_event_objs,
            all_event_objs=entry_obj.all_event_objs,
            all_participant_ops_data=entry_obj.all_participant_ops_data
        )

        if GAE_PROJECT != 'localhost' and entry_obj.summary_updates:
            cloud_task = GCPCloudTask()
            for summary_update in entry_obj.summary_updates:
                cloud_task.execute(
                    endpoint='update_participant_summary_for_nph_task',
                    payload=summary_update,
                    queue='nph'
                )


# FHIR specific payloads
class PostIntakePayloadFHIR(PostIntakePayload):

    def get_bundle_identifier(self, entry: dict) -> int:
        return entry['identifier']['value']

    def create_ops_data_elements(self, *, participant_id: str, participant_obj: dict) -> List[dict]:
        elements_found = []
        for key, value in participant_obj['resource'].items():
            try:
                elements_found.append({
                    'created': clock.CLOCK.now(),
                    'modified': clock.CLOCK.now(),
                    'participant_id': participant_id,
                    'source_data_element': ParticipantOpsElementTypes.lookup_by_name(key.upper()),
                    'source_value': value
                })
            except KeyError:
                pass
        return elements_found

    def extract_activity_data(self, entry: dict, activity_name: str = None):
        activity_name, activity_source = None, None

        try:
            activity_name = entry['resource']['resourceType']

            if entry['resource'].get('class'):
                activity_name = activity_source = entry['resource']['class']['code']

                if 'module' in activity_name:
                    activity_name = 'enrollment'

            if entry['resource'].get('serviceType'):
                activity_source = entry['resource']['serviceType']['coding'][0]['code']

            activity_name = activity_name.lower()
            current_activity = list(filter(lambda x: x.name.lower() == activity_name,
                                           self.current_activities))
            if not current_activity:
                raise BadRequest(f'Cannot reconcile activity type bundle_id: {self.bundle_identifier}')

            return ActivityData(
                id=current_activity[0].id,
                name=activity_name,
                source=activity_source
            )

        except KeyError as e:
            return BadRequest(f'Key error on activity lookup: {e} bundle_id: {self.bundle_identifier}')

    def get_site_id(self, entry: dict):
        try:
            pairing_site_code = entry['resource']['type'][0]['coding'][0]['code']

            if not pairing_site_code:
                raise BadRequest(f'Cannot find site pairing code: bundle_id: {self.bundle_identifier}')

            site = self.nph_site_dao.get_site_id_from_external(external_id=pairing_site_code)

            if not site:
                raise BadRequest(f'Cannot find site from site code: bundle_id: {self.bundle_identifier}')

            return site.id

        except KeyError as e:
            raise BadRequest(f'Key error on site lookup: {e} bundle_id: {self.bundle_identifier}')

    def get_consent_events(self, entry: dict) -> List[dict]:
        consents = []
        consent_module_data = entry['resource'].get('provision')
        if not consent_module_data:
            return []
        try:
            consents.append({
                'opt_in': ConsentOptInTypes.lookup_by_name(consent_module_data.get('type').upper()),
                'code': consent_module_data.get('purpose')[0]['code']
            })
            for provision in consent_module_data.get('provision'):
                consents.append({
                    'opt_in': ConsentOptInTypes.lookup_by_name(provision.get('type').upper()),
                    'code': provision.get('purpose')[0]['code']
                })
            return consents

        except KeyError as e:
            raise BadRequest(f'Key error on provision lookup: {e} bundle_id: {self.bundle_identifier}')

    def extract_participant_id(self, participant_obj: dict):
        try:
            participant_id = participant_obj['resource']['identifier'][0]['value']
            participant_str_data = participant_id.split('/')
            with self.nph_participant_dao.session() as session:
                is_nph_participant = self.nph_participant_dao.get_participant_by_id(
                    participant_str_data[-1],
                    session
                )
                if not is_nph_participant:
                    raise NotFound(f'NPH participant {participant_str_data[-1]} not found bundle_id:'
                                   f' {self.bundle_identifier}')
                return participant_str_data[1]
        except (KeyError, Exception) as e:
            raise BadRequest(f'Cannot parse participant information from payload: {e} bundle_id:'
                             f' {self.bundle_identifier}')

    def extract_authored_time(self, entry: dict):
        try:
            date_time = entry['resource'].get('dateTime')

            if not date_time and entry['resource'].get('period'):
                date_time = entry['resource']['period']['start']

            if not date_time:
                raise BadRequest(f'Cannot get value on authored time lookup bundle_id: {self.bundle_identifier}')

            return date_time

        except KeyError as e:
            raise BadRequest(f'KeyError on authored time lookup: {e} bundle_id: {self.bundle_identifier}')

    def iterate_entries(self) -> EntryObjData:
        participant_event_objs, all_event_objs, all_participant_ops_data, summary_updates = [], [], [], []

        for resource in self.intake_payload:
            self.bundle_identifier = self.get_bundle_identifier(resource)

            participant_obj = list(filter(lambda x: x['resource']['resourceType'].lower() == 'patient',
                                          resource['entry']))[0]
            participant_id = self.extract_participant_id(participant_obj=participant_obj)

            all_participant_ops_data.extend(
                self.create_ops_data_elements(
                    participant_id=participant_id,
                    participant_obj=participant_obj
                )
            )

            self.participant_response.append({'nph_participant_id': participant_id})

            applicable_entries = [
                obj for obj in resource['entry'] if obj['resource']['resourceType'].lower() in self.applicable_entries
            ]

            for entry in applicable_entries:
                activity_data = self.extract_activity_data(entry)

                # fhir
                entry['bundle_identifier'] = self.bundle_identifier

                participant_event_objs.append({
                    'created': clock.CLOCK.now(),
                    'modified': clock.CLOCK.now(),
                    'participant_id': participant_id,
                    'activity_id': activity_data.id,
                    'resource': entry
                })

                event_objs = self.create_event_objs(
                    participant_id=participant_id,
                    entry=entry,
                    activity_data=activity_data
                )

                if activity_data.name in ('consent', 'withdrawal', 'deactivation'):
                    summary_update = {
                        'event_type': activity_data.name,
                        'participant_id': participant_id,
                        'event_authored_time': event_objs[0]['event_authored_time']
                    }
                    summary_updates.append(summary_update)

                all_event_objs.extend(event_objs)

        return EntryObjData(
            participant_event_objs=participant_event_objs,
            all_event_objs=all_event_objs,
            all_participant_ops_data=all_participant_ops_data,
            summary_updates=summary_updates
        )


# Custom JSON specific payloads
class PostIntakePayloadJSON(PostIntakePayload):

    def __init__(self, intake_payload):
        super().__init__(intake_payload)

        self.current_module = None

    def get_bundle_identifier(self, entry: dict) -> str:
        return entry['info'].get('ListId')

    def create_ops_data_elements(self, *, participant_id: str, participant_obj: dict) -> List[dict]:
        # sigh have to maintain this
        elements_found, enum_mapping = [], {
            'DOB': 'Birthdate'
        }
        for key, value in participant_obj.items():
            try:
                elements_found.append({
                    'created': clock.CLOCK.now(),
                    'modified': clock.CLOCK.now(),
                    'participant_id': participant_id,
                    'source_data_element': ParticipantOpsElementTypes.lookup_by_name(enum_mapping[key.upper()].upper()),
                    'source_value': value
                })
            except KeyError:
                pass
        return elements_found

    def get_site_id(self, entry: dict) -> Optional[int]:
        ...

    def extract_participant_id(self, participant_obj: dict) -> Optional[str]:
        try:
            participant_id = participant_obj.get('NPHId', None)
            with self.nph_participant_dao.session() as session:
                is_nph_participant = self.nph_participant_dao.get_participant_by_id(
                    participant_id,
                    session
                )
                if not is_nph_participant:
                    raise NotFound(f'NPH participant {participant_id} not found bundle_id:'
                                   f' {self.bundle_identifier}')
                return participant_id
        except (KeyError, Exception) as e:
            raise BadRequest(f'Cannot parse participant information from payload: {e} bundle_id:'
                             f' {self.bundle_identifier}')

    def extract_activity_data(self, entry: List[dict], activity_name: str = None) -> List[ActivityData]:
        current_entry_activity_data = []

        try:
            current_activity = list(filter(lambda x: x.name.lower() == activity_name,
                                           self.current_activities))
            if not current_activity:
                raise BadRequest(f'Cannot reconcile activity type bundle_id: {self.bundle_identifier}')

            # activity_source_map = {
            #     'enrollment': 'Status'
            # }[activity_name]

            # for _ in activity_entries:
            #     current_entry_activity_data.append(
            #         ActivityData(
            #             id=current_activity[0].id,
            #             name=activity_name,
            #             source=f'{self.current_module}_{activity_source_map}'.lower()
            #         )
            #     )
            return current_entry_activity_data

        except KeyError as e:
            return BadRequest(f'Key error on activity lookup: {e} bundle_id: {self.bundle_identifier}')

    def extract_authored_time(self, entry: dict) -> str:
        ...

    def get_consent_events(self, entry: dict) -> List[dict]:
        ...

    def iterate_entries(self):
        participant_event_objs, all_event_objs, all_participant_ops_data, summary_updates = [], [], [], []

        for resource in self.intake_payload:
            self.bundle_identifier = self.get_bundle_identifier(resource)
            participants = resource['info'].get('Participants')

            for participant_obj in participants:

                self.current_module = participant_obj.get('Module')
                if not self.current_module:
                    return BadRequest(f'Key error on module lookup: bundle_id: {self.bundle_identifier}')

                participant_id = self.extract_participant_id(participant_obj=participant_obj)

                all_participant_ops_data.extend(
                    self.create_ops_data_elements(
                        participant_id=participant_id,
                        participant_obj=participant_obj
                    )
                )

                self.participant_response.append({'nph_participant_id': participant_id})

                applicable_entries: dict = {k.lower(): v for k, v in participant_obj.items()
                                            if k.lower() in self.applicable_entries}

                for key, entry in applicable_entries.items():
                    # activity_entries = [entry] if type(entry) is not list else entry
                    activity_name = key.lower().replace('status', '')
                    # should be list of activity_data
                    activity_data_list = self.extract_activity_data(
                        entry=entry,
                        activity_name=activity_name
                    )
                    for activity_data in activity_data_list:
                        # fhir
                        entry['bundle_identifier'] = self.bundle_identifier

                        participant_event_objs.append({
                            'created': clock.CLOCK.now(),
                            'modified': clock.CLOCK.now(),
                            'participant_id': participant_id,
                            'activity_id': activity_data.id,
                            'resource': entry
                        })

                        event_objs = self.create_event_objs(
                            participant_id=participant_id,
                            entry=entry,
                            activity_data=activity_data
                        )

                        if activity_data.name in ('consent', 'withdrawal', 'deactivation'):
                            summary_update = {
                                'event_type': activity_data.name,
                                'participant_id': participant_id,
                                'event_authored_time': event_objs[0]['event_authored_time']
                            }
                            summary_updates.append(summary_update)

                        all_event_objs.extend(event_objs)

        return EntryObjData(
            participant_event_objs=participant_event_objs,
            all_event_objs=all_event_objs,
            all_participant_ops_data=all_participant_ops_data,
            summary_updates=summary_updates
        )


class NphIntakeAPI(BaseApi):
    def __init__(self):
        super().__init__(NphIntakeDao())

    @auth_required([RTI, RDR])
    def post(self):
        # Adding request log here so if exception is raised
        # per validation fail the payload is stored
        log_api_request(log=request.log_record)
        intake_payload = request.get_json(force=True)

        post_map = {
            'fhir': PostIntakePayloadFHIR,
            'json': PostIntakePayloadJSON
        }['fhir' if 'fhir' in request.path.lower() else 'json']

        post_intake_api = post_map.create_post_intake(
            intake_payload=intake_payload
        )
        post_intake_api.handle_data_from_payload()
        return self._make_response(post_intake_api.participant_response)
