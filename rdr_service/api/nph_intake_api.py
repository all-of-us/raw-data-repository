from dataclasses import dataclass

from flask import request
from werkzeug.exceptions import BadRequest

from rdr_service import clock
from rdr_service.api.base_api import BaseApi, log_api_request
from rdr_service.api_util import RTI, RDR
from rdr_service.app_util import auth_required
from rdr_service.dao.study_nph_dao import NphIntakeDao, NphParticipantEventActivityDao, NphActivityDao, \
    NphConsentEventDao, NphEnrollmentEventDao, NphPairingEventDao, NphSiteDao


@dataclass
class ActivityData:
    id: int
    name: str
    source: str


class NphIntakeAPI(BaseApi):
    def __init__(self):
        super().__init__(NphIntakeDao())

        self.current_activities = NphActivityDao().get_all()

        self.nph_participant_activity_dao = NphParticipantEventActivityDao()
        self.nph_site_dao = NphSiteDao()

        self.nph_consent_event_dao = NphConsentEventDao()
        self.nph_enrollment_event_dao = NphEnrollmentEventDao()
        self.nph_pairing_event_dao = NphPairingEventDao()

        self.bundle_identifier = None

    def extract_activity_data(self, entry: dict):
        activity_name, activity_source = None, None

        try:
            # consent payload
            activity_name = entry['resource']['resourceType']

            # if not consent
            if entry['resource'].get('class'):
                activity_name = activity_source = entry['resource']['class']['code']

                if activity_name not in ['pairing']:
                    activity_name = 'enrollment'

            if entry['resource'].get('serviceType'):
                activity_source = entry['resource']['serviceType']['coding']['code']

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

            site_id = self.nph_site_dao.get_site_id_from_external(external_id=pairing_site_code)
            return site_id

        except KeyError as e:
            raise BadRequest(f'Key error on site lookup: {e} bundle_id: {self.bundle_identifier}')

    def get_event_type_id(self):
        print('Darryl')

    @classmethod
    def extract_participant_id(cls, participant_obj: dict) -> str:
        participant_id = participant_obj['resource']['identifier'][0]['value']
        return participant_id.split('/')[-1]

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

    @auth_required([RTI, RDR])
    def post(self):
        intake_payload = request.get_json(force=True)
        intake_payload = [intake_payload] if type(intake_payload) is not list else intake_payload

        event_map_dao = {
            'consent': self.nph_consent_event_dao,
            'enrollment': self.nph_enrollment_event_dao,
            'pairing': self.nph_pairing_event_dao
        }

        participant_event_objs, event_objs = [], []

        for resource in intake_payload:
            self.bundle_identifier = resource['identifier']['value']

            participant_obj = list(filter(lambda x: x['resource']['resourceType'].lower() == 'patient',
                                          resource['entry']))[0]

            participant_id = self.extract_participant_id(participant_obj=participant_obj)

            applicable_entries = [obj for obj in resource['entry'] if obj['resource']['resourceType'].lower() in [
                'consent', 'encounter']]

            for entry in applicable_entries:

                activity_data = self.extract_activity_data(entry)
                entry['bundle_identifier'] = self.bundle_identifier

                participant_event_objs.append({
                    'created': clock.CLOCK.now(),
                    'modified': clock.CLOCK.now(),
                    'participant_id': participant_id,
                    'activity_id': activity_data.id,
                    'resource': entry
                })

                nph_dao = event_map_dao[activity_data.name]

                event_obj = {
                    'created': clock.CLOCK.now(),
                    'modified': clock.CLOCK.now(),
                    'event_authored_time': self.extract_authored_time(entry),
                    'participant_id': participant_id,
                    'event_type_id': self.get_event_type_id(),
                    'additional': {
                        'nph_dao': nph_dao,
                        'activity_id': activity_data.id,
                        'bundle_identifier': self.bundle_identifier
                    }
                }

                if activity_data.name == 'pairing':
                    event_obj['site_id'] = self.get_site_id(entry)

                event_objs.append(event_obj)

        self.nph_participant_activity_dao.insert_bulk(participant_event_objs)

        for dao_key, dao in event_map_dao.items():
            dao_event_objs = list(filter(
                lambda x: x.hasattr('additional', x) and dao_key in x['additional'][
                    'nph_dao'].__class__.__name__.lower(), event_objs
            ))
            for dao_obj in dao_event_objs:
                participant_event_obj = self.nph_participant_activity_dao.get_activity_event_intake(
                    participant_id=dao_obj['participant_id'],
                    resource_identifier=dao_obj['additional']['bundle_identifier'],
                    activity_id=dao_obj['additional']['activity_id']
                )
                dao_obj['event_id'] = participant_event_obj.id
                del dao_obj['additional']

            if dao_event_objs:
                dao.insert_bulk(dao_event_objs)

        log_api_request(log=request.log_record)
        return self._make_response([])
