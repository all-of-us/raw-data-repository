from flask import request
from werkzeug.exceptions import BadRequest

from rdr_service import clock
from rdr_service.api.base_api import BaseApi, log_api_request
from rdr_service.api_util import RTI, RDR
from rdr_service.app_util import auth_required
from rdr_service.dao.study_nph_dao import NphIntakeDao, NphParticipantEventActivityDao, NphActivityDao, \
    NphConsentEventDao, NphEnrollmentEventDao, NphPairingEventDao, NphSiteDao


class NphIntakeAPI(BaseApi):
    def __init__(self):
        super().__init__(NphIntakeDao())

        self.nph_activity_dao = NphActivityDao()
        self.nph_participant_activity_dao = NphParticipantEventActivityDao()
        self.nph_site_dao = NphSiteDao()

        self.nph_consent_event_dao = NphConsentEventDao()
        self.nph_enrollment_event_dao = NphEnrollmentEventDao()
        self.nph_pairing_event_dao = NphPairingEventDao()

        self.current_activities = self.nph_activity_dao.get_all()

    def extract_activity(self, entry: dict):
        if not self.current_activities:
            return BadRequest('')

        activity_name = entry['resource']['resourceType']

        if entry['resource'].get('class'):
            activity_name = entry['resource']['class']['code']

            if 'module' in activity_name:
                activity_name = 'enrollment'

        activity_name = activity_name.lower()
        current_activity = list(filter(lambda x: x.name.lower() == activity_name,
                                       self.current_activities))
        if not current_activity:
            return BadRequest('')

        return current_activity[0].id, activity_name

    def get_site_id(self, entry: dict):
        try:
            pairing_site_code = entry['resource']['type'][0]['coding'][0]['code']

            if not pairing_site_code:
                return BadRequest('')

            site_id = self.nph_site_dao.get_site_id_from_external(external_id=pairing_site_code)
            return site_id

        except KeyError:
            return BadRequest('')

    @classmethod
    def extract_participant_id(cls, participant_obj: dict) -> str:
        participant_id = participant_obj['resource']['identifier'][0]['value']
        return participant_id.split('/')[-1]

    @classmethod
    def extract_authored_time(cls, entry: dict):
        try:
            date_time = entry['resource'].get('dateTime')

            if not date_time and entry['resource'].get('period'):
                date_time = entry['resource']['period']['start']

            if not date_time:
                return BadRequest('')

            return date_time

        except KeyError:
            return BadRequest('')

    @auth_required([RTI, RDR])
    def post(self):
        intake_payload = request.get_json(force=True)
        intake_payload = [intake_payload] if type(intake_payload) is not list else intake_payload

        response_participant_ids = []

        event_map_dao = {
            'consent': self.nph_consent_event_dao,
            'enrollment': self.nph_enrollment_event_dao,
            'pairing': self.nph_pairing_event_dao
        }

        for resource in intake_payload:
            participant_obj = list(filter(lambda x: x['resource']['resourceType'].lower() == 'patient',
                                          resource['entry']))[0]
            participant_id = self.extract_participant_id(participant_obj=participant_obj)
            if not participant_id:
                return BadRequest

            response_participant_ids.append({'nph_participant_id': participant_id})
            applicable_entries = [obj for obj in resource['entry'] if obj['resource']['resourceType'].lower() in [
                'consent', 'encounter']]

            for entry in applicable_entries:

                current_activity_id, activity_name = self.extract_activity(entry)

                participant_event_obj = self.nph_participant_activity_dao.insert(
                    self.nph_participant_activity_dao.model_type(**{
                        'created': clock.CLOCK.now(),
                        'modified': clock.CLOCK.now(),
                        'participant_id': participant_id,
                        'activity_id': current_activity_id,
                        'resource': entry
                    })
                )

                nph_dao = event_map_dao[activity_name]

                event_obj = {
                    'created': clock.CLOCK.now(),
                    'modified': clock.CLOCK.now(),
                    'event_authored_time': self.extract_authored_time(entry),
                    'participant_id': participant_id,
                    'event_id': participant_event_obj.id,
                    'event_type_id': 1  # defaulting for now
                }

                if activity_name == 'pairing':
                    event_obj['site_id'] = self.get_site_id(entry)

                nph_dao.insert(nph_dao.model_type(**event_obj))

        log_api_request(log=request.log_record)
        return self._make_response(response_participant_ids)

