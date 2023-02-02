from flask import request
from werkzeug.exceptions import BadRequest

from rdr_service import clock
from rdr_service.api.base_api import BaseApi
from rdr_service.api_util import RTI, RDR
from rdr_service.app_util import auth_required
from rdr_service.dao.study_nph_dao import NphIntakeDao, NphParticipantEventActivityDao, NphActivityDao, \
    NphConsentEventDao, NphEnrollmentEventDao, NphPairingEventDao
from rdr_service.model.study_nph import Activity


class NphIntakeAPI(BaseApi):
    def __init__(self):
        super().__init__(NphIntakeDao())

        self.nph_activity_dao = NphActivityDao()
        self.nph_participant_activity_dao = NphParticipantEventActivityDao()

        self.nph_consent_event_dao = NphConsentEventDao()
        self.nph_enrollment_event_dao = NphEnrollmentEventDao()
        self.nph_pairing_event_dao = NphPairingEventDao()

        self.current_activities = self.nph_activity_dao.get_all()

    @classmethod
    def extract_participant_id(cls, participant_obj: dict) -> str:
        participant_id = participant_obj['resource']['identifier'][0]['value']
        return participant_id.split('/')[-1]

    def extract_activity(self, entry: dict) -> Activity:
        if not self.current_activities:
            return BadRequest

        activity_path = entry['resource']['resourceType']

        if entry['resource'].get('class'):
            activity_path = entry['resource']['class']['code']

        current_activity = list(filter(lambda x: x.name.lower() == activity_path.lower(),
                                       self.current_activities))
        if not current_activity:
            return BadRequest

        return current_activity[0]

    @auth_required([RTI, RDR])
    def post(self):
        intake_payload = request.get_json(force=True)
        intake_payload = [intake_payload] if type(intake_payload) is not list else intake_payload

        # response_participant_ids = []
        for resource in intake_payload:
            participant_obj = list(filter(lambda x: x['resource']['resourceType'].lower() == 'patient',
                                          resource['entry']))[0]
            participant_id = self.extract_participant_id(participant_obj=participant_obj)
            if not participant_id:
                return BadRequest

            applicable_entries = [obj for obj in resource['entry'] if obj['resource']['resourceType'].lower() in [
                'consent', 'encounter']]

            # participant_event_activity = []
            for entry in applicable_entries:

                # event_map_dao = {
                #     'consent': self.nph_consent_event_dao,
                #     'enrollement': self.nph_enrollment_event_dao,
                #     'pairing': self.nph_pairing_event_dao
                # }

                current_activity = self.extract_activity(entry)
                self.nph_participant_activity_dao.insert(
                    self.nph_participant_activity_dao.model_type(**{
                        'created': clock.CLOCK.now(),
                        'modified': clock.CLOCK.now(),
                        'participant_id': participant_id,
                        'activity_id': current_activity.id,
                        'resource': entry
                    })
                )
                print('Darryl')

            # self.nph_participant_activity_dao.insert_bulk(participant_event_activity)
            # print('Darryl')
