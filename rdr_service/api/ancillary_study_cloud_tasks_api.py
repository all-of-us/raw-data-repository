import logging

from flask import request
from flask_restful import Resource
from rdr_service.api.cloud_tasks_api import log_task_headers
from rdr_service.app_util import task_auth_required
from rdr_service.dao.study_nph_dao import NphConsentEventDao, NphPairingEventDao, NphEnrollmentEventDao, \
    NphParticipantEventActivityDao
from rdr_service.dao.rex_dao import RexParticipantMappingDao
from rdr_service.dao.participant_summary_dao import ParticipantSummaryDao
from rdr_service.model.participant_summary import ParticipantSummary
from rdr_service.config import NPH_STUDY_ID, AOU_STUDY_ID


class BaseAncillaryTaskApi(Resource):

    def __init__(self):
        self.data = None

    @task_auth_required
    def post(self):
        log_task_headers()
        self.data = request.get_json(force=True)


class InsertStudyEventTaskApi(BaseAncillaryTaskApi):
    """
    Cloud Task endpoint: Inserts an event into a corresponding study [activity]_event table
    Expected data fields are:
        study: i.e. 'nph'
        activity_id: i.e. 1
        participant_id: i.e. NPH PID
        event_type_ie: i.e. 1
        event_authored_time: i.e. "2023-02-07T13:28:17.239+02:00"
    """
    def post(self):
        super(InsertStudyEventTaskApi, self).post()
        log_msg = f'Insert {self.data.get("study")} ' \
                  f'{self.data.get("activity_id")} ' \
                  f'PID: {self.data.get("participant_id")}'
        logging.info(log_msg)

        self.run_insert_operations()

        logging.info('Complete.')
        return {"success": True}

    def get_event_dao_from_request(self):

        dao_map = {
            'nph': {
                1: NphEnrollmentEventDao(),
                2: NphPairingEventDao(),
                3: NphConsentEventDao(),
            }
        }
        dao = dao_map.get(self.data.get('study')).get(self.data.get('activity_id'))

        return dao

    def get_study_participant_event_dao_from_request(self):
        dao_map = {
            'nph': NphParticipantEventActivityDao()
        }
        return dao_map.get(self.data.get('study'))

    def run_insert_operations(self):
        # get relevant participant_event_activity table for study
        pea_dao = self.get_study_participant_event_dao_from_request()
        pea_params = {
            "participant_id": self.data['participant_id'],
            "activity_id": self.data['activity_id']
        }
        pea = pea_dao.insert(pea_dao.model_type(**pea_params))

        # Insert event into corresponding dao
        dao = self.get_event_dao_from_request()
        dao.insert(dao.model_type(
            participant_id=self.data['participant_id'],
            event_authored_time=self.data['event_authored_time'],
            event_type_id=self.data['event_type_id'],
            event_id=pea.id
        ))


class UpdateParticipantSummaryForNphTaskApi(BaseAncillaryTaskApi):
    """
    Cloud Task endpoint: Updates ParticipantSummary when CONSENT, WITHDRAW, or DEACTIVATE events are received
    Expected data fields are:
        participant_id: NPH PID
        event_type: i.e. consent, withdrawal, or deactivate
        event_authored_time: i.e. "2023-02-07T13:28:17.239+02:00"
    """
    def post(self):
        super(UpdateParticipantSummaryForNphTaskApi, self).post()
        log_msg = f'Update ParticipantSummary for NPH ' \
                  f'Event: {self.data.get("event_type")}' \
                  f'PID: {self.data.get("participant_id")}'
        logging.info(log_msg)

        rex_dao = RexParticipantMappingDao()
        ps_dao = ParticipantSummaryDao()

        participant_mapping = rex_dao.get_from_ancillary_id(AOU_STUDY_ID, NPH_STUDY_ID, self.data.get("participant_id"))
        aou_pid = participant_mapping.primary_participant_id
        ps: ParticipantSummary = ps_dao.get_by_participant_id(aou_pid)

        event_type = self.data.get("event_type")
        event_authored = self.data.get("event_authored_time")
        update_types = ('consent', 'withdrawal', 'deactivation')

        if event_type and event_type not in update_types:
            logging.info(f'{event_type} cannot be used in this task.')
            return {"success": False}

        if event_type.lower() == "consent":
            ps.consentForNphModule1 = True
            ps.consentForNphModule1Authored = event_authored
        elif event_type.lower() == "withdrawal":
            ps.nphWithdrawal = True
            ps.nphWithdrawalAuthored = event_authored
        elif event_type.lower() == "deactivation":
            ps.nphDeactivation = True
            ps.nphDeactivationAuthored = event_authored

        ps_dao.update(ps)

        logging.info('Complete.')
        return {"success": True}
