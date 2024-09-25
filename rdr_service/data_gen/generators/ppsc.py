from datetime import datetime

from rdr_service import clock
from rdr_service.dao import database_factory
from rdr_service.model.ppsc import Participant, Activity, EnrollmentEventType, ConsentEvent, ProfileUpdatesEvent, \
    SurveyCompletionEvent, PartnerActivity
from rdr_service.model.ppsc_data_transfer import (
    PPSCDataTransferAuth, PPSCDataTransferEndpoint,
    PPSCDataTransferRecord, PPSCCore, PPSCEHR, PPSCBiobankSample,
    PPSCHealthData, PPSCHealthData, PPSCBiobankSample, PPSCEHR, PPSCCore
)

DATETIME_FORMAT = "%Y-%m-%d %H:%M:%S"
TIME = datetime.strptime(datetime.now().strftime(DATETIME_FORMAT), DATETIME_FORMAT)


class PPSCBaseDataGenerator:
    def __init__(self):
        self.session = database_factory.get_database().make_session()

    def _commit_to_database(self, model):
        self.session.add(model)
        self.session.commit()


class PPSCDataGenerator(PPSCBaseDataGenerator):
    def __init__(self):
        super().__init__()
        self._next_unique_participant_id = 100000000
        self._next_unique_biobank_id = 1100000000
        self._next_unique_research_id = 10000

    def unique_participant_id(self):
        next_participant_id = self._next_unique_participant_id
        self._next_unique_participant_id += 1
        return next_participant_id

    def unique_biobank_id(self):
        next_biobank_id = self._next_unique_biobank_id
        self._next_unique_biobank_id += 1
        return next_biobank_id

    @staticmethod
    def create_participant(**kwargs):
        return Participant(**kwargs)

    def create_database_participant(self, **kwargs):
        participant = {
            "id": self.unique_participant_id(),
            "biobank_id": self.unique_biobank_id(),
            "registered_date": clock.CLOCK.now()
        }
        participant.update(kwargs)
        participant = self.create_participant(**participant)
        self._commit_to_database(participant)
        return participant

    @staticmethod
    def _activity(**kwargs):
        return Activity(**kwargs)

    def create_database_activity(self, **kwargs):
        activity = self._activity(**kwargs)
        self._commit_to_database(activity)
        return activity

    @staticmethod
    def _partner_activity(**kwargs):
        return PartnerActivity(**kwargs)

    def create_database_partner_activity(self, **kwargs):
        partner_activity = self._partner_activity(**kwargs)
        self._commit_to_database(partner_activity)
        return partner_activity

    @staticmethod
    def _enrollment_event_type(**kwargs):
        return EnrollmentEventType(**kwargs)

    def create_database_enrollment_event_type(self, **kwargs):
        event_type = self._enrollment_event_type(**kwargs)
        self._commit_to_database(event_type)
        return event_type

    @staticmethod
    def _consent_event(**kwargs):
        return ConsentEvent(**kwargs)

    def create_database_consent_event(self, **kwargs):
        consent_event = self._consent_event(**kwargs)
        self._commit_to_database(consent_event)
        return consent_event

    @staticmethod
    def _profile_updates_event(**kwargs):
        return ProfileUpdatesEvent(**kwargs)

    def create_database_profile_updates_event(self, **kwargs):
        profile_updates_event = self._profile_updates_event(**kwargs)
        self._commit_to_database(profile_updates_event)
        return profile_updates_event

    @staticmethod
    def _survey_completion_event(**kwargs):
        return SurveyCompletionEvent(**kwargs)

    def create_database_survey_completion_event(self, **kwargs):
        survey_completion_event = self._survey_completion_event(**kwargs)
        self._commit_to_database(survey_completion_event)
        return survey_completion_event

    @staticmethod
    def _ppsc_sync_auth(**kwargs):
        return PPSCDataTransferAuth(**kwargs)

    def create_database_ppsc_sync_auth(self, **kwargs):
        auth_event = self._ppsc_sync_auth(**kwargs)
        self._commit_to_database(auth_event)
        return auth_event

    @staticmethod
    def _ppsc_data_sync_endpoint(**kwargs):
        return PPSCDataTransferEndpoint(**kwargs)

    def create_database_ppsc_data_sync_endpoint(self, **kwargs):
        ppsc_data_endpoint = self._ppsc_data_sync_endpoint(**kwargs)
        self._commit_to_database(ppsc_data_endpoint)
        return ppsc_data_endpoint

    @staticmethod
    def _ppsc_data_sync_record(**kwargs):
        return PPSCDataTransferRecord(**kwargs)

    def create_database_ppsc_data_sync_record(self, **kwargs):
        record = self._ppsc_data_sync_record(**kwargs)
        self._commit_to_database(record)
        return record

    @staticmethod
    def _ppsc_data_core(**kwargs):
        return PPSCCore(**kwargs)

    def create_database_ppsc_data_core(self, **kwargs):
        core = self._ppsc_data_core(**kwargs)
        self._commit_to_database(core)
        return core

    @staticmethod
    def _ppsc_data_ehr(**kwargs):
        return PPSCEHR(**kwargs)

    def create_database_ppsc_data_ehr(self, **kwargs):
        ehr = self._ppsc_data_ehr(**kwargs)
        self._commit_to_database(ehr)
        return ehr

    @staticmethod
    def _ppsc_data_biobank(**kwargs):
        return PPSCBiobankSample(**kwargs)

    def create_database_ppsc_data_biobank(self, **kwargs):
        biobank = self._ppsc_data_biobank(**kwargs)
        self._commit_to_database(biobank)
        return biobank

    @staticmethod
    def _ppsc_data_health_data(**kwargs):
        return PPSCHealthData(**kwargs)

    def create_database_ppsc_data_health_data(self, **kwargs):
        health = self._ppsc_data_health_data(**kwargs)
        self._commit_to_database(health)
        return health
