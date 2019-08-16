import unittest
from rdr_service.code_constants import PPI_SYSTEM
from rdr_service.concepts import Concept
from rdr_service.dao.participant_dao import ParticipantDao
from rdr_service.dao import questionnaire_dao, questionnaire_response_dao
from rdr_service.model.participant import Participant, ParticipantHistory
from rdr_service.model.participant_summary import ParticipantSummary
from rdr_service.participant_enums import (
    EnrollmentStatus,
    OrganizationType,
    SuspensionStatus,
    UNSET_HPO_ID,
    WithdrawalStatus,
)
from rdr_service.test.test_data import data_path
from rdr_service.unicode_csv import UnicodeDictReader
from tests.helpers.mysql_helper import reset_mysql_instance

class BaseTestCase(unittest.TestCase):
    """ Base class for unit tests."""

    def setUp(self):
        reset_mysql_instance()
        # Allow printing the full diff report on errors.
        self.maxDiff = None
        # Always add codes if missing when handling questionnaire responses.
        questionnaire_dao._add_codes_if_missing = lambda: True
        questionnaire_response_dao._add_codes_if_missing = lambda email: True

    @staticmethod
    def _participant_with_defaults(**kwargs):
        """Creates a new Participant model, filling in some default constructor args.

        This is intended especially for updates, where more fields are required than for inserts.
        """
        common_args = {
            "hpoId": UNSET_HPO_ID,
            "withdrawalStatus": WithdrawalStatus.NOT_WITHDRAWN,
            "suspensionStatus": SuspensionStatus.NOT_SUSPENDED,
        }
        common_args.update(kwargs)
        return Participant(**common_args)

    @staticmethod
    def _participant_summary_with_defaults(**kwargs):
        common_args = {
            "hpoId": UNSET_HPO_ID,
            "numCompletedPPIModules": 0,
            "numCompletedBaselinePPIModules": 0,
            "numBaselineSamplesArrived": 0,
            "numberDistinctVisits": 0,
            "withdrawalStatus": WithdrawalStatus.NOT_WITHDRAWN,
            "suspensionStatus": SuspensionStatus.NOT_SUSPENDED,
            "enrollmentStatus": EnrollmentStatus.INTERESTED,
        }
        common_args.update(kwargs)
        return ParticipantSummary(**common_args)

    @staticmethod
    def _participant_history_with_defaults(**kwargs):
        common_args = {
            "hpoId": UNSET_HPO_ID,
            "version": 1,
            "withdrawalStatus": WithdrawalStatus.NOT_WITHDRAWN,
            "suspensionStatus": SuspensionStatus.NOT_SUSPENDED,
        }
        common_args.update(kwargs)
        return ParticipantHistory(**common_args)

    def submit_questionnaire_response(
        self, participant_id, questionnaire_id, race_code, gender_code, state, date_of_birth
    ):
        code_answers = []
        date_answers = []
        if race_code:
            code_answers.append(("race", Concept(PPI_SYSTEM, race_code)))
        if gender_code:
            code_answers.append(("genderIdentity", Concept(PPI_SYSTEM, gender_code)))
        if date_of_birth:
            date_answers.append(("dateOfBirth", date_of_birth))
        if state:
            code_answers.append(("state", Concept(PPI_SYSTEM, state)))
        qr = make_questionnaire_response_json(
            participant_id, questionnaire_id, code_answers=code_answers, date_answers=date_answers
        )
        self.send_post("Participant/%s/QuestionnaireResponse" % participant_id, qr)

    def submit_consent_questionnaire_response(self, participant_id, questionnaire_id, ehr_consent_answer):
        code_answers = [("ehrConsent", Concept(PPI_SYSTEM, ehr_consent_answer))]
        qr = make_questionnaire_response_json(participant_id, questionnaire_id, code_answers=code_answers)
        self.send_post("Participant/%s/QuestionnaireResponse" % participant_id, qr)

    def participant_summary(self, participant):
        summary = ParticipantDao.create_summary_for_participant(participant)
        summary.firstName = self.fake.first_name()
        summary.lastName = self.fake.last_name()
        summary.email = self.fake.email()
        return summary

