import os
import faker
import unittest

from rdr_service.code_constants import PPI_SYSTEM
from rdr_service.concepts import Concept
from rdr_service.dao import questionnaire_dao, questionnaire_response_dao
from rdr_service.dao.participant_dao import ParticipantDao
from rdr_service.model.participant import Participant, ParticipantHistory
from rdr_service.model.participant_summary import ParticipantSummary
from rdr_service.participant_enums import (
    EnrollmentStatus,
    SuspensionStatus,
    UNSET_HPO_ID,
    WithdrawalStatus,
)
from tests.helpers.mysql_helper import reset_mysql_instance


class BaseTestCase(unittest.TestCase):
    """ Base class for unit tests."""

    def __init__(self, *args, **kwargs):
        super(BaseTestCase, self).__init__(*args, **kwargs)
        # Set this so the database factory knows to use the unittest connection string from the config.
        os.environ["UNITTEST_FLAG"] = "True"
        self.fake = faker.Faker()

    def setUp(self) -> None:
        super(BaseTestCase, self).setUp()

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


def make_questionnaire_response_json(
    participant_id,
    questionnaire_id,
    code_answers=None,
    string_answers=None,
    date_answers=None,
    uri_answers=None,
    language=None,
    authored=None,
):
    results = []
    if code_answers:
        for answer in code_answers:
            results.append(
                {
                    "linkId": answer[0],
                    "answer": [{"valueCoding": {"code": answer[1].code, "system": answer[1].system}}],
                }
            )
    if string_answers:
        for answer in string_answers:
            results.append({"linkId": answer[0], "answer": [{"valueString": answer[1]}]})
    if date_answers:
        for answer in date_answers:
            results.append({"linkId": answer[0], "answer": [{"valueDate": "%s" % answer[1].isoformat()}]})
    if uri_answers:
        for answer in uri_answers:
            results.append({"linkId": answer[0], "answer": [{"valueUri": answer[1]}]})

    response_json = {
        "resourceType": "QuestionnaireResponse",
        "status": "completed",
        "subject": {"reference": "Patient/{}".format(participant_id)},
        "questionnaire": {"reference": "Questionnaire/{}".format(questionnaire_id)},
        "group": {"question": results},
    }
    if language is not None:
        response_json.update(
            {
                "extension": [
                    {
                        "url": "http://hl7.org/fhir/StructureDefinition/iso21090-ST-language",
                        "valueCode": "{}".format(language),
                    }
                ]
            }
        )
    if authored is not None:
        response_json.update({"authored": authored.isoformat()})
    return response_json
