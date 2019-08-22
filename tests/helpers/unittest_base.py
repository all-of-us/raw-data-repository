import copy
import json
import logging
import os
import faker
import unittest
import http.client

from rdr_service import config
from tests.test_data import data_path
from rdr_service.code_constants import PPI_SYSTEM
from rdr_service.concepts import Concept
from rdr_service.dao import questionnaire_dao, questionnaire_response_dao
from rdr_service.dao.participant_dao import ParticipantDao
from rdr_service import main
from rdr_service.model.participant import Participant, ParticipantHistory
from rdr_service.model.participant_summary import ParticipantSummary
from rdr_service.participant_enums import (
    EnrollmentStatus,
    SuspensionStatus,
    UNSET_HPO_ID,
    WithdrawalStatus,
)
from tests.helpers.mysql_helper import reset_mysql_instance


class QuestionnaireTestMixin:

    @staticmethod
    def questionnaire_response_url(participant_id):
        return "Participant/%s/QuestionnaireResponse" % participant_id

    def create_questionnaire(self, filename):
        with open(data_path(filename)) as f:
            questionnaire = json.load(f)
            response = self.send_post("Questionnaire", questionnaire)
            return response["id"]

    @staticmethod
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


class BaseTestCase(unittest.TestCase, QuestionnaireTestMixin):
    """ Base class for unit tests."""

    def __init__(self, *args, **kwargs):
        super(BaseTestCase, self).__init__(*args, **kwargs)
        # Set this so the database factory knows to use the unittest connection string from the config.
        os.environ["UNITTEST_FLAG"] = "True"
        self.fake = faker.Faker()
        logging.getLogger().setLevel(logging.CRITICAL)

    def setUp(self, with_data=True, with_consent_codes=False) -> None:
        super(BaseTestCase, self).setUp()
        self.setup_config()
        self.app = main.app.test_client()

        reset_mysql_instance(with_data, with_consent_codes)

        # Allow printing the full diff report on errors.
        self.maxDiff = None
        # Always add codes if missing when handling questionnaire responses.
        questionnaire_dao._add_codes_if_missing = lambda: True
        questionnaire_response_dao._add_codes_if_missing = lambda email: True
        self._consent_questionnaire_id = None

    @staticmethod
    def setup_config():
        if os.environ.get('UNITTEST_CONFIG_FLAG'):
            return
        data = read_dev_config(os.path.join(os.path.dirname(__file__), "../../rdr_service/config/base_config.json"),
                               os.path.join(os.path.dirname(__file__), "../../rdr_service/config/config_dev.json"))
        test_configs_dir = os.path.join(os.path.dirname(__file__), "../.test_configs")
        os.environ['RDR_CONFIG_ROOT'] = test_configs_dir
        config.store_current_config(data)
        os.environ['UNITTEST_CONFIG_FLAG'] = 'True'

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
        qr = self.make_questionnaire_response_json(
            participant_id, questionnaire_id, code_answers=code_answers, date_answers=date_answers
        )
        self.send_post("Participant/%s/QuestionnaireResponse" % participant_id, qr)

    def submit_consent_questionnaire_response(self, participant_id, questionnaire_id, ehr_consent_answer):
        code_answers = [("ehrConsent", Concept(PPI_SYSTEM, ehr_consent_answer))]
        qr = self.make_questionnaire_response_json(participant_id, questionnaire_id, code_answers=code_answers)
        self.send_post("Participant/%s/QuestionnaireResponse" % participant_id, qr)

    def participant_summary(self, participant):
        summary = ParticipantDao.create_summary_for_participant(participant)
        summary.firstName = self.fake.first_name()
        summary.lastName = self.fake.last_name()
        summary.email = self.fake.email()
        return summary

    def create_participant(self):
        response = self.send_post("Participant", {})
        return response["participantId"]

    def send_post(self, *args, **kwargs):
        return self.send_request("POST", *args, **kwargs)

    def send_put(self, *args, **kwargs):
        return self.send_request("PUT", *args, **kwargs)

    def send_patch(self, *args, **kwargs):
        return self.send_request("PATCH", *args, **kwargs)

    def send_get(self, *args, **kwargs):
        return self.send_request("GET", *args, **kwargs)

    def send_request(self, method, local_path, request_data=None, query_string=None,
        expected_status=http.client.OK,
        headers=None,
        expected_response_headers=None,
    ):
        """Makes a JSON API call against the test client and returns its response data.

    Args:
      method: HTTP method, as a string.
      local_path: The API endpoint's URL (excluding main.PREFIX).
      request_data: Parsed JSON payload for the request.
      expected_status: What HTTP status to assert, if not 200 (OK).
    """
        response = self.app.open(
            main.PREFIX + local_path,
            method=method,
            data=json.dumps(request_data) if request_data is not None else None,
            query_string=query_string,
            content_type="application/json",
            headers=headers,
        )
        self.assertEqual(response.status_code, expected_status, response.data)
        if expected_response_headers:
            self.assertTrue(
                set(expected_response_headers.items()).issubset(set(response.headers.items())),
                "Expected response headers: %s; actual: %s" % (expected_response_headers, response.headers),
            )
        if expected_status == http.client.OK:
            return json.loads(response.data)
        if expected_status == http.client.CREATED:
            return response
        return None


    def send_consent(self, participant_id, email=None, language=None, code_values=None, authored=None):
        if not self._consent_questionnaire_id:
            self._consent_questionnaire_id = self.create_questionnaire("study_consent.json")
        self.first_name = self.fake.first_name()
        self.last_name = self.fake.last_name()
        if not email:
            self.email = self.fake.email()
            email = self.email
        self.streetAddress = "1234 Main Street"
        self.streetAddress2 = "APT C"
        qr_json = self.make_questionnaire_response_json(
            participant_id,
            self._consent_questionnaire_id,
            string_answers=[
                ("firstName", self.first_name),
                ("lastName", self.last_name),
                ("email", email),
                ("streetAddress", self.streetAddress),
                ("streetAddress2", self.streetAddress2),
            ],
            language=language,
            code_answers=code_values,
            authored=authored,
        )
        self.send_post(self.questionnaire_response_url(participant_id), qr_json)

    def assertJsonResponseMatches(self, obj_a, obj_b):
        self.assertMultiLineEqual(self._clean_and_format_response_json(obj_a), self._clean_and_format_response_json(obj_b))

    @staticmethod
    def pretty(obj):
        return json.dumps(obj, sort_keys=True, indent=4, separators=(",", ": "))

    def _clean_and_format_response_json(self, input_obj):
        obj = self.sort_lists(copy.deepcopy(input_obj))
        for ephemeral_key in ("meta", "lastModified"):
            if ephemeral_key in obj:
                del obj[ephemeral_key]
        s = self.pretty(obj)
        # TODO(DA-226) Make sure times are not skewed on round trip to CloudSQL. For now, strip tzinfo.
        s = s.replace("+00:00", "")
        s = s.replace('Z",', '",')
        return s

    @staticmethod
    def sort_lists(obj):
        for key, val in obj.items():
            if isinstance(val, list):
                obj[key] = sorted(val)
        return obj

    @staticmethod
    def get_restore_or_cancel_info(reason=None, author=None, site=None, status=None):
        """get a patch request to cancel or restore a PM order,
      if called with no params it defaults to a cancel order."""
        if reason is None:
            reason = "a mistake was made."
        if author is None:
            author = "mike@pmi-ops.org"
        if site is None:
            site = "hpo-site-monroeville"
        if status is None:
            status = "cancelled"
            info = "cancelledInfo"
        elif status == "restored":
            info = "restoredInfo"

        return {
            "reason": reason,
            info: {
                "author": {"system": "https://www.pmi-ops.org/healthpro-username", "value": author},
                "site": {"system": "https://www.pmi-ops.org/site-id", "value": site},
            },
            "status": status,
        }


def read_dev_config(*files):
    data = {}
    for filename in files:
        with open(filename) as file:
            data.update(json.load(file))
    return data

