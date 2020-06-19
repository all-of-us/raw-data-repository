import atexit
import collections
import contextlib
import copy
import csv
from datetime import datetime
import http.client
import io
import json
import logging
import os
import shutil
import sys
import tempfile
import unittest
from tempfile import mkdtemp

import faker

from rdr_service import api_util
from rdr_service import config
from rdr_service import main
from rdr_service.code_constants import PPI_SYSTEM
from rdr_service.concepts import Concept
from rdr_service.dao import database_factory, questionnaire_dao, questionnaire_response_dao
from rdr_service.dao.code_dao import CodeDao
from rdr_service.dao.participant_dao import ParticipantDao
from rdr_service.model.biobank_order import BiobankOrder, BiobankOrderedSample, BiobankOrderIdentifier
from rdr_service.model.biobank_stored_sample import BiobankStoredSample
from rdr_service.model.code import Code
from rdr_service.model.log_position import LogPosition
from rdr_service.model.participant import Participant, ParticipantHistory
from rdr_service.model.participant_summary import ParticipantSummary
from rdr_service.model.organization import Organization
from rdr_service.model.questionnaire import Questionnaire, QuestionnaireConcept, QuestionnaireHistory,\
    QuestionnaireQuestion
from rdr_service.model.questionnaire_response import QuestionnaireResponse, QuestionnaireResponseAnswer
from rdr_service.model.hpo import HPO
from rdr_service.model.site import Site
from rdr_service.offline import sql_exporter
from rdr_service.participant_enums import (
    EnrollmentStatus,
    SuspensionStatus,
    UNSET_HPO_ID,
    WithdrawalStatus,
)
from rdr_service.storage import LocalFilesystemStorageProvider
from tests.helpers.mysql_helper import reset_mysql_instance
from tests.test_data import data_path

QUESTIONNAIRE_NONE_ANSWER = 'no_answer_given'


class CodebookTestMixin:

    @staticmethod
    def setup_codes(values, code_type):
        code_dao = CodeDao()
        for value in values:
            code_dao.insert(Code(system=PPI_SYSTEM, value=value, codeType=code_type, mapped=True))


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
    def make_code_answer(link_id, value):
        return (link_id, Concept(PPI_SYSTEM, value))

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
        if isinstance(participant_id, int):
            participant_id = f'P{participant_id}'

        results = []
        if code_answers:
            for answer in code_answers:
                results.append(
                    {
                        "linkId": answer[0],
                        "answer": [{"valueCoding": {"code": answer[1].code, "system": answer[1].system}}],
                    }
                )

        def add_question_result(question_data, answer_value, answer_structure):
            result = {"linkId": question_data}
            if answer_value != QUESTIONNAIRE_NONE_ANSWER:
                result["answer"] = [answer_structure]
            results.append(result)

        if string_answers:
            for answer in string_answers:
                add_question_result(answer[0], answer[1], {"valueString": answer[1]})
        if date_answers:
            for answer in date_answers:
                add_question_result(answer[0], answer[1], {"valueDate": "%s" % answer[1].isoformat()})
        if uri_answers:
            for answer in uri_answers:
                results.append({"linkId": answer[0], "answer": []})
                add_question_result(answer[0], answer[1], {"valueUri": answer[1]})

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

    @staticmethod
    def _commit_to_database(session, model):
        session.add(model)
        session.commit()

    def create_database_questionnaire(self, **kwargs):
        questionnaire = self._questionnaire(**kwargs)
        self._commit_to_database(self.session, questionnaire)
        return questionnaire

    def _questionnaire(self, **kwargs):
        for field, default in [('version', 1),
                               ('created', datetime.now()),
                               ('lastModified', datetime.now()),
                               ('resource', 'test')]:
            if field not in kwargs:
                kwargs[field] = default

        return Questionnaire(**kwargs)

    def create_database_questionnaire_concept(self, **kwargs):
        questionnaire_concept = self._questionnaire_concept(**kwargs)
        self._commit_to_database(self.session, questionnaire_concept)
        return questionnaire_concept

    def _questionnaire_concept(self, **kwargs):
        return QuestionnaireConcept(**kwargs)

    def create_database_questionnaire_history(self, **kwargs):
        questionnaire_history = self._questionnaire_history(**kwargs)
        self._commit_to_database(self.session, questionnaire_history)
        return questionnaire_history

    def _questionnaire_history(self, **kwargs):
        for field, default in [('version', 1),
                               ('created', datetime.now()),
                               ('lastModified', datetime.now()),
                               ('resource', 'test')]:
            if field not in kwargs:
                kwargs[field] = default

        if 'questionnaireId' not in kwargs:
            questionnaire = self.create_database_questionnaire()
            kwargs['questionnaireId'] = questionnaire.questionnaireId

        return QuestionnaireHistory(**kwargs)

    def create_database_questionnaire_response_answer(self, **kwargs):
        questionnaire_response_answer = self._questionnaire_response_answer(**kwargs)
        self._commit_to_database(self.session, questionnaire_response_answer)
        return questionnaire_response_answer

    def _questionnaire_response_answer(self, **kwargs):
        return QuestionnaireResponseAnswer(**kwargs)

    def create_database_questionnaire_response(self, **kwargs):
        questionnaire_response = self._questionnaire_response(**kwargs)
        self._commit_to_database(self.session, questionnaire_response)
        return questionnaire_response

    def _questionnaire_response(self, **kwargs):
        for field, default in [('created', datetime.now()),
                               ('resource', 'test')]:
            if field not in kwargs:
                kwargs[field] = default

        if 'questionnaireResponseId' not in kwargs:
            kwargs['questionnaireResponseId'] = self.unique_questionnaire_response_id()

        return QuestionnaireResponse(**kwargs)

    def create_database_questionnaire_question(self, **kwargs):
        questionnaire_question = self._questionnaire_question(**kwargs)
        self._commit_to_database(self.session, questionnaire_question)
        return questionnaire_question

    def _questionnaire_question(self, **kwargs):
        if 'repeats' not in kwargs:
            kwargs['repeats'] = True

        return QuestionnaireQuestion(**kwargs)


class BaseTestCase(unittest.TestCase, QuestionnaireTestMixin, CodebookTestMixin):
    """ Base class for unit tests."""

    _configs_dir = os.path.join(tempfile.gettempdir(), 'configs')

    def __init__(self, *args, **kwargs):
        super(BaseTestCase, self).__init__(*args, **kwargs)
        self.fake = faker.Faker()
        self._next_unique_participant_id = 900000000
        self._next_unique_participant_biobank_id = 500000000
        self._next_unique_biobank_order_id = 100000000
        self._next_unique_biobank_stored_sample_id = 800000000
        self._next_unique_questionnaire_response_id = 500000000

    def setUp(self, with_data=True, with_consent_codes=False) -> None:
        super(BaseTestCase, self).setUp()

        logger = logging.getLogger()
        stream_handler = logging.StreamHandler(sys.stdout)
        logger.addHandler(stream_handler)

        # Change this to logging.ERROR when you want to see API server errors.
        logger.setLevel(logging.CRITICAL)

        self.setup_config()
        self.setup_storage()
        self.app = main.app.test_client()

        reset_mysql_instance(with_data, with_consent_codes)

        # Allow printing the full diff report on errors.
        self.maxDiff = None
        # Always add codes if missing when handling questionnaire responses.
        questionnaire_dao._add_codes_if_missing = lambda: True
        questionnaire_response_dao._add_codes_if_missing = lambda email: True
        self._consent_questionnaire_id = None

        self.session = database_factory.get_database().make_session()

    def tearDown(self):
        super(BaseTestCase, self).tearDown()
        self.session.close()

    def setup_storage(self):
        temp_folder_path = mkdtemp()
        self.addCleanup(shutil.rmtree, temp_folder_path)
        os.environ['RDR_STORAGE_ROOT'] = temp_folder_path

    def setup_config(self):
        os.environ['RDR_CONFIG_ROOT'] = self._configs_dir
        if not os.path.exists(self._configs_dir) or \
                not os.path.exists(os.path.join(self._configs_dir, 'current_config.json')) or \
                not os.path.exists(os.path.join(self._configs_dir, 'db_config.json')):
            os.mkdir(self._configs_dir)
            data = read_dev_config(os.path.join(os.path.dirname(__file__), "..", "..",
                                                "rdr_service", "config", "base_config.json"),
                                   os.path.join(os.path.dirname(__file__), "..", "..",
                                                "rdr_service", "config", "config_dev.json"))

            shutil.copy(os.path.join(os.path.dirname(__file__), "..", ".test_configs", "db_config.json"),
                        self._configs_dir)
            config.store_current_config(data)
            atexit.register(self.remove_config)

    def remove_config(self):
        if os.path.exists(self._configs_dir):
            shutil.rmtree(self._configs_dir)

    def load_test_storage_fixture(self, test_file_name, bucket_name):
        bucket_dir = os.path.join(os.environ.get("RDR_STORAGE_ROOT"), bucket_name)
        if not os.path.exists(bucket_dir):
            os.mkdir(bucket_dir)
        shutil.copy(os.path.join(os.path.dirname(__file__), "..", "test-data", test_file_name), bucket_dir)

    def unique_participant_id(self):
        next_participant_id = self._next_unique_participant_id
        self._next_unique_participant_id += 1
        return next_participant_id

    def unique_participant_biobank_id(self):
        next_biobank_id = self._next_unique_participant_biobank_id
        self._next_unique_participant_biobank_id += 1
        return next_biobank_id

    def unique_biobank_order_id(self):
        next_biobank_order_id = self._next_unique_biobank_order_id
        self._next_unique_biobank_order_id += 1
        return next_biobank_order_id

    def unique_biobank_stored_sample_id(self):
        next_biobank_stored_sameple_id = self._next_unique_biobank_stored_sample_id
        self._next_unique_biobank_stored_sample_id += 1
        return next_biobank_stored_sameple_id

    def unique_questionnaire_response_id(self):
        next_questionnaire_response_id = self._next_unique_questionnaire_response_id
        self._next_unique_questionnaire_response_id += 1
        return next_questionnaire_response_id

    def create_database_site(self, **kwargs):
        site = self._site_with_defaults(**kwargs)
        self._commit_to_database(self.session, site)
        return site

    def _site_with_defaults(self, **kwargs):
        defaults = {
            'siteName': 'example_site'
        }
        defaults.update(kwargs)
        return Site(**defaults)

    def create_database_organization(self, **kwargs):
        organization = self._organization_with_defaults(**kwargs)
        self._commit_to_database(self.session, organization)
        return organization

    def _organization_with_defaults(self, **kwargs):
        defaults = {
            'displayName': 'example_org_display'
        }
        defaults.update(kwargs)

        if 'hpoId' not in defaults:
            hpo = self.create_database_hpo()
            defaults['hpoId'] = hpo.hpoId

        return Organization(**defaults)

    def create_database_hpo(self, **kwargs):
        hpo = self._hpo_with_defaults(**kwargs)

        # hpoId is the primary key but is not automatically set when inserting
        if hpo.hpoId is None:
            hpo.hpoId = self.session.query(HPO).count() + 50  # There was code somewhere using lower numbers
        self._commit_to_database(self.session, hpo)

        return hpo

    def _hpo_with_defaults(self, **kwargs):
        return HPO(**kwargs)

    def create_database_participant(self, **kwargs):
        participant = self._participant_with_defaults(**kwargs)
        self._commit_to_database(self.session, participant)
        return participant

    def _participant_with_defaults(self, **kwargs):
        """Creates a new Participant model, filling in some default constructor args.

        This is intended especially for updates, where more fields are required than for inserts.
        """
        defaults = {
            'hpoId': UNSET_HPO_ID,
            'withdrawalStatus': WithdrawalStatus.NOT_WITHDRAWN,
            'suspensionStatus': SuspensionStatus.NOT_SUSPENDED,
            'participantOrigin': 'example',
            'version': 1,
            'lastModified': datetime.now(),
            'signUpTime': datetime.now()
        }
        defaults.update(kwargs)

        if 'biobankId' not in defaults:
            defaults['biobankId'] = self.unique_participant_biobank_id()
        if 'participantId' not in defaults:
            defaults['participantId'] = self.unique_participant_id()

        return Participant(**defaults)

    def create_database_participant_summary(self, **kwargs):
        participant_summary = self._participant_summary_with_defaults(**kwargs)
        self._commit_to_database(self.session, participant_summary)
        return participant_summary

    def _participant_summary_with_defaults(self, **kwargs):
        participant = kwargs.get('participant')
        if participant is None:
            participant = self.create_database_participant()

        defaults = {
            "participantId": participant.participantId,
            "biobankId": participant.biobankId,
            "hpoId": participant.hpoId,
            "firstName": self.fake.first_name(),
            "lastName": self.fake.last_name(),
            "numCompletedPPIModules": 0,
            "numCompletedBaselinePPIModules": 0,
            "numBaselineSamplesArrived": 0,
            "numberDistinctVisits": 0,
            "withdrawalStatus": WithdrawalStatus.NOT_WITHDRAWN,
            "suspensionStatus": SuspensionStatus.NOT_SUSPENDED,
            "enrollmentStatus": EnrollmentStatus.INTERESTED,
            "participantOrigin": participant.participantOrigin
        }

        defaults.update(kwargs)
        for questionnaire_field in ['consentForStudyEnrollment']:
            if questionnaire_field in defaults:
                if f'{questionnaire_field}Time' not in defaults:
                    defaults[f'{questionnaire_field}Time'] = datetime.now()
                if f'{questionnaire_field}Authored' not in defaults:
                    defaults[f'{questionnaire_field}Authored'] = datetime.now()

        return ParticipantSummary(**defaults)

    @staticmethod
    def _participant_history_with_defaults(**kwargs):
        common_args = {
            "hpoId": UNSET_HPO_ID,
            "version": 1,
            "withdrawalStatus": WithdrawalStatus.NOT_WITHDRAWN,
            "suspensionStatus": SuspensionStatus.NOT_SUSPENDED,
            "participantOrigin": "example"
        }
        common_args.update(kwargs)
        return ParticipantHistory(**common_args)

    def create_database_code(self, **kwargs):
        code = self._code(**kwargs)
        self._commit_to_database(self.session, code)
        return code

    def _code(self, **kwargs):
        for field, default in [('system', 'test'),
                               ('codeType', 1),
                               ('mapped', False),
                               ('created', datetime.now())]:
            if field not in kwargs:
                kwargs[field] = default

        return Code(**kwargs)

    def create_database_biobank_order(self, **kwargs):
        biobank_order = self._biobank_order(**kwargs)
        self._commit_to_database(self.session, biobank_order)
        return biobank_order

    def _biobank_order(self, log_position=None, **kwargs):
        for field, default in [('version', 1),
                               ('created', datetime.now())]:
            if field not in kwargs:
                kwargs[field] = default

        if 'logPositionId' not in kwargs:
            if log_position is None:
                log_position = self.create_database_log_position()
            kwargs['logPositionId'] = log_position.logPositionId
        if 'biobankOrderId' not in kwargs:
            kwargs['biobankOrderId'] = self.unique_biobank_order_id()

        return BiobankOrder(**kwargs)

    def create_database_biobank_order_identifier(self, **kwargs):
        biobank_order_identifier = self._biobank_order_identifier(**kwargs)
        self._commit_to_database(self.session, biobank_order_identifier)
        return biobank_order_identifier

    def _biobank_order_identifier(self, **kwargs):
        return BiobankOrderIdentifier(**kwargs)

    def create_database_biobank_ordered_sample(self, **kwargs):
        biobank_ordered_sample = self._biobank_ordered_sample(**kwargs)
        self._commit_to_database(self.session, biobank_ordered_sample)
        return biobank_ordered_sample

    def _biobank_ordered_sample(self, **kwargs):
        for field, default in [('description', 'test ordered sample'),
                               ('processingRequired', False),
                               ('test', 'C3PO')]:
            if field not in kwargs:
                kwargs[field] = default

        return BiobankOrderedSample(**kwargs)

    def create_database_biobank_stored_sample(self, **kwargs):
        biobank_stored_sample = self._biobank_stored_sample(**kwargs)
        self._commit_to_database(self.session, biobank_stored_sample)
        return biobank_stored_sample

    def _biobank_stored_sample(self, **kwargs):
        if 'biobankStoredSampleId' not in kwargs:
            kwargs['biobankStoredSampleId'] = self.unique_biobank_stored_sample_id()

        return BiobankStoredSample(**kwargs)

    def create_database_log_position(self, **kwargs):
        log_position = self._log_position(**kwargs)
        self._commit_to_database(self.session, log_position)
        return log_position

    def _log_position(self, **kwargs):
        return LogPosition(**kwargs)

    def submit_questionnaire_response(
        self, participant_id, questionnaire_id, race_code, gender_code, state, date_of_birth):
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

    def create_participant(self, provider_link=None):
        if provider_link:
            provider_link = {"providerLink": [provider_link]}
        else:
            provider_link = {}
        response = self.send_post("Participant", provider_link)
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
            main.API_PREFIX + local_path,
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

        if isinstance(participant_id, int):
            participant_id = f'P{participant_id}'

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
        self.assertMultiLineEqual(self._clean_and_format_response_json(obj_a),
                                  self._clean_and_format_response_json(obj_b))

    @staticmethod
    def pretty(obj):
        return json.dumps(obj, sort_keys=True, indent=4, separators=(",", ": "))

    def _clean_and_format_response_json(self, input_obj):
        obj = self.sort_lists(copy.deepcopy(input_obj))
        for ephemeral_key in ("meta", "lastModified", "origin"):
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
                try:
                    obj[key] = sorted(val)
                except TypeError:
                    if isinstance(val[0], dict):
                        obj[key] = sorted(val, key=lambda x: tuple(x.values()))
                    else:
                        obj[key] = val
        return obj

    def assertBundle(self, expected_entries, response, has_next=False):
        self.assertEqual("Bundle", response["resourceType"])
        self.assertEqual("searchset", response["type"])
        if len(expected_entries) != len(response["entry"]):
            self.fail(
                "Expected %d entries, got %d: %s" % (len(expected_entries), len(response["entry"]), response["entry"])
            )
        for i in range(0, len(expected_entries)):
            self.assertJsonResponseMatches(expected_entries[i], response["entry"][i])
        if has_next:
            self.assertEqual("next", response["link"][0]["relation"])
            return response["link"][0]["url"]
        else:
            self.assertIsNone(response.get("link"))
            return None

    def assertListAsDictEquals(self, list_a, list_b):

        def list_as_dict(items):
            return [item.asdict() for item in items]

        if len(list_a) != len(list_b):
            self.fail(
                "List lengths don't match: %d != %d; %s, %s"
                % (len(list_a), len(list_b), list_as_dict(list_a), list_as_dict(list_b))
            )
        for i in range(0, len(list_a)):
            self.assertEqual(list_a[i].asdict(), list_b[i].asdict())

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


    @staticmethod
    def cancel_biobank_order():
        return {
            "amendedReason": "messed up",
            "cancelledInfo": {
                "author": {"system": "https://www.pmi-ops.org/healthpro-username", "value": "mike@pmi-ops.org"},
                "site": {"system": "https://www.pmi-ops.org/site-id", "value": "hpo-site-monroeville"},
            },
            "status": "cancelled",
        }

    def create_and_verify_created_obj(self, path, resource):
        response = self.send_post(path, resource)
        resource_id = response["id"]
        del response["id"]
        self.assertJsonResponseMatches(resource, response)

        response = self.send_get("{}/{}".format(path, resource_id))
        del response["id"]
        self.assertJsonResponseMatches(resource, response)

    @staticmethod
    def clear_default_storage():
        local_storage_provider = LocalFilesystemStorageProvider()
        root_path = local_storage_provider.get_storage_root()
        for the_file in os.listdir(root_path):
            file_path = os.path.join(root_path, the_file)
            try:
                if os.path.isfile(file_path):
                    os.unlink(file_path)
                elif os.path.isdir(file_path):
                    shutil.rmtree(file_path)
            except Exception as e:  # pylint: disable=broad-except
                print(str(e))

    @staticmethod
    def create_mock_buckets(paths):
        local_storage_provider = LocalFilesystemStorageProvider()
        root_path = local_storage_provider.get_storage_root()
        try:
            for path in paths:
                os.mkdir(root_path + os.sep + path)
        except OSError:
            print("Creation mock buckets failed")

    @staticmethod
    def switch_auth_user(new_auth_user, client_id=None):
        config.LOCAL_AUTH_USER = new_auth_user
        if client_id:
            config_user_info = {
                new_auth_user: {
                    'roles': api_util.ALL_ROLES,
                    'clientId': client_id
                }
            }
        else:
            config_user_info = {
                new_auth_user: {
                    'roles': api_util.ALL_ROLES,
                }
            }
        config.override_setting("user_info", config_user_info)


class InMemorySqlExporter(sql_exporter.SqlExporter):
    """Store rows that would be written to GCS CSV in a StringIO instead.

  Provide some assertion helpers related to CSV contents.
  """

    def __init__(self, test):
        super(InMemorySqlExporter, self).__init__("inmemory")  # fake bucket name
        self._test = test
        self._path_to_buffer = collections.defaultdict(io.StringIO)

    @contextlib.contextmanager
    def open_cloud_writer(self, file_name, predicate=None):
        yield sql_exporter.SqlExportFileWriter(self._path_to_buffer[file_name], predicate)

    def assertFilesEqual(self, paths):
        self._test.assertCountEqual(paths, list(self._path_to_buffer.keys()))

    def _get_dict_reader(self, file_name):
        return csv.DictReader(
            io.StringIO(self._path_to_buffer[file_name].getvalue()), delimiter=sql_exporter.DELIMITER
        )

    def assertColumnNamesEqual(self, file_name, col_names):
        self._test.assertCountEqual(col_names, self._get_dict_reader(file_name).fieldnames)

    def assertRowCount(self, file_name, n):
        rows = list(self._get_dict_reader(file_name))
        self._test.assertEqual(
            n, len(rows), "Expected %d rows in %r but found %d: %s." % (n, file_name, len(rows), rows)
        )

    def assertHasRow(self, file_name, expected_row):
        """Asserts that the writer got a row that has all the values specified in the given row.

    Args:
      file_name: The bucket-relative path of the file that should have the row.
      expected_row: A dict like {'biobank_id': 557741928, sent_test: None} specifying a subset of
          the fields in a row that should have been written.
    Returns:
      The matched row.
    """
        rows = list(self._get_dict_reader(file_name))
        for row in rows:
            found_all = True
            for required_k, required_v in expected_row.items():
                if required_k not in row or row[required_k] != required_v:
                    found_all = False
                    break
            if found_all:
                return row
        self._test.fail("No match found for expected row %s among %d rows: %s" % (expected_row, len(rows), rows))


def read_dev_config(*files):
    data = {}
    for filename in files:
        with open(filename) as file:
            data.update(json.load(file))
    return data

