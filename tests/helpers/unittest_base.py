import atexit
import collections
import contextlib
import copy
import csv
import http.client
import io
import json
import logging
import os
import random
import shutil
import string
import sys
import tempfile
import unittest
from datetime import datetime
from tempfile import mkdtemp

import faker

from rdr_service import api_util
from rdr_service import config
from rdr_service import main
from rdr_service.code_constants import PPI_SYSTEM
from rdr_service.concepts import Concept
from rdr_service.dao import database_factory
from rdr_service.dao.code_dao import CodeDao
from rdr_service.dao.participant_dao import ParticipantDao
from rdr_service.model.biobank_order import BiobankOrderIdentifier, BiobankOrder, BiobankOrderedSample
from rdr_service.model.biobank_stored_sample import BiobankStoredSample
from rdr_service.model.code import Code, CodeType
from rdr_service.model.participant import Participant
from rdr_service.offline import sql_exporter
from rdr_service.storage import LocalFilesystemStorageProvider
from tests.helpers.data_generator import DataGenerator
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

    def create_code_if_needed(self, code_value, code_type, system=PPI_SYSTEM):
        code_dao = CodeDao()
        code_dao.get_or_create_with_session(
            self.session,
            insert_if_created=True,
            value=code_value,
            system=system,
            codeType=code_type,
            mapped=True
        )
        self.session.commit()  # get_or_create adds to the session, but doesn't commit it

    def _save_codes(self, structure, code_type=CodeType.MODULE):
        """
        Convenience method for establishing questionnaire codes in the database before submitting the json payload.
        Finds any lists within a FHIR json structure that are named as concepts for a questionnaire or any fields
        with the name of 'code' (to catch any question or answer codes).
        """
        if isinstance(structure, dict):
            if 'code' in structure:  # 'code' found outside 'concept' list, likely an answer code on a question
                answer_system = structure.get('system', PPI_SYSTEM)  # Not all answers in test data give a system
                self.create_code_if_needed(structure['code'], CodeType.ANSWER, system=answer_system)
            else:
                for key, value in structure.items():
                    if key == 'concept':  # Could be questionnaire or question concepts
                        for code_json in value:
                            self.create_code_if_needed(code_json['code'], code_type, code_json['system'])
                    else:
                        self._save_codes(value, code_type)
        elif isinstance(structure, list):
            for sub_structure in structure:
                self._save_codes(sub_structure, CodeType.QUESTION)


    def create_questionnaire(self, filename):
        with open(data_path(filename)) as f:
            questionnaire = json.load(f)
            self._save_codes(questionnaire)
            response = self.send_post("Questionnaire", questionnaire)
            return response["id"]

    @staticmethod
    def make_code_answer(link_id, value):
        return (link_id, Concept(PPI_SYSTEM, value))

    def make_questionnaire_response_json(
        self,
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
            for link_id, code in code_answers:
                results.append(
                    {
                        "linkId": link_id,
                        "answer": [{"valueCoding": {"code": code.code, "system": code.system}}],
                    }
                )
                self.create_code_if_needed(code.code, CodeType.ANSWER, code.system)

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


class BiobankTestMixin:
    """ Base class for creating Biobank table records """

    # BiobankOrder object defaults.
    bbo_order_id = '1'
    bbo_created = datetime(2018, 9, 20, 10, 0, 0)
    bbo_finalized = datetime(2018, 9, 20, 10, 10, 0)

    # BiobankStoredSample object defaults.
    bos_test = '1ED04'

    # BiobankStoredSample object defaults.
    bss_confirmed = datetime(2018, 9, 22, 14, 0, 0)

    def _create_biobank_order(self, participant_id, **kwargs):
        """
        Makes a new BiobankOrder object (same values every time) with valid/complete defaults.
        Kwargs pass through to BiobankOrder constructor, overriding defaults.
        :param participant_id: participant id.
        :param kwargs: Dict of override values for BiobankOrder object.
        :return: BiobankOrder object
        """
        if not participant_id:
            raise ValueError('Argument "participant_id" not passed.')

        bbo_order_id = self.bbo_order_id
        bbo_created = self.bbo_created
        bss_biobank_order_identifier = f'MAYO-{bbo_order_id}'

        if 'biobankOrderId' in kwargs:
            bbo_order_id = kwargs.pop('biobankOrderId')
            bss_biobank_order_identifier = f'MAYO-{bbo_order_id}'
        if 'created' in kwargs:
            bbo_created = kwargs.pop('created')
        # allow overriding the mayo clinic biobank_order_identifier value.
        if 'biobankOrderIdentifier' in kwargs:
            bss_biobank_order_identifier = kwargs.pop('biobankOrderIdentifier')

        bbo_defaults = {
            'participantId': participant_id,
            'biobankOrderId': bbo_order_id,
            'created': bbo_created,
            'sourceSiteId': 1,
            'sourceUsername': 'fred@pmi-ops.org',
            'collectedSiteId': 1,
            'collectedUsername': 'joe@pmi-ops.org',
            'processedSiteId': 1,
            'processedUsername': 'sue@pmi-ops.org',
            'finalizedSiteId': 2,
            'finalizedUsername': 'bob@pmi-ops.org',
            'finalizedTime': self.bbo_finalized,
            'version': 1,
            'logPositionId': 1,
            'identifiers': [BiobankOrderIdentifier(
                system='https://www.pmi-ops.org', value=bss_biobank_order_identifier)]
        }
        # Override defaults if overrides provided
        if kwargs:
            bbo_defaults.update(kwargs)
        bbo = BiobankOrder(**bbo_defaults)

        with self.dao.session() as session:
            session.add(bbo)

        return bbo

    def _create_biobank_ordered_sample(self, biobank_order_id, **kwargs):
        """
        Makes a new BiobankStoredSample object (same values every time) with valid/complete defaults.
        Kwargs pass through to BiobankStoredSample constructor, overriding defaults.
        :param biobank_order_id: Biobank Order ID value.
        :param kwargs: Dict of override values for BiobankOrder object.
        :return: BiobankStoredSample object
        """
        if not biobank_order_id:
            raise ValueError('Argument "biobank_order_id" must be a valid biobank_order_id value.')

        bos_test = self.bos_test
        bos_finalized = self.bbo_finalized

        if kwargs:
            if 'test' in kwargs:
                bos_test = kwargs.pop('test')
            if 'finalized' in kwargs:
                bos_finalized = kwargs.pop('finalized')

        bos_defaults = {
            'biobankOrderId': biobank_order_id,
            'test': bos_test,
            'description': 'description',
            'finalized': bos_finalized,
            'processingRequired': True
        }
        if kwargs:
            bos_defaults.update(kwargs)
        bos = BiobankOrderedSample(**bos_defaults)

        with self.dao.session() as session:
            session.add(bos)

        return bos

    def _create_biobank_stored_sample(self, biobank_id, **kwargs):
        """
        Makes a new BiobankStoredSample object (same values every time) with valid/complete defaults.
        Kwargs pass through to BiobankStoredSample constructor, overriding defaults.
        :param biobank_id: participant's biobank_id.
        :param kwargs: Dict of override values for BiobankOrder object.
        :return: BiobankStoredSample object
        """
        if not biobank_id:
            raise ValueError('Argument "biobank_id" not passed.')

        bss_test = self.bos_test
        bss_confirmed = self.bss_confirmed
        bss_biobank_order_identifier = f'MAYO-{self.bbo_order_id}'

        if kwargs:
            if 'biobankOrderId' in kwargs:
                bbo_order_id = kwargs.pop('biobankOrderId')
                bss_biobank_order_identifier = f'MAYO-{bbo_order_id}'
            if 'biobankOrderIdentifier' in kwargs:
                bss_biobank_order_identifier = kwargs.pop('biobankOrderIdentifier')
            if 'test' in kwargs:
                bss_test = kwargs.pop('test')
            if 'confirmed' in kwargs:
                bss_confirmed = kwargs.pop('confirmed')

        if not bss_biobank_order_identifier:
            raise ValueError('biobank_order_identifier has not been set for stored sample insert.')

        bss_defaults = {
            'biobankId': biobank_id,
            'biobankOrderIdentifier': bss_biobank_order_identifier,
            'test': bss_test,
            'created': bss_confirmed,
            'confirmed': bss_confirmed,
            'biobankStoredSampleId': 'I'.join(random.choice(string.digits) for i in range(8)),
            'family_id': 'F11111111',
        }

        if kwargs:
            bss_defaults.update(kwargs)
        bss = BiobankStoredSample(**bss_defaults)

        with self.dao.session() as session:
            session.add(bss)

        return bss

    def _make_default_biobank_order(self, participant_id, biobank_order_id=None, stored_sample=True):
        """
        Create the basic '1ED04' DNA biobank order.
        :param participant_id: participant_id
        :param biobank_order_id: Override the default biobank_order_id string value.
        :param stored_sample: Create the BiobankStoredSample record.
        :return: tuple (BiobankOrder object, BiobankOrderedSample object, BiobankStoredSample object)
        """
        if not participant_id:
            raise ValueError('Participant ID argument not passed.')

        with self.dao.session() as session:
            rec = session.query(Participant.biobankId).filter(Participant.participantId == participant_id).first()
            if not rec or not rec.biobankId:
                raise ValueError('Failed to lookup participant biobank_id')
            biobank_id = rec.biobankId

        bbo_order_id = self.bbo_order_id
        if biobank_order_id:
            bbo_order_id = bbo_order_id

        bbo = self._create_biobank_order(participant_id, biobankOrderId=bbo_order_id)
        bos = self._create_biobank_ordered_sample(bbo.biobankOrderId)
        if stored_sample:
            bss = self._create_biobank_stored_sample(biobank_id=biobank_id)
        else:
            bss = None
        return bbo, bos, bss

    def _make_biobank_order_with_baseline_tests(self, participant_id, biobank_order_id=None, stored_sample=True):
        """
        Create a biobank order with all of the baseline tests
        :param participant_id: participant_id
        :param biobank_order_id: Override the default biobank_order_id string value.
        :param stored_sample: Create the BiobankStoredSample records.
        :return: bbo
        """
        if not participant_id:
            raise ValueError('Participant ID argument not passed.')

        with self.dao.session() as session:
            rec = session.query(Participant.biobankId).filter(Participant.participantId == participant_id).first()
            if not rec or not rec.biobankId:
                raise ValueError('Failed to lookup participant biobank_id')
            biobank_id = rec.biobankId

        tests = config.getSettingList('baseline_sample_test_codes')

        bbo_order_id = self.bbo_order_id
        if biobank_order_id:
            bbo_order_id = bbo_order_id

        bbo = self._create_biobank_order(participant_id, biobankOrderId=bbo_order_id)
        for test in tests:
            # pylint: disable=unused-variable
            bos = self._create_biobank_ordered_sample(bbo.biobankOrderId, test=test)
            if stored_sample:
                # pylint: disable=unused-variable
                bss = self._create_biobank_stored_sample(biobank_id=biobank_id, biobankOrderId=bbo_order_id, test=test)
        return bbo


class BaseTestCase(unittest.TestCase, QuestionnaireTestMixin, CodebookTestMixin):
    """ Base class for unit tests."""

    _configs_dir = os.path.join(tempfile.gettempdir(), 'configs')

    def __init__(self, *args, **kwargs):
        super(BaseTestCase, self).__init__(*args, **kwargs)
        self.fake = faker.Faker()

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
        self._consent_questionnaire_id = None

        self.session = database_factory.get_database().make_session()
        self.data_generator = DataGenerator(self.session, self.fake)

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
        self.assertEqual(expected_status, response.status_code, response.data)
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

    def send_consent(self, participant_id, email=None, language=None, code_values=None, string_answers=None,
                     extra_string_values=[], authored=None, expected_status=200):

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

        if not string_answers:
            string_answers = [
                ("firstName", self.first_name),
                ("lastName", self.last_name),
                ("email", email),
                ("streetAddress", self.streetAddress),
                ("streetAddress2", self.streetAddress2),
            ]

        qr_json = self.make_questionnaire_response_json(
            participant_id,
            self._consent_questionnaire_id,
            string_answers=string_answers + extra_string_values,
            language=language,
            code_answers=code_values,
            authored=authored,
        )

        return self.send_post(self.questionnaire_response_url(participant_id), qr_json, expected_status=expected_status)

    def assertJsonResponseMatches(self, obj_a, obj_b, strip_tz=True):
        self.assertMultiLineEqual(self._clean_and_format_response_json(obj_a, strip_tz=strip_tz),
                                  self._clean_and_format_response_json(obj_b, strip_tz=strip_tz))

    @staticmethod
    def pretty(obj):
        return json.dumps(obj, sort_keys=True, indent=4, separators=(",", ": "))

    def _clean_and_format_response_json(self, input_obj, strip_tz=True):
        obj = self.sort_lists(copy.deepcopy(input_obj))
        for ephemeral_key in ("meta", "lastModified", "origin"):
            if ephemeral_key in obj:
                del obj[ephemeral_key]
        s = self.pretty(obj)
        if strip_tz:
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

