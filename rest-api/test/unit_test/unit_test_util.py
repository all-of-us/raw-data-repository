import copy
import faker
import httplib
import json
import mock
import os
import unittest
import dao.database_factory

from google.appengine.api import app_identity
from google.appengine.ext import deferred
from google.appengine.ext import ndb
from google.appengine.ext import testbed
from testlib import testutil

import api_util
import config
import config_api
import executors
import main
import dao.base_dao
import singletons

from code_constants import PPI_SYSTEM
from contextlib import contextmanager
from dao.code_dao import CodeDao
from dao.hpo_dao import HPODao
from model.code import Code
from model.hpo import HPO
from model.participant import Participant, ParticipantHistory
from model.participant_summary import ParticipantSummary
from participant_enums import UNSET_HPO_ID, WithdrawalStatus, SuspensionStatus, EnrollmentStatus
from mock import patch
from test.test_data import data_path

PITT_HPO_ID = 2


class TestBase(unittest.TestCase):
  """Base class for unit tests."""
  def setUp(self):
    # Allow printing the full diff report on errors.
    self.maxDiff = None
    # Make a faker which produces unicode text available.
    self.fake = faker.Faker('ru_RU')
    self.fake.seed(1)

  @staticmethod
  def _participant_with_defaults(**kwargs):
    """Creates a new Participant model, filling in some default constructor args.

    This is intended especially for updates, where more fields are required than for inserts.
    """
    common_args = {
      'hpoId': UNSET_HPO_ID,
      'withdrawalStatus': WithdrawalStatus.NOT_WITHDRAWN,
      'suspensionStatus': SuspensionStatus.NOT_SUSPENDED,
    }
    common_args.update(kwargs)
    return Participant(**common_args)

  @staticmethod
  def _participant_summary_with_defaults(**kwargs):
    common_args = {
      'hpoId': UNSET_HPO_ID,
      'numCompletedBaselinePPIModules': 0,
      'numBaselineSamplesArrived': 0,
      'withdrawalStatus': WithdrawalStatus.NOT_WITHDRAWN,
      'suspensionStatus': SuspensionStatus.NOT_SUSPENDED,
      'enrollmentStatus': EnrollmentStatus.INTERESTED
    }
    common_args.update(kwargs)
    return ParticipantSummary(**common_args)

  @staticmethod
  def _participant_history_with_defaults(**kwargs):
    common_args = {
      'hpoId': UNSET_HPO_ID,
      'version': 1,
      'withdrawalStatus': WithdrawalStatus.NOT_WITHDRAWN,
      'suspensionStatus': SuspensionStatus.NOT_SUSPENDED,
    }
    common_args.update(kwargs)
    return ParticipantHistory(**common_args)


class TestbedTestBase(TestBase):
  """Base class for unit tests that need the testbed."""
  def setUp(self):
    super(TestbedTestBase, self).setUp()
    # Reset singletons, including the database, between tests.
    singletons.reset_for_tests()
    self.testbed = testbed.Testbed()
    self.testbed.activate()
    self.testbed.init_taskqueue_stub()

  def tearDown(self):
    self.testbed.deactivate()
    super(TestbedTestBase, self).tearDown()

class SqlTestBase(TestbedTestBase):
  """Base class for unit tests that use the SQL database."""
  def setUp(self, with_data=True):
    super(SqlTestBase, self).setUp()
    SqlTestBase.setup_database()
    self.database = dao.database_factory.get_database()
    if with_data:
      self.setup_data()

  def tearDown(self):
    SqlTestBase.teardown_database()
    super(SqlTestBase, self).tearDown()

  @staticmethod
  def setup_database():
    dao.database_factory.DB_CONNECTION_STRING = 'sqlite:///:memory:'
    dao.database_factory.get_database().create_schema()

  @staticmethod
  def teardown_database():
    dao.database_factory.get_database().get_engine().dispose()

  def get_database(self):
    return self.database

  def setup_data(self):
    """Creates default data necessary for basic testing."""
    SqlTestBase.setup_hpos()

  @staticmethod
  def setup_hpos():
    hpo_dao = HPODao()
    hpo_dao.insert(HPO(hpoId=UNSET_HPO_ID, name='UNSET'))
    hpo_dao.insert(HPO(hpoId=PITT_HPO_ID, name='PITT'))

  @staticmethod
  def setup_codes(values, code_type):
    code_dao = CodeDao()
    for value in values:
      code_dao.insert(Code(system=PPI_SYSTEM, value=value, codeType=code_type, mapped=True))

  def assertObjEqualsExceptLastModified(self, obj1, obj2):
    dict1 = obj1.asdict()
    dict2 = obj2.asdict()
    del dict1['lastModified']
    del dict2['lastModified']
    self.assertEquals(dict1, dict2)

  def assertListAsDictEquals(self, list_a, list_b):
    if len(list_a) != len(list_b):
      self.fail("List lengths don't match: %d != %d; %s, %s" % (len(list_a), len(list_b),
                                                                list_as_dict(list_a),
                                                                list_as_dict(list_b)))
    for i in range(0, len(list_a)):
      self.assertEquals(list_a[i].asdict(), list_b[i].asdict())

class NdbTestBase(SqlTestBase):
  """Base class for unit tests that need the NDB testbed."""
  _AUTH_USER = 'authorized@gservices.act'  # authorized for typical API usage roles
  _CONFIG_USER_INFO = {
    _AUTH_USER: {
      'roles': api_util.ALL_ROLES,
    },
  }

  def setUp(self):
    super(NdbTestBase, self).setUp()
    self.testbed.init_datastore_v3_stub()
    self.testbed.init_memcache_stub()
    ndb.get_context().clear_cache()
    self.doSetUp()

  def doSetUp(self):
    dev_config = read_dev_config()
    dev_config[config.USER_INFO] = self._CONFIG_USER_INFO
    config.store_current_config(dev_config)
    config.CONFIG_OVERRIDES = {}

class CloudStorageSqlTestBase(testutil.CloudStorageTestBase):
  """Merge AppEngine's provided CloudStorageTestBase and our SqlTestBase.

  Both try to set up a testbed (which stubs out various AppEngine APIs, including cloudstorage_api).
  """
  def setUp(self):
    super(CloudStorageSqlTestBase, self).setUp()
    SqlTestBase.setup_database()
    SqlTestBase.setup_hpos()

  def tearDown(self):
    super(CloudStorageSqlTestBase, self).tearDown()
    SqlTestBase.teardown_database()


def read_dev_config():
  with open(os.path.join(os.path.dirname(__file__), '../../config/config_dev.json')) as config_file:
    with open(os.path.join(os.path.dirname(__file__), '../../config/base_config.json')) as b_cfg:
      config_json = json.load(b_cfg)
      config_json.update(json.load(config_file))
      return config_json


class FlaskTestBase(NdbTestBase):
  """Provide a local flask server to exercise handlers and storage."""
  _ADMIN_USER = 'config_admin@fake.google.com'  # allowed to update config

  def doSetUp(self):
    super(FlaskTestBase, self).doSetUp()
    # http://flask.pocoo.org/docs/0.12/testing/
    main.app.config['TESTING'] = True
    self._app = main.app.test_client()

    self._patchers = []
    mock_oauth = mock.patch('api_util.get_oauth_id')
    self._mock_get_oauth_id = mock_oauth.start()
    self._patchers.append(mock_oauth)

    config_api.CONFIG_ADMIN_MAP[app_identity.get_application_id()] = self._ADMIN_USER

    self.set_auth_user(self._ADMIN_USER)
    self.set_auth_user(self._AUTH_USER)
    self._consent_questionnaire_id = None

  def tearDown(self):
    super(FlaskTestBase, self).tearDown()
    self.doTearDown()

  def doTearDown(self):
    for patcher in self._patchers:
      patcher.stop()

  def set_auth_user(self, auth_user):
    self._mock_get_oauth_id.return_value = auth_user

  def send_post(self, *args, **kwargs):
    return self.send_request('POST', *args, **kwargs)

  def send_put(self, *args, **kwargs):
    return self.send_request('PUT', *args, **kwargs)

  def send_get(self, *args, **kwargs):
    return self.send_request('GET', *args, **kwargs)

  def send_request(self, method, local_path, request_data=None, expected_status=httplib.OK,
                   headers=None, expected_response_headers=None):
    """Makes a JSON API call against the test client and returns its response data.

    Args:
      method: HTTP method, as a string.
      local_path: The API endpoint's URL (excluding main.PREFIX).
      request_data: Parsed JSON payload for the request.
      expected_status: What HTTP status to assert, if not 200 (OK).
    """
    response = self._app.open(
        main.PREFIX + local_path,
        method=method,
        data=json.dumps(request_data) if request_data is not None else None,
        content_type='application/json',
        headers=headers)
    self.assertEquals(response.status_code, expected_status, response.data)
    if expected_response_headers:
      self.assertTrue(set(expected_response_headers.items())
                          .issubset(set(response.headers.items())),
                      "Expected response headers: %s; actual: %s" %
                      (expected_response_headers, response.headers))
    if expected_status == httplib.OK:
      return json.loads(response.data)
    return None

  def create_participant(self):
    response = self.send_post('Participant', {})
    return response['participantId']

  def send_consent(self, participant_id):
    if not self._consent_questionnaire_id:
      self._consent_questionnaire_id = self.create_questionnaire('study_consent.json')
    qr_json = make_questionnaire_response_json(participant_id, self._consent_questionnaire_id,
                                               string_answers=[("firstName", "Bob"),
                                                               ("lastName", "Jones")])
    self.send_post(questionnaire_response_url(participant_id), qr_json)

  def create_questionnaire(self, filename):
    with open(data_path(filename)) as f:
      questionnaire = json.load(f)
      response = self.send_post('Questionnaire', questionnaire)
      return response['id']

  def create_and_verify_created_obj(self, path, resource):
    response = self.send_post(path, resource)
    resource_id = response['id']
    del response['id']
    self.assertJsonResponseMatches(resource, response)

    response = self.send_get('{}/{}'.format(path, resource_id))
    del response['id']
    self.assertJsonResponseMatches(resource, response)

  def assertJsonResponseMatches(self, obj_a, obj_b):
    self.assertMultiLineEqual(
        _clean_and_format_response_json(obj_a), _clean_and_format_response_json(obj_b))

  def assertBundle(self, expected_entries, response, has_next=False):
    self.assertEquals('Bundle', response['resourceType'])
    self.assertEquals('searchset', response['type'])
    self.assertEquals(len(expected_entries), len(response['entry']))
    for i in range(0, len(expected_entries)):
      self.assertJsonResponseMatches(expected_entries[i], response['entry'][i])
    if has_next:
      self.assertEquals('next', response['link'][0]['relation'])
      return response['link'][0]['url']
    else:
      self.assertIsNone(response.get('link'))
      return None

def _clean_and_format_response_json(input_obj):
  obj = sort_lists(copy.deepcopy(input_obj))
  for ephemeral_key in ('meta', 'lastModified'):
    if ephemeral_key in obj:
      del obj[ephemeral_key]
  s = pretty(obj)
  # TODO(DA-226) Make sure times are not skewed on round trip to CloudSQL. For now, strip tzinfo.
  s = s.replace('+00:00', '')
  s = s.replace('Z",', '",')
  return s

def list_as_dict(items):
  return [item.asdict() for item in items]

def to_dict_strip_last_modified(obj):
  assert obj.last_modified, 'Missing last_modified: {}'.format(obj)
  obj_json = obj.to_dict()
  del obj_json['last_modified']
  if obj_json.get('signUpTime'):
    del obj_json['signUpTime']
  return obj_json


def sort_lists(obj):
  for key, val in obj.iteritems():
    if type(val) is list:
      obj[key] = sorted(val)
  return obj


def make_deferred_run():
  executors.defer = executors._do_defer

def make_deferred_not_run():
  executors.defer = (lambda fn, *args, **kwargs: None)


def make_questionnaire_response_json(participant_id, questionnaire_id, code_answers=None,
                                string_answers=None, date_answers=None):
  results = []
  if code_answers:
    for answer in code_answers:
      results.append({"linkId": answer[0],
                      "answer": [
                         { "valueCoding": {
                           "code": answer[1].code,
                           "system": answer[1].system
                         }
                       }]
                    })
  if string_answers:
    for answer in string_answers:
      results.append({"linkId": answer[0],
                      "answer": [
                         { "valueString": answer[1] }
                       ]
                    })
  if date_answers:
    for answer in date_answers:
      results.append({"linkId": answer[0],
                      "answer": [
                         { "valueDate": "%s" % answer[1].isoformat() }
                        ]
                    })
  return {"resourceType": "QuestionnaireResponse",
          "status": "completed",
          "subject": { "reference": "Patient/{}".format(participant_id) },
          "questionnaire": { "reference": "Questionnaire/{}".format(questionnaire_id) },
          "group": {
            "question": results
          }
      }

def questionnaire_response_url(participant_id):
    return 'Participant/%s/QuestionnaireResponse' % participant_id

def pretty(obj):
  return json.dumps(obj, sort_keys=True, indent=4, separators=(',', ': '))

@contextmanager
def random_ids(ids):
  with patch('random.randint', side_effect=ids):
    yield

def run_deferred_tasks(test):
  tasks = test.taskqueue.get_filtered_tasks()
  test.taskqueue.FlushQueue("default")
  while tasks:
    for task in tasks:
      if task.url == '/_ah/queue/deferred':
        deferred.run(task.payload)
    tasks = test.taskqueue.get_filtered_tasks()
    for task in tasks:
      # As soon as we hit a non-deferred task, bail out of here.
      if task.url != '/_ah/queue/deferred':
        return
    test.taskqueue.FlushQueue("default")

