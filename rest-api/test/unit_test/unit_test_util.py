import copy
import httplib
import json
import mock
import os
import unittest
import dao.database_factory

from google.appengine.api import app_identity
from google.appengine.ext import ndb
from google.appengine.ext import testbed

import api_util
import config
import config_api
import executors
import main
import questionnaire_response
import dao.base_dao
import singletons

from contextlib import contextmanager
from dao.hpo_dao import HPODao
from model.hpo import HPO
from participant_enums import UNSET_HPO_ID
from mock import patch

PITT_HPO_ID = 2

class TestBase(unittest.TestCase):
  """Base class for unit tests."""
  def setUp(self):
    # Allow printing the full diff report on errors.
    self.maxDiff = None


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
    hpo_dao = HPODao()
    hpo_dao.insert(HPO(hpoId=UNSET_HPO_ID, name='UNSET'))
    hpo_dao.insert(HPO(hpoId=PITT_HPO_ID, name='PITT'))

  def assertObjEqualsExceptLastModified(self, obj1, obj2):
    dict1 = obj1.asdict()
    dict2 = obj2.asdict()
    del dict1['lastModified']
    del dict2['lastModified']
    self.assertEquals(dict1, dict2)
    

class NdbTestBase(SqlTestBase):
  """Base class for unit tests that need the NDB testbed."""
  def setUp(self):
    super(NdbTestBase, self).setUp()
    self.testbed.init_datastore_v3_stub()
    self.testbed.init_memcache_stub()
    ndb.get_context().clear_cache()


def read_dev_config():
  with open(os.path.join(os.path.dirname(__file__), '../../config/config_dev.json')) as config_file: 
    return json.load(config_file)


class FlaskTestBase(NdbTestBase):
  """Provide a local flask server to exercise handlers and storage."""
  _ADMIN_USER = 'config_admin@fake.google.com'  # allowed to update config
  _AUTH_USER = 'authorized@gservices.act'  # authorized for typical API usage roles
  _CONFIG_USER_INFO = {
    _AUTH_USER: {
      'roles': api_util.ALL_ROLES,
    },
  }

  def setUp(self):
    super(FlaskTestBase, self).setUp()
    # http://flask.pocoo.org/docs/0.12/testing/
    main.app.config['TESTING'] = True
    self._app = main.app.test_client()

    self._patchers = []
    mock_oauth = mock.patch('api_util.get_oauth_id')
    self._mock_get_oauth_id = mock_oauth.start()
    self._patchers.append(mock_oauth)

    config_api.CONFIG_ADMIN_MAP[app_identity.get_application_id()] = self._ADMIN_USER

    config.initialize_config()
    dev_config = read_dev_config()
    dev_config[config.USER_INFO] = self._CONFIG_USER_INFO
    self.set_auth_user(self._ADMIN_USER)
    self.send_request('PUT', 'Config', request_data=dev_config)
    self.set_auth_user(self._AUTH_USER)

  def tearDown(self):
    super(FlaskTestBase, self).tearDown()
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
    return json.loads(response.data)

  def create_and_verify_created_obj(self, path, resource):
    response = self.send_post(path, resource)  
    q_id = response['id']  
    del response['id']
    self.assertJsonResponseMatches(resource, response)

    response = self.send_get('{}/{}'.format(path, q_id))
    del response['id']
    self.assertJsonResponseMatches(resource, response)
    
  def assertJsonResponseMatches(self, obj_a, obj_b):
    obj_b = copy.deepcopy(obj_b)
    if 'meta' in obj_b and not 'meta' in obj_a:
      del obj_b['meta']
    self.assertMultiLineEqual(pretty(obj_a), pretty(obj_b))


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


def make_deferred_not_run():
  executors.defer = (lambda fn, *args, **kwargs: None)


def make_questionnaire_response(participant_id, questionnaire_id, answers):
  results = []
  for answer in answers:
    results.append({"linkId": answer[0],
                    "answer": [
                       { "valueCoding": {
                         "code": answer[1].code,
                         "system": answer[1].system
                       }
                     }]
                  })
  return questionnaire_response.DAO().from_json({
        "resourceType": "QuestionnaireResponse",
        "status": "completed",
        "subject": { "reference": "Patient/{}".format(participant_id) },
        "questionnaire": { "reference": "Questionnaire/{}".format(questionnaire_id) },
        "group": {
          "question": results
        }
      }, participant_id, questionnaire_response.DAO().allocate_id())

def pretty(obj):
  return json.dumps(obj, sort_keys=True, indent=4, separators=(',', ': '))
  
@contextmanager
def random_ids(ids):
  with patch('random.randint', side_effect=ids):
    yield