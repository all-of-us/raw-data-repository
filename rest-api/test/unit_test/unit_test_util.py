import datetime
import httplib
import json
import mock
import os
import unittest

from google.appengine.ext import ndb
from google.appengine.ext import testbed

import api_util
import config
import executors
import main
import questionnaire_response


class TestBase(unittest.TestCase):
  """Base class for unit tests."""

  def setUp(self):
    # Allow printing the full diff report on errors.
    self.maxDiff = None


class TestbedTestBase(TestBase):
  """Base class for unit tests that need the testbed."""

  def setUp(self):
    super(TestbedTestBase, self).setUp()
    self.testbed = testbed.Testbed()
    self.testbed.activate()
    self.testbed.init_taskqueue_stub()

  def tearDown(self):
    self.testbed.deactivate()
    super(TestbedTestBase, self).tearDown()


class NdbTestBase(TestbedTestBase):
  """Base class for unit tests that need the NDB testbed."""

  def setUp(self):
    super(NdbTestBase, self).setUp()
    self.testbed.init_datastore_v3_stub()
    self.testbed.init_memcache_stub()
    ndb.get_context().clear_cache()


class FlaskTestBase(NdbTestBase):
  """Provide a local flask server to exercise handlers and storage."""
  _AUTH_USER = 'authorized@gservices.act'
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
    config.override_setting(config.USER_INFO, self._CONFIG_USER_INFO)

    self._patchers = []
    mock_oauth = mock.patch('api_util.get_oauth_id')
    mock_oauth.start().return_value = self._AUTH_USER
    self._patchers.append(mock_oauth)

  def post_json(self, local_path, post_data, expected_status=httplib.OK):
    """Makes a JSON API call against the test client and returns its response data.

    Args:
      local_path: The API endpoint's URL (excluding main.PREFIX).
      post_data: Parsed JSON payload for the request.
      expected_status: What HTTP status to assert, if not 200 (OK).
    """
    response = self._app.post(
        main.PREFIX + local_path,
        data=json.dumps(post_data),
        content_type='application/json')
    self.assertEquals(response.status_code, expected_status, response.data)
    return json.loads(response.data)

  def tearDown(self):
    super(FlaskTestBase, self).tearDown()
    config.remove_override(config.USER_INFO)
    for patcher in self._patchers:
      patcher.stop()


def to_dict_strip_last_modified(obj):
  assert obj.last_modified, 'Missing last_modified: {}'.format(obj)
  json = obj.to_dict()
  del json['last_modified']
  if json.get('signUpTime'):
    del json['signUpTime']
  return json


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
