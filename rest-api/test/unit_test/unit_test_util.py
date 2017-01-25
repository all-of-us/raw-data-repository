"""Utils for unit tests."""

import os
import unittest
import questionnaire_response

from google.appengine.ext import ndb
from google.appengine.ext import testbed


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

def to_dict_strip_last_modified(obj):
  assert obj.last_modified, 'Missing last_modified: {}'.format(obj)
  json = obj.to_dict()
  del json['last_modified']
  return json

def make_questionnaire_response(participant_id, questionnaire_id, answers):
    results = []
    for answer in answers:
      results.append({ "linkId": answer[0],
                       "answer": [
                          { "valueCoding": {
                            "code": answer[1].code,
                            "system": answer[1].system
                          }
                        }]
                    })
    return questionnaire_response.DAO.from_json({"resourceType": "QuestionnaireResponse",
            "status": "completed",
            "subject": { "reference": "Patient/{}".format(participant_id) },
            "questionnaire": { "reference": "Questionnaire/{}".format(questionnaire_id) },
            "group": {
              "question": results
            }
            }, participant_id, questionnaire_response.DAO.allocate_id())

def _data_path(filename):
    return os.path.join(os.path.dirname(__file__), '..', 'test-data', filename)
