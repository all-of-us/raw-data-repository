"""Tests for extraction."""

import datetime
import os
import json
import unittest

import extraction

from questionnaire import QuestionnaireExtractor
from questionnaire_response import QuestionnaireResponseExtractor



from google.appengine.api import memcache
from google.appengine.ext import ndb
from google.appengine.ext import testbed


RACE_LINKID = 'race'
ETHNICITY_LINKID = 'ethnicity'
STATE_OF_RESIDENCE_LINKID = 'state_of_residence'

class ExtractionTest(unittest.TestCase):
  def setUp(self):
    self.maxDiff = None
    self.longMessage = True
    self.testbed = testbed.Testbed()
    self.testbed.activate()
    self.testbed.init_datastore_v3_stub()
    self.testbed.init_memcache_stub()
    ndb.get_context().clear_cache()

  def tearDown(self):
    pass

  def test_questionnaire_extract(self):
    questionnaire = json.loads(open(_data_path('questionnaire_example.json')).read())
    extractor = QuestionnaireExtractor(questionnaire)
    self.assertEquals([RACE_LINKID],
                      extractor.extract_link_id_for_concept(extraction.RACE_CONCEPT))
    self.assertEquals([ETHNICITY_LINKID],
                      extractor.extract_link_id_for_concept(extraction.ETHNICITY_CONCEPT))
    self.assertEquals([STATE_OF_RESIDENCE_LINKID],
                      extractor.extract_link_id_for_concept(extraction.STATE_OF_RESIDENCE_CONCEPT))

  def test_questionnaire_response_extract(self):
    template = open(_data_path('questionnaire_response_example.json')).read()
    response = _fill_response(
        template, 'Q1234', 'P1',
        extraction.Concept('http://hl7.org/fhir/v3/Race', '2106-3'),
        extraction.Concept('http://hl7.org/fhir/v3/Ethnicity', '2135-5'),
        extraction.Concept('http://terminology.pmi-ops.org/CodeSystem/us-state', 'TX'))

    extractor = QuestionnaireResponseExtractor(json.loads(response))
    self.assertEquals('Q1234', extractor.extract_questionnaire_id())
    self.assertEquals('white', extractor.extract_answer(RACE_LINKID, extraction.RACE_CONCEPT))
    self.assertEquals('TX', extractor.extract_answer(STATE_OF_RESIDENCE_LINKID,
                                                     extraction.STATE_OF_RESIDENCE_CONCEPT))

def _fill_response(template, q_id, p_id, race, ethnicity, state_of_residence):
  for k, v in {
      '$questionnaire_id': q_id,
      '$participant_id': p_id,
      '$race_code': race.code,
      '$race_system': race.system,
      '$race_display': 'Not Used',
      '$ethnicity_code': ethnicity.code,
      '$ethnicity_system': ethnicity.system,
      '$ethnicity_display': 'Not Used',
      '$state_of_residence_code': state_of_residence.code,
      '$state_of_residence_system': state_of_residence.system,
      '$state_of_residence_display': 'Not Used',
      '$authored': datetime.datetime.now().date().isoformat(),
  }.iteritems():
    template = template.replace(k, v)
  return template

def _data_path(filename):
  return os.path.join(os.path.dirname(__file__), '..', 'test-data', filename)


if __name__ == '__main__':
  unittest.main()
