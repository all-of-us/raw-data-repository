"""Tests for extraction."""

import concepts
import datetime
import extraction
import os
import json
import unittest

from questionnaire import QuestionnaireExtractor
from questionnaire_response import QuestionnaireResponseExtractor

from test.unit_test.unit_test_util import NdbTestBase

RACE_LINKID = 'race'
ETHNICITY_LINKID = 'ethnicity'
STATE_OF_RESIDENCE_LINKID = 'state_of_residence'

class ExtractionTest(NdbTestBase):
  def setUp(self):
    super(ExtractionTest, self).setUp()
    self.longMessage = True

  def test_questionnaire_extract(self):
    questionnaire = json.loads(open(_data_path('questionnaire_example.json')).read())
    extractor = QuestionnaireExtractor(questionnaire)
    self.assertEquals([RACE_LINKID],
                      extractor.extract_link_id_for_concept(concepts.RACE))
    self.assertEquals([ETHNICITY_LINKID],
                      extractor.extract_link_id_for_concept(concepts.ETHNICITY))
    self.assertEquals([STATE_OF_RESIDENCE_LINKID],
                      extractor.extract_link_id_for_concept(concepts.STATE_OF_RESIDENCE))

  def test_questionnaire_response_extract(self):
    template = open(_data_path('questionnaire_response_example.json')).read()
    response = _fill_response(
        template, 'Q1234', 'P1',
        concepts.WHITE,
        concepts.NON_HISPANIC,
        concepts.STATES_BY_ABBREV['TX'])

    extractor = QuestionnaireResponseExtractor(json.loads(response))
    self.assertEquals('Q1234', extractor.extract_questionnaire_id())
    self.assertEquals('white', extractor.extract_answer(RACE_LINKID, concepts.RACE))
    self.assertEquals('TX', extractor.extract_answer(STATE_OF_RESIDENCE_LINKID,
                                                     concepts.STATE_OF_RESIDENCE))

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
