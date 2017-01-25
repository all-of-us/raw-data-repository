"""Tests for age range pipeline."""

import concepts
import datetime
import dateutil
import json
import participant
import participant_summary
import questionnaire
import questionnaire_response
import unittest

from clock import FakeClock
from offline import age_range_pipeline
from google.appengine.ext import ndb
from mapreduce import test_support
from testlib import testutil
from unit_test_util import make_questionnaire_response, _data_path

class AgeRangePipelineTest(testutil.CloudStorageTestBase):
  def setUp(self):
    testutil.HandlerTestBase.setUp(self)

  def test_end_to_end(self):
    participant_id = '1'
    participant_key = ndb.Key(participant.Participant, participant_id)
    participant_entry = participant.Participant(key=participant_key, biobankId=None)
    participant.DAO.insert(participant_entry, datetime.datetime(2015, 9, 1))

    questionnaire_json = json.loads(open(_data_path('consent_questionnaire.json')).read())
    questionnaire_key = questionnaire.DAO.store(questionnaire.DAO.from_json(questionnaire_json,
                                                                            None,
                                                                            questionnaire.DAO.allocate_id()))
    questionnaire_response_template = open(_data_path('consent_questionnaire_response.json')).read()
    replacements = {'consent_questionnaire_id': questionnaire_key.id(),
                    'middle_name': 'Quentin',
                    'first_name': 'Bob',
                    'last_name': 'Jones',
                    'state': 'TX',
                    'consent_questionnaire_authored': '2016-12-30 11:23',
                    'gender_identity': 'male',
                    'participant_id': participant_id,
                    'date_of_birth': '1996-01-01' }
    response_json = questionnaire_response_template
    for k, v in replacements.iteritems():
      response_json = response_json.replace('$%s'%k, v)
    response_obj = questionnaire_response.DAO.from_json(json.loads(response_json), participant_id,
                                                        questionnaire_response.DAO.allocate_id())
    with FakeClock(datetime.datetime(2016, 1, 1)):
      questionnaire_response.DAO.store(response_obj, datetime.datetime(2016, 9, 1, 11, 0, 2))

    summary = participant_summary.DAO.get_summary_for_participant(participant_id)
    self.assertEquals('18-25', summary.ageRange)

    with FakeClock(datetime.datetime(2026, 1, 1)) as now:
      new_summary = participant_summary.DAO.get_summary_for_participant(participant_id)
      self.assertEquals('18-25', new_summary.ageRange)

      age_range_pipeline.AgeRangePipeline(now).start()
      test_support.execute_until_empty(self.taskqueue)
      # After running the pipeline, the age range has been updated.
      new_summary = participant_summary.DAO.get_summary_for_participant(participant_id)
      self.assertEquals('26-35', new_summary.ageRange)


