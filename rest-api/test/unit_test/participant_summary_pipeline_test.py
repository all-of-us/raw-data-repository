import concepts
import datetime
import json
import participant
import participant_dao
import participant_summary
import questionnaire
import questionnaire_response
import unittest

from offline import participant_summary_pipeline
from google.appengine.ext import ndb
from mapreduce import test_support
from testlib import testutil
from unit_test_util import make_questionnaire_response, data_path


class ParticipantSummaryPipelineTest(testutil.CloudStorageTestBase):
  def setUp(self):
    testutil.HandlerTestBase.setUp(self)

  def test_end_to_end(self):
    participant_id = '1'
    participant_key = ndb.Key(participant.Participant, participant_id)
    participant_entry = participant.Participant(key=participant_key, biobankId=None)
    participant_dao.DAO().insert(participant_entry, datetime.datetime(2015, 9, 1))

    participant_id_2 = '2'
    participant_key_2 = ndb.Key(participant.Participant, participant_id_2)
    participant_entry_2 = participant.Participant(key=participant_key_2, biobankId=None)
    participant_dao.DAO().insert(participant_entry_2, datetime.datetime(2015, 9, 1))

    questionnaire_json = json.loads(open(data_path('questionnaire_example.json')).read())
    questionnaire_key = questionnaire.DAO().store(questionnaire.DAO().from_json(questionnaire_json,
                                                                            None,
                                                                            questionnaire.DAO().allocate_id()))
    response = make_questionnaire_response(participant_key.id(),
                                           questionnaire_key.id(),
                                           [("race", concepts.WHITE),
                                            ("ethnicity", concepts.NON_HISPANIC),
                                            ("stateOfResidence", concepts.STATES_BY_ABBREV['TX']),
                                            ("membershipTier", concepts.REGISTERED)])
    questionnaire_response.DAO().store(response, datetime.datetime(2016, 9, 1, 11, 0, 2))

    participant_result = participant_dao.DAO().load(participant_id)
    self.assertTrue(participant_result.biobankId)
    summary = participant_summary.DAO().get_summary_for_participant(participant_id)
    summary_2 = participant_summary.DAO().get_summary_for_participant(participant_id_2)
    self.assertTrue(summary)
    self.assertTrue(summary_2)
    self.assertEquals(participant_summary.Race.WHITE, summary.race)
    self.assertEquals(participant_summary.Ethnicity.NON_HISPANIC, summary.ethnicity)
    self.assertEquals(participant_summary.MembershipTier.REGISTERED, summary.membershipTier)

    # Delete the first participant summary, but not the second.
    summary.key.delete()
    self.assertFalse(participant_summary.DAO().get_summary_for_participant(participant_id))

    participant_summary_pipeline.ParticipantSummaryPipeline().start()
    test_support.execute_until_empty(self.taskqueue)

    new_summary = participant_summary.DAO().get_summary_for_participant(participant_id)
    new_summary_2 = participant_summary.DAO().get_summary_for_participant(participant_id_2)

    self.assertEquals(summary, new_summary)
    self.assertEquals(summary_2, new_summary_2)



