import json

from base64 import urlsafe_b64encode, urlsafe_b64decode
from dao.participant_dao import ParticipantDao
from dao.participant_summary_dao import ParticipantSummaryDao
from model.participant import Participant
from model.participant_summary import ParticipantSummary
from query import Query, Operator, FieldFilter, OrderBy, Results
from unit_test_util import SqlTestBase

class ParticipantSummaryDaoTest(SqlTestBase):
  def setUp(self):
    super(ParticipantSummaryDaoTest, self).setUp()
    self.dao = ParticipantSummaryDao()
    self.participant_dao = ParticipantDao()
    self.no_filter_query = Query([], None, 2, None)
    self.one_filter_query = Query([FieldFilter("participantId", Operator.EQUALS, 1)],
                                  None, 2, None)
    self.two_filter_query = Query([FieldFilter("participantId", Operator.EQUALS, 1),
                                   FieldFilter("hpoId", Operator.EQUALS, 1)],
                                  None, 2, None)
    self.ascending_query = Query([], OrderBy("biobankId", True), 2, None)
    self.descending_query = Query([], OrderBy("biobankId", False), 2, None)


  def assert_no_results(self, query):
    results = self.dao.query(query)
    self.assertEquals([], results.items)
    self.assertIsNone(results.pagination_token)

  def assert_results(self, query, items, pagination_token=None):
    results = self.dao.query(query)
    self.assertListAsDictEquals(items, results.items)
    self.assertEquals(pagination_token, results.pagination_token,
                      "Pagination tokens don't match; decoded = %s, %s" %
                      (_decode_token(pagination_token), _decode_token(results.pagination_token)))

  def testQuery_noSummaries(self):
    self.assert_no_results(self.no_filter_query)
    self.assert_no_results(self.one_filter_query)
    self.assert_no_results(self.two_filter_query)
    self.assert_no_results(self.ascending_query)
    self.assert_no_results(self.descending_query)

  def testQuery_oneSummary(self):
    participant = Participant(participantId=1, biobankId=2)
    self.participant_dao.insert(participant)
    participant_summary = self.dao.get(1)
    self.assert_results(self.no_filter_query, [participant_summary])
    self.assert_results(self.one_filter_query, [participant_summary])
    self.assert_no_results(self.two_filter_query)
    self.assert_results(self.ascending_query, [participant_summary])
    self.assert_results(self.descending_query, [participant_summary])

  def testQuery_twoSummaries(self):
    participant_1 = Participant(participantId=1, biobankId=2)
    self.participant_dao.insert(participant_1)
    participant_2 = Participant(participantId=2, biobankId=1)
    self.participant_dao.insert(participant_2)
    ps_1 = self.dao.get(1)
    ps_2 = self.dao.get(2)
    self.assert_results(self.no_filter_query, [ps_1, ps_2])
    self.assert_results(self.one_filter_query, [ps_1])
    self.assert_no_results(self.two_filter_query)
    self.assert_results(self.ascending_query, [ps_2, ps_1])
    self.assert_results(self.descending_query, [ps_1, ps_2])

  def testQuery_threeSummaries_paginate(self):
    participant_1 = Participant(participantId=1, biobankId=4)
    self.participant_dao.insert(participant_1)
    participant_2 = Participant(participantId=2, biobankId=1)
    self.participant_dao.insert(participant_2)
    participant_3 = Participant(participantId=3, biobankId=3)
    self.participant_dao.insert(participant_3)
    ps_1 = self.dao.get(1)
    ps_2 = self.dao.get(2)
    ps_3 = self.dao.get(3)
    self.assert_results(self.no_filter_query, [ps_1, ps_2],
                        _make_pagination_token([None, None, None, 2]))
    self.assert_results(self.one_filter_query, [ps_1])
    self.assert_no_results(self.two_filter_query)
    self.assert_results(self.ascending_query, [ps_2, ps_3],
                        _make_pagination_token([3, None, None, None, 3]))
    self.assert_results(self.descending_query, [ps_1, ps_3],
                        _make_pagination_token([3, None, None, None, 3]))

def _make_pagination_token(vals):
  vals_json = json.dumps(vals)
  return urlsafe_b64encode(vals_json)

def _decode_token(token):
  if token is None:
    return None
  return json.loads(urlsafe_b64decode(token))