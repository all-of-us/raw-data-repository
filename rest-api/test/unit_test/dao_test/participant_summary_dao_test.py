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
    self.descending_query = Query([], OrderBy("biobankId", True), 2, None)


  def assert_no_results(self, query):
    results = self.dao.query(query)
    self.assertEquals([], results.items)
    self.assertIsNone(results.pagination_token)

  def assert_results(self, query, items, pagination_token=None):
    results = self.dao.query(query)
    self.assertListAsDictEquals(items, results.items)
    self.assertEquals(pagination_token, results.pagination_token)


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
