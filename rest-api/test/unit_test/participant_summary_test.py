import participant_summary

from google.appengine.ext import ndb
from test.unit_test.unit_test_util import NdbTestBase, TestBase, to_dict_strip_last_modified

class ParticipantSummaryNdbTest(NdbTestBase):
  def test_query_on_fields(self):
    bob_json = { 'first_name': 'Bob',
              'fields': [{ 'key': 'K1', 'value': 'b', 'canonicalized': 'B' },
                         { 'key': 'K2', 'value': 'b', 'canonicalized': 'B' }] };
    joe_json = { 'first_name': 'Joe',
              'fields': [{ 'key': 'K1', 'value': 'c', 'canonicalized': 'C' },
                         { 'key': 'K2', 'value': 'b', 'canonicalized': 'B' }] };
    bob_summary = participant_summary.DAO.from_json(bob_json, None, '123')
    joe_summary = participant_summary.DAO.from_json(joe_json, None, '456')
    participant_summary.DAO.insert(bob_summary)
    participant_summary.DAO.insert(joe_summary)
    bob_summary_json = participant_summary.DAO.to_json(participant_summary.DAO.load('123'))
    joe_summary_json = participant_summary.DAO.to_json(participant_summary.DAO.load('456'))
    self.assertEquals({'items': [bob_summary_json]},
                      participant_summary.DAO.query_on_fields({'K1': 'B'}))
    self.assertEquals({'items': [bob_summary_json]},
                      participant_summary.DAO.query_on_fields({'K1': 'B', 'K2': 'B'}))
    self.assertEquals({'items': [joe_summary_json]},
                      participant_summary.DAO.query_on_fields({'K1': 'C'}))
    self.assertEquals({'items': [bob_summary_json, joe_summary_json]},
                      participant_summary.DAO.query_on_fields({'K2': 'B'}))
