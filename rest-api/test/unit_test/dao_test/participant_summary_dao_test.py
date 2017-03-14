import datetime
import json
from base64 import urlsafe_b64encode, urlsafe_b64decode

from query import Query, Operator, FieldFilter, OrderBy

import config
from dao.base_dao import json_serial
from dao.biobank_stored_sample_dao import BiobankStoredSampleDao
from dao.participant_dao import ParticipantDao
from dao.participant_summary_dao import ParticipantSummaryDao
from model.participant import Participant
from model.biobank_stored_sample import BiobankStoredSample
from participant_enums import MembershipTier
from unit_test_util import SqlTestBase, PITT_HPO_ID


class ParticipantSummaryDaoTest(SqlTestBase):
  def setUp(self):
    super(ParticipantSummaryDaoTest, self).setUp(with_data=True)
    self.dao = ParticipantSummaryDao()
    self.participant_dao = ParticipantDao()
    self.no_filter_query = Query([], None, 2, None)
    self.one_filter_query = Query([FieldFilter("participantId", Operator.EQUALS, 1)],
                                  None, 2, None)
    self.two_filter_query = Query([FieldFilter("participantId", Operator.EQUALS, 1),
                                   FieldFilter("hpoId", Operator.EQUALS, PITT_HPO_ID)],
                                  None, 2, None)
    self.ascending_biobank_id_query = Query([], OrderBy("biobankId", True), 2, None)
    self.descending_biobank_id_query = Query([], OrderBy("biobankId", False), 2, None)
    self.membership_tier_order_query = Query([], OrderBy("membershipTier", True), 2, None)
    self.hpo_id_order_query = Query([], OrderBy("hpoId", True), 2, None)
    self.first_name_order_query = Query([], OrderBy("firstName", True), 2, None)

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
    self.assert_no_results(self.ascending_biobank_id_query)
    self.assert_no_results(self.descending_biobank_id_query)

  def testQuery_oneSummary(self):
    participant = Participant(participantId=1, biobankId=2)
    self.participant_dao.insert(participant)
    participant_summary = self.dao.get(1)
    self.assert_results(self.no_filter_query, [participant_summary])
    self.assert_results(self.one_filter_query, [participant_summary])
    self.assert_no_results(self.two_filter_query)
    self.assert_results(self.ascending_biobank_id_query, [participant_summary])
    self.assert_results(self.descending_biobank_id_query, [participant_summary])

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
    self.assert_results(self.ascending_biobank_id_query, [ps_2, ps_1])
    self.assert_results(self.descending_biobank_id_query, [ps_1, ps_2])

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
    self.assert_results(self.ascending_biobank_id_query, [ps_2, ps_3],
                        _make_pagination_token([3, None, None, None, 3]))
    self.assert_results(self.descending_biobank_id_query, [ps_1, ps_3],
                        _make_pagination_token([3, None, None, None, 3]))

    self.assert_results(_with_token(self.no_filter_query,
                                    _make_pagination_token([None, None, None, 2])), [ps_3])
    self.assert_results(_with_token(self.ascending_biobank_id_query,
                                    _make_pagination_token([3, None, None, None, 3])), [ps_1])
    self.assert_results(_with_token(self.descending_biobank_id_query,
                                    _make_pagination_token([3, None, None, None, 3])), [ps_2])

  def testQuery_fourFullSummaries_paginate(self):
    participant_1 = Participant(participantId=1, biobankId=4)
    self.participant_dao.insert(participant_1)
    participant_2 = Participant(participantId=2, biobankId=1)
    self.participant_dao.insert(participant_2)
    participant_3 = Participant(participantId=3, biobankId=3)
    self.participant_dao.insert(participant_3)
    participant_4 = Participant(participantId=4, biobankId=2)
    self.participant_dao.insert(participant_4)
    ps_1 = self.dao.get(1)
    ps_2 = self.dao.get(2)
    ps_3 = self.dao.get(3)
    ps_4 = self.dao.get(4)

    ps_1.lastName = 'Jones'
    ps_1.firstName = 'Bob'
    ps_1.dateOfBirth = datetime.date(1978, 10, 9)
    ps_1.hpoId = PITT_HPO_ID
    self.dao.update(ps_1)

    ps_2.lastName = 'Aardvark'
    ps_2.firstName = 'Bob'
    ps_2.dateOfBirth = datetime.date(1978, 10, 10)
    ps_2.membershipTier = MembershipTier.SKIPPED
    self.dao.update(ps_2)

    ps_3.lastName = 'Jones'
    ps_3.firstName = 'Bob'
    ps_3.dateOfBirth = datetime.date(1978, 10, 10)
    ps_3.hpoId = PITT_HPO_ID
    ps_3.membershipTier = MembershipTier.REGISTERED
    self.dao.update(ps_3)

    ps_4.lastName = 'Jones'
    ps_4.membershipTier = MembershipTier.VOLUNTEER
    self.dao.update(ps_4)

    self.assert_results(self.no_filter_query, [ps_2, ps_4],
                        _make_pagination_token(['Jones', None, None, 4]))
    self.assert_results(self.one_filter_query, [ps_1])
    self.assert_results(self.two_filter_query, [ps_1])
    self.assert_results(self.ascending_biobank_id_query, [ps_2, ps_4],
                        _make_pagination_token([2, 'Jones', None, None, 4]))
    self.assert_results(self.descending_biobank_id_query, [ps_1, ps_3],
                        _make_pagination_token([3, 'Jones', 'Bob', datetime.date(1978, 10, 10), 3]))
    self.assert_results(self.hpo_id_order_query, [ps_2, ps_4],
                        _make_pagination_token([0, 'Jones', None, None, 4]))
    self.assert_results(self.membership_tier_order_query, [ps_1, ps_2],
                        _make_pagination_token(['SKIPPED', 'Aardvark', 'Bob',
                                                datetime.date(1978, 10, 10), 2]))

    self.assert_results(_with_token(self.no_filter_query,
                                    _make_pagination_token(['Jones', None, None, 4])),
                        [ps_1, ps_3])
    self.assert_results(_with_token(self.ascending_biobank_id_query,
                                    _make_pagination_token([2, 'Jones', None, None, 4])),
                        [ps_3, ps_1])
    self.assert_results(_with_token(self.descending_biobank_id_query,
                                    _make_pagination_token([3, 'Jones', 'Bob',
                                                            datetime.date(1978, 10, 10), 3])),
                        [ps_4, ps_2])
    self.assert_results(_with_token(self.hpo_id_order_query,
                                    _make_pagination_token([0, 'Jones', None, None, 4])),
                        [ps_1, ps_3])
    self.assert_results(_with_token(self.membership_tier_order_query,
                                    _make_pagination_token(['SKIPPED', 'Aardvark', 'Bob',
                                                datetime.date(1978, 10, 10), 2])),
                        [ps_3, ps_4])

  def test_update_from_samples(self):
    baseline_tests = ['BASELINE1', 'BASELINE2']
    config.override_setting(config.BASELINE_SAMPLE_TEST_CODES, baseline_tests)
    self.dao.update_from_biobank_stored_samples()  # safe noop

    p_baseline_samples = self.participant_dao.insert(Participant(participantId=1, biobankId=11))
    p_mixed_samples = self.participant_dao.insert(Participant(participantId=2, biobankId=22))
    p_no_samples = self.participant_dao.insert(Participant(participantId=3, biobankId=33))
    self.assertEquals(self.dao.get(p_baseline_samples.participantId).numBaselineSamplesArrived, 0)

    sample_dao = BiobankStoredSampleDao()
    def add_sample(participant, test_code, sample_id):
      sample_dao.insert(BiobankStoredSample(
          biobankStoredSampleId=sample_id, biobankId=participant.biobankId, test=test_code))

    add_sample(p_baseline_samples, baseline_tests[0], '11111')
    add_sample(p_baseline_samples, baseline_tests[1], '22223')
    add_sample(p_mixed_samples, baseline_tests[0], '11112')
    add_sample(p_mixed_samples, 'NOT1', '44441')

    self.dao.update_from_biobank_stored_samples()
    self.assertEquals(self.dao.get(p_baseline_samples.participantId).numBaselineSamplesArrived, 2)
    self.assertEquals(self.dao.get(p_mixed_samples.participantId).numBaselineSamplesArrived, 1)
    self.assertEquals(self.dao.get(p_no_samples.participantId).numBaselineSamplesArrived, 0)


def _with_token(query, token):
  return Query(query.field_filters, query.order_by, query.max_results, token)

def _make_pagination_token(vals):
  vals_json = json.dumps(vals, default=json_serial)
  return urlsafe_b64encode(vals_json)

def _decode_token(token):
  if token is None:
    return None
  return json.loads(urlsafe_b64decode(token))
