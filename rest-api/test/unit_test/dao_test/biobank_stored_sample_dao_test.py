import config
from dao.biobank_stored_sample_dao import BiobankStoredSampleDao
from dao.participant_summary_dao import ParticipantSummaryDao
from model.biobank_stored_sample import BiobankStoredSample
from unit_test_util import SqlTestBase

from werkzeug.exceptions import BadRequest


class BiobankStoredSampleDaoTest(SqlTestBase):
  _BASELINE_CODE = 'B1TEST'

  def setUp(self):
    super(BiobankStoredSampleDaoTest, self).setUp()
    self.dao = BiobankStoredSampleDao()
    config.override_setting(config.BASELINE_SAMPLE_TEST_CODES, [self._BASELINE_CODE])

  def test_no_individual_insert(self):
    with self.assertRaises(NotImplementedError):
      self.dao.insert(BiobankStoredSample())

  def test_bad_participant(self):
    with self.assertRaises(BadRequest):
      with self.dao.session() as session:
        self.dao.insert_or_update(session, 999, [BiobankStoredSample(biobankStoredSampleId=1)])

  def _assert_count(self, expected):
    # self.participant's relationship isn't auto-refreshed, so separately fetch the summary object.
    summary = ParticipantSummaryDao().get(self.participant.participantId)
    self.assertEquals(expected, summary.numBaselineSamplesArrived)

  def _insert_sample(self, sample_ids, test_codes):
    self.assertEquals(len(sample_ids), len(test_codes))
    with self.dao.session() as session:
      self.dao.insert_or_update(
          session,
          self.participant.participantId,
          [BiobankStoredSample(biobankStoredSampleId=s, testCode=t)
           for s, t in zip(sample_ids, test_codes)])

  def test_updates_summary(self):
    self._assert_count(0)
    self._insert_sample((1,), (self._BASELINE_CODE,))
    self._assert_count(1)

  def test_updates_summary_duplicate_code_counts_twice(self):
    self._insert_sample((1, 2), (self._BASELINE_CODE, self._BASELINE_CODE))
    self._assert_count(2)

  def test_no_summary_update_if_not_baseline(self):
    self._insert_sample((1,), ('NOT BASELINE',))
    self._assert_count(0)
