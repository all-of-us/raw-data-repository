import itertools

import clock
from dao.biobank_order_dao import VALID_TESTS
from dao.biobank_stored_sample_dao import BiobankStoredSampleDao, _split_into_batches
from model.biobank_stored_sample import BiobankStoredSample
from model.participant import Participant
from dao.participant_dao import ParticipantDao
from unit_test_util import SqlTestBase


class BiobankStoredSampleDaoTest(SqlTestBase):
  """Tests only that a sample can be written and read; see the reconciliation pipeline."""

  def setUp(self):
    super(BiobankStoredSampleDaoTest, self).setUp()
    self.participant = Participant(participantId=123, biobankId=555)
    ParticipantDao().insert(self.participant)
    self.dao = BiobankStoredSampleDao()

  def test_insert_and_read_sample(self):
    sample_id = 'WEB123456'
    test_code = '1U234'
    now = clock.CLOCK.now()
    created = self.dao.insert(BiobankStoredSample(
        biobankStoredSampleId=sample_id,
        biobankId=self.participant.biobankId,
        test=test_code,
        confirmed=now))
    fetched = self.dao.get(sample_id)
    self.assertEquals(test_code, created.test)
    self.assertEquals(test_code, fetched.test)

  def test_batched_write_multi_batch(self):
    num_samples = int(2.5 * self.dao._UPDATE_BATCH_SIZE)
    test_code = iter(VALID_TESTS).next()
    self.dao.upsert_batched(BiobankStoredSample(
        biobankStoredSampleId='W%d' % i,
        biobankId=self.participant.biobankId,
        test=test_code,
        confirmed=clock.CLOCK.now()) for i in xrange(num_samples))
    self.assertEquals(self.dao.count(), num_samples)

  def test_batching(self):
    zero_to_ten = range(10)
    batches = list(_split_into_batches(zero_to_ten, 4))
    self.assertItemsEqual(zero_to_ten, itertools.chain(*batches))
    self.assertEquals(len(batches), 3)
    self.assertEquals(batches[0], [0, 1, 2, 3])
    self.assertEquals(batches[1], [4, 5, 6, 7])
    self.assertEquals(batches[2], [8, 9])
