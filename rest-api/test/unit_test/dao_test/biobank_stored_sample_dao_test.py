import clock
from dao.biobank_stored_sample_dao import BiobankStoredSampleDao
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
