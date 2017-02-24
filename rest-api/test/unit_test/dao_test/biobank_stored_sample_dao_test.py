from dao.biobank_stored_sample_dao import BiobankStoredSampleDao
from model.biobank_stored_sample import BiobankStoredSample
from unit_test_util import SqlTestBase

from werkzeug.exceptions import BadRequest


class BiobankStoredSampleDaoTest(SqlTestBase):
  def setUp(self):
    super(BiobankStoredSampleDaoTest, self).setUp()
    self.dao = BiobankStoredSampleDao()

  def test_bad_participant(self):
    with self.assertRaises(BadRequest):
      self.dao.insert(BiobankStoredSample(participantId=999))
