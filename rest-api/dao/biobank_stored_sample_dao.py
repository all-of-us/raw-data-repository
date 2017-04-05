from code_constants import BIOBANK_TESTS_SET
from dao.base_dao import BaseDao
from dao.participant_dao import ParticipantDao
from model.biobank_stored_sample import BiobankStoredSample


class BiobankStoredSampleDao(BaseDao):
  """Batch operations for updating samples. Individual insert/get operations are testing only."""
  def __init__(self):
    super(BiobankStoredSampleDao, self).__init__(BiobankStoredSample)

  def get_id(self, obj):
    return obj.biobankStoredSampleId

  def upsert_all(self, sample_generator):
    """Inserts/updates samples. Raises ValueError for invalid samples."""
    # TODO(DA-230) Scale this to handle full study data.
    # SQLAlchemy does not provide batch upserting; individual session.merge() calls as below may be
    # expensive but cannot be effectively batched, see stackoverflow.com/questions/25955200. If this
    # proves to be a bottleneck, we can switch to generating "INSERT .. ON DUPLICATE KEY UPDATE".
    written = 0
    with self.session() as session:
      for sample in sample_generator:
        if sample.test not in BIOBANK_TESTS_SET:
          raise ValueError(
              'Sample %r has invalid test code %r.'
              % (sample.test, sample.biobankStoredSampleId))
        session.merge(sample)
        written += 1
    return written
