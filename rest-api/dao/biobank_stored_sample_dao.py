import logging

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

  def upsert_batched(self, sample_generator):
    """Inserts/updates samples, skipping any invalid samples."""
    # TODO(DA-230) Scale this to handle full study data.
    # SQLAlchemy does not provide batch upserting; individual session.merge() calls as below may be
    # expensive but cannot be effectively batched, see stackoverflow.com/questions/25955200. If this
    # proves to be a bottleneck, we can switch to generating "INSERT .. ON DUPLICATE KEY UPDATE".
    written = 0
    skipped = 0
    with self.session() as session:
      valid_biobank_ids = ParticipantDao().get_valid_biobank_id_set(session)
      for sample in sample_generator:
        if sample.biobankId not in valid_biobank_ids:
          logging.warning(
              'Skipping sample %r: invalid participant Biobank ID %r (%d valid IDs).',
              sample.biobankStoredSampleId, sample.biobankId, len(valid_biobank_ids))
          skipped += 1
          continue
        if sample.test not in BIOBANK_TESTS_SET:
          logging.warning(
              'Skipping sample %r: invalid test code %r.',
              sample.test, sample.biobankStoredSampleId)
          skipped += 1
          continue
        session.merge(sample)
        written += 1
    logging.info('Wrote %d samples, skipped %d invalid samples.', written, skipped)
    return written, skipped
