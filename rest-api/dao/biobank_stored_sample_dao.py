import logging

from dao.base_dao import BaseDao
from dao.biobank_order_dao import VALID_TESTS
from dao.participant_dao import ParticipantDao
from model.biobank_stored_sample import BiobankStoredSample


class BiobankStoredSampleDao(BaseDao):
  """Batch operations for updating samples. Individual insert/get operations are testing only."""
  _UPDATE_BATCH_SIZE = 1000

  def __init__(self):
    super(BiobankStoredSampleDao, self).__init__(BiobankStoredSample)

  def get_id(self, obj):
    return obj.biobankStoredSampleId

  def upsert_batched(self, sample_generator):
    """Inserts/updates samples, skipping any invalid samples."""
    with self.session() as session:
      # It's OK for the valid ID set to be slightly stale relative to upcoming sessions used for
      # insertion, since we don't delete participant IDs.
      valid_biobank_ids = ParticipantDao().get_valid_biobank_id_set(session)
    for samples_batch in _split_into_batches(sample_generator, self._UPDATE_BATCH_SIZE):
      with self.session() as session:
        written, skipped = self._upsert_batch(samples_batch, valid_biobank_ids)
      logging.info('Wrote %d samples, skipped %d invalid samples.', written, skipped)

  def _upsert_batch(self, session, samples, valid_biobank_ids):
    written = 0
    skipped = 0
    for sample in samples:
      if sample.biobankId not in valid_biobank_ids:
        logging.warning(
            'Skipping sample %r: invalid participant Biobank ID %r (%d valid IDs).',
            sample.biobankStoredSampleId, sample.biobankId, len(valid_biobank_ids))
        skipped += 1
        continue
      if sample.confirmed is None:
        logging.warning(
            'Skipping sample %r: no "confirmed" timestmap.', sample.biobankStoredSampleId)
        skipped += 1
        continue
      if sample.test not in VALID_TESTS:
        logging.warning(
            'Skipping sample %r: invalid test code %r.', sample.test, sample.biobankStoredSampleId)
        skipped += 1
        continue
      # We could switch to add_all or bulk_save_objects if this is slow.
      session.merge(sample)
      written += 1
    return written, skipped


def _split_into_batches(iterable, batch_size):
  batch = []
  for i, v in enumerate(iterable, start=1):
    batch.append(v)
    if i % batch_size == 0:
      yield batch
      batch = []
  if batch:
    yield batch
