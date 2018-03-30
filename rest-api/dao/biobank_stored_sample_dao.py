from code_constants import BIOBANK_TESTS_SET
from dao.base_dao import BaseDao
import logging
from model.biobank_stored_sample import BiobankStoredSample
from sqlalchemy.exc import DBAPIError


class BiobankStoredSampleDao(BaseDao):
  """Batch operations for updating samples. Individual insert/get operations are testing only."""
  def __init__(self):
    super(BiobankStoredSampleDao, self).__init__(BiobankStoredSample)

  def get_id(self, obj):
    return obj.biobankStoredSampleId

  def upsert_all(self, sample_generator):
    """Inserts/updates samples. Raises ValueError for invalid samples and re-attempts in case of a
    database connection lost."""
    # TODO(DA-230) Scale this to handle full study data.
    # SQLAlchemy does not provide batch upserting; individual session.merge() calls as below may be
    # expensive but cannot be effectively batched, see stackoverflow.com/questions/25955200. If this
    # proves to be a bottleneck, we can switch to generating "INSERT .. ON DUPLICATE KEY UPDATE".
    tries = 0
    while tries < 10:
      written = 0
      session = self._database.make_session()
      for sample in sample_generator:
        if sample.test not in BIOBANK_TESTS_SET:
          logging.warn('test sample %s not recognized.' % sample.test)
        else:
          session.merge(sample)
          written += 1
      try:
        session.commit()
      except DBAPIError as exc:
        session.rollback()
        if exc.connection_invalidated:
          tries += 1
          continue
        else:
          raise
      else:
        return written
      finally:
        session.close()
