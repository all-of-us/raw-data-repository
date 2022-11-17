import logging

from sqlalchemy.orm import Session

from rdr_service import config
from rdr_service.code_constants import BIOBANK_TESTS_SET
from rdr_service.dao.base_dao import BaseDao
from rdr_service.model.biobank_order import BiobankOrderIdentifier, BiobankOrder
from rdr_service.model.biobank_stored_sample import BiobankStoredSample
from rdr_service.model.site import Site


class BiobankStoredSampleDao(BaseDao):
    """Batch operations for updating samples. Individual insert/get operations are testing only."""

    def __init__(self):
        super(BiobankStoredSampleDao, self).__init__(BiobankStoredSample)

    def get_id(self, obj):
        return obj.biobankStoredSampleId

    def upsert_all(self, samples):
        """Inserts/updates samples. """
        # Ensure that the sample set can be re-iterated if the operation needs to be retried
        samples = list(samples)

        def upsert(session):
            # TODO(DA-230) Scale this to handle full study data.  SQLAlchemy does not provide batch
            # upserting; individual session.merge() calls as below may be expensive but cannot be
            # effectively batched, see stackoverflow.com/questions/25955200. If this proves to be a
            # bottleneck, we can switch to generating "INSERT .. ON DUPLICATE KEY UPDATE".
            written = 0
            for sample in samples:
                if sample.test not in BIOBANK_TESTS_SET:
                    logging.warn("test sample %s not recognized." % sample.test)
                else:
                    session.merge(sample)
                    written += 1
            return written

        return self._database.autoretry(upsert)

    def get_diversion_pouch_site_id(self, biobank_stored_sample_id):
        with self.session() as session:
            results = session.query(
                BiobankStoredSample.biobankStoredSampleId,
                Site.siteId
            ).join(
                BiobankOrderIdentifier,
                BiobankOrderIdentifier.value == BiobankStoredSample.biobankOrderIdentifier
            ).join(
                BiobankOrder,
                BiobankOrder.biobankOrderId == BiobankOrderIdentifier.biobankOrderId
            ).join(
                Site,
                Site.siteId == BiobankOrder.collectedSiteId
            ).filter(
                Site.siteType == "Diversion Pouch",
                BiobankStoredSample.biobankStoredSampleId == biobank_stored_sample_id
            ).distinct().one_or_none()

            if results:
                return getattr(results, 'siteId')
            else:
                return

    @classmethod
    def load_confirmed_dna_samples(cls, session: Session, biobank_id):
        return session.query(
            BiobankStoredSample
        ).filter(
            BiobankStoredSample.biobankId == biobank_id,
            BiobankStoredSample.confirmed.isnot(None),
            BiobankStoredSample.test.in_(config.getSettingList(config.DNA_SAMPLE_TEST_CODES))
        ).all()
