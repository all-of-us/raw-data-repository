from datetime import datetime
from typing import Optional

from sqlalchemy.orm import Session

from rdr_service import config
from rdr_service.dao.base_dao import BaseDao
from rdr_service.model.biobank_order import BiobankOrderIdentifier, BiobankOrder
from rdr_service.model.biobank_stored_sample import BiobankStoredSample
from rdr_service.model.site import Site
from rdr_service.services.system_utils import min_or_none


class BiobankStoredSampleDao(BaseDao):
    """Batch operations for updating samples. Individual insert/get operations are testing only."""

    def __init__(self):
        super(BiobankStoredSampleDao, self).__init__(BiobankStoredSample)

    def get_id(self, obj):
        return obj.biobankStoredSampleId

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
                BiobankStoredSample.biobankStoredSampleId == biobank_stored_sample_id,
                BiobankOrder.is_not_ignored()
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

    @classmethod
    def get_earliest_confirmed_dna_sample_timestamp(cls, session, biobank_id) -> Optional[datetime]:
        confirmed_dna_sample_list = BiobankStoredSampleDao.load_confirmed_dna_samples(
            session=session,
            biobank_id=biobank_id
        )
        return min_or_none(sample.confirmed for sample in confirmed_dna_sample_list)
