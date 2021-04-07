#
# Genomic data generator.
#
import logging
import pytz

from rdr_service import clock
from rdr_service.dao.genomics_dao import GenomicSetMemberDao, GenomicSetDao
from rdr_service.data_gen.generators.base_gen import BaseGen
from rdr_service.model.genomics import GenomicSet, GenomicSetMember
from rdr_service.genomic_enums import GenomicSetStatus

_logger = logging.getLogger("rdr_logger")

_US_CENTRAL = pytz.timezone("US/Central")
_UTC = pytz.utc


class GenomicSetMemberGen(BaseGen):
    """
    Fake genomic set member data generator
    """
    def __init__(self):
        """ initialize stored sample generator """
        super(GenomicSetMemberGen, self).__init__(load_data=False)

        self.set_dao = GenomicSetDao()
        self.member_dao = GenomicSetMemberDao()

        # Genomic attributes
        self.OUTPUT_CSV_TIME_FORMAT = "%Y-%m-%d-%H-%M-%S"
        self.DRC_BIOBANK_PREFIX = "Genomic-Manifest-AoU"

        self.nowts = clock.CLOCK.now()
        self.nowf = _UTC.localize(self.nowts).astimezone(_US_CENTRAL) \
            .replace(tzinfo=None).strftime(self.OUTPUT_CSV_TIME_FORMAT)

    def make_new_genomic_set(self):
        _logger.info("    Creating new Genomic Set...")
        attributes = {
            'genomicSetName': f'data_spec_gen_{self.nowf}',
            'genomicSetCriteria': '.',
            'genomicSetVersion': 1,
            'genomicSetStatus': GenomicSetStatus.VALID,
        }
        new_set_obj = GenomicSet(**attributes)
        return self.set_dao.insert(new_set_obj)

    def make_new_genomic_set_member(self, set_id=None,
                                    sample=None,
                                    participant_id=None):
        _logger.info("    Creating new Genomic Set Member...")
        attributes = {
            'genomicSetId': set_id,
            'sampleId': sample.biobankStoredSampleId,
            'participantId': participant_id,
            'biobankId': sample.biobankId,
        }
        new_member_obj = GenomicSetMember(**attributes)
        return self.member_dao.insert(new_member_obj)
