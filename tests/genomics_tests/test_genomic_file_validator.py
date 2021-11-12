
from rdr_service.config import GENOME_TYPE_ARRAY, GENOME_TYPE_WGS
from rdr_service.genomic_enums import GenomicJob
from rdr_service.genomic.genomic_job_components import GenomicFileValidator
from tests.helpers.unittest_base import BaseTestCase


class GenomicFileValidatorTest(BaseTestCase):
    def setUp(self):
        super(GenomicFileValidatorTest, self).setUp()

    def test_set_genome_type_filename(self):
        array_filename = 'RDR_AoU_GEN_TestData.csv'
        wgs_filename = 'RDR_AoU_SEQ_TestData.csv'

        file_validator = GenomicFileValidator(
            filename=wgs_filename,
            job_id=GenomicJob.AW1_MANIFEST
        )

        self.assertIsNone(file_validator.genome_type)

        file_validator = GenomicFileValidator(
           filename=array_filename,
           job_id=GenomicJob.METRICS_INGESTION
        )

        file_validator.set_genome_type()

        self.assertIsNotNone(file_validator.genome_type)
        self.assertEqual(file_validator.genome_type, GENOME_TYPE_ARRAY)

        file_validator = GenomicFileValidator(
            filename=wgs_filename,
            job_id=GenomicJob.METRICS_INGESTION
        )

        file_validator.set_genome_type()

        self.assertIsNotNone(file_validator.genome_type)
        self.assertEqual(file_validator.genome_type, GENOME_TYPE_WGS)

    def test_set_gc_site_id_filename(self):
        pass


