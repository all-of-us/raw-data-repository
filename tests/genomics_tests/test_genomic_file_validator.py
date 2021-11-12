
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

        gc_file_name = 'RDR_AoU_GEN_PKG-1908-218051.csv'
        rdr_file_components = [x.lower() for x in gc_file_name.split('/')[-1].split("_")]

        file_validator = GenomicFileValidator(
            filename=gc_file_name,
            job_id=GenomicJob.AW1_MANIFEST
        )

        file_validator.set_gc_site_id(rdr_file_components[0])

        self.assertIsNotNone(file_validator.gc_site_id)
        self.assertTrue(file_validator.gc_site_id in file_validator.VALID_GENOME_CENTERS)
        self.assertEqual(file_validator.gc_site_id, rdr_file_components[0])

