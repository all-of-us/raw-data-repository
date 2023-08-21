import logging

from rdr_service import config
from rdr_service.config import BIOBANK_SAMPLES_BUCKET_NAME
from rdr_service.dao.genomics_dao import GenomicLongReadDao
from rdr_service.genomic.genomic_job_controller import GenomicJobController
from rdr_service.genomic_enums import GenomicJob, GenomicManifestTypes
from rdr_service.offline.genomics.genomic_dispatch import load_manifest_into_raw_table


def lr_l0_manifest_workflow():
    long_read_max_set = GenomicLongReadDao().get_max_set()[0]
    with GenomicJobController(
        GenomicJob.LR_L0_WORKFLOW,
        bucket_name=BIOBANK_SAMPLES_BUCKET_NAME
    ) as controller:
        controller.generate_manifest(
            manifest_type=GenomicManifestTypes.LR_L0,
            genome_type=config.GENOME_TYPE_LR,
            long_read_max_set=long_read_max_set
        )
        for manifest in controller.manifests_generated:
            logging.info(
                f"Loading L0 Manifest Raw Data: {manifest['file_path']}"
            )
            # Call pipeline function to load raw
            load_manifest_into_raw_table(
                manifest['file_path'],
                "l0",
            )
