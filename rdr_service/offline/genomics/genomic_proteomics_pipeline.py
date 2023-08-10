import logging

from rdr_service import config
from rdr_service.config import BIOBANK_SAMPLES_BUCKET_NAME
from rdr_service.dao.genomics_dao import GenomicPRDao
from rdr_service.genomic.genomic_job_controller import GenomicJobController
from rdr_service.genomic_enums import GenomicJob, GenomicManifestTypes
from rdr_service.offline.genomics.genomic_dispatch import load_awn_manifest_into_raw_table


def pr_p0_manifest_workflow():
    pr_max_set = GenomicPRDao().get_max_set()[0]
    with GenomicJobController(
        GenomicJob.PR_P0_WORKFLOW,
        bucket_name=BIOBANK_SAMPLES_BUCKET_NAME
    ) as controller:
        controller.generate_manifest(
            manifest_type=GenomicManifestTypes.PR_P0,
            genome_type=config.GENOME_TYPE_PR,
            pr_max_set=pr_max_set
        )
        for manifest in controller.manifests_generated:
            logging.info(
                f"Loading P0 Manifest Raw Data: {manifest['file_path']}"
            )
            # Call pipeline function to load raw
            load_awn_manifest_into_raw_table(
                manifest['file_path'],
                "p0",
            )
