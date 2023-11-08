import logging

from rdr_service import config
from rdr_service.genomic.genomic_job_controller import GenomicJobController
from rdr_service.genomic_enums import GenomicJob, GenomicManifestTypes
from rdr_service.offline.genomics.genomic_dispatch import load_manifest_into_raw_table


def gem_a1_manifest_workflow():
    """
    Entrypoint for GEM A1 Workflow
    First workflow in GEM Workflow
    """
    with GenomicJobController(GenomicJob.GEM_A1_MANIFEST,
                              bucket_name=config.GENOMIC_GEM_BUCKET_NAME) as controller:
        controller.reconcile_report_states(genome_type=config.GENOME_TYPE_ARRAY)
        controller.generate_manifest(
            GenomicManifestTypes.GEM_A1,
            genome_type=config.GENOME_TYPE_ARRAY
        )

        for manifest in controller.manifests_generated:
            logging.info(
                f"Loading A1 Raw Data: {manifest['file_path']}")

            # Call pipeline function to load raw
            load_manifest_into_raw_table(manifest['file_path'], "a1")


def gem_a3_manifest_workflow():
    """
    Entrypoint for GEM A3 Workflow
    """
    with GenomicJobController(GenomicJob.GEM_A3_MANIFEST,
                              bucket_name=config.GENOMIC_GEM_BUCKET_NAME) as controller:
        controller.reconcile_report_states(genome_type=config.GENOME_TYPE_ARRAY)
        controller.generate_manifest(
            GenomicManifestTypes.GEM_A3,
            genome_type=config.GENOME_TYPE_ARRAY
        )

        for manifest in controller.manifests_generated:
            logging.info(
                f"Loading A3 Raw Data: {manifest['file_path']}")

            # Call pipeline function to load raw
            load_manifest_into_raw_table(manifest['file_path'], "a3")
