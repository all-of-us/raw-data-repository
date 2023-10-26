import logging

from rdr_service import config
from rdr_service.dao.genomics_dao import GenomicAW1RawDao, GenomicAW2RawDao, GenomicDefaultBaseDao
from rdr_service.genomic.genomic_job_controller import GenomicJobController
from rdr_service.genomic_enums import GenomicJob, GenomicSubProcessResult
from rdr_service.model.genomics import (GenomicLRRaw, GenomicL0Raw, GenomicPRRaw, GenomicP0Raw, GenomicW1ILRaw,
                                        GenomicW2SCRaw, GenomicW2WRaw, GenomicW3NSRaw, GenomicW3SCRaw, GenomicW3SRRaw,
                                        GenomicW3SSRaw, GenomicW4WRRaw,
                                        GenomicW5NFRaw, GenomicAW4Raw, GenomicAW3Raw, GenomicP1Raw, GenomicP2Raw,
                                        GenomicRRRaw, GenomicR0Raw,
                                        GenomicR1Raw, GenomicA2Raw)
from rdr_service.services.system_utils import JSONObject


def load_manifest_into_raw_table(
    file_path,
    manifest_type,
    project_id=None,
    provider=None,
    cvl_site_id=None
):
    short_read_raw_map = {
        "aw1": {
            'job_id': GenomicJob.LOAD_AW1_TO_RAW_TABLE,
            'dao': GenomicAW1RawDao
        },
        "aw2": {
            'job_id': GenomicJob.LOAD_AW2_TO_RAW_TABLE,
            'dao': GenomicAW2RawDao
        },
        "aw3": {
            'job_id': GenomicJob.LOAD_AW3_TO_RAW_TABLE,
            'model': GenomicAW3Raw
        },
        "aw4": {
            'job_id': GenomicJob.LOAD_AW4_TO_RAW_TABLE,
            'model': GenomicAW4Raw
        }
    }
    gem_map = {
        "a2": {
            'job_id': GenomicJob.LOAD_A2_TO_RAW_TABLE,
            'model': GenomicA2Raw
        }
    }
    cvl_raw_map = {
        "w1il": {
            'job_id': GenomicJob.LOAD_CVL_W1IL_TO_RAW_TABLE,
            'model': GenomicW1ILRaw
        },
        "w2sc": {
            'job_id': GenomicJob.LOAD_CVL_W2SC_TO_RAW_TABLE,
            'model': GenomicW2SCRaw
        },
        "w2w": {
            'job_id': GenomicJob.LOAD_CVL_W2W_TO_RAW_TABLE,
            'model': GenomicW2WRaw
        },
        "w3ns": {
            'job_id': GenomicJob.LOAD_CVL_W3NS_TO_RAW_TABLE,
            'model': GenomicW3NSRaw
        },
        "w3sc": {
            'job_id': GenomicJob.LOAD_CVL_W3SC_TO_RAW_TABLE,
            'model': GenomicW3SCRaw
        },
        "w3ss": {
            'job_id': GenomicJob.LOAD_CVL_W3SS_TO_RAW_TABLE,
            'model': GenomicW3SSRaw
        },
        "w3sr": {
            'job_id': GenomicJob.LOAD_CVL_W3SR_TO_RAW_TABLE,
            'model': GenomicW3SRRaw
        },
        "w4wr": {
            'job_id': GenomicJob.LOAD_CVL_W4WR_TO_RAW_TABLE,
            'model': GenomicW4WRRaw
        },
        "w5nf": {
            'job_id': GenomicJob.LOAD_CVL_W5NF_TO_RAW_TABLE,
            'model': GenomicW5NFRaw
        },
    }
    long_read_raw_map = {
        "lr": {
            'job_id': GenomicJob.LOAD_LR_TO_RAW_TABLE,
            'model': GenomicLRRaw
        },
        "l0": {
            'job_id': GenomicJob.LOAD_L0_TO_RAW_TABLE,
            'model': GenomicL0Raw
        }
    }
    pr_raw_map = {
        "pr": {
            'job_id': GenomicJob.LOAD_PR_TO_RAW_TABLE,
            'model': GenomicPRRaw
        },
        "p0": {
            'job_id': GenomicJob.LOAD_P0_TO_RAW_TABLE,
            'model': GenomicP0Raw
        },
        "p1": {
            'job_id': GenomicJob.LOAD_P1_TO_RAW_TABLE,
            'model': GenomicP1Raw
        },
        "p2": {
            'job_id': GenomicJob.LOAD_P2_TO_RAW_TABLE,
            'model': GenomicP2Raw
        }
    }
    rna_raw_map = {
        "rr": {
            'job_id': GenomicJob.LOAD_RR_TO_RAW_TABLE,
            'model': GenomicRRRaw
        },
        "r0": {
            'job_id': GenomicJob.LOAD_RO_TO_RAW_TABLE,
            'model': GenomicR0Raw
        },
        "r1": {
            'job_id': GenomicJob.LOAD_R1_TO_RAW_TABLE,
            'model': GenomicR1Raw,
            'special_mappings': {
                '260_230': 'two_sixty_two_thirty',
                '260_280': 'two_sixty_two_eighty'
            }
        }
    }

    try:
        raw_jobs_map = {
            **short_read_raw_map,
            **gem_map,
            **long_read_raw_map,
            **cvl_raw_map,
            **pr_raw_map,
            **rna_raw_map
        }[manifest_type]

        with GenomicJobController(
            job_id=raw_jobs_map.get('job_id'),
            bq_project_id=project_id,
            storage_provider=provider
        ) as controller:
            controller.load_raw_manifest_data_from_filepath(
                file_path=file_path,
                raw_dao=raw_jobs_map.get('dao', GenomicDefaultBaseDao),
                cvl_site_id=cvl_site_id,
                model=raw_jobs_map.get('model'),
                special_mappings=raw_jobs_map.get('special_mappings')
            )
    except KeyError:
        pass


def dispatch_genomic_job_from_task(_task_data: JSONObject, project_id=None):
    """
    Entrypoint for new genomic manifest file pipelines
    Sets up the genomic manifest file record and begin pipeline
    :param project_id:
    :param _task_data: dictionary of metadata needed by the controller
    """

    ingestion_workflows = (
        GenomicJob.AW1_MANIFEST,
        GenomicJob.AW1F_MANIFEST,
        GenomicJob.METRICS_INGESTION,
        GenomicJob.AW4_ARRAY_WORKFLOW,
        GenomicJob.AW4_WGS_WORKFLOW,
        GenomicJob.AW5_ARRAY_MANIFEST,
        GenomicJob.AW5_WGS_MANIFEST,
        GenomicJob.CVL_W2SC_WORKFLOW,
        GenomicJob.CVL_W3NS_WORKFLOW,
        GenomicJob.CVL_W3SC_WORKFLOW,
        GenomicJob.CVL_W3SS_WORKFLOW,
        GenomicJob.CVL_W4WR_WORKFLOW,
        GenomicJob.CVL_W5NF_WORKFLOW,
        GenomicJob.LR_LR_WORKFLOW,
        GenomicJob.PR_PR_WORKFLOW,
        GenomicJob.PR_P1_WORKFLOW,
        GenomicJob.PR_P2_WORKFLOW,
        GenomicJob.RNA_RR_WORKFLOW,
        GenomicJob.RNA_R1_WORKFLOW,
        GenomicJob.GEM_A2_MANIFEST
    )

    if _task_data.job in ingestion_workflows:
        # Ingestion Job
        with GenomicJobController(_task_data.job,
                                  task_data=_task_data,
                                  sub_folder_name=_task_data.subfolder if hasattr(_task_data, 'subfolder') else None,
                                  bq_project_id=project_id,
                                  max_num=config.getSetting(config.GENOMIC_MAX_NUM_INGEST, default=1000)
                                  ) as controller:

            controller.bucket_name = _task_data.bucket
            file_name = '/'.join(_task_data.file_data.file_path.split('/')[1:])
            controller.ingest_specific_manifest(file_name)

        if _task_data.job == GenomicJob.AW1_MANIFEST:
            # count records for AW1 manifest in new job
            _task_data.job = GenomicJob.CALCULATE_RECORD_COUNT_AW1
            dispatch_genomic_job_from_task(_task_data)

    if _task_data.job == GenomicJob.CALCULATE_RECORD_COUNT_AW1:
        # Calculate manifest record counts job
        with GenomicJobController(_task_data.job,
                                  bq_project_id=project_id) as controller:

            logging.info("Calculating record count for AW1 manifest...")

            rec_count = controller.manifest_file_dao.count_records_for_manifest_file(
                _task_data.manifest_file
            )

            controller.manifest_file_dao.update_record_count(
                _task_data.manifest_file,
                rec_count
            )


def execute_genomic_manifest_file_pipeline(_task_data: dict, project_id=None):
    """
    Entrypoint for new genomic manifest file pipelines
    Sets up the genomic manifest file record and begin pipeline
    :param project_id:
    :param _task_data: dictionary of metadata needed by the controller
    """
    task_data = JSONObject(_task_data)

    if not hasattr(task_data, 'job'):
        raise AttributeError("job are required to execute manifest file pipeline")

    if not hasattr(task_data, 'bucket'):
        raise AttributeError("bucket is required to execute manifest file pipeline")

    if not hasattr(task_data, 'file_data'):
        raise AttributeError("file_data is required to execute manifest file pipeline")

    with GenomicJobController(GenomicJob.GENOMIC_MANIFEST_FILE_TRIGGER,
                              task_data=task_data,
                              bq_project_id=project_id) as controller:
        manifest_file = controller.insert_genomic_manifest_file_record()

        if task_data.file_data.create_feedback_record:
            controller.insert_genomic_manifest_feedback_record(manifest_file)

        controller.job_result = GenomicSubProcessResult.SUCCESS

    if task_data.job:
        task_data.manifest_file = manifest_file
        dispatch_genomic_job_from_task(task_data)
    else:
        return manifest_file
