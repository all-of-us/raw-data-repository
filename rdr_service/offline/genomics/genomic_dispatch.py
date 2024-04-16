import logging

from rdr_service import config
from rdr_service.dao.genomics_dao import GenomicDefaultBaseDao
from rdr_service.genomic.genomic_job_controller import GenomicJobController
from rdr_service.genomic.genomic_manifest_mappings import GENOMIC_FULL_INGESTION_MAP
from rdr_service.genomic_enums import GenomicJob, GenomicSubProcessResult
from rdr_service.model.genomics import (GenomicL0Raw, GenomicP0Raw, GenomicW1ILRaw, GenomicW2WRaw, GenomicW3SRRaw,
  GenomicR0Raw, GenomicA3Raw, GenomicA1Raw, GenomicL3Raw, GenomicAW3Raw)

from rdr_service.services.system_utils import JSONObject


def load_manifest_into_raw_table(
    file_path,
    manifest_type,
    project_id=None,
    provider=None,
    cvl_site_id=None
):
    short_read_raw_map = {
        "aw3": {
            'job_id': GenomicJob.LOAD_AW3_TO_RAW_TABLE,
            'model': GenomicAW3Raw
        }
    }
    gem_map = {
        "a1": {
            'job_id': GenomicJob.LOAD_A1_TO_RAW_TABLE,
            'model': GenomicA1Raw
        },
        "a3": {
            'job_id': GenomicJob.LOAD_A3_TO_RAW_TABLE,
            'model': GenomicA3Raw
        }
    }
    cvl_raw_map = {
        "w1il": {
            'job_id': GenomicJob.LOAD_CVL_W1IL_TO_RAW_TABLE,
            'model': GenomicW1ILRaw
        },
        "w2w": {
            'job_id': GenomicJob.LOAD_CVL_W2W_TO_RAW_TABLE,
            'model': GenomicW2WRaw
        },
        "w3sr": {
            'job_id': GenomicJob.LOAD_CVL_W3SR_TO_RAW_TABLE,
            'model': GenomicW3SRRaw
        }
    }
    long_read_raw_map = {
        "l0": {
            'job_id': GenomicJob.LOAD_L0_TO_RAW_TABLE,
            'model': GenomicL0Raw
        },
        "l3": {
            'job_id': GenomicJob.LOAD_L3_TO_RAW_TABLE,
            'model': GenomicL3Raw
        },
    }
    pr_raw_map = {
        "p0": {
            'job_id': GenomicJob.LOAD_P0_TO_RAW_TABLE,
            'model': GenomicP0Raw
        },
    }
    rna_raw_map = {
        "r0": {
            'job_id': GenomicJob.LOAD_RO_TO_RAW_TABLE,
            'model': GenomicR0Raw
        },
    }

    try:
        raw_map, generation_map, ingestion_map = None, {
            **short_read_raw_map,
            **gem_map,
            **long_read_raw_map,
            **cvl_raw_map,
            **pr_raw_map,
            **rna_raw_map,
        }, {k: v.get('raw') for k, v in GENOMIC_FULL_INGESTION_MAP.items() if v.get('raw')}

        if manifest_type in generation_map:
            raw_map = generation_map.get(manifest_type)
        else:
            enum_job_type = GenomicJob.lookup_by_name(manifest_type) if type(manifest_type) is str else manifest_type
            raw_map = ingestion_map.get(enum_job_type)

        with GenomicJobController(
            job_id=raw_map.get('job_id'),
            bq_project_id=project_id,
            storage_provider=provider
        ) as controller:
            controller.load_raw_manifest_data_from_filepath(
                file_path=file_path,
                raw_dao=raw_map.get('dao', GenomicDefaultBaseDao),
                cvl_site_id=cvl_site_id,
                model=raw_map.get('model'),
                special_mappings=raw_map.get('special_mappings')
            )
    # pylint: disable=broad-except
    except (Exception, KeyError) as e:
        logging.warning(f'Raw ingestion error occurred: {e}')


def dispatch_genomic_job_from_task(
    _task_data: JSONObject,
    project_id=None
):
    """
    Entrypoint for new genomic manifest file pipelines
    Sets up the genomic manifest file record and begin pipeline
    :param project_id:
    :param _task_data: dictionary of metadata needed by the controller
    """
    if _task_data.job in GENOMIC_FULL_INGESTION_MAP:
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
