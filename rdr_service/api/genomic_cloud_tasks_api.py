import logging

from flask import request
from flask_restful import Resource
from werkzeug.exceptions import NotFound

from rdr_service.api.cloud_tasks_api import log_task_headers
from rdr_service.app_util import task_auth_required
from rdr_service.config import getSetting, GENOMIC_AW5_WGS_SUBFOLDERS, GENOMIC_AW5_ARRAY_SUBFOLDERS
from rdr_service.dao.bq_genomics_dao import bq_genomic_set_batch_update, bq_genomic_set_member_batch_update, \
    bq_genomic_job_run_batch_update, bq_genomic_file_processed_batch_update, \
    bq_genomic_gc_validation_metrics_batch_update, bq_genomic_manifest_file_batch_update, \
    bq_genomic_manifest_feedback_batch_update
from rdr_service.dao.genomics_dao import GenomicManifestFileDao, GenomicSetMemberDao
from rdr_service.genomic.genomic_job_components import GenomicFileIngester
from rdr_service.genomic_enums import GenomicJob, GenomicManifestTypes
from rdr_service.model.genomics import GenomicSetMember, GenomicGCValidationMetrics
from rdr_service.offline import genomic_pipeline
from rdr_service.resource.generators.genomics import genomic_set_batch_update, genomic_set_member_batch_update, \
    genomic_job_run_batch_update, genomic_file_processed_batch_update, genomic_gc_validation_metrics_batch_update, \
    genomic_manifest_file_batch_update, genomic_manifest_feedback_batch_update
from rdr_service.services.system_utils import JSONObject


class LoadRawAWNManifestDataAPI(Resource):
    """
    Cloud Task endpoint: Load raw AW1/AW2 Manifest to
    genomic_aw1_raw or genomic_aw2_raw table
    """

    @task_auth_required
    def post(self):
        log_task_headers()

        # from cloud function
        data = request.get_json(force=True)

        logging.info(f'Loading {data.get("file_type").upper()} Raw Data: {data.get("filename")}')

        # Call pipeline function
        genomic_pipeline.load_awn_manifest_into_raw_table(data.get("file_path"), data.get("file_type"))

        logging.info('Complete.')
        return '{"success": "true"}'


class IngestAW1ManifestTaskApi(Resource):
    """
    Cloud Task endpoint: Ingest AW1 Manifest.
    """
    @task_auth_required
    def post(self):
        log_task_headers()
        data = request.get_json(force=True)
        logging.info(f'Ingesting AW1 File: {data.get("filename")}')

        # Set manifest_type and job
        job = GenomicJob.AW1_MANIFEST
        manifest_type = GenomicManifestTypes.BIOBANK_GC
        create_fb = True

        # Write a different manifest type and JOB ID if an AW1F
        if "FAILURE" in data["file_path"]:
            job = GenomicJob.AW1F_MANIFEST
            manifest_type = GenomicManifestTypes.AW1F
            create_fb = False

        # Set up file/JSON
        task_data = {
            "job": job,
            "bucket": data["bucket_name"],
            "file_data": {
                "create_feedback_record": create_fb,
                "upload_date": data["upload_date"],
                "manifest_type": manifest_type,
                "file_path": data["file_path"],
            }
        }

        # Call pipeline function
        genomic_pipeline.execute_genomic_manifest_file_pipeline(task_data)

        logging.info('Complete.')
        return '{"success": "true"}'


class IngestAW2ManifestTaskApi(Resource):
    """
    Cloud Task endpoint: Ingest AW2 Manifest.
    """
    @task_auth_required
    def post(self):
        log_task_headers()
        data = request.get_json(force=True)
        logging.info(f'Ingesting AW2 File: {data.get("filename")}')

        # Set up file/JSON
        task_data = {
            "job": GenomicJob.METRICS_INGESTION,
            "bucket": data["bucket_name"],
            "file_data": {
                "create_feedback_record": False,
                "upload_date": data["upload_date"],
                "manifest_type": GenomicManifestTypes.GC_DRC,
                "file_path": data["file_path"],
            }
        }

        # Call pipeline function
        genomic_pipeline.execute_genomic_manifest_file_pipeline(task_data)

        logging.info('Complete.')
        return '{"success": "true"}'


class IngestAW5ManifestTaskApi(Resource):
    """
    Cloud Task endpoint: Ingest AW5 Manifest.
    """
    @task_auth_required
    def post(self):
        log_task_headers()
        data = request.get_json(force=True)
        logging.info(f'Ingesting AW5 File: {data.get("filename")}')

        if getSetting(GENOMIC_AW5_WGS_SUBFOLDERS) in data["file_path"]:
            job_id = GenomicJob.AW5_WGS_MANIFEST
            manifest_type = GenomicManifestTypes.AW5_WGS
        elif getSetting(GENOMIC_AW5_ARRAY_SUBFOLDERS) in data["file_path"]:
            job_id = GenomicJob.AW5_ARRAY_MANIFEST
            manifest_type = GenomicManifestTypes.AW5_ARRAY
        else:
            logging.error(f'can not determine manifest type from file_path: {data["file_path"]}')
            return '{"success": "false"}'

        # Set up file/JSON
        task_data = {
            "job": job_id,
            "bucket": data["bucket_name"],
            "file_data": {
                "create_feedback_record": False,
                "upload_date": data["upload_date"],
                "manifest_type": manifest_type,
                "file_path": data["file_path"],
            }
        }

        # Call pipeline function
        genomic_pipeline.execute_genomic_manifest_file_pipeline(task_data)

        logging.info('Complete.')
        return '{"success": "true"}'


class CalculateRecordCountTaskApi(Resource):
    """
    Cloud Task endpoint: Calculates genomic_manifest_file.record_count.
    """
    @task_auth_required
    def post(self):
        log_task_headers()

        # from cloud function
        data = request.get_json(force=True)

        mid = data.get("manifest_file_id")

        logging.info(f'Calculating record count for manifest file ID: {mid}')

        manifest_file_dao = GenomicManifestFileDao()

        manifest_file_obj = manifest_file_dao.get(mid)

        if manifest_file_obj is None:
            raise NotFound(f"Manifest ID {mid} not found.")

        else:
            # Set up task JSON
            task_data = {
                "job": GenomicJob.CALCULATE_RECORD_COUNT_AW1,
                "manifest_file": manifest_file_obj
            }

            task_data = JSONObject(task_data)

            # Call pipeline function
            genomic_pipeline.dispatch_genomic_job_from_task(task_data)

        logging.info('Complete.')
        return '{"success": "true"}'


class CalculateContaminationCategoryApi(Resource):
    """
    Cloud Task endpoint: Calculate contamination category
    """

    def __init__(self):
        self.dao = GenomicSetMemberDao()

    @task_auth_required
    def post(self):
        log_task_headers()
        data = request.get_json(force=True)
        batch = data['member_ids']
        logging.info(f'Calculating Contamination Category for batch of {len(batch)} member IDs')

        # iterate through batch and calculate contamination category
        for _mid in batch:
            self.process_member_id_contamination_category(_mid)

        logging.info(f'Batch of {len(batch)} Complete.')
        return '{"success": "true"}'

    def process_member_id_contamination_category(self, member_id):

        genomic_ingester = GenomicFileIngester(job_id=GenomicJob.RECALCULATE_CONTAMINATION_CATEGORY)

        # Get genomic_set_member and gc metric objects
        with self.dao.session() as s:
            record = s.query(GenomicSetMember, GenomicGCValidationMetrics).filter(
                GenomicSetMember.id == member_id,
                GenomicSetMember.collectionTubeId != None,
                GenomicGCValidationMetrics.genomicSetMemberId == member_id
            ).one_or_none()

            if record is not None:
                # calculate new contamination category
                contamination_category = genomic_ingester.calculate_contamination_category(
                    record.GenomicSetMember.collectionTubeId,
                    float(record.GenomicGCValidationMetrics.contamination),
                    record.GenomicSetMember
                )

                # Update the contamination category
                record.GenomicGCValidationMetrics.contaminationCategory = contamination_category
                s.merge(record.GenomicGCValidationMetrics)

                logging.info(f"Updated contamination category for member id: {member_id}")


class RebuildGenomicTableRecordsApi(Resource):
    """
    Cloud Task endpoint: Rebuild Genomic table records for Resource/BigQuery.
    """
    @task_auth_required
    def post(self):
        log_task_headers()
        data = request.get_json(force=True)
        table = data['table']
        batch = data['ids']

        logging.info(f'Rebuilding {len(batch)} records for table {table}.')

        if table == 'genomic_set':
            bq_genomic_set_batch_update(batch)
            genomic_set_batch_update(batch)
        elif table == 'genomic_set_member':
            bq_genomic_set_member_batch_update(batch)
            genomic_set_member_batch_update(batch)
        elif table == 'genomic_job_run':
            bq_genomic_job_run_batch_update(batch)
            genomic_job_run_batch_update(batch)
        elif table == 'genomic_file_processed':
            bq_genomic_file_processed_batch_update(batch)
            genomic_file_processed_batch_update(batch)
        elif table == 'genomic_gc_validation_metrics':
            bq_genomic_gc_validation_metrics_batch_update(batch)
            genomic_gc_validation_metrics_batch_update(batch)
        elif table == 'genomic_manifest_file':
            bq_genomic_manifest_file_batch_update(batch)
            genomic_manifest_file_batch_update(batch)
        elif table == 'genomic_manifest_feedback':
            bq_genomic_manifest_feedback_batch_update(batch)
            genomic_manifest_feedback_batch_update(batch)

        logging.info(f'Rebuild complete.')

        return '{"success": "true"}'
