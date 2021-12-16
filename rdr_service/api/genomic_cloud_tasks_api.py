import logging

from flask import request
from flask_restful import Resource
from werkzeug.exceptions import NotFound

from rdr_service.api.cloud_tasks_api import log_task_headers
from rdr_service.app_util import task_auth_required
from rdr_service.config import getSetting, getSettingJson, DRC_BROAD_AW4_SUBFOLDERS, GENOMIC_AW5_WGS_SUBFOLDERS, \
    GENOMIC_AW5_ARRAY_SUBFOLDERS, GENOMIC_INGESTIONS
from rdr_service.dao.bq_genomics_dao import bq_genomic_set_batch_update, bq_genomic_set_member_batch_update, \
    bq_genomic_job_run_batch_update, bq_genomic_file_processed_batch_update, \
    bq_genomic_gc_validation_metrics_batch_update, bq_genomic_manifest_file_batch_update, \
    bq_genomic_manifest_feedback_batch_update
from rdr_service.dao.genomics_dao import GenomicManifestFileDao, GenomicCloudRequestsDao, GenomicSetMemberDao
from rdr_service.genomic.genomic_job_components import GenomicFileIngester
from rdr_service.genomic.genomic_job_controller import GenomicJobController
from rdr_service.genomic_enums import GenomicJob, GenomicManifestTypes
from rdr_service.model.genomics import GenomicSetMember, GenomicGCValidationMetrics
from rdr_service.offline import genomic_pipeline
from rdr_service.resource.generators.genomics import genomic_set_batch_update, genomic_set_member_batch_update, \
    genomic_job_run_batch_update, genomic_file_processed_batch_update, genomic_gc_validation_metrics_batch_update, \
    genomic_manifest_file_batch_update, genomic_manifest_feedback_batch_update
from rdr_service.services.system_utils import JSONObject


class BaseGenomicTaskApi(Resource):

    def __init__(self):
        self.data = None
        self.cloud_req_dao = GenomicCloudRequestsDao()
        self.member_dao = GenomicSetMemberDao()
        self.file_paths = None
        self.disallowed_jobs = []

    @task_auth_required
    def post(self):
        log_task_headers()
        self.set_disallowed_jobs()
        self.data = request.get_json(force=True)
        self.file_paths = [self.data.get('file_path')] \
            if type(self.data.get('file_path')) is not list \
            else self.data.get('file_path')

    def create_cloud_record(self):
        if self.data.get('cloud_function'):
            insert_obj = self.cloud_req_dao.get_model_obj_from_items(self.data.items())
            self.cloud_req_dao.insert(insert_obj)

    def set_disallowed_jobs(self):

        if 'manifesttaskapi' not in self.__class__.__name__.lower():
            return

        ingestion_config = getSettingJson(GENOMIC_INGESTIONS, {})

        if not ingestion_config:
            return

        ingestion_config_map = {
            'aw1_manifest': GenomicJob.AW1_MANIFEST,
            'aw1f_manifest': GenomicJob.AW1F_MANIFEST,
            'aw2_manifest': GenomicJob.METRICS_INGESTION,
            'aw4_array_manifest': GenomicJob.AW4_ARRAY_WORKFLOW,
            'aw4_wgs_manifest': GenomicJob.AW4_WGS_WORKFLOW,
            'aw5_array_manifest': GenomicJob.AW5_ARRAY_MANIFEST,
            'aw5_wgs_manifest': GenomicJob.AW5_WGS_MANIFEST
        }

        for config_key, job_type in ingestion_config_map.items():
            if ingestion_config.get(config_key) == 0:
                self.disallowed_jobs.append(job_type)

    def execute_manifest_ingestion(self, task_data, _type):
        if not task_data.get('job') in self.disallowed_jobs:
            logging.info(f'Ingesting {_type} File: {self.data.get("filename")}')
            # Call pipeline function
            genomic_pipeline.execute_genomic_manifest_file_pipeline(task_data)
        else:
            logging.warning(f'Cannot run ingestion task. {task_data.get("job")} is currently disabled.')


class LoadRawAWNManifestDataAPI(BaseGenomicTaskApi):
    """
    Cloud Task endpoint: Load raw AW1/AW2 Manifest to
    genomic_aw1_raw or genomic_aw2_raw table
    """
    def post(self):
        super(LoadRawAWNManifestDataAPI, self).post()
        logging.info(f'Loading {self.data.get("file_type").upper()} Raw Data: {self.data.get("filename")}')

        # Call pipeline function
        genomic_pipeline.load_awn_manifest_into_raw_table(self.data.get("file_path"), self.data.get("file_type"))

        self.create_cloud_record()

        logging.info('Complete.')
        return {"success": True}


class IngestAW1ManifestTaskApi(BaseGenomicTaskApi):
    """
    Cloud Task endpoint: Ingest AW1/AW1F Manifest.
    """
    def post(self):
        super(IngestAW1ManifestTaskApi, self).post()

        for file_path in self.file_paths:

            # Set manifest_type and job
            job = GenomicJob.AW1_MANIFEST
            manifest_type = GenomicManifestTypes.AW1
            create_fb = True
            message = 'AW1'

            # Write a different manifest type and JOB ID if an AW1F
            if "failure" in file_path.lower():
                job = GenomicJob.AW1F_MANIFEST
                manifest_type = GenomicManifestTypes.AW1F
                create_fb = False
                message = 'AW1F'

            # Set up file/JSON
            task_data = {
                "job": job,
                "bucket": self.data["bucket_name"],
                "file_data": {
                    "create_feedback_record": create_fb,
                    "upload_date": self.data["upload_date"],
                    "manifest_type": manifest_type,
                    "file_path": file_path,
                }
            }

            logging.info(f'{message} task data: {task_data}')

            self.execute_manifest_ingestion(task_data, message)

        self.create_cloud_record()

        logging.info('Complete.')
        return {"success": True}


class IngestAW2ManifestTaskApi(BaseGenomicTaskApi):
    """
    Cloud Task endpoint: Ingest AW2 Manifest.
    """
    def post(self):
        super(IngestAW2ManifestTaskApi, self).post()

        for file_path in self.file_paths:
            logging.info(f'Ingesting AW2 File: {self.data.get("filename")}')

            # Set up file/JSON
            task_data = {
                "job": GenomicJob.METRICS_INGESTION,
                "bucket": self.data["bucket_name"],
                "file_data": {
                    "create_feedback_record": False,
                    "upload_date": self.data["upload_date"],
                    "manifest_type": GenomicManifestTypes.AW2,
                    "file_path": file_path,
                }
            }

            logging.info(f'AW2 task data: {task_data}')

            self.execute_manifest_ingestion(task_data, 'AW2')

        self.create_cloud_record()

        logging.info('Complete.')
        return {"success": True}


class IngestAW4ManifestTaskApi(BaseGenomicTaskApi):
    """
    Cloud Task endpoint: Ingest AW4 Manifest.
    """
    def post(self):
        super(IngestAW4ManifestTaskApi, self).post()

        for file_path in self.file_paths:
            logging.info(f'Ingesting AW4 File: {self.data.get("filename")}')

            if getSetting(DRC_BROAD_AW4_SUBFOLDERS[0]) in file_path:
                job = GenomicJob.AW4_ARRAY_WORKFLOW
                manifest_type = GenomicManifestTypes.AW4_ARRAY
                subfolder = getSetting(DRC_BROAD_AW4_SUBFOLDERS[0])

            elif getSetting(DRC_BROAD_AW4_SUBFOLDERS[1]) in file_path:
                job = GenomicJob.AW4_WGS_WORKFLOW
                manifest_type = GenomicManifestTypes.AW4_WGS
                subfolder = getSetting(DRC_BROAD_AW4_SUBFOLDERS[1])

            else:
                logging.warning(f'Can not determine manifest type from file_path: {file_path}.')
                return {"success": False}

            # Set up file/JSON
            task_data = {
                "job": job,
                "bucket": self.data["bucket_name"],
                "subfolder": subfolder,
                "file_data": {
                    "create_feedback_record": False,
                    "upload_date": self.data["upload_date"],
                    "manifest_type": manifest_type,
                    "file_path": file_path,
                }
            }

            logging.info(f'AW4 task data: {task_data}')

            self.execute_manifest_ingestion(task_data, 'AW4')

        self.create_cloud_record()

        logging.info('Complete.')
        return {"success": True}


class IngestAW5ManifestTaskApi(BaseGenomicTaskApi):
    """
    Cloud Task endpoint: Ingest AW5 Manifest.
    """
    def post(self):
        super(IngestAW5ManifestTaskApi, self).post()

        for file_path in self.file_paths:
            logging.info(f'Ingesting AW5 File: {self.data.get("filename")}')

            if getSetting(GENOMIC_AW5_ARRAY_SUBFOLDERS) in file_path:
                job = GenomicJob.AW5_ARRAY_MANIFEST
                manifest_type = GenomicManifestTypes.AW5_ARRAY
            elif getSetting(GENOMIC_AW5_WGS_SUBFOLDERS) in file_path:
                job = GenomicJob.AW5_WGS_MANIFEST
                manifest_type = GenomicManifestTypes.AW5_WGS
            else:
                logging.warning(f'Can not determine manifest type from file_path: {file_path}.')
                return {"success": False}

            # Set up file/JSON
            task_data = {
                "job": job,
                "bucket": self.data["bucket_name"],
                "file_data": {
                    "create_feedback_record": False,
                    "upload_date": self.data["upload_date"],
                    "manifest_type": manifest_type,
                    "file_path": file_path,
                }
            }

            logging.info(f'AW5 task data: {task_data}')

            self.execute_manifest_ingestion(task_data, 'AW5')

        self.create_cloud_record()

        logging.info('Complete.')
        return {"success": True}


class IngestSamplesFromRawTaskAPI(BaseGenomicTaskApi):
    """
    Cloud Task endpoint: Ingest samples based on list
    from Genomic RAW tables
    """
    def post(self):
        super(IngestSamplesFromRawTaskAPI, self).post()
        logging.info(f'Ingesting Samples From List')

        gen_enum = GenomicJob.__dict__[self.data['job']]
        with GenomicJobController(gen_enum,
                                  server_config=self.data['server_config']
                                  ) as controller:
            results = controller.ingest_member_ids_from_awn_raw_table(self.data['member_ids'])

        logging.info(f'{results}')

        self.create_cloud_record()

        logging.info('Complete.')
        return {"success": True}


class IngestDataFilesTaskApi(BaseGenomicTaskApi):
    """
    Cloud Task endpoint: Ingest data files from buckets and saves
    records to GenomicGcDataFile
    """
    def post(self):
        super(IngestDataFilesTaskApi, self).post()
        logging.info(f'Ingesting data files: {self.data["file_path"]}')

        with GenomicJobController(GenomicJob.ACCESSION_DATA_FILES,
                                  ) as controller:
            # ingest files into GenomicGcDataFile

            for file_path in self.file_paths:
                controller.accession_data_files(
                    file_path,
                    self.data['bucket_name']
                )

        self.create_cloud_record()

        logging.info('Complete.')
        return {"success": True}


class IngestInformingLoopTaskApi(BaseGenomicTaskApi):
    """
    Cloud Task endpoint: Ingest informing loop decision from
    Message Broker Event Data
    """
    def post(self):
        super(IngestInformingLoopTaskApi, self).post()

        logging.info('Ingesting informing loop.')

        with GenomicJobController(GenomicJob.INGEST_INFORMING_LOOP,
                                  ) as controller:
            controller.ingest_informing_loop_records(
                message_record_id=self.data['message_record_id'],
                loop_type=self.data['event_type']
            )

        self.create_cloud_record()

        logging.info('Complete.')
        return {"success": True}


class CalculateRecordCountTaskApi(BaseGenomicTaskApi):
    """
    Cloud Task endpoint: Calculates genomic_manifest_file.record_count.
    """
    def post(self):
        super(CalculateRecordCountTaskApi, self).post()

        mid = self.data.get("manifest_file_id")
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

            logging.info(f'Calculate Record Count task data: {task_data}')

            # Call pipeline function
            genomic_pipeline.dispatch_genomic_job_from_task(JSONObject(task_data))

        self.create_cloud_record()

        logging.info('Complete.')
        return {"success": True}


class CalculateContaminationCategoryApi(BaseGenomicTaskApi):
    """
    Cloud Task endpoint: Calculate contamination category
    """
    def __init__(self):
        super(CalculateContaminationCategoryApi, self).__init__()

    def post(self):
        super(CalculateContaminationCategoryApi, self).post()
        batch = self.data['member_ids']
        logging.info(f'Calculating Contamination Category for batch of {len(batch)} member IDs')

        # iterate through batch and calculate contamination category
        for _mid in batch:
            self.process_member_id_contamination_category(_mid)

        logging.info(f'Batch of {len(batch)} Complete.')

        self.create_cloud_record()

        logging.info('Complete.')
        return {"success": True}

    def process_member_id_contamination_category(self, member_id):

        genomic_ingester = GenomicFileIngester(job_id=GenomicJob.RECALCULATE_CONTAMINATION_CATEGORY)

        # Get genomic_set_member and gc metric objects
        with self.member_dao.session() as s:
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


class RebuildGenomicTableRecordsApi(BaseGenomicTaskApi):
    """
    Cloud Task endpoint: Rebuild Genomic table records for Resource/BigQuery.
    """
    def post(self):
        super(RebuildGenomicTableRecordsApi, self).post()

        table = self.data['table']
        batch = self.data['ids']

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

        logging.info('Rebuild complete.')

        self.create_cloud_record()

        logging.info('Complete.')
        return {"success": True}


class GenomicSetMemberUpdateApi(BaseGenomicTaskApi):
    """
    Cloud Task endpoint: Update GenomicSetMember field with job run id
    """
    def post(self):
        super(GenomicSetMemberUpdateApi, self).post()
        member_ids = self.data.get('member_ids')
        field = self.data.get('field')
        value = self.data.get('value')
        is_job_run = self.data.get('is_job_run')
        project_id = self.data.get('project_id')

        if not member_ids:
            logging.warning('List of member ids are required.')
            return {"success": False}

        if not field or not value:
            logging.warning('Combination of field/value is required.')
            return {"success": False}

        self.member_dao.batch_update_member_field(
            member_ids,
            field,
            value,
            is_job_run,
            project_id
        )

        logging.info('Complete.')
        return {"success": True}

