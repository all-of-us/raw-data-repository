import logging

from flask import request
from flask_restful import Resource
from werkzeug.exceptions import NotFound

from rdr_service.cloud_utils.gcp_google_pubsub import publish_pdr_pubsub
from rdr_service.api.cloud_tasks_api import log_task_headers
from rdr_service.app_util import task_auth_required
from rdr_service.config import getSetting, getSettingJson, DRC_BROAD_AW4_SUBFOLDERS, GENOMIC_AW5_WGS_SUBFOLDERS, \
    GENOMIC_AW5_ARRAY_SUBFOLDERS, GENOMIC_INGESTIONS
from rdr_service.dao.bq_genomics_dao import bq_genomic_set_batch_update, bq_genomic_set_member_batch_update, \
    bq_genomic_job_run_batch_update, bq_genomic_file_processed_batch_update, \
    bq_genomic_gc_validation_metrics_batch_update, bq_genomic_manifest_file_batch_update, \
    bq_genomic_manifest_feedback_batch_update
from rdr_service.dao.genomics_dao import GenomicManifestFileDao, GenomicCloudRequestsDao, GenomicSetMemberDao, \
    GenomicGCValidationMetricsDao
from rdr_service.genomic.genomic_job_components import GenomicFileIngester
from rdr_service.genomic.genomic_job_controller import GenomicJobController
from rdr_service.genomic_enums import GenomicJob, GenomicManifestTypes
from rdr_service.model.genomics import GenomicSetMember, GenomicGCValidationMetrics
from rdr_service.offline import genomic_pipeline
from rdr_service.resource.generators.genomics import genomic_set_batch_update, genomic_set_member_batch_update, \
    genomic_job_run_batch_update, genomic_file_processed_batch_update, genomic_gc_validation_metrics_batch_update, \
    genomic_manifest_file_batch_update, genomic_manifest_feedback_batch_update, \
    genomic_user_event_metrics_batch_update, genomic_informing_loop_batch_update, \
    genomic_cvl_result_past_due_batch_update, genomic_member_report_state_batch_update, \
    genomic_result_viewed_batch_update, genomic_appointment_event_batch_update
from rdr_service.services.system_utils import JSONObject


class BaseGenomicTaskApi(Resource):

    def __init__(self):
        self.data = None
        self.cloud_req_dao = GenomicCloudRequestsDao()
        self.member_dao = GenomicSetMemberDao()
        self.metrics_dao = GenomicGCValidationMetricsDao()
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
                "bucket": self.data.get('bucket_name'),
                "file_data": {
                    "create_feedback_record": create_fb,
                    "upload_date": self.data.get("upload_date"),
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
                "bucket": self.data.get('bucket_name'),
                "file_data": {
                    "create_feedback_record": False,
                    "upload_date": self.data.get("upload_date"),
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


class IngestCVLManifestTaskApi(BaseGenomicTaskApi):
    """
    Cloud Task endpoint: Ingest CVL Manifest.
    """
    def post(self):
        super(IngestCVLManifestTaskApi, self).post()

        cvl_manifest_map = {
            'w2sc': {
                'job': GenomicJob.CVL_W2SC_WORKFLOW,
                'manifest_type': GenomicManifestTypes.CVL_W2SC
            },
            'w3ns': {
                'job': GenomicJob.CVL_W3NS_WORKFLOW,
                'manifest_type': GenomicManifestTypes.CVL_W3NS
            },
            'w3sc': {
                'job': GenomicJob.CVL_W3SC_WORKFLOW,
                'manifest_type': GenomicManifestTypes.CVL_W3SC
            },
            'w3ss': {
                'job': GenomicJob.CVL_W3SS_WORKFLOW,
                'manifest_type': GenomicManifestTypes.CVL_W3SS
            },
            'w4wr': {
                'job': GenomicJob.CVL_W4WR_WORKFLOW,
                'manifest_type': GenomicManifestTypes.CVL_W4WR
            },
            'w5nf': {
                'job': GenomicJob.CVL_W5NF_WORKFLOW,
                'manifest_type': GenomicManifestTypes.CVL_W5NF
            }
        }

        for file_path in self.file_paths:
            logging.info(f'Ingesting CVL Manifest File: {self.data.get("filename")}')

            task_type = self.data.get("file_type")
            workflow_data = cvl_manifest_map[task_type]

            # Set up file/JSON
            task_data = {
                "job": workflow_data.get('job'),
                "bucket": self.data["bucket_name"],
                "file_data": {
                    "create_feedback_record": False,
                    "upload_date": self.data["upload_date"],
                    "manifest_type": workflow_data.get('manifest_type'),
                    "file_path": file_path,
                }
            }

            logging.info(f'{task_type.upper()} task data: {task_data}')

            self.execute_manifest_ingestion(task_data, task_type.upper())

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


class IngestGenomicMessageBrokerDataApi(BaseGenomicTaskApi):
    """
    Cloud Task endpoint: Ingesting Message Broker event data
    informing_loop(s), result_viewed, result_ready
    """

    def post(self):
        super(IngestGenomicMessageBrokerDataApi, self).post()

        if not self.data.get('event_type') or not self.data.get('message_record_id'):
            logging.warning('Event type and message record id is required for ingestion from Message broker')

            return {"success": False}

        event_type = self.data.get('event_type')

        logging.info(f'Ingesting {event_type}')

        ingest_method_map = {
            'informing_loop': GenomicJob.INGEST_INFORMING_LOOP,
            'result_viewed': GenomicJob.INGEST_RESULT_VIEWED,
            'result_ready': GenomicJob.INGEST_RESULT_READY
        }

        job_type = ingest_method_map[
            list(filter(lambda x: x in event_type, ingest_method_map.keys()))[0]
        ]

        with GenomicJobController(job_type) as controller:
            controller.ingest_records_from_message_broker_data(
                message_record_id=self.data.get('message_record_id'),
                event_type=event_type
            )

        self.create_cloud_record()

        logging.info('Complete.')
        return {"success": True}


class IngestGenomicMessageBrokerAppointmentApi(BaseGenomicTaskApi):
    """
    Cloud Task endpoint: Ingesting Message Broker event data
    appointments only
    """

    def post(self):
        super(IngestGenomicMessageBrokerAppointmentApi, self).post()

        if not self.data.get('message_record_id'):
            logging.warning('Event type and message record id is required for ingestion from Message broker')

            return {"success": False}

        event_type = self.data.get('event_type')

        logging.info(f'Ingesting {event_type}')

        with GenomicJobController(GenomicJob.INGEST_APPOINTMENT) as controller:
            controller.ingest_records_from_message_broker_data(
                message_record_id=self.data.get('message_record_id'),
                event_type=event_type
            )

        self.create_cloud_record()

        logging.info('Complete.')
        return {"success": True}


class IngestUserEventMetricsApi(BaseGenomicTaskApi):
    """
    Cloud task endpoint: Inserting records for GHR3 User event metrics
    """
    def post(self):
        super(IngestUserEventMetricsApi, self).post()

        if not self.data.get('file_path'):
            logging.warning('Can not run user metrics ingestion for missing file path')
            return {"success": False}

        logging.info(f"Ingesting user event metrics for {self.data.get('file_path')}")

        with GenomicJobController(GenomicJob.METRICS_FILE_INGEST,
                                  ) as controller:
            controller.ingest_metrics_file(
                metric_type='user_events',
                file_path=self.data['file_path']
            )

        self.create_cloud_record()

        logging.info('Complete.')
        return {"success": True}


class IngestAppointmentMetricsApi(BaseGenomicTaskApi):
    """
    Cloud task endpoint: Inserting records for GHR3 appointment metrics
    """
    def post(self):
        super(IngestAppointmentMetricsApi, self).post()

        if not self.data.get('file_path'):
            logging.warning('Can not run appointment ingestion for missing file path')
            return {"success": False}

        logging.info(f"Ingesting appointment metrics for {self.data.get('file_path')}")

        with GenomicJobController(GenomicJob.APPOINTMENT_METRICS_FILE_INGEST,
                                  ) as controller:
            controller.ingest_appointment_metrics_file(
                file_path=self.data['file_path']
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

        table = self.data.get('table')
        batch = self.data.get('ids')

        if not table or not batch:
            logging.warning('Table and batch are both required in rebuild genomics payload')
            return {"success": False}

        logging.info(f'Rebuilding {len(batch)} records for table {table}.')

        rebuild_map = {
            'genomic_set': [
                bq_genomic_set_batch_update,
                genomic_set_batch_update
            ],
            'genomic_set_member': [
                bq_genomic_set_member_batch_update,
                genomic_set_member_batch_update
            ],
            'genomic_job_run': [
                bq_genomic_job_run_batch_update,
                genomic_job_run_batch_update
            ],
            'genomic_file_processed': [
                bq_genomic_file_processed_batch_update,
                genomic_file_processed_batch_update
            ],
            'genomic_gc_validation_metrics': [
                bq_genomic_gc_validation_metrics_batch_update,
                genomic_gc_validation_metrics_batch_update
            ],
            'genomic_informing_loop': [
                genomic_informing_loop_batch_update
            ],
            'genomic_manifest_file': [
                bq_genomic_manifest_file_batch_update,
                genomic_manifest_file_batch_update
            ],
            'genomic_manifest_feedback': [
                bq_genomic_manifest_feedback_batch_update,
                genomic_manifest_feedback_batch_update
            ],
            'user_event_metrics': [
                genomic_user_event_metrics_batch_update
            ],
            'genomic_cvl_result_past_due': [
                genomic_cvl_result_past_due_batch_update
            ],
            'genomic_member_report_state': [
                genomic_member_report_state_batch_update
            ],
            'genomic_result_viewed': [
                genomic_result_viewed_batch_update
            ],
            'genomic_appointment_event': [
                genomic_appointment_event_batch_update
            ]
        }

        try:
            for method in rebuild_map[table]:
                method(batch)
            logging.info('Rebuild complete.')

            # Publish PDR data-pipeline pub-sub event.
            publish_pdr_pubsub(table, action='upsert', pk_columns=['id'], pk_values=batch)
            logging.info('PubSub notification sent.')

            self.create_cloud_record()
            logging.info('Complete.')

            return {"success": True}

        except KeyError:
            logging.warning(f'Table {table} is invalid for genomic rebuild task')

            return {"success": False}


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
            is_job_run
        )

        logging.info('Complete.')
        return {"success": True}


class GenomicGCMetricsUpsertApi(BaseGenomicTaskApi):
    """
    Cloud task endpoint: Upserts Genomic GC validation metric records
    """
    def post(self):
        super().post()
        metric_id = self.data.get('metric_id')
        payload_dict = self.data.get('payload_dict')

        if not metric_id or not payload_dict:
            logging.warning('Combination of metric_id/payload_dict is required.')
            return {"success": False}

        self.metrics_dao.upsert_gc_validation_metrics_from_dict(
            data_to_upsert=payload_dict,
            existing_id=metric_id
        )

        logging.info('Complete.')
        return {"success": True}
