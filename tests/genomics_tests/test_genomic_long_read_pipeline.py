# import csv
# import datetime
# import mock
# import os
#
# from dateutil import parser
# from typing import Tuple
#
# from rdr_service import clock, config
# from rdr_service.api_util import open_cloud_file
# from rdr_service.dao.genomics_dao import GenomicSetMemberDao, GenomicFileProcessedDao, GenomicJobRunDao, \
#     GenomicManifestFileDao, GenomicW2SCRawDao, GenomicW3SRRawDao, GenomicW4WRRawDao, \
#     GenomicW3SCRawDao, GenomicResultWorkflowStateDao, GenomicW3NSRawDao, GenomicW5NFRawDao, GenomicW3SSRawDao, \
#     GenomicCVLSecondSampleDao, GenomicW1ILRawDao, GenomicW2WRawDao, GenomicCVLResultPastDueDao
# from rdr_service.genomic_enums import GenomicManifestTypes, GenomicJob, GenomicQcStatus, GenomicSubProcessStatus, \
#     GenomicSubProcessResult, GenomicWorkflowState, ResultsWorkflowState, ResultsModuleType
# from rdr_service.genomic.genomic_job_components import ManifestDefinitionProvider
# from rdr_service.model.config_utils import to_client_biobank_id
# from rdr_service.model.genomics import GenomicGCValidationMetrics, GenomicSetMember
# from rdr_service.model.participant_summary import ParticipantSummary
# from rdr_service.offline import genomic_pipeline, genomic_cvl_pipeline
# from rdr_service.participant_enums import QuestionnaireStatus, WithdrawalStatus
# from tests.genomics_tests.test_genomic_utils import create_ingestion_test_file
# from tests.helpers.unittest_base import BaseTestCase
#
#
# class GenomicLongReadPipelineTest(BaseTestCase):
#     def setUp(self):
#         super().setUp()
#         # self.job_run_dao = GenomicJobRunDao()
#         # self.member_dao = GenomicSetMemberDao()
#         # self.file_processed_dao = GenomicFileProcessedDao()
#         # self.manifest_file_dao = GenomicManifestFileDao()
#         # self.results_workflow_dao = GenomicResultWorkflowStateDao()
#         #
#         # self.gen_set = self.data_generator.create_database_genomic_set(
#         #     genomicSetName=".",
#         #     genomicSetCriteria=".",
#         #     genomicSetVersion=1
#         # )
#
#     def execute_base_lr_ingestion(self, **kwargs):
#         test_date = datetime.datetime(2020, 10, 13, 0, 0, 0, 0)
#         bucket_name = 'test_lr_bucket'
#         subfolder = 'lr_subfolder'
#
#         # wgs members which should be updated
#         for num in range(1, 4):
#             self.data_generator.create_database_genomic_set_member(
#                 genomicSetId=self.gen_set.id,
#                 biobankId=f"{num}",
#                 sampleId=f"100{num}",
#                 genomeType="aou_wgs",
#                 cvlW3srManifestJobRunID=kwargs.get('cvl_w3sr_manifest_job_run_id')
#             )
#
#         test_file_name = create_ingestion_test_file(
#             kwargs.get('test_file'),
#             bucket_name,
#             folder=subfolder,
#             include_timestamp=kwargs.get('include_timestamp', True),
#             include_sub_num=kwargs.get('include_sub_num')
#         )
#
#         task_data = {
#             "job": kwargs.get('job_id'),
#             "bucket": 'test_cvl_bucket',
#             "file_data": {
#                 "create_feedback_record": False,
#                 "upload_date": test_date.isoformat(),
#                 "manifest_type": kwargs.get('manifest_type'),
#                 "file_path": f"{bucket_name}/{subfolder}/{test_file_name}"
#             }
#         }
#
#         # Execute from cloud task
#         genomic_pipeline.execute_genomic_manifest_file_pipeline(task_data)
