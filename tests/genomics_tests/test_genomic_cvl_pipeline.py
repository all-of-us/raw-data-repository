import csv
import datetime
import mock
import os

from rdr_service import clock, config
from rdr_service.api_util import open_cloud_file
from rdr_service.dao.genomics_dao import GenomicSetMemberDao, GenomicFileProcessedDao, GenomicJobRunDao, \
    GenomicManifestFileDao, GenomicW2SCRawDao, GenomicW3SRRawDao, GenomicW4WRRawDao, GenomicCVLAnalysisDao
from rdr_service.genomic_enums import GenomicManifestTypes, GenomicJob, GenomicSubProcessStatus, \
    GenomicSubProcessResult, ResultsWorkflowState
from rdr_service.genomic.genomic_job_components import ManifestDefinitionProvider
from rdr_service.offline import genomic_pipeline
from rdr_service.participant_enums import QuestionnaireStatus
from tests.genomics_tests.test_genomic_pipeline import create_ingestion_test_file
from tests.helpers.unittest_base import BaseTestCase


class GenomicCVLPipelineTest(BaseTestCase):
    def setUp(self):
        super(GenomicCVLPipelineTest, self).setUp()
        self.job_run_dao = GenomicJobRunDao()
        self.member_dao = GenomicSetMemberDao()
        self.file_processed_dao = GenomicFileProcessedDao()
        self.manifest_file_dao = GenomicManifestFileDao()
        self.gen_set = self.data_generator.create_database_genomic_set(
            genomicSetName=".",
            genomicSetCriteria=".",
            genomicSetVersion=1
        )

    def execute_base_cvl_ingestion(self, **kwargs):
        test_date = datetime.datetime(2020, 10, 13, 0, 0, 0, 0)
        bucket_name = 'test_cvl_bucket'
        subfolder = 'cvl_subfolder'

        # wgs members which should be updated
        for num in range(1, 4):
            self.data_generator.create_database_genomic_set_member(
                genomicSetId=self.gen_set.id,
                biobankId=f"{num}",
                sampleId=f"100{num}",
                genomeType="aou_wgs",
                resultsWorkflowState=kwargs.get('current_results_workflow_state')
            )

        test_file_name = create_ingestion_test_file(
            kwargs.get('test_file'),
            bucket_name,
            folder=subfolder
        )

        task_data = {
            "job": kwargs.get('job_id'),
            "bucket": 'test_cvl_bucket',
            "file_data": {
                "create_feedback_record": False,
                "upload_date": test_date.isoformat(),
                "manifest_type": kwargs.get('manifest_type'),
                "file_path": f"{bucket_name}/{subfolder}/{test_file_name}"
            }
        }

        # Execute from cloud task
        genomic_pipeline.execute_genomic_manifest_file_pipeline(task_data)

    def test_w2sc_manifest_ingestion(self):

        self.execute_base_cvl_ingestion(
            test_file='RDR_AoU_CVL_W2SC.csv',
            job_id=GenomicJob.CVL_W2SC_WORKFLOW,
            manifest_type=GenomicManifestTypes.CVL_W2SC,
            current_results_workflow_state=ResultsWorkflowState.CVL_W1IL
        )

        current_members = self.member_dao.get_all()
        self.assertEqual(len(current_members), 3)

        w2sc_job_run = list(filter(lambda x: x.jobId == GenomicJob.CVL_W2SC_WORKFLOW, self.job_run_dao.get_all()))[0]

        self.assertIsNotNone(w2sc_job_run)
        self.assertEqual(w2sc_job_run.runStatus, GenomicSubProcessStatus.COMPLETED)
        self.assertEqual(w2sc_job_run.runResult, GenomicSubProcessResult.SUCCESS)

        self.assertTrue(len(self.file_processed_dao.get_all()), 1)
        w2sc_file_processed = self.file_processed_dao.get(1)
        self.assertTrue(w2sc_file_processed.runId, w2sc_job_run.jobId)

        self.assertTrue(all(obj.cvlW2scManifestJobRunID is not None for obj in current_members))
        self.assertTrue(all(obj.cvlW2scManifestJobRunID == w2sc_job_run.id for obj in current_members))

        self.assertTrue(all(obj.resultsWorkflowState is not None for obj in current_members))
        self.assertTrue(all(obj.resultsWorkflowStateStr is not None for obj in current_members))
        self.assertTrue(all(obj.resultsWorkflowStateModifiedTime is not None for obj in current_members))

        self.assertTrue(all(obj.resultsWorkflowState == ResultsWorkflowState.CVL_W2SC for obj in current_members))
        self.assertTrue(all(obj.resultsWorkflowStateStr == ResultsWorkflowState.CVL_W2SC.name for obj in
                            current_members))

    def test_w2sc_manifest_to_raw_ingestion(self):

        self.execute_base_cvl_ingestion(
            test_file='RDR_AoU_CVL_W2SC.csv',
            job_id=GenomicJob.CVL_W2SC_WORKFLOW,
            manifest_type=GenomicManifestTypes.CVL_W2SC,
            results_workflow_state=ResultsWorkflowState.CVL_W1IL
        )

        w2sc_raw_dao = GenomicW2SCRawDao()

        manifest_type = 'w2sc'
        w2sc_manifest_file = self.manifest_file_dao.get(1)

        genomic_pipeline.load_awn_manifest_into_raw_table(
            w2sc_manifest_file.filePath,
            manifest_type
        )

        w2sr_raw_records = w2sc_raw_dao.get_all()

        self.assertEqual(len(w2sr_raw_records), 3)
        self.assertTrue(all(obj.file_path is not None for obj in w2sr_raw_records))
        self.assertTrue(all(obj.biobank_id is not None for obj in w2sr_raw_records))
        self.assertTrue(all(obj.sample_id is not None for obj in w2sr_raw_records))

    @mock.patch('rdr_service.genomic.genomic_job_controller.GenomicJobController.execute_cloud_task')
    def test_w3sr_manifest_generation(self, cloud_task):
        cvl_w2sc_gen_job_run = self.data_generator.create_database_genomic_job_run(
            jobId=GenomicJob.AW1_MANIFEST,
            startTime=clock.CLOCK.now(),
            endTime=clock.CLOCK.now(),
            runResult=GenomicSubProcessResult.SUCCESS
        )

        for num in range(1, 4):
            summary = self.data_generator.create_database_participant_summary(
                consentForGenomicsROR=QuestionnaireStatus.SUBMITTED,
                consentForStudyEnrollment=QuestionnaireStatus.SUBMITTED
            )
            self.data_generator.create_database_genomic_set_member(
                genomicSetId=self.gen_set.id,
                biobankId=summary.biobankId,
                sampleId=f"100{num}",
                gcManifestParentSampleId=f"200{num}",
                collectionTubeId=f"300{num}",
                sexAtBirth='M',
                ai_an='N',
                nyFlag=0,
                genomeType="aou_wgs",
                resultsWorkflowState=ResultsWorkflowState.CVL_W2SC,
                participantId=summary.participantId,
                cvlW2scManifestJobRunID=cvl_w2sc_gen_job_run.id
            )

        gc_site_ids = ['bi', 'uw', 'bcm']
        current_members = self.member_dao.get_all()

        # one member per gc created
        self.assertEqual(len(current_members), len(gc_site_ids))

        for num, site_id in enumerate(gc_site_ids, start=1):
            member = self.member_dao.get(num)
            member.gcSiteId = site_id
            self.member_dao.update(member)

        fake_date = datetime.datetime(2020, 8, 3, 0, 0, 0, 0)

        # main workflow
        with clock.FakeClock(fake_date):
            genomic_pipeline.cvl_w3sr_manifest_workflow()

        # check members have results workflow state updated correctly
        current_members = self.member_dao.get_all()
        self.assertTrue(all(obj.resultsWorkflowState == ResultsWorkflowState.CVL_W3SR for obj in current_members))
        self.assertTrue(all(obj.resultsWorkflowStateStr == ResultsWorkflowState.CVL_W3SR.name for obj in
                            current_members))
        self.assertTrue(all(obj.resultsWorkflowStateModifiedTime is not None for obj in current_members))

        bucket_name = config.getSetting(config.BIOBANK_SAMPLES_BUCKET_NAME)
        sub_folder = config.CVL_W3SR_MANIFEST_SUBFOLDER
        w3sr_fake_time = fake_date.strftime("%Y-%m-%d-%H-%M-%S")

        cvl_sites = config.GENOMIC_CVL_SITES

        # check genomic manifests records created
        w3sr_manifests = self.manifest_file_dao.get_all()
        self.assertEqual(len(cvl_sites), len(w3sr_manifests))
        self.assertTrue(all(obj.recordCount == 1 for obj in w3sr_manifests))
        self.assertTrue(all(obj.manifestTypeId == GenomicManifestTypes.CVL_W3SR for obj in w3sr_manifests))
        self.assertTrue(all(obj.manifestTypeIdStr == GenomicManifestTypes.CVL_W3SR.name for obj in w3sr_manifests))

        manifest_def_provider = ManifestDefinitionProvider(kwargs={})
        columns_expected = manifest_def_provider.manifest_columns_config[GenomicManifestTypes.CVL_W3SR]

        physical_manifest_count = 0
        for cvl_site in cvl_sites:
            with open_cloud_file(
                os.path.normpath(
                    f'{bucket_name}/{sub_folder}/{cvl_site.upper()}_AoU_CVL_W3SR_{w3sr_fake_time}.csv'
                )
            ) as csv_file:
                physical_manifest_count += 1
                csv_reader = csv.DictReader(csv_file)
                csv_rows = list(csv_reader)
                self.assertEqual(len(csv_rows), 1)

                # check for all columns
                manifest_columns = csv_reader.fieldnames
                self.assertTrue(list(columns_expected) == manifest_columns)

                for row in csv_rows:
                    self.assertIsNotNone(row['biobank_id'])
                    self.assertIsNotNone(row['sample_id'])
                    self.assertIsNotNone(row['parent_sample_id'])
                    self.assertIsNotNone(row['collection_tubeid'])
                    self.assertEqual(row['sex_at_birth'], 'M')
                    self.assertEqual(row['ny_flag'], 'N')
                    self.assertEqual(row['genome_type'], 'aou_cvl')
                    self.assertEqual(row['site_name'], cvl_site)
                    self.assertEqual(row['ai_an'], 'N')

                    # check color picked up bi set member
                    if cvl_site == 'co':
                        bi_member = list(filter(lambda x: x.gcSiteId == 'bi', current_members))[0]
                        self.assertIsNotNone(bi_member)

                        self.assertEqual(row['biobank_id'], bi_member.biobankId)
                        self.assertEqual(row['site_name'], 'co')

        # check num of manifests generated cp to bucket
        self.assertEqual(len(cvl_sites), physical_manifest_count)

        # check genomic file processed records created
        w3sr_files_processed = self.file_processed_dao.get_all()
        self.assertEqual(len(cvl_sites), len(w3sr_files_processed))

        # check job run record
        w3sr_job_runs = list(filter(lambda x: x.jobId == GenomicJob.CVL_W3SR_WORKFLOW, self.job_run_dao.get_all()))

        self.assertIsNotNone(w3sr_job_runs)
        self.assertEqual(len(cvl_sites), len(w3sr_job_runs))
        self.assertTrue(all(obj.runStatus == GenomicSubProcessStatus.COMPLETED for obj in w3sr_job_runs))
        self.assertTrue(all(obj.runResult == GenomicSubProcessResult.SUCCESS for obj in w3sr_job_runs))

        # check cloud tasks called for updating job run id on member
        self.assertTrue(cloud_task.called)
        self.assertEqual(cloud_task.call_count, len(current_members))

        call_args = cloud_task.call_args_list
        for num, call_arg in enumerate(call_args):
            base_arg = call_arg.args[0]
            member_ids = base_arg['member_ids']
            updated_field = base_arg['field']
            updated_value = base_arg['value']

            self.assertTrue(type(member_ids) is list)
            self.assertTrue(updated_value == w3sr_job_runs[num].id)
            self.assertEqual(len(member_ids), 1)
            self.assertTrue(hasattr(self.member_dao.get(num+1), updated_field))
            self.assertEqual(updated_field, 'cvlW3srManifestJobRunID')

        # check raw records
        w3sr_raw_dao = GenomicW3SRRawDao()

        w3sr_raw_records = w3sr_raw_dao.get_all()
        self.assertEqual(len(cvl_sites), len(w3sr_raw_records))
        self.assertTrue(all(obj.file_path is not None for obj in w3sr_raw_records))
        self.assertTrue(all(obj.biobank_id is not None for obj in w3sr_raw_records))
        self.assertTrue(all(obj.sample_id is not None for obj in w3sr_raw_records))
        self.assertTrue(all(obj.parent_sample_id is not None for obj in w3sr_raw_records))
        self.assertTrue(all(obj.collection_tubeid is not None for obj in w3sr_raw_records))
        self.assertTrue(all(obj.sex_at_birth == 'M' for obj in w3sr_raw_records))
        self.assertTrue(all(obj.ny_flag == 'N' for obj in w3sr_raw_records))
        self.assertTrue(all(obj.genome_type == 'aou_cvl' for obj in w3sr_raw_records))
        self.assertTrue(all(obj.site_name in cvl_sites for obj in w3sr_raw_records))
        self.assertTrue(all(obj.ai_an == 'N' for obj in w3sr_raw_records))

        w3sr_raw_job_runs = list(filter(lambda x: x.jobId == GenomicJob.LOAD_CVL_W3SR_TO_RAW_TABLE, self.job_run_dao.get_all()))

        self.assertIsNotNone(w3sr_raw_job_runs)
        self.assertEqual(len(cvl_sites), len(w3sr_raw_job_runs))
        self.assertTrue(all(obj.runStatus == GenomicSubProcessStatus.COMPLETED for obj in w3sr_raw_job_runs))
        self.assertTrue(all(obj.runResult == GenomicSubProcessResult.SUCCESS for obj in w3sr_raw_job_runs))

    def test_w4wr_manifest_ingestion(self):

        self.execute_base_cvl_ingestion(
            test_file='RDR_AoU_CVL_W4WR_HDRV1.csv',
            job_id=GenomicJob.CVL_W4WR_WORKFLOW,
            manifest_type=GenomicManifestTypes.CVL_W4WR,
            current_results_workflow_state=ResultsWorkflowState.CVL_W1IL
        )

        current_members = self.member_dao.get_all()
        self.assertEqual(len(current_members), 3)

        w4wr_job_run = list(filter(lambda x: x.jobId == GenomicJob.CVL_W4WR_WORKFLOW, self.job_run_dao.get_all()))[0]

        self.assertIsNotNone(w4wr_job_run)
        self.assertEqual(w4wr_job_run.runStatus, GenomicSubProcessStatus.COMPLETED)
        self.assertEqual(w4wr_job_run.runResult, GenomicSubProcessResult.SUCCESS)

        self.assertTrue(len(self.file_processed_dao.get_all()), 1)
        w4wr_file_processed = self.file_processed_dao.get(1)
        self.assertTrue(w4wr_file_processed.runId, w4wr_job_run.jobId)

        self.assertTrue(all(obj.cvlW4wrManifestJobRunID is not None for obj in current_members))
        self.assertTrue(all(obj.cvlW4wrManifestJobRunID == w4wr_job_run.id for obj in current_members))

        self.assertTrue(all(obj.resultsWorkflowState is not None for obj in current_members))
        self.assertTrue(all(obj.resultsWorkflowStateStr is not None for obj in current_members))
        self.assertTrue(all(obj.resultsWorkflowStateModifiedTime is not None for obj in current_members))

        self.assertTrue(all(obj.resultsWorkflowState == ResultsWorkflowState.CVL_W4WR for obj in current_members))
        self.assertTrue(all(obj.resultsWorkflowStateStr == ResultsWorkflowState.CVL_W4WR.name for obj in
                            current_members))

        # check cvl analysis records
        cvl_analysis_dao = GenomicCVLAnalysisDao()
        current_analysis_results = cvl_analysis_dao.get_all()
        member_ids = [obj.id for obj in current_members]

        self.assertEqual(len(current_analysis_results), len(current_members))
        self.assertTrue(all(obj.genomic_set_member_id in member_ids for obj in current_analysis_results))
        self.assertTrue(all(obj.clinical_analysis_type is not None for obj in current_analysis_results))
        self.assertTrue(all(obj.health_related_data_file_name is not None for obj in current_analysis_results))

        self.assertTrue(all(obj.clinical_analysis_type == 'HDRV1' for obj in current_analysis_results))
        self.assertTrue(all('HDRV1' in obj.health_related_data_file_name for obj in current_analysis_results))

    def test_w4wr_manifest_to_raw_ingestion(self):

        self.execute_base_cvl_ingestion(
            test_file='RDR_AoU_CVL_W4WR_HDRV1.csv',
            job_id=GenomicJob.CVL_W4WR_WORKFLOW,
            manifest_type=GenomicManifestTypes.CVL_W4WR
        )

        w4wr_raw_dao = GenomicW4WRRawDao()

        manifest_type = 'w4wr'
        w4wr_manifest_file = self.manifest_file_dao.get(1)

        genomic_pipeline.load_awn_manifest_into_raw_table(
            w4wr_manifest_file.filePath,
            manifest_type
        )

        w4wr_raw_records = w4wr_raw_dao.get_all()

        self.assertEqual(len(w4wr_raw_records), 3)
        self.assertTrue(all(obj.file_path is not None for obj in w4wr_raw_records))
        self.assertTrue(all(obj.biobank_id is not None for obj in w4wr_raw_records))
        self.assertTrue(all(obj.sample_id is not None for obj in w4wr_raw_records))
        self.assertTrue(all(obj.health_related_data_file_name is not None for obj in w4wr_raw_records))
        self.assertTrue(all(obj.clinical_analysis_type is not None for obj in w4wr_raw_records))
