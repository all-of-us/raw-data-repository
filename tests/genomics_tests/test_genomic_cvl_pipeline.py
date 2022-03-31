import csv
import datetime
import mock
import os
from typing import Tuple

from rdr_service import clock, config
from rdr_service.api_util import open_cloud_file
from rdr_service.dao.genomics_dao import GenomicSetMemberDao, GenomicFileProcessedDao, GenomicJobRunDao, \
    GenomicManifestFileDao, GenomicW2SCRawDao, GenomicW3SRRawDao, GenomicW4WRRawDao, GenomicCVLAnalysisDao, \
    GenomicW3SCRawDao, GenomicResultWorkflowStateDao, GenomicW3NSRawDao, GenomicW5NFRawDao
from rdr_service.genomic_enums import GenomicManifestTypes, GenomicJob, GenomicQcStatus, GenomicSubProcessStatus, \
    GenomicSubProcessResult, GenomicWorkflowState, ResultsWorkflowState, ResultsModuleType
from rdr_service.genomic.genomic_job_components import ManifestDefinitionProvider
from rdr_service.model.config_utils import to_client_biobank_id
from rdr_service.model.genomics import GenomicGCValidationMetrics, GenomicSetMember
from rdr_service.model.participant_summary import ParticipantSummary
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
        self.results_workflow_dao = GenomicResultWorkflowStateDao()

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
            member = self.data_generator.create_database_genomic_set_member(
                genomicSetId=self.gen_set.id,
                biobankId=f"{num}",
                sampleId=f"100{num}",
                genomeType="aou_wgs"
            )

            self.data_generator.create_database_genomic_result_workflow_state(
                genomic_set_member_id=member.id,
                results_workflow_state=kwargs.get('current_results_workflow_state'),
                results_module=kwargs.get('results_module')
            )

            if kwargs.get('set_cvl_analysis_records'):
                self.data_generator.create_database_genomic_cvl_analysis(
                    genomic_set_member_id=member.id,
                    clinical_analysis_type=kwargs.get('results_module'),
                    health_related_data_file_name=f'HDR_{num}_test_data_file'
                )

        test_file_name = create_ingestion_test_file(
            kwargs.get('test_file'),
            bucket_name,
            folder=subfolder,
            include_sub_num=kwargs.get('include_sub_num')
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
            current_results_workflow_state=ResultsWorkflowState.CVL_W1IL,
            results_module=ResultsModuleType.HDRV1
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

        current_workflow_states = self.results_workflow_dao.get_all()
        self.assertEqual(len(current_workflow_states), 3)
        self.assertTrue(all(obj.results_workflow_state is not None for obj in current_workflow_states))
        self.assertTrue(all(obj.results_workflow_state_str is not None for obj in current_workflow_states))

        self.assertTrue(all(obj.results_workflow_state == ResultsWorkflowState.CVL_W2SC for
                            obj in current_workflow_states))
        self.assertTrue(all(obj.results_workflow_state_str == ResultsWorkflowState.CVL_W2SC.name for obj in
                            current_workflow_states))


    def test_w2sc_manifest_to_raw_ingestion(self):

        self.execute_base_cvl_ingestion(
            test_file='RDR_AoU_CVL_W2SC.csv',
            job_id=GenomicJob.CVL_W2SC_WORKFLOW,
            manifest_type=GenomicManifestTypes.CVL_W2SC,
            results_workflow_state=ResultsWorkflowState.CVL_W1IL,
            results_module=ResultsModuleType.HDRV1
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

    @mock.patch('rdr_service.genomic.genomic_job_components.SqlExporter')
    @mock.patch('rdr_service.genomic.genomic_job_controller.GenomicJobController.execute_cloud_task')
    def test_w1il_manifest_generation(self, _, sql_exporter_class_mock):
        # Generate some set members that would go on a W1IL for BCM
        default_summary, default_set_member, default_validation_metrics = self._generate_cvl_participant(
            set_member_params={'gcSiteId': 'bcm'}
        )
        ny_summary, ny_set_member, ny_validation_metrics = self._generate_cvl_participant(
            set_member_params={'gcSiteId': 'bcm', 'nyFlag': 1})
        male_summary, male_set_member, male_validation_metrics = self._generate_cvl_participant(
            set_member_params={'gcSiteId': 'bcm', 'sexAtBirth': 'M'}
        )
        hdr_and_pgx_summary, hdr_and_pgx_set_member, hdr_and_pgx_validation_metrics = self._generate_cvl_participant(
            set_member_params={'gcSiteId': 'bcm', 'nyFlag': 1},
            informing_loop_decision_param_list=[
                {},  # default 'yes' for PGX
                {'module_type': 'hdr'}
            ]
        )
        co_summary, co_set_member, co_validation_metrics = self._generate_cvl_participant(
            set_member_params={'gcSiteId': 'bi'},
            result_workflow_state={'results_module': ResultsModuleType.HDRV1}
        )

        # Create some records that shouldn't exist in a W1IL for BCM
        self._generate_cvl_participant(
            set_member_params={'gcSiteId': 'bcm'},
            informing_loop_decision_param_list=[{'decision_value': 'no'}]
        )
        self._generate_cvl_participant(set_member_params={'gcSiteId': 'bcm', 'qcStatus': GenomicQcStatus.FAIL})
        self._generate_cvl_participant(
            set_member_params={'gcSiteId': 'bcm'},
            validation_metrics_params={'drcSexConcordance': 'fail'}
        )
        self._generate_cvl_participant(
            set_member_params={'gcSiteId': 'bcm'},
            validation_metrics_params={'craiReceived': 0}
        )
        self._generate_cvl_participant(
            set_member_params={'gcSiteId': 'bcm'},
            participant_summary_params={'consentForGenomicsROR': QuestionnaireStatus.SUBMITTED_NOT_SURE}
        )
        self._generate_cvl_participant(
            set_member_params={'gcSiteId': 'bcm'},
            collection_site_params={'siteType': 'diversion pouch'}
        )
        # Member with a latest answer of 'no' should not be in a W1IL
        self._generate_cvl_participant(
            set_member_params={'gcSiteId': 'bcm'},
            informing_loop_decision_param_list=[
                {
                    'decision_value': 'yes',
                    'event_authored_time': datetime.datetime(2020, 10, 20)
                },
                {
                    'decision_value': 'no',
                    'event_authored_time': datetime.datetime(2020, 11, 17)
                }
            ]
        )
        # Member already in a the CVL workflow shouldn't be in a W1IL
        self._generate_cvl_participant(
            set_member_params={'gcSiteId': 'bcm'},
            result_workflow_state={}  # letting defaults get set for the workflow state
        )

        manifest_generation_datetime = datetime.datetime(2021, 2, 7, 1, 13)
        manifest_file_timestamp_str = manifest_generation_datetime.strftime("%Y-%m-%d-%H-%M-%S")

        co_w1il_manifest = mock.MagicMock()
        bcm_pgx_w1il_manifest = mock.MagicMock()
        bcm_hdr_w1il_manifest = mock.MagicMock()

        def side_effect(file_name):
            if file_name == f'W1IL_manifests/BCM_AoU_CVL_W1IL_PGX_{manifest_file_timestamp_str}.csv':
                return bcm_pgx_w1il_manifest
            elif file_name == f'W1IL_manifests/BCM_AoU_CVL_W1IL_HDR_{manifest_file_timestamp_str}.csv':
                return bcm_hdr_w1il_manifest
            elif file_name == f'W1IL_manifests/CO_AoU_CVL_W1IL_PGX_{manifest_file_timestamp_str}.csv':
                return co_w1il_manifest
            else:
                self.fail(f'Unexpected manifest generated: "{file_name}"')

        exporter_instance = sql_exporter_class_mock.return_value
        exporter_instance.open_cloud_writer.side_effect = side_effect

        with clock.FakeClock(manifest_generation_datetime):
            genomic_pipeline.cvl_w1il_manifest_workflow({
                cvl_site_id: f'{cvl_site_id}_test_bucket'
                for cvl_site_id in config.GENOMIC_CVL_SITES
            })

        self.assert_manifest_has_rows(
            manifest_cloud_writer_mock=bcm_pgx_w1il_manifest,
            expected_rows=[
                self.expected_w1il_row(default_set_member, default_validation_metrics, default_summary),
                self.expected_w1il_row(ny_set_member, ny_validation_metrics, ny_summary),
                self.expected_w1il_row(male_set_member, male_validation_metrics, male_summary),
                self.expected_w1il_row(hdr_and_pgx_set_member, hdr_and_pgx_validation_metrics, hdr_and_pgx_summary)
            ]
        )
        self.assert_manifest_has_rows(
            manifest_cloud_writer_mock=bcm_hdr_w1il_manifest,
            expected_rows=[
                self.expected_w1il_row(hdr_and_pgx_set_member, hdr_and_pgx_validation_metrics, hdr_and_pgx_summary)
            ]
        )
        self.assert_manifest_has_rows(
            manifest_cloud_writer_mock=co_w1il_manifest,
            expected_rows=[
                self.expected_w1il_row(co_set_member, co_validation_metrics, co_summary)
            ]
        )

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

        # TODO check members have results workflow state updated correctly for manifest generation
        current_members = self.member_dao.get_all()

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

                prefix = config.getSetting(config.BIOBANK_ID_PREFIX)

                for row in csv_rows:
                    self.assertIsNotNone(row['biobank_id'])
                    self.assertTrue(prefix in row['biobank_id'])
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

                        self.assertEqual(row['biobank_id'], f'{prefix}{bi_member.biobankId}')
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

    def test_cvl_skip_week_manifest_generation(self):

        from rdr_service.offline.main import app, OFFLINE_PREFIX
        offline_test_client = app.test_client()

        # create initial job run
        initial_job_run = self.data_generator.create_database_genomic_job_run(
            jobId=GenomicJob.CVL_W3SR_WORKFLOW,
            jobIdStr=GenomicJob.CVL_W3SR_WORKFLOW.name,
            startTime=clock.CLOCK.now(),
            endTime=clock.CLOCK.now(),
            runResult=GenomicSubProcessResult.SUCCESS,
            runStatus=GenomicSubProcessStatus.COMPLETED,
        )

        response = self.send_get(
            'GenomicCVLW3SRWorkflow',
            test_client=offline_test_client,
            prefix=OFFLINE_PREFIX,
            headers={'X-Appengine-Cron': True},
            expected_status=500
        )

        self.assertTrue(response.status_code == 500)

        current_job_runs = self.job_run_dao.get_all()
        # remove initial
        current_job_runs = list(filter(lambda x: x.id != initial_job_run.id, current_job_runs))
        self.assertTrue(len(current_job_runs) == 0)

        today_plus_seven = clock.CLOCK.now() + datetime.timedelta(days=7)

        with clock.FakeClock(today_plus_seven):
            response = self.send_get(
                'GenomicCVLW3SRWorkflow',
                test_client=offline_test_client,
                prefix=OFFLINE_PREFIX,
                headers={'X-Appengine-Cron': True},
                expected_status=500
            )

        self.assertTrue(response.status_code == 500)

        current_job_runs = self.job_run_dao.get_all()
        # remove initial
        current_job_runs = list(filter(lambda x: x.id != initial_job_run.id, current_job_runs))
        self.assertTrue(len(current_job_runs) == 0)

        today_plus_fourteen = clock.CLOCK.now() + datetime.timedelta(days=14)

        with clock.FakeClock(today_plus_fourteen):
            response = self.send_get(
                'GenomicCVLW3SRWorkflow',
                test_client=offline_test_client,
                prefix=OFFLINE_PREFIX,
                headers={'X-Appengine-Cron': True}
            )

        self.assertTrue(response['success'] == 'true')

        current_job_runs = self.job_run_dao.get_all()
        # remove initial
        current_job_runs = list(filter(lambda x: x.id != initial_job_run.id, current_job_runs))

        self.assertEqual(len(current_job_runs), len(config.GENOMIC_CVL_SITES))
        self.assertTrue(all(obj.runResult == GenomicSubProcessResult.NO_FILES for obj in current_job_runs))

        today_plus_fourteen_plus_seven = today_plus_fourteen + datetime.timedelta(days=7)

        with clock.FakeClock(today_plus_fourteen_plus_seven):
            response = self.send_get(
                'GenomicCVLW3SRWorkflow',
                test_client=offline_test_client,
                prefix=OFFLINE_PREFIX,
                headers={'X-Appengine-Cron': True},
                expected_status=500
            )

        self.assertTrue(response.status_code == 500)

    def test_w4wr_manifest_ingestion(self):

        self.execute_base_cvl_ingestion(
            test_file='RDR_AoU_CVL_W4WR_HDRV1.csv',
            job_id=GenomicJob.CVL_W4WR_WORKFLOW,
            manifest_type=GenomicManifestTypes.CVL_W4WR,
            current_results_workflow_state=ResultsWorkflowState.CVL_W1IL,
            results_module=ResultsModuleType.HDRV1
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

        self.assertTrue(all(obj.cvlW4wrHdrManifestJobRunID is not None for obj in current_members))
        self.assertTrue(all(obj.cvlW4wrHdrManifestJobRunID == w4wr_job_run.id for obj in current_members))

        current_workflow_states = self.results_workflow_dao.get_all()
        self.assertEqual(len(current_workflow_states), 3)
        self.assertTrue(all(obj.results_workflow_state is not None for obj in current_workflow_states))
        self.assertTrue(all(obj.results_workflow_state_str is not None for obj in current_workflow_states))

        self.assertTrue(all(obj.results_workflow_state == ResultsWorkflowState.CVL_W4WR for
                            obj in current_workflow_states))
        self.assertTrue(all(obj.results_workflow_state_str == ResultsWorkflowState.CVL_W4WR.name for obj in
                            current_workflow_states))

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
            manifest_type=GenomicManifestTypes.CVL_W4WR,
            current_results_workflow_state=ResultsWorkflowState.CVL_W1IL,
            results_module=ResultsModuleType.HDRV1
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

    def test_w5nf_manifest_ingestion(self):

        self.execute_base_cvl_ingestion(
            test_file='RDR_AoU_CVL_W5NF_HDRV1_1.csv',
            job_id=GenomicJob.CVL_W5NF_WORKFLOW,
            manifest_type=GenomicManifestTypes.CVL_W5NF,
            current_results_workflow_state=ResultsWorkflowState.CVL_W4WR,
            results_module=ResultsModuleType.HDRV1,
            set_cvl_analysis_records=True  # need to set initial cvl analysis records from W4WR
        )

        current_members = self.member_dao.get_all()
        self.assertEqual(len(current_members), 3)

        w5nf_job_run = list(filter(lambda x: x.jobId == GenomicJob.CVL_W5NF_WORKFLOW, self.job_run_dao.get_all()))[0]

        self.assertIsNotNone(w5nf_job_run)
        self.assertEqual(w5nf_job_run.runStatus, GenomicSubProcessStatus.COMPLETED)
        self.assertEqual(w5nf_job_run.runResult, GenomicSubProcessResult.SUCCESS)

        self.assertTrue(len(self.file_processed_dao.get_all()), 1)
        w5nf_file_processed = self.file_processed_dao.get(1)
        self.assertTrue(w5nf_file_processed.runId, w5nf_job_run.jobId)

        self.assertTrue(all(obj.cvlW5nfHdrManifestJobRunID is not None for obj in current_members))
        self.assertTrue(all(obj.cvlW5nfHdrManifestJobRunID == w5nf_job_run.id for obj in current_members))

        current_workflow_states = self.results_workflow_dao.get_all()
        self.assertEqual(len(current_workflow_states), 3)
        self.assertTrue(all(obj.results_workflow_state is not None for obj in current_workflow_states))
        self.assertTrue(all(obj.results_workflow_state_str is not None for obj in current_workflow_states))

        self.assertTrue(all(obj.results_workflow_state == ResultsWorkflowState.CVL_W5NF for
                            obj in current_workflow_states))
        self.assertTrue(all(obj.results_workflow_state_str == ResultsWorkflowState.CVL_W5NF.name for obj in
                            current_workflow_states))

        # check cvl analysis records
        cvl_analysis_dao = GenomicCVLAnalysisDao()
        current_analysis_results = cvl_analysis_dao.get_all()

        failed_analysis_records = list(filter(lambda x: x.failed == 1, current_analysis_results))
        new_analysis_records = list(filter(lambda x: x.failed == 0, current_analysis_results))

        member_ids = [obj.id for obj in current_members]

        self.assertEqual(len(current_analysis_results), len(current_members) * 2)
        self.assertTrue(all(obj.clinical_analysis_type is not None for obj in current_analysis_results))
        self.assertTrue(all(obj.health_related_data_file_name is not None for obj in current_analysis_results))
        self.assertTrue(all(obj.clinical_analysis_type == 'HDRV1' for obj in current_analysis_results))

        self.assertTrue(all(obj.genomic_set_member_id in member_ids for obj in failed_analysis_records))
        self.assertTrue(all(obj.failed_request_reason is not None for obj in failed_analysis_records))
        self.assertTrue(all(obj.failed_request_reason_free is not None for obj in failed_analysis_records))

        self.assertTrue(all(obj.genomic_set_member_id in member_ids for obj in new_analysis_records))
        self.assertTrue(all(obj.failed_request_reason is None for obj in new_analysis_records))
        self.assertTrue(all(obj.failed_request_reason_free is None for obj in new_analysis_records))

    def test_w5nf_manifest_to_raw_ingestion(self):

        self.execute_base_cvl_ingestion(
            test_file='RDR_AoU_CVL_W5NF_HDRV1_1.csv',
            job_id=GenomicJob.CVL_W5NF_WORKFLOW,
            manifest_type=GenomicManifestTypes.CVL_W5NF,
            current_results_workflow_state=ResultsWorkflowState.CVL_W1IL,
            results_module=ResultsModuleType.HDRV1
        )

        w5nf_raw_dao = GenomicW5NFRawDao()

        manifest_type = 'w5nf'
        w5nf_manifest_file = self.manifest_file_dao.get(1)

        genomic_pipeline.load_awn_manifest_into_raw_table(
            w5nf_manifest_file.filePath,
            manifest_type
        )

        w5nf_raw_records = w5nf_raw_dao.get_all()

        self.assertEqual(len(w5nf_raw_records), 3)
        self.assertTrue(all(obj.file_path is not None for obj in w5nf_raw_records))
        self.assertTrue(all(obj.biobank_id is not None for obj in w5nf_raw_records))
        self.assertTrue(all(obj.sample_id is not None for obj in w5nf_raw_records))
        self.assertTrue(all(obj.request_reason is not None for obj in w5nf_raw_records))
        self.assertTrue(all(obj.request_reason_free is not None for obj in w5nf_raw_records))
        self.assertTrue(all(obj.health_related_data_file_name is not None for obj in w5nf_raw_records))
        self.assertTrue(all(obj.clinical_analysis_type is not None for obj in w5nf_raw_records))

    def test_w3ns_manifest_ingestion(self):

        self.execute_base_cvl_ingestion(
            test_file='RDR_AoU_CVL_W3NS.csv',
            job_id=GenomicJob.CVL_W3NS_WORKFLOW,
            manifest_type=GenomicManifestTypes.CVL_W3NS,
            current_results_workflow_state=ResultsWorkflowState.CVL_W3SR,
            results_module=ResultsModuleType.HDRV1
        )

        current_members = self.member_dao.get_all()
        self.assertEqual(len(current_members), 3)

        w3ns_job_run = list(filter(lambda x: x.jobId == GenomicJob.CVL_W3NS_WORKFLOW, self.job_run_dao.get_all()))[0]

        self.assertIsNotNone(w3ns_job_run)
        self.assertEqual(w3ns_job_run.runStatus, GenomicSubProcessStatus.COMPLETED)
        self.assertEqual(w3ns_job_run.runResult, GenomicSubProcessResult.SUCCESS)

        self.assertTrue(len(self.file_processed_dao.get_all()), 1)
        w3sc_file_processed = self.file_processed_dao.get(1)
        self.assertTrue(w3sc_file_processed.runId, w3ns_job_run.jobId)

        self.assertTrue(all(obj.cvlW3nsManifestJobRunID is not None for obj in current_members))
        self.assertTrue(all(obj.cvlW3nsManifestJobRunID == w3ns_job_run.id for obj in current_members))

        current_workflow_states = self.results_workflow_dao.get_all()
        self.assertEqual(len(current_workflow_states), 3)
        self.assertTrue(all(obj.results_workflow_state is not None for obj in current_workflow_states))
        self.assertTrue(all(obj.results_workflow_state_str is not None for obj in current_workflow_states))

        self.assertTrue(all(obj.results_workflow_state == ResultsWorkflowState.CVL_W3NS for
                            obj in current_workflow_states))
        self.assertTrue(all(obj.results_workflow_state_str == ResultsWorkflowState.CVL_W3NS.name for obj in
                            current_workflow_states))

    def test_w3ns_manifest_to_raw_ingestion(self):

        self.execute_base_cvl_ingestion(
            test_file='RDR_AoU_CVL_W3NS.csv',
            job_id=GenomicJob.CVL_W3NS_WORKFLOW,
            manifest_type=GenomicManifestTypes.CVL_W3NS,
            current_results_workflow_state=ResultsWorkflowState.CVL_W3SR,
            results_module=ResultsModuleType.HDRV1
        )

        w3ns_raw_dao = GenomicW3NSRawDao()

        manifest_type = 'w3ns'
        w3sc_manifest_file = self.manifest_file_dao.get(1)

        genomic_pipeline.load_awn_manifest_into_raw_table(
            w3sc_manifest_file.filePath,
            manifest_type
        )

        w3ns_raw_records = w3ns_raw_dao.get_all()

        self.assertEqual(len(w3ns_raw_records), 3)
        self.assertTrue(all(obj.file_path is not None for obj in w3ns_raw_records))
        self.assertTrue(all(obj.biobank_id is not None for obj in w3ns_raw_records))
        self.assertTrue(all(obj.sample_id is not None for obj in w3ns_raw_records))
        self.assertTrue(all(obj.unavailable_reason is not None for obj in w3ns_raw_records))

    def test_w3sc_manifest_ingestion(self):

        self.execute_base_cvl_ingestion(
            test_file='RDR_AoU_CVL_W3SC.csv',
            job_id=GenomicJob.CVL_W3SC_WORKFLOW,
            manifest_type=GenomicManifestTypes.CVL_W3SC,
            current_results_workflow_state=ResultsWorkflowState.CVL_W3SR,
            results_module=ResultsModuleType.HDRV1
        )

        current_members = self.member_dao.get_all()
        self.assertEqual(len(current_members), 3)

        w3sc_job_run = list(filter(lambda x: x.jobId == GenomicJob.CVL_W3SC_WORKFLOW, self.job_run_dao.get_all()))[0]

        self.assertIsNotNone(w3sc_job_run)
        self.assertEqual(w3sc_job_run.runStatus, GenomicSubProcessStatus.COMPLETED)
        self.assertEqual(w3sc_job_run.runResult, GenomicSubProcessResult.SUCCESS)

        self.assertTrue(len(self.file_processed_dao.get_all()), 1)
        w3sc_file_processed = self.file_processed_dao.get(1)
        self.assertTrue(w3sc_file_processed.runId, w3sc_job_run.jobId)

        self.assertTrue(all(obj.cvlW3scManifestJobRunID is not None for obj in current_members))
        self.assertTrue(all(obj.cvlW3scManifestJobRunID == w3sc_job_run.id for obj in current_members))

        self.assertTrue(all(obj.cvlSecondaryConfFailure is not None for obj in current_members))

        current_workflow_states = self.results_workflow_dao.get_all()
        self.assertEqual(len(current_workflow_states), 3)
        self.assertTrue(all(obj.results_workflow_state is not None for obj in current_workflow_states))
        self.assertTrue(all(obj.results_workflow_state_str is not None for obj in current_workflow_states))

        self.assertTrue(all(obj.results_workflow_state == ResultsWorkflowState.CVL_W3SC for
                            obj in current_workflow_states))
        self.assertTrue(all(obj.results_workflow_state_str == ResultsWorkflowState.CVL_W3SC.name for obj in
                            current_workflow_states))

    def test_w3sc_manifest_to_raw_ingestion(self):

        self.execute_base_cvl_ingestion(
            test_file='RDR_AoU_CVL_W3SC.csv',
            job_id=GenomicJob.CVL_W3SC_WORKFLOW,
            manifest_type=GenomicManifestTypes.CVL_W3SC,
            current_results_workflow_state=ResultsWorkflowState.CVL_W3SR,
            results_module=ResultsModuleType.HDRV1
        )

        w3sc_raw_dao = GenomicW3SCRawDao()

        manifest_type = 'w3sc'
        w3sc_manifest_file = self.manifest_file_dao.get(1)

        genomic_pipeline.load_awn_manifest_into_raw_table(
            w3sc_manifest_file.filePath,
            manifest_type
        )

        w3sc_raw_records = w3sc_raw_dao.get_all()

        self.assertEqual(len(w3sc_raw_records), 3)
        self.assertTrue(all(obj.file_path is not None for obj in w3sc_raw_records))
        self.assertTrue(all(obj.biobank_id is not None for obj in w3sc_raw_records))
        self.assertTrue(all(obj.sample_id is not None for obj in w3sc_raw_records))
        self.assertTrue(all(obj.cvl_secondary_conf_failure is not None for obj in w3sc_raw_records))

    def _generate_cvl_participant(
        self,
        participant_summary_params=None,
        collection_site_params=None,
        informing_loop_decision_param_list=None,
        set_member_params=None,
        validation_metrics_params=None,
        result_workflow_state=None
    ) -> Tuple[ParticipantSummary, GenomicSetMember, GenomicGCValidationMetrics]:

        participant_summary_params = participant_summary_params or {}
        collection_site_params = collection_site_params or {}
        informing_loop_decision_param_list = informing_loop_decision_param_list or [{}]  # Default to having a decision
        set_member_params = set_member_params or {}
        validation_metrics_params = validation_metrics_params or {}

        participant_summary_params = {
            **{
                'consentForStudyEnrollment': QuestionnaireStatus.SUBMITTED,
                'consentForGenomicsROR': QuestionnaireStatus.SUBMITTED,
            },
            **participant_summary_params
        }
        summary = self.data_generator.create_database_participant_summary(**participant_summary_params)

        # Create the stored sample and order information
        stored_sample = self.data_generator.create_database_biobank_stored_sample(
            biobankId=summary.biobankId,
            biobankOrderIdentifier=self.fake.pyint()
        )
        collection_site_params = {
            **{'siteType': 'Clinic Site'},
            **collection_site_params
        }
        collection_site = self.data_generator.create_database_site(**collection_site_params)
        order = self.data_generator.create_database_biobank_order(
            collectedSiteId=collection_site.siteId,
            participantId=summary.participantId
        )
        self.data_generator.create_database_biobank_order_identifier(
            value=stored_sample.biobankOrderIdentifier,
            biobankOrderId=order.biobankOrderId
        )

        # Set the informing loop decision
        for decision_params in informing_loop_decision_param_list:
            decision_params = {
                **{
                    'participant_id': summary.participantId,
                    'decision_value': 'yes',
                    'module_type': 'pgx'
                },
                **decision_params
            }
            self.data_generator.create_database_genomic_informing_loop(**decision_params)

        set_member_params = {
            **{
                'genomicSetId': self.gen_set.id,
                'biobankId': summary.biobankId,
                'sampleId': stored_sample.biobankStoredSampleId,
                'collectionTubeId': stored_sample.biobankStoredSampleId,
                'sexAtBirth': 'F',
                'nyFlag': 0,
                'genomeType': 'aou_wgs',
                'participantId': summary.participantId,
                'qcStatus': GenomicQcStatus.PASS,
                'gcManifestSampleSource': 'whole blood',
                'genomicWorkflowState': GenomicWorkflowState.CVL_READY
            },
            **set_member_params
        }
        genomic_set_member = self.data_generator.create_database_genomic_set_member(**set_member_params)
        validation_metrics_params = {
            **{
                'genomicSetMemberId': genomic_set_member.id,
                'processingStatus': 'pass',
                'sexConcordance': 'true',
                'drcSexConcordance': 'pass',
                'drcFpConcordance': 'pass',
                'hfVcfReceived': 1,
                'hfVcfTbiReceived': 1,
                'hfVcfMd5Received': 1,
                'cramReceived': 1,
                'cramMd5Received': 1,
                'craiReceived': 1,
                'gvcfReceived': 1,
                'gvcfMd5Received': 1,
                'hfVcfPath': self.fake.pystr(),
                'hfVcfTbiPath': self.fake.pystr(),
                'hfVcfMd5Path': self.fake.pystr(),
                'cramPath': self.fake.pystr(),
                'aouHdrCoverage': self.fake.pyfloat(right_digits=4, min_value=0, max_value=100),
                'contamination': self.fake.pyfloat(right_digits=4, min_value=0, max_value=100)
            },
            **validation_metrics_params
        }
        validation_metrics = self.data_generator.create_database_genomic_gc_validation_metrics(
            **validation_metrics_params
        )

        if result_workflow_state is not None:
            result_workflow_params = {
                **{
                    'genomic_set_member_id': genomic_set_member.id,
                    'results_workflow_state': ResultsWorkflowState.CVL_W1IL,
                    'results_module': ResultsModuleType.PGXV1
                },
                **result_workflow_state
            }
            self.data_generator.create_database_genomic_result_workflow_state(**result_workflow_params)

        return summary, genomic_set_member, validation_metrics

    def assert_manifest_has_rows(self, manifest_cloud_writer_mock, expected_rows):
        write_rows_func = manifest_cloud_writer_mock.__enter__.return_value.write_rows
        actual_rows = write_rows_func.call_args[0][0]

        self.assertListEqual(expected_rows, actual_rows)

    @classmethod
    def expected_w1il_row(cls, set_member: GenomicSetMember, validation_metrics: GenomicGCValidationMetrics,
                          summary: ParticipantSummary):
        return (
            to_client_biobank_id(set_member.biobankId),
            str(set_member.sampleId),
            validation_metrics.hfVcfPath,
            validation_metrics.hfVcfTbiPath,
            validation_metrics.hfVcfMd5Path,
            validation_metrics.cramPath,
            'Y' if set_member.nyFlag == 1 else 'N',
            set_member.gcSiteId.upper(),
            'Y' if summary.consentForGenomicsROR == QuestionnaireStatus.SUBMITTED else 'N',
            'aou_cvl',  # genome type
            'Y',  # informing loop decision
            str(validation_metrics.aouHdrCoverage),
            str(validation_metrics.contamination)
        )
