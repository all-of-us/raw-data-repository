import datetime
import json

from dateutil import parser
import mock

from rdr_service import clock, config
from rdr_service.api_util import open_cloud_file
from rdr_service.clock import FakeClock
from rdr_service.dao.database_utils import format_datetime
from rdr_service.dao.genomics_dao import GenomicGcDataFileDao, GenomicGCValidationMetricsDao, GenomicIncidentDao, \
    GenomicSetMemberDao, UserEventMetricsDao, GenomicJobRunDao, GenomicResultWithdrawalsDao, \
    GenomicMemberReportStateDao, GenomicAppointmentEventMetricsDao, GenomicAppointmentEventDao, GenomicResultViewedDao, \
    GenomicInformingLoopDao, GenomicAppointmentEventNotifiedDao, GenomicGCROutreachEscalationNotifiedDao
from rdr_service.dao.message_broker_dao import MessageBrokenEventDataDao
from rdr_service.genomic_enums import GenomicIncidentCode, GenomicJob, GenomicWorkflowState, GenomicSubProcessResult, \
    GenomicSubProcessStatus, GenomicManifestTypes, GenomicQcStatus, GenomicReportState
from rdr_service.genomic.genomic_job_components import GenomicFileIngester
from rdr_service.genomic.genomic_job_controller import GenomicJobController
from rdr_service.model.genomics import GenomicGcDataFile, GenomicIncident, GenomicSetMember, GenomicGCValidationMetrics
from rdr_service.offline import genomic_pipeline
from rdr_service.participant_enums import WithdrawalStatus
from tests import test_data
from tests.genomics_tests.test_genomic_pipeline import create_ingestion_test_file
from tests.helpers.unittest_base import BaseTestCase


class GenomicJobControllerTest(BaseTestCase):
    def setUp(self):
        super(GenomicJobControllerTest, self).setUp()
        self.data_file_dao = GenomicGcDataFileDao()
        self.event_data_dao = MessageBrokenEventDataDao()
        self.incident_dao = GenomicIncidentDao()
        self.member_dao = GenomicSetMemberDao()
        self.metrics_dao = GenomicGCValidationMetricsDao()
        self.user_event_metrics_dao = UserEventMetricsDao()
        self.job_run_dao = GenomicJobRunDao()
        self.report_state_dao = GenomicMemberReportStateDao()
        self.appointment_event_dao = GenomicAppointmentEventDao()
        self.appointment_metrics_dao = GenomicAppointmentEventMetricsDao()

    def test_incident_with_long_message(self):
        """Make sure the length of incident messages doesn't cause issues when recording them"""
        incident_message = "1" * (GenomicIncident.message.type.length + 20)
        mock_slack_handler = mock.MagicMock()

        job_controller = GenomicJobController(job_id=1)
        job_controller.genomic_alert_slack = mock_slack_handler
        job_controller.create_incident(message=incident_message, slack=True)

        # Double check that the incident was saved successfully, with part of the message
        incident: GenomicIncident = self.session.query(GenomicIncident).one()
        self.assertTrue(incident_message.startswith(incident.message))

        # Make sure Slack received the full message
        mock_slack_handler.send_message_to_webhook.assert_called_with(
            message_data={
                'text': incident_message
            }
        )

    def test_gvcf_files_ingestion(self):
        job_controller = GenomicJobController(job_id=38)
        bucket_name = "test_bucket"

        file_path = "Wgs_sample_raw_data/SS_VCF_research/BCM_A100153482_21042005280_SIA0013441__1.hard-filtered.gvcf.gz"
        file_path_md5 = "Wgs_sample_raw_data/SS_VCF_research/" \
                        "BCM_A100153482_21042005280_SIA0013441__1.hard-filtered.gvcf.gz.md5sum"

        full_path = f'{bucket_name}/{file_path}'
        full_path_md5 = f'{bucket_name}/{file_path_md5}'

        gen_set = self.data_generator.create_database_genomic_set(
            genomicSetName=".",
            genomicSetCriteria=".",
            genomicSetVersion=1
        )

        gen_member = self.data_generator.create_database_genomic_set_member(
            genomicSetId=gen_set.id,
            biobankId="100153482",
            sampleId="21042005280",
            genomeType="aou_wgs",
            genomicWorkflowState=GenomicWorkflowState.AW1
        )

        gen_job_run = self.data_generator.create_database_genomic_job_run(
            jobId=GenomicJob.AW1_MANIFEST,
            startTime=clock.CLOCK.now(),
            runResult=GenomicSubProcessResult.SUCCESS
        )

        gen_processed_file = self.data_generator.create_database_genomic_file_processed(
            runId=gen_job_run.id,
            startTime=clock.CLOCK.now(),
            filePath='/test_file_path',
            bucketName='test_bucket',
            fileName='test_file_name',
        )

        self.data_generator.create_database_genomic_gc_validation_metrics(
            genomicSetMemberId=gen_member.id,
            genomicFileProcessedId=gen_processed_file.id
        )

        job_controller.ingest_data_files_into_gc_metrics(file_path_md5, bucket_name)

        metrics = self.metrics_dao.get_metrics_by_member_id(gen_member.id)

        self.assertIsNotNone(metrics.gvcfMd5Path)
        self.assertEqual(metrics.gvcfMd5Path, full_path_md5)

        job_controller.ingest_data_files_into_gc_metrics(file_path, bucket_name)

        metrics = self.metrics_dao.get_metrics_by_member_id(gen_member.id)

        self.assertIsNotNone(metrics.gvcfPath)
        self.assertEqual(metrics.gvcfPath, full_path)

    def test_gvcf_files_ingestion_create_incident(self):
        bucket_name = "test_bucket"
        file_path = "Wgs_sample_raw_data/SS_VCF_research/BCM_A100153482_21042005280_SIA0013441__1.hard-filtered.gvcf.gz"

        gen_set = self.data_generator.create_database_genomic_set(
            genomicSetName=".",
            genomicSetCriteria=".",
            genomicSetVersion=1
        )

        gen_member = self.data_generator.create_database_genomic_set_member(
            genomicSetId=gen_set.id,
            biobankId="111111111",
            sampleId="222222222222",
            genomeType="aou_wgs",
            genomicWorkflowState=GenomicWorkflowState.AW1
        )

        gen_job_run = self.data_generator.create_database_genomic_job_run(
            jobId=GenomicJob.AW1_MANIFEST,
            startTime=clock.CLOCK.now(),
            runResult=GenomicSubProcessResult.SUCCESS
        )

        gen_processed_file = self.data_generator.create_database_genomic_file_processed(
            runId=gen_job_run.id,
            startTime=clock.CLOCK.now(),
            filePath='/test_file_path',
            bucketName=bucket_name,
            fileName='test_file_name',
        )

        self.data_generator.create_database_genomic_gc_validation_metrics(
            genomicSetMemberId=gen_member.id,
            genomicFileProcessedId=gen_processed_file.id
        )

        with GenomicJobController(GenomicJob.INGEST_DATA_FILES) as controller:
            controller.ingest_data_files_into_gc_metrics(file_path, bucket_name)

        incident = self.incident_dao.get(1)
        self.assertIsNotNone(incident)
        self.assertEqual(incident.code, GenomicIncidentCode.UNABLE_TO_FIND_METRIC.name)
        self.assertEqual(incident.data_file_path, file_path)
        self.assertEqual(incident.message, 'INGEST_DATA_FILES: Cannot find '
                                           'genomics metric record for sample id: '
                                           '21042005280')

    def test_accession_data_files(self):
        test_bucket_baylor = "fake-data-bucket-baylor"
        test_idat_file = "fake-data-bucket-baylor/Genotyping_sample_raw_data/204027270091_R02C01_Grn.idat"
        test_vcf_file = "fake-data-bucket-baylor/Genotyping_sample_raw_data/204027270091_R02C01.vcf.gz"

        test_cram_file = "fake-data-bucket-baylor/Wgs_sample_raw_data/" \
                         "CRAMs_CRAIs/BCM_A100134256_21063006771_SIA0017196_1.cram"

        test_files = [test_idat_file, test_vcf_file, test_cram_file]

        test_time = datetime.datetime(2021, 7, 9, 14, 1, 1)

        # run job controller method on each file
        with clock.FakeClock(test_time):

            for file_path in test_files:
                with GenomicJobController(GenomicJob.ACCESSION_DATA_FILES) as controller:
                    controller.accession_data_files(file_path, test_bucket_baylor)

        inserted_files = self.data_file_dao.get_all()

        # idat
        expected_idat = GenomicGcDataFile(
            id=1,
            created=test_time,
            modified=test_time,
            file_path=test_idat_file,
            gc_site_id='jh',
            bucket_name='fake-data-bucket-baylor',
            file_prefix='Genotyping_sample_raw_data',
            file_name='204027270091_R02C01_Grn.idat',
            file_type='Grn.idat',
            identifier_type='chipwellbarcode',
            identifier_value='204027270091_R02C01',
            ignore_flag=0,
        )

        # vcf
        expected_vcf = GenomicGcDataFile(
            id=2,
            created=test_time,
            modified=test_time,
            file_path=test_vcf_file,
            gc_site_id='jh',
            bucket_name='fake-data-bucket-baylor',
            file_prefix='Genotyping_sample_raw_data',
            file_name='204027270091_R02C01.vcf.gz',
            file_type='vcf.gz',
            identifier_type='chipwellbarcode',
            identifier_value='204027270091_R02C01',
            ignore_flag=0,
        )

        # cram
        expected_cram = GenomicGcDataFile(
            id=3,
            created=test_time,
            modified=test_time,
            file_path=test_cram_file,
            gc_site_id='bcm',
            bucket_name='fake-data-bucket-baylor',
            file_prefix='Wgs_sample_raw_data/CRAMs_CRAIs',
            file_name='BCM_A100134256_21063006771_SIA0017196_1.cram',
            file_type='cram',
            identifier_type='sample_id',
            identifier_value='21063006771',
            ignore_flag=0,
        )

        # obj mapping
        expected_objs = {
            0: expected_idat,
            1: expected_vcf,
            2: expected_cram
        }

        # verify test objects match expectations
        for i in range(3):
            self.assertEqual(expected_objs[i].bucket_name, inserted_files[i].bucket_name)
            self.assertEqual(expected_objs[i].created, inserted_files[i].created)
            self.assertEqual(expected_objs[i].file_name, inserted_files[i].file_name)
            self.assertEqual(expected_objs[i].file_path, inserted_files[i].file_path)
            self.assertEqual(expected_objs[i].file_prefix, inserted_files[i].file_prefix)
            self.assertEqual(expected_objs[i].file_type, inserted_files[i].file_type)
            self.assertEqual(expected_objs[i].gc_site_id, inserted_files[i].gc_site_id)
            self.assertEqual(expected_objs[i].id, inserted_files[i].id)
            self.assertEqual(expected_objs[i].identifier_type, inserted_files[i].identifier_type)
            self.assertEqual(expected_objs[i].identifier_value, inserted_files[i].identifier_value)
            self.assertEqual(expected_objs[i].ignore_flag, inserted_files[i].ignore_flag)
            self.assertEqual(expected_objs[i].metadata, inserted_files[i].metadata)
            self.assertEqual(expected_objs[i].modified, inserted_files[i].modified)

    def test_updating_members_blocklists(self):

        gen_set = self.data_generator.create_database_genomic_set(
            genomicSetName=".",
            genomicSetCriteria=".",
            genomicSetVersion=1
        )

        ids_should_be_updated = []
        # for just created and wf state query and MATCHES criteria
        for i in range(4):
            ids_should_be_updated.append(
                self.data_generator.create_database_genomic_set_member(
                    genomicSetId=gen_set.id,
                    biobankId="100153482",
                    sampleId="21042005280",
                    genomeType='test_investigation_one' if i & 2 != 0 else 'aou_wgs',
                    genomicWorkflowState=GenomicWorkflowState.AW0,
                    ai_an='Y' if i & 2 == 0 else 'N'
                ).id
            )

        # for just created and wf state query and DOES NOT MATCH criteria
        for i in range(2):
            self.data_generator.create_database_genomic_set_member(
                genomicSetId=gen_set.id,
                biobankId="100153482",
                sampleId="21042005280",
                genomeType='aou_array',
                genomicWorkflowState=GenomicWorkflowState.AW0,
                ai_an='N'
            )

        with GenomicJobController(GenomicJob.UPDATE_MEMBERS_BLOCKLISTS) as controller:
            controller.update_members_blocklists()

        # current config json in base_config.json
        created_members = self.member_dao.get_all()

        blocklisted = list(filter(lambda x: x.blockResults == 1 or x.blockResearch == 1, created_members))
        self.assertTrue(ids_should_be_updated.sort() == [obj.id for obj in blocklisted].sort())

        # should be RESEARCH blocked
        self.assertTrue(all(
            obj.blockResearch == 1 and obj.blockResearchReason is not None and obj.blockResearchReason == 'aian'
            for obj in created_members if obj.ai_an == 'Y' and obj.genomicWorkflowState == GenomicWorkflowState.AW0)
        )

        # should NOT be RESULTS blocked
        self.assertTrue(all(
            obj.blockResults == 0 and obj.blockResultsReason is None
            for obj in created_members if obj.ai_an == 'Y' and obj.genomicWorkflowState == GenomicWorkflowState.AW0)
        )

        # should be RESEARCH blocked
        self.assertTrue(all(
            obj.blockResearch == 1 and obj.blockResearchReason is not None and obj.blockResearchReason == 'test_sample_swap'
            for obj in created_members if obj.genomeType == 'test_investigation_one' and obj.genomicWorkflowState ==
            GenomicWorkflowState.AW0)
        )

        # should be RESULTS blocked
        self.assertTrue(all(
            obj.blockResults == 1 and obj.blockResultsReason is not None and obj.blockResultsReason == 'test_sample_swap'
            for obj in created_members if obj.genomeType == 'test_investigation_one' and obj.genomicWorkflowState ==
            GenomicWorkflowState.AW0)
        )

        # should NOT be RESEARCH/RESULTS blocked
        self.assertTrue(all(
            obj.blockResearch == 0 and obj.blockResearchReason is None
            for obj in created_members if obj.genomeType == 'aou_array' and obj.genomicWorkflowState ==
            GenomicWorkflowState.AW0)
        )

        self.assertTrue(all(
            obj.blockResults == 0 and obj.blockResultsReason is None
            for obj in created_members if obj.genomeType == 'aou_array' and obj.genomicWorkflowState ==
            GenomicWorkflowState.AW0)
        )

        # clear current set member records
        with self.member_dao.session() as session:
            session.query(GenomicSetMember).delete()

        run_result = self.job_run_dao.get(1)

        self.assertEqual(run_result.runStatus, GenomicSubProcessStatus.COMPLETED)
        self.assertEqual(run_result.runResult, GenomicSubProcessResult.SUCCESS)

        # for modified data query and MATCHES criteria
        for i in range(4):
            self.data_generator.create_database_genomic_set_member(
                genomicSetId=gen_set.id,
                biobankId="100153482",
                sampleId="21042005280",
                genomeType='test_investigation_one' if i & 2 != 0 else 'aou_wgs',
                genomicWorkflowState=GenomicWorkflowState.AW1,
                ai_an='Y' if i & 2 == 0 else 'N'
            )

        with GenomicJobController(GenomicJob.UPDATE_MEMBERS_BLOCKLISTS) as controller:
            controller.update_members_blocklists()

        modified_members = self.member_dao.get_all()

        # should be RESEARCH blocked
        self.assertTrue(all(
            obj.blockResearch == 1 and obj.blockResearchReason is not None and obj.blockResearchReason == 'aian'
            for obj in modified_members if obj.ai_an == 'Y' and obj.genomicWorkflowState == GenomicWorkflowState.AW1)
        )

        # should NOT be RESULTS blocked
        self.assertTrue(all(
            obj.blockResults == 0 and obj.blockResultsReason is None
            for obj in modified_members if obj.ai_an == 'Y' and obj.genomicWorkflowState == GenomicWorkflowState.AW1)
        )

        # should be RESEARCH blocked
        self.assertTrue(all(
            obj.blockResearch == 1 and obj.blockResearchReason is not None and obj.blockResearchReason == 'test_sample_swap'
            for obj in modified_members if obj.genomeType == 'test_investigation_one' and obj.genomicWorkflowState ==
            GenomicWorkflowState.AW1)
        )

        # should be RESULTS blocked
        self.assertTrue(all(
            obj.blockResults == 1 and obj.blockResultsReason is not None and obj.blockResultsReason == 'test_sample_swap'
            for obj in modified_members if obj.genomeType == 'test_investigation_one' and obj.genomicWorkflowState ==
            GenomicWorkflowState.AW1)
        )

        run_result = self.job_run_dao.get(2)

        self.assertEqual(run_result.runStatus, GenomicSubProcessStatus.COMPLETED)
        self.assertEqual(run_result.runResult, GenomicSubProcessResult.SUCCESS)

    def test_ingest_user_metrics_file(self):
        test_file = 'Genomic-Metrics-File-User-Events-Test.csv'
        bucket_name = 'test_bucket'
        sub_folder = 'user_events'
        pids = []

        file_ingester = GenomicFileIngester()

        for _ in range(2):
            pid = self.data_generator.create_database_participant()
            pids.append(pid.participantId)

        test_metrics_file = create_ingestion_test_file(
            test_file,
            bucket_name,
            sub_folder)

        test_file_path = f'{bucket_name}/{sub_folder}/{test_metrics_file}'

        with open_cloud_file(test_file_path) as csv_file:
            metrics_to_ingest = file_ingester._read_data_to_ingest(csv_file)

        with GenomicJobController(GenomicJob.METRICS_FILE_INGEST) as controller:
            controller.ingest_metrics_file(
                metric_type='user_events',
                file_path=test_file_path,
            )

        job_run_id = controller.job_run.id
        metrics = self.user_event_metrics_dao.get_all()

        for pid in pids:
            file_metrics = list(filter(lambda x: int(x['participant_id'].split('P')[-1]) == pid, metrics_to_ingest[
                'rows']))
            participant_ingested_metrics = list(filter(lambda x: x.participant_id == pid, metrics))

            self.assertEqual(len(file_metrics), len(participant_ingested_metrics))
            self.assertTrue(all(obj.run_id == job_run_id for obj in participant_ingested_metrics))

    @mock.patch('rdr_service.genomic.genomic_job_controller.GenomicJobController.execute_cloud_task')
    def test_reconcile_pdr_data(self, mock_cloud_task):

        # init new job run in __enter__
        with GenomicJobController(GenomicJob.RECONCILE_PDR_DATA) as controller:
            controller.reconcile_pdr_data()

        cloud_task_endpoint = 'rebuild_genomic_table_records_task'

        first_run = self.job_run_dao.get_all()

        self.assertEqual(mock_cloud_task.call_count, 1)
        call_args = mock_cloud_task.call_args_list

        self.assertEqual(len(call_args), 1)
        self.assertEqual(call_args[0].args[0]['table'], self.job_run_dao.model_type.__tablename__)

        self.assertTrue(type(call_args[0].args[0]['ids']) is list)
        self.assertEqual(call_args[0].args[0]['ids'], [obj.id for obj in first_run])
        self.assertEqual(call_args[0].args[1], cloud_task_endpoint)

        participant = self.data_generator.create_database_participant()

        gen_set = self.data_generator.create_database_genomic_set(
            genomicSetName=".",
            genomicSetCriteria=".",
            genomicSetVersion=1
        )

        plus_ten = clock.CLOCK.now() + datetime.timedelta(minutes=10)
        plus_ten = plus_ten.replace(microsecond=0)
        with FakeClock(plus_ten):
            for i in range(2):
                gen_member = self.data_generator.create_database_genomic_set_member(
                    genomicSetId=gen_set.id,
                    biobankId="100153482",
                    sampleId="21042005280",
                    genomeType="aou_wgs",
                    genomicWorkflowState=GenomicWorkflowState.AW1
                )

                gen_processed_file = self.data_generator.create_database_genomic_file_processed(
                    runId=first_run[0].id,
                    startTime=clock.CLOCK.now(),
                    filePath=f'test_file_path_{i}',
                    bucketName='test_bucket',
                    fileName='test_file_name',
                )

                self.data_generator.create_database_genomic_gc_validation_metrics(
                    genomicSetMemberId=gen_member.id,
                    genomicFileProcessedId=gen_processed_file.id
                )

                manifest = self.data_generator.create_database_genomic_manifest_file(
                    manifestTypeId=2,
                    filePath=f'test_file_path_{i}'
                )

                self.data_generator.create_database_genomic_manifest_feedback(
                    inputManifestFileId=manifest.id,
                    feedbackRecordCount=2
                )

                self.data_generator.create_database_genomic_user_event_metrics(
                    participant_id=participant.participantId,
                    event_name='test_event',
                    run_id=1,
                )

                self.data_generator.create_database_genomic_informing_loop(
                    message_record_id=1,
                    event_type='informing_loop_decision',
                    module_type='gem',
                    participant_id=participant.participantId,
                    decision_value='maybe_later',
                    event_authored_time=clock.CLOCK.now()
                )

                self.data_generator.create_database_genomic_cvl_past_due(
                    cvl_site_id='co',
                    email_notification_sent=0,
                    sample_id='sample_test',
                    results_type='hdr',
                    genomic_set_member_id=gen_member.id
                )

        # gets new records that were created with last job run from above
        with GenomicJobController(GenomicJob.RECONCILE_PDR_DATA) as controller:
            controller.reconcile_pdr_data()

        affected_tables = [
            'genomic_set',
            'genomic_set_member',
            'genomic_job_run',
            'genomic_file_processed',
            'genomic_gc_validation_metrics',
            'genomic_manifest_file',
            'genomic_manifest_feedback',
            'genomic_informing_loop',
            'genomic_cvl_results_past_due'
        ]

        num_calls = len(affected_tables) + 1

        self.assertEqual(mock_cloud_task.call_count, num_calls)
        call_args = mock_cloud_task.call_args_list
        self.assertEqual(len(call_args), num_calls)

        mock_tables = set([obj[0][0]['table'] for obj in call_args])
        mock_endpoint = [obj[0][1] for obj in call_args]

        self.assertTrue([mock_tables].sort() == affected_tables.sort())
        self.assertTrue(all(obj for obj in mock_endpoint if obj == cloud_task_endpoint))

    @mock.patch('rdr_service.genomic.genomic_job_controller.GenomicJobController.execute_cloud_task')
    def test_retry_manifest_ingestions_if_deltas(self, mock_cloud_task):

        bucket_name = "test-bucket"
        aw1_file_name = "AW1_wgs_sample_manifests/RDR_AoU_SEQ_PKG-2104-026571.csv"
        aw1_manifest_path = f"{bucket_name}/{aw1_file_name}"

        aw2_file_name = "AW2_wgs_data_manifests/RDR_AoU_SEQ_DataManifest_04092021.csv"
        aw2_manifest_path = f"{bucket_name}/{aw2_file_name}"

        gen_set = self.data_generator.create_database_genomic_set(
            genomicSetName=".",
            genomicSetCriteria=".",
            genomicSetVersion=1
        )

        # Create AW1 job_run
        aw1_job_run = self.data_generator.create_database_genomic_job_run(
            jobId=GenomicJob.AW1_MANIFEST,
            startTime=clock.CLOCK.now(),
            endTime=clock.CLOCK.now(),
            runResult=GenomicSubProcessResult.SUCCESS
        )

        # Create AW2 job_run
        aw2_job_run = self.data_generator.create_database_genomic_job_run(
            jobId=GenomicJob.METRICS_INGESTION,
            startTime=clock.CLOCK.now(),
            endTime=clock.CLOCK.now(),
            runResult=GenomicSubProcessResult.SUCCESS
        )

        # should have no data
        with GenomicJobController(GenomicJob.RETRY_MANIFEST_INGESTIONS) as controller:
            controller.retry_manifest_ingestions()

        job_run = self.job_run_dao.get(3)
        self.assertEqual(job_run.jobId, GenomicJob.RETRY_MANIFEST_INGESTIONS)
        self.assertEqual(job_run.runStatus, GenomicSubProcessStatus.COMPLETED)
        self.assertEqual(job_run.runResult, GenomicSubProcessResult.NO_FILES)

        self.assertEqual(mock_cloud_task.call_count, 0)
        self.assertFalse(mock_cloud_task.call_count)

        # Create genomic_aw1_raw record
        self.data_generator.create_database_genomic_aw1_raw(
            file_path=aw1_manifest_path,
            package_id="PKG-2104-026571",
            biobank_id="A10001",
        )

        # Create genomic_aw2_raw record
        self.data_generator.create_database_genomic_aw2_raw(
            file_path=aw2_manifest_path,
            biobank_id="A10001",
            sample_id="100001",
            biobankidsampleid="A10001_100001",
        )

        # Create AW1 genomic_manifest_file record
        aw1_manifest_file = self.data_generator.create_database_genomic_manifest_file(
            created=clock.CLOCK.now(),
            modified=clock.CLOCK.now(),
            uploadDate=clock.CLOCK.now(),
            manifestTypeId=GenomicManifestTypes.AW1,
            filePath=aw1_manifest_path,
            fileName=aw1_file_name,
            bucketName=bucket_name,
            recordCount=1,
            rdrProcessingComplete=1,
            rdrProcessingCompleteDate=clock.CLOCK.now(),
        )

        # Create AW2 genomic_manifest_file record
        aw2_manifest_file = self.data_generator.create_database_genomic_manifest_file(
            created=clock.CLOCK.now(),
            modified=clock.CLOCK.now(),
            uploadDate=clock.CLOCK.now(),
            manifestTypeId=GenomicManifestTypes.AW2,
            filePath=aw2_manifest_path,
            fileName=aw2_file_name,
            bucketName=bucket_name,
            recordCount=1,
            rdrProcessingComplete=1,
            rdrProcessingCompleteDate=clock.CLOCK.now(),
        )

        # Create AW1 file_processed
        aw1_file_processed = self.data_generator.create_database_genomic_file_processed(
            runId=aw1_job_run.id,
            startTime=clock.CLOCK.now(),
            genomicManifestFileId=aw1_manifest_file.id,
            filePath=f"/{aw1_manifest_path}",
            bucketName=bucket_name,
            fileName=aw1_file_name,
        )

        # Create AW2 file_processed
        aw2_file_processed = self.data_generator.create_database_genomic_file_processed(
            runId=aw2_job_run.id,
            startTime=clock.CLOCK.now(),
            genomicManifestFileId=aw2_manifest_file.id,
            filePath=f"/{aw2_manifest_path}",
            bucketName=bucket_name,
            fileName=aw2_file_name,
        )

        # genomic_set_member for AW1
        gen_member = self.data_generator.create_database_genomic_set_member(
            genomicSetId=gen_set.id,
            biobankId="100153482",
            sampleId="21042005280",
            genomeType="aou_wgs",
            genomicWorkflowState=GenomicWorkflowState.AW1,
            aw1FileProcessedId=aw1_file_processed.id
        )

        # genomic_gc_validation_metrics for AW1
        self.data_generator.create_database_genomic_gc_validation_metrics(
            genomicSetMemberId=gen_member.id,
            genomicFileProcessedId=aw2_file_processed.id
        )

        # one AW1/AW2 with no deltas
        with GenomicJobController(GenomicJob.RETRY_MANIFEST_INGESTIONS) as controller:
            controller.retry_manifest_ingestions()

        job_run = self.job_run_dao.get(4)
        self.assertEqual(job_run.jobId, GenomicJob.RETRY_MANIFEST_INGESTIONS)
        self.assertEqual(job_run.runStatus, GenomicSubProcessStatus.COMPLETED)
        self.assertEqual(job_run.runResult, GenomicSubProcessResult.NO_FILES)

        self.assertEqual(mock_cloud_task.call_count, 0)
        self.assertFalse(mock_cloud_task.call_count)

        # empty tables resulting in deltas and cloud task calls
        with self.member_dao.session() as session:
            session.query(GenomicGCValidationMetrics).delete()
            session.query(GenomicSetMember).delete()

        with GenomicJobController(GenomicJob.RETRY_MANIFEST_INGESTIONS) as controller:
            controller.retry_manifest_ingestions()

        job_run = self.job_run_dao.get(5)
        self.assertEqual(job_run.jobId, GenomicJob.RETRY_MANIFEST_INGESTIONS)
        self.assertEqual(job_run.runStatus, GenomicSubProcessStatus.COMPLETED)
        self.assertEqual(job_run.runResult, GenomicSubProcessResult.SUCCESS)

        # one AW1/AW2 with deltas
        self.assertEqual(mock_cloud_task.call_count, 2)
        self.assertTrue(mock_cloud_task.call_count)

        call_args = mock_cloud_task.call_args_list
        self.assertEqual(len(call_args), 2)

        cloud_task_endpoint = ['ingest_aw1_manifest_task', 'ingest_aw2_manifest_task']
        mock_endpoint = [obj[0][1] for obj in call_args]
        self.assertTrue(all(obj for obj in mock_endpoint if obj == cloud_task_endpoint))

        mock_buckets = set([obj[0][0]['bucket_name'] for obj in call_args])
        self.assertTrue(len(mock_buckets), 1)
        self.assertTrue(list(mock_buckets)[0] == bucket_name)

    def test_calculate_informing_loop_ready_flags(self):
        num_participants = 4
        gen_set = self.data_generator.create_database_genomic_set(
            genomicSetName=".",
            genomicSetCriteria=".",
            genomicSetVersion=1
        )

        for num in range(num_participants):
            plus_num = clock.CLOCK.now() + datetime.timedelta(minutes=num)
            plus_num = plus_num.replace(microsecond=0)
            with FakeClock(plus_num):
                summary = self.data_generator.create_database_participant_summary(
                    consentForStudyEnrollment=1,
                    consentForGenomicsROR=1
                )
                stored_sample = self.data_generator.create_database_biobank_stored_sample(
                    biobankId=summary.biobankId,
                    biobankOrderIdentifier=self.fake.pyint()
                )
                collection_site = self.data_generator.create_database_site(
                    siteType='Clinic'
                )
                order = self.data_generator.create_database_biobank_order(
                    collectedSiteId=collection_site.siteId,
                    participantId=summary.participantId,
                    finalizedTime=plus_num
                )
                self.data_generator.create_database_biobank_order_identifier(
                    value=stored_sample.biobankOrderIdentifier,
                    biobankOrderId=order.biobankOrderId,
                    system="1",
                )
                self.data_generator.create_database_biobank_order_identifier(
                    value=stored_sample.biobankOrderIdentifier,
                    biobankOrderId=order.biobankOrderId,
                    system="2",
                )
                member = self.data_generator.create_database_genomic_set_member(
                    genomicSetId=gen_set.id,
                    participantId=summary.participantId,
                    genomeType=config.GENOME_TYPE_WGS,
                    qcStatus=GenomicQcStatus.PASS,
                    gcManifestSampleSource='Whole Blood',
                    collectionTubeId=stored_sample.biobankStoredSampleId
                )
                self.data_generator.create_database_genomic_gc_validation_metrics(
                    genomicSetMemberId=member.id,
                    sexConcordance='True',
                    drcFpConcordance='Pass',
                    drcSexConcordance='Pass',
                    processingStatus='Pass'
                )


        members_for_ready_loop = self.member_dao.get_members_for_informing_loop_ready()
        self.assertEqual(len(members_for_ready_loop), num_participants)

        current_set_members = self.member_dao.get_all()
        self.assertTrue(all(obj.informingLoopReadyFlag == 0 for obj in current_set_members))
        self.assertTrue(all(obj.informingLoopReadyFlagModified is None for obj in current_set_members))

        with GenomicJobController(GenomicJob.CALCULATE_INFORMING_LOOP_READY) as controller:
            controller.calculate_informing_loop_ready_flags()

        # no config object, controller method should return
        members_for_ready_loop = self.member_dao.get_members_for_informing_loop_ready()
        self.assertEqual(len(members_for_ready_loop), num_participants)

        calculation_limit = 2
        config.override_setting(config.CALCULATE_READY_FLAG_LIMIT, [calculation_limit])

        with GenomicJobController(GenomicJob.CALCULATE_INFORMING_LOOP_READY) as controller:
            controller.calculate_informing_loop_ready_flags()

        current_set_members = self.member_dao.get_all()
        self.assertTrue(any(obj.informingLoopReadyFlag == 1 for obj in current_set_members))
        self.assertTrue(any(obj.informingLoopReadyFlagModified is not None for obj in current_set_members))

        current_loops_set = [obj for obj in current_set_members if obj.informingLoopReadyFlag == 1
                             and obj.informingLoopReadyFlagModified is not None]
        self.assertEqual(len(current_loops_set), calculation_limit)

        members_for_ready_loop = self.member_dao.get_members_for_informing_loop_ready()
        self.assertEqual(len(members_for_ready_loop), num_participants // 2)

        with GenomicJobController(GenomicJob.CALCULATE_INFORMING_LOOP_READY) as controller:
            controller.calculate_informing_loop_ready_flags()

        current_set_members = self.member_dao.get_all()
        self.assertTrue(all(obj.informingLoopReadyFlag == 1 for obj in current_set_members))
        self.assertTrue(all(obj.informingLoopReadyFlagModified is not None for obj in current_set_members))

        members_for_ready_loop = self.member_dao.get_members_for_informing_loop_ready()
        self.assertEqual(len(members_for_ready_loop), 0)

    @mock.patch('rdr_service.services.email_service.EmailService.send_email')
    def test_getting_results_withdrawn(self, email_mock):
        num_participants = 4
        result_withdrawal_dao = GenomicResultWithdrawalsDao()

        gen_set = self.data_generator.create_database_genomic_set(
            genomicSetName=".",
            genomicSetCriteria=".",
            genomicSetVersion=1
        )
        gen_job_run = self.data_generator.create_database_genomic_job_run(
            jobId=GenomicJob.AW1_MANIFEST,
            startTime=clock.CLOCK.now(),
            runResult=GenomicSubProcessResult.SUCCESS
        )

        pids = []
        for num in range(num_participants):
            summary = self.data_generator.create_database_participant_summary(
                consentForStudyEnrollment=1,
                consentForGenomicsROR=1,
                withdrawalStatus=WithdrawalStatus.EARLY_OUT
            )

            self.data_generator.create_database_genomic_set_member(
                genomicSetId=gen_set.id,
                participantId=summary.participantId,
                genomeType=config.GENOME_TYPE_ARRAY,
                gemA1ManifestJobRunId=gen_job_run.id if num % 2 == 0 else None
            )

            self.data_generator.create_database_genomic_set_member(
                genomicSetId=gen_set.id,
                participantId=summary.participantId,
                genomeType=config.GENOME_TYPE_WGS,
                cvlW1ilHdrJobRunId=gen_job_run.id
            )

            pids.append(summary.participantId)

        config.override_setting(config.RDR_GENOMICS_NOTIFICATION_EMAIL, 'email@test.com')

        with GenomicJobController(GenomicJob.RESULTS_PIPELINE_WITHDRAWALS) as controller:
            controller.check_results_withdrawals()

        # mock checks
        self.assertEqual(email_mock.call_count, 1)

        job_runs = self.job_run_dao.get_all()
        current_job_run = list(filter(lambda x: x.jobId == GenomicJob.RESULTS_PIPELINE_WITHDRAWALS, job_runs))[0]
        self.assertTrue(current_job_run.runResult == GenomicSubProcessResult.SUCCESS)

        all_withdrawal_records = result_withdrawal_dao.get_all()

        self.assertTrue(len(all_withdrawal_records) == len(pids))
        self.assertTrue(all(obj.participant_id in pids for obj in all_withdrawal_records))

        array_results = list(filter(lambda x: x.array_results == 1, all_withdrawal_records))

        # should only be 2
        self.assertTrue(len(array_results), 2)

        cvl_results = list(filter(lambda x: x.cvl_results == 1, all_withdrawal_records))

        # should be 4 for num of participants
        self.assertTrue(len(cvl_results), num_participants)

        with GenomicJobController(GenomicJob.RESULTS_PIPELINE_WITHDRAWALS) as controller:
            controller.check_results_withdrawals()

        # mock checks should still be one on account of no records
        self.assertEqual(email_mock.call_count, 1)

        job_runs = self.job_run_dao.get_all()
        current_job_run = list(filter(lambda x: x.jobId == GenomicJob.RESULTS_PIPELINE_WITHDRAWALS, job_runs))[1]

        self.assertTrue(current_job_run.runResult == GenomicSubProcessResult.NO_RESULTS)

    def test_gem_results_to_report_state(self):
        num_participants = 8

        gen_set = self.data_generator.create_database_genomic_set(
            genomicSetName=".",
            genomicSetCriteria=".",
            genomicSetVersion=1
        )

        gem_a2_job_run = self.data_generator.create_database_genomic_job_run(
            jobId=GenomicJob.GEM_A2_MANIFEST,
            startTime=clock.CLOCK.now(),
            runResult=GenomicSubProcessResult.SUCCESS
        )

        pids_to_update, member_ids = [], []
        for num in range(num_participants):
            summary = self.data_generator.create_database_participant_summary(
                consentForStudyEnrollment=1,
                consentForGenomicsROR=1,
                withdrawalStatus=WithdrawalStatus.EARLY_OUT
            )

            member = self.data_generator.create_database_genomic_set_member(
                genomicSetId=gen_set.id,
                participantId=summary.participantId,
                genomeType=config.GENOME_TYPE_ARRAY
            )

            if num % 2 == 0:
                member_ids.append(member.id)
                pids_to_update.append(summary.participantId)

        with GenomicJobController(GenomicJob.GEM_RESULT_REPORTS) as controller:
            controller.gem_results_to_report_state()

        current_job_runs = self.job_run_dao.get_all()
        self.assertEqual(len(current_job_runs), 2)

        current_job_run = list(filter(lambda x: x.jobId == GenomicJob.GEM_RESULT_REPORTS, current_job_runs))[0]
        self.assertTrue(current_job_run.runResult == GenomicSubProcessResult.NO_RESULTS)

        current_members = self.member_dao.get_all()

        # 4 members updated correctly should return
        for member in current_members:
            if member.participantId in pids_to_update:
                member.gemA2ManifestJobRunId = gem_a2_job_run.id
                member.genomicWorkflowState = GenomicWorkflowState.GEM_RPT_READY
                self.member_dao.update(member)

        with GenomicJobController(GenomicJob.GEM_RESULT_REPORTS) as controller:
            controller.gem_results_to_report_state()

        current_job_runs = self.job_run_dao.get_all()
        self.assertEqual(len(current_job_runs), 3)

        current_job_run = list(filter(lambda x: x.jobId == GenomicJob.GEM_RESULT_REPORTS, current_job_runs))[1]
        self.assertTrue(current_job_run.runResult == GenomicSubProcessResult.SUCCESS)

        current_gem_report_states = self.report_state_dao.get_all()
        self.assertEqual(len(current_gem_report_states), len(pids_to_update))
        self.assertTrue(all(obj.event_type == 'result_ready' for obj in current_gem_report_states))
        self.assertTrue(all(obj.event_authored_time is not None for obj in current_gem_report_states))
        self.assertTrue(all(obj.module == 'gem' for obj in current_gem_report_states))
        self.assertTrue(
            all(obj.genomic_report_state == GenomicReportState.GEM_RPT_READY for obj in current_gem_report_states)
        )
        self.assertTrue(
            all(obj.genomic_report_state_str == GenomicReportState.GEM_RPT_READY.name for obj in
                current_gem_report_states)
        )
        self.assertTrue(
            all(obj.genomic_set_member_id in member_ids for obj in
                current_gem_report_states)
        )

        # 4 members inserted already should not return
        with GenomicJobController(GenomicJob.GEM_RESULT_REPORTS) as controller:
            controller.gem_results_to_report_state()

        current_job_runs = self.job_run_dao.get_all()
        self.assertEqual(len(current_job_runs), 4)

        current_job_run = list(filter(lambda x: x.jobId == GenomicJob.GEM_RESULT_REPORTS, current_job_runs))[2]
        self.assertTrue(current_job_run.runResult == GenomicSubProcessResult.NO_RESULTS)

        self.clear_table_after_test('genomic_member_report_state')

    def test_reconcile_informing_loop(self):
        event_dao = UserEventMetricsDao()
        event_dao.truncate()  # for test suite
        il_dao = GenomicInformingLoopDao()

        for pid in range(8):
            self.data_generator.create_database_participant(participantId=1 + pid, biobankId=1 + pid)

        # Set up initial job run ID
        self.data_generator.create_database_genomic_job_run(
            jobId=GenomicJob.METRICS_FILE_INGEST,
            startTime=clock.CLOCK.now()
        )

        # create genomic set
        self.data_generator.create_database_genomic_set(
            genomicSetName='test',
            genomicSetCriteria='.',
            genomicSetVersion=1
        )
        # insert set members
        for b in ["aou_array", "aou_wgs"]:
            for i in range(1, 9):
                self.data_generator.create_database_genomic_set_member(
                    participantId=i,
                    genomicSetId=1,
                    biobankId=i,
                    collectionTubeId=100 + i,
                    sampleId=10 + i,
                    genomeType=b,
                )

        # Set up ingested metrics data
        events = ['gem.informing_loop.started',
                  'gem.informing_loop.screen8_no',
                  'gem.informing_loop.screen8_yes',
                  'hdr.informing_loop.started',
                  'gem.informing_loop.screen3',
                  'pgx.informing_loop.screen8_no',
                  'hdr.informing_loop.screen10_no']

        for p in range(4):
            for i in range(len(events)):
                self.data_generator.create_database_genomic_user_event_metrics(
                    created=clock.CLOCK.now(),
                    modified=clock.CLOCK.now(),
                    participant_id=p + 1,
                    created_at=datetime.datetime(2021, 12, 29, 00) + datetime.timedelta(hours=i),
                    event_name=events[i],
                    run_id=1,
                    ignore_flag=0,
                )
        # Set up informing loop from message broker records
        decisions = [None, 'no', 'yes']
        for p in range(3):
            for i in range(2):
                self.data_generator.create_database_genomic_informing_loop(
                    message_record_id=i,
                    event_type='informing_loop_started' if i == 0 else 'informing_loop_decision',
                    module_type='gem',
                    participant_id=p + 1,
                    decision_value=decisions[i],
                    sample_id=100 + p,
                    event_authored_time=datetime.datetime(2021, 12, 29, 00) + datetime.timedelta(hours=i)
                )

        # Test for no message but yes user event
        self.data_generator.create_database_genomic_user_event_metrics(
            created=clock.CLOCK.now(),
            modified=clock.CLOCK.now(),
            participant_id=6,
            created_at=datetime.datetime(2021, 12, 29, 00),
            event_name='gem.informing_loop.screen8_yes',
            run_id=1,
            ignore_flag=0,
        )

        # Run reconcile job
        genomic_pipeline.reconcile_informing_loop_responses()

        # Test mismatched GEM data ingested correctly
        pid_list = [1, 2, 3, 6]

        new_il_values = il_dao.get_latest_il_for_pids(
            pid_list=pid_list,
            module="gem"
        )

        for value in new_il_values:
            self.assertEqual("yes", value.decision_value)

        pid_list = [1, 2, 3, 4]
        for module in ["hdr", "pgx"]:
            new_il_values = il_dao.get_latest_il_for_pids(
                pid_list=pid_list,
                module=module
            )

            for value in new_il_values:
                self.assertEqual("no", value.decision_value)
                self.assertIsNotNone(value.created_from_metric_id)

    def test_reconcile_message_broker_results_ready(self):
        # Create Test Participants' data
        # create genomic set
        self.data_generator.create_database_genomic_set(
            genomicSetName='test',
            genomicSetCriteria='.',
            genomicSetVersion=1
        )
        # Set up initial job run ID
        self.data_generator.create_database_genomic_job_run(
            jobId=GenomicJob.METRICS_FILE_INGEST,
            startTime=clock.CLOCK.now()
        )

        for pid in range(7):
            self.data_generator.create_database_participant(participantId=1 + pid, biobankId=1 + pid)

        # insert set members and event metrics records
        for i in range(1, 6):
            self.data_generator.create_database_genomic_set_member(
                participantId=i,
                genomicSetId=1,
                biobankId=i,
                collectionTubeId=100 + i,
                sampleId=10 + i,
                genomeType="aou_wgs",
            )

            # 3 PGX records
            if i < 4:
                self.data_generator.create_database_genomic_user_event_metrics(
                    participant_id=i,
                    created_at=datetime.datetime(2022, 10, 6, 00),
                    event_name="pgx.result_ready",
                    run_id=1,
                )

            # 1 HDR Positive
            if i == 4:
                self.data_generator.create_database_genomic_user_event_metrics(
                    participant_id=i,
                    created_at=datetime.datetime(2022, 10, 6, 00),
                    event_name="hdr.result_ready.informative",
                    run_id=1,
                )

            # 1 HDR uninformative
            if i == 5:
                self.data_generator.create_database_genomic_user_event_metrics(
                    participant_id=i,
                    created_at=datetime.datetime(2022, 10, 6, 00),
                    event_name="hdr.result_ready.uninformative",
                    run_id=1,
                )

        # Run job
        genomic_pipeline.reconcile_message_broker_results_ready()

        # Test correct data inserted
        report_state_dao = GenomicMemberReportStateDao()
        states = report_state_dao.get_all()

        self.assertEqual(5, len(states))

        pgx_records = [rec for rec in states if rec.module == "pgx_v1"]
        hdr_record_uninf = [rec for rec in states
                            if rec.genomic_report_state == GenomicReportState.HDR_RPT_UNINFORMATIVE][0]

        hdr_record_pos = [rec for rec in states
                          if rec.genomic_report_state == GenomicReportState.HDR_RPT_POSITIVE][0]

        for pgx_record in pgx_records:
            self.assertEqual(GenomicReportState.PGX_RPT_READY, pgx_record.genomic_report_state)
            self.assertEqual("PGX_RPT_READY", pgx_record.genomic_report_state_str)
            self.assertEqual(int(pgx_record.sample_id), pgx_record.participant_id + 10)
            self.assertEqual("result_ready", pgx_record.event_type)
            self.assertEqual(datetime.datetime(2022, 10, 6, 00), pgx_record.event_authored_time)
            self.assertIsNotNone(pgx_record.created_from_metric_id)

        self.assertEqual("HDR_RPT_UNINFORMATIVE", hdr_record_uninf.genomic_report_state_str)
        self.assertEqual(int(hdr_record_uninf.sample_id), hdr_record_uninf.participant_id + 10)
        self.assertEqual("result_ready", hdr_record_uninf.event_type)
        self.assertEqual(datetime.datetime(2022, 10, 6, 00), hdr_record_uninf.event_authored_time)
        self.assertIsNotNone(hdr_record_uninf.created_from_metric_id)

        self.assertEqual("HDR_RPT_POSITIVE", hdr_record_pos.genomic_report_state_str)
        self.assertEqual(int(hdr_record_pos.sample_id), hdr_record_pos.participant_id + 10)
        self.assertEqual("result_ready", hdr_record_pos.event_type)
        self.assertEqual(datetime.datetime(2022, 10, 6, 00), hdr_record_pos.event_authored_time)
        self.assertIsNotNone(hdr_record_pos.created_from_metric_id)

    def test_reconcile_message_broker_results_viewed(self):
        # Create Test Participants' data
        # create genomic set
        self.data_generator.create_database_genomic_set(
            genomicSetName='test',
            genomicSetCriteria='.',
            genomicSetVersion=1
        )
        # Set up initial job run ID
        self.data_generator.create_database_genomic_job_run(
            jobId=GenomicJob.METRICS_FILE_INGEST,
            startTime=clock.CLOCK.now()
        )

        for pid in range(3):
            self.data_generator.create_database_participant(participantId=1 + pid, biobankId=1 + pid)

        # insert set members and event metrics records
        for i in range(1, 3):
            self.data_generator.create_database_genomic_set_member(
                participantId=i,
                genomicSetId=1,
                biobankId=i,
                collectionTubeId=100 + i,
                sampleId=10 + i,
                genomeType="aou_wgs",
            )

            # 1 PGX Viewed
            if i == 1:
                self.data_generator.create_database_genomic_user_event_metrics(
                    participant_id=i,
                    created_at=datetime.datetime(2022, 10, 6, 00),
                    event_name="pgx.opened_at",
                    run_id=1,
                )

            # 1 HDR Viewed
            if i == 2:
                self.data_generator.create_database_genomic_user_event_metrics(
                    participant_id=i,
                    created_at=datetime.datetime(2022, 10, 6, 00),
                    event_name="hdr.opened_at",
                    run_id=1,
                )

        genomic_pipeline.reconcile_message_broker_results_viewed()

        # Test correct data inserted
        result_viewed_dao = GenomicResultViewedDao()
        results = result_viewed_dao.get_all()

        self.assertEqual(2, len(results))

        for record in results:
            if record.participant_id == 1:
                self.assertEqual("pgx_v1", record.module_type)
            else:
                self.assertEqual("hdr_v1", record.module_type)
            self.assertEqual(int(record.sample_id), record.participant_id + 10)
            self.assertEqual("result_viewed", record.event_type)
            self.assertEqual(datetime.datetime(2022, 10, 6, 00), record.first_viewed)
            self.assertIsNotNone(record.created_from_metric_id)

    def test_ingest_appointment_metrics_file(self):
        test_file = 'Genomic-Metrics-File-Appointment-Events-Test.json'
        bucket_name = 'test_bucket'
        sub_folder = 'appointment_events'
        pids = []

        for _ in range(4):
            summary = self.data_generator.create_database_participant_summary()
            pids.append(summary.participantId)

        test_file_path = f'{bucket_name}/{sub_folder}/{test_file}'

        appointment_data = test_data.load_test_data_json(
            "Genomic-Metrics-File-Appointment-Events-Test.json")
        appointment_data_str = json.dumps(appointment_data, indent=4)

        with open_cloud_file(test_file_path, mode='wb') as cloud_file:
            cloud_file.write(appointment_data_str.encode("utf-8"))

        with GenomicJobController(GenomicJob.APPOINTMENT_METRICS_FILE_INGEST) as controller:
            controller.ingest_appointment_metrics_file(
                file_path=test_file_path,
            )

        all_metrics = self.appointment_metrics_dao.get_all()

        # should be 5 metric records for whats in json file
        self.assertEqual(len(all_metrics), 5)
        self.assertTrue(all((obj.participant_id in pids for obj in all_metrics)))
        self.assertTrue(all((obj.file_path == test_file_path for obj in all_metrics)))
        self.assertTrue(all((obj.appointment_event is not None for obj in all_metrics)))
        self.assertTrue(all((obj.created is not None for obj in all_metrics)))
        self.assertTrue(all((obj.modified is not None for obj in all_metrics)))
        self.assertTrue(all((obj.module_type is not None for obj in all_metrics)))
        self.assertTrue(all((obj.event_authored_time is not None for obj in all_metrics)))
        self.assertTrue(all((obj.event_type is not None for obj in all_metrics)))

        current_job_runs = self.job_run_dao.get_all()
        self.assertEqual(len(current_job_runs), 1)

        current_job_run = current_job_runs[0]
        self.assertTrue(current_job_run.jobId == GenomicJob.APPOINTMENT_METRICS_FILE_INGEST)
        self.assertTrue(current_job_run.runResult == GenomicSubProcessResult.SUCCESS)

        self.clear_table_after_test('genomic_appointment_event_metrics')

    def test_reconcile_appointments_with_metrics(self):
        fake_date = parser.parse('2020-05-29T08:00:01-05:00')

        for num in range(4):
            summary = self.data_generator.create_database_participant_summary()

            missing_json = {
                "event": "appointment_updated",
                "eventAuthoredTime": "2022-09-16T17:18:38Z",
                "participantId": f'P{summary.participantId}',
                "messageBody": {
                    "module_type": "hdr",
                    "appointment_timestamp": "2022-09-19T19:30:00+00:00",
                    "id": 55,
                    "appointment_timezone": "America/Los_Angeles",
                    "location": "CA",
                    "contact_number": "18043704252",
                    "language": "en",
                    "source": "Color"
                }
            }

            if num % 2 == 0:
                self.data_generator.create_database_genomic_appointment(
                    message_record_id=num,
                    appointment_id=num,
                    event_type='appointment_scheduled',
                    module_type='hdr',
                    participant_id=summary.participantId,
                    event_authored_time=fake_date,
                    source='Color',
                    appointment_timestamp=format_datetime(clock.CLOCK.now()),
                    appointment_timezone='America/Los_Angeles',
                    location='123 address st',
                    contact_number='17348675309',
                    language='en'
                )

            self.data_generator.create_database_genomic_appointment_metric(
                participant_id=summary.participantId,
                appointment_event=json.dumps(missing_json, indent=4) if num % 2 != 0 else 'foo',
                file_path='test_file_path',
                module_type='hdr',
                event_authored_time=fake_date,
                event_type='appointment_updated' if num % 2 != 0 else 'appointment_scheduled'
            )

        current_events = self.appointment_event_dao.get_all()
        # should be 2 initial appointment events
        self.assertEqual(len(current_events), 2)

        current_metrics = self.appointment_metrics_dao.get_all()
        # should be 4 initial appointment events
        self.assertEqual(len(current_metrics), 4)
        self.assertTrue(all(obj.reconcile_job_run_id is None for obj in current_metrics))

        with GenomicJobController(GenomicJob.APPOINTMENT_METRICS_RECONCILE) as controller:
            controller.reconcile_appointment_events_from_metrics()

        job_run = self.job_run_dao.get_all()
        self.assertEqual(len(job_run), 1)
        self.assertTrue(job_run[0].jobId == GenomicJob.APPOINTMENT_METRICS_RECONCILE)

        current_events = self.appointment_event_dao.get_all()
        # should be 4  appointment events 2 initial + 2 added
        self.assertEqual(len(current_events), 4)

        scheduled = list(filter(lambda x: x.event_type == 'appointment_scheduled', current_events))
        self.assertEqual(len(scheduled), 2)
        self.assertTrue(all(obj.created_from_metric_id is None for obj in scheduled))

        updated = list(filter(lambda x: x.event_type == 'appointment_updated', current_events))
        self.assertEqual(len(updated), 2)
        self.assertTrue(all(obj.created_from_metric_id is not None for obj in updated))

        current_metrics = self.appointment_metrics_dao.get_all()
        # should STILL be 4 initial appointment events
        self.assertEqual(len(current_metrics), 4)
        self.assertTrue(all(obj.reconcile_job_run_id is not None for obj in current_metrics))
        self.assertTrue(all(obj.reconcile_job_run_id == job_run[0].id for obj in current_metrics))

        self.clear_table_after_test('genomic_appointment_event_metrics')

    @mock.patch('rdr_service.services.email_service.EmailService.send_email')
    def test_check_appointments_gror_changed(self, email_mock):
        fake_date = parser.parse("2022-09-01T13:43:23")
        notified_dao = GenomicAppointmentEventNotifiedDao()
        config.override_setting(config.GENOMIC_COLOR_PM_EMAIL, ['test@example.com'])
        num_participants = 4
        for num in range(num_participants):
            gror = num if num > 1 else 1
            summary = self.data_generator.create_database_participant_summary(
                consentForStudyEnrollment=1,
                consentForGenomicsROR=gror
            )
            self.data_generator.create_database_genomic_appointment(
                message_record_id=num,
                appointment_id=num,
                event_type='appointment_scheduled',
                module_type='hdr',
                participant_id=summary.participantId,
                event_authored_time=fake_date,
                source='Color',
                appointment_timestamp=format_datetime(clock.CLOCK.now()),
                appointment_timezone='America/Los_Angeles',
                location='123 address st',
                contact_number='17348675309',
                language='en'
            )

        changed_ppts = self.appointment_event_dao.get_appointments_gror_changed()
        self.assertEqual(2, len(changed_ppts))
        with genomic_pipeline.GenomicJobController(GenomicJob.CHECK_APPOINTMENT_GROR_CHANGED) as controller:
            controller.check_appointments_gror_changed()

        self.assertEqual(email_mock.call_count, 1)
        notified_appointments = notified_dao.get_all()
        self.assertEqual(2, len(notified_appointments))

        # test notified not returned by query
        summary = self.data_generator.create_database_participant_summary(
            consentForStudyEnrollment=1,
            consentForGenomicsROR=2
        )
        self.data_generator.create_database_genomic_appointment(
            message_record_id=5,
            appointment_id=5,
            event_type='appointment_scheduled',
            module_type='hdr',
            participant_id=summary.participantId,
            event_authored_time=fake_date,
            source='Color',
            appointment_timestamp=format_datetime(clock.CLOCK.now()),
            appointment_timezone='America/Los_Angeles',
            location='123 address st',
            contact_number='17348675309',
            language='en'
        )

        changed_ppts = self.appointment_event_dao.get_appointments_gror_changed()
        self.assertEqual(1, len(changed_ppts))

    @mock.patch('rdr_service.services.email_service.EmailService.send_email')
    def test_check_gcr_14day_escalation(self, email_mock):
        fake_date = parser.parse("2022-09-01T13:43:23")
        fake_date2 = parser.parse("2022-09-02T14:14:00")
        fake_date3 = parser.parse("2022-09-03T15:15:00")
        config.override_setting(config.GENOMIC_GCR_ESCALATION_EMAILS, ['test@example.com'])
        self.data_generator.create_database_genomic_set(
            genomicSetName='test',
            genomicSetCriteria='.',
            genomicSetVersion=1
        )
        pids = []
        for _ in range(5):
            summary = self.data_generator.create_database_participant_summary(
                consentForStudyEnrollment=1,
                consentForGenomicsROR=1
            )
            set_member = self.data_generator.create_database_genomic_set_member(
                participantId=summary.participantId,
                genomicSetId=1,
                biobankId=1001,
                collectionTubeId=100,
                sampleId=10,
                genomeType="aou_wgs",
            )
            self.data_generator.create_database_genomic_member_report_state(
                participant_id=summary.participantId,
                genomic_report_state=GenomicReportState.HDR_RPT_POSITIVE,
                genomic_set_member_id=set_member.id,
                module='hdr_v1',
                event_authored_time=fake_date
            )
            pids.append(summary.participantId)

        # Appointment scheduled in future: don't notify
        self.data_generator.create_database_genomic_appointment(
            message_record_id=101,
            appointment_id=102,
            event_type='appointment_scheduled',
            module_type='hdr',
            participant_id=pids[0],
            event_authored_time=fake_date,
            source='Color',
            appointment_timestamp=format_datetime(clock.CLOCK.now()),
            appointment_timezone='America/Los_Angeles',
            location='123 address st',
            contact_number='17348675309',
            language='en'
        )

        # Appointment completed: don't notify
        self.data_generator.create_database_genomic_appointment(
            message_record_id=102,
            appointment_id=103,
            event_type='appointment_completed',
            module_type='hdr',
            participant_id=pids[1],
            event_authored_time=fake_date,
            source='Color',
            appointment_timestamp=fake_date,
            appointment_timezone='America/Los_Angeles',
            location='123 address st',
            contact_number='17348675309',
            language='en'
        )

        # Appointment scheduled then canceled: notify
        self.data_generator.create_database_genomic_appointment(
            message_record_id=103,
            appointment_id=104,
            event_type='appointment_scheduled',
            module_type='hdr',
            participant_id=pids[2],
            event_authored_time=fake_date2,
            source='Color',
            appointment_timestamp=format_datetime(clock.CLOCK.now()),
            appointment_timezone='America/Los_Angeles',
            location='123 address st',
            contact_number='17348675309',
            language='en'
        )
        self.data_generator.create_database_genomic_appointment(
            message_record_id=104,
            appointment_id=104,
            event_type='appointment_cancelled',
            module_type='hdr',
            participant_id=pids[2],
            event_authored_time=fake_date3,
            source='Color',
            appointment_timestamp=format_datetime(clock.CLOCK.now()),
            appointment_timezone='America/Los_Angeles',
            location='123 address st',
            contact_number='17348675309',
            language='en'
        )

        notified_dao = GenomicGCROutreachEscalationNotifiedDao()
        notified_dao.insert_bulk([{
            'participant_id': pids[4],
            'created': clock.CLOCK.now(),
            'modified': clock.CLOCK.now()
        }])

        with clock.FakeClock(parser.parse('2022-11-1T05:15:00')):
            results = self.report_state_dao.get_hdr_result_positive_no_appointment()
        self.assertIn(pids[2], results)
        self.assertIn(pids[3], results)
        self.assertNotIn(pids[0], results)
        self.assertNotIn(pids[1], results)
        self.assertNotIn(pids[4], results)

        with genomic_pipeline.GenomicJobController(GenomicJob.CHECK_GCR_OUTREACH_ESCALATION) as controller:
            controller.check_gcr_14day_escalation()

        self.assertEqual(email_mock.call_count, 2)
