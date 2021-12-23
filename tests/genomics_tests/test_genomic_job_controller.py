from datetime import datetime
import mock

from rdr_service import clock
from rdr_service.api_util import open_cloud_file
from rdr_service.dao.genomics_dao import GenomicGCValidationMetricsDao, GenomicIncidentDao, GenomicInformingLoopDao, \
    GenomicGcDataFileDao, GenomicSetMemberDao, UserEventMetricsDao
from rdr_service.dao.message_broker_dao import MessageBrokenEventDataDao
from rdr_service.genomic_enums import GenomicIncidentCode, GenomicJob, GenomicWorkflowState, GenomicSubProcessResult
from rdr_service.genomic.genomic_job_components import GenomicFileIngester
from rdr_service.genomic.genomic_job_controller import GenomicJobController
from rdr_service.model.genomics import GenomicGcDataFile, GenomicIncident
from tests.genomics_tests.test_genomic_pipeline import create_ingestion_test_file
from tests.helpers.unittest_base import BaseTestCase


class GenomicJobControllerTest(BaseTestCase):
    def setUp(self):
        super(GenomicJobControllerTest, self).setUp()
        self.data_file_dao = GenomicGcDataFileDao()
        self.event_data_dao = MessageBrokenEventDataDao()
        self.incident_dao = GenomicIncidentDao()
        self.informing_loop_dao = GenomicInformingLoopDao()
        self.member_dao = GenomicSetMemberDao()
        self.metrics_dao = GenomicGCValidationMetricsDao()
        self.user_event_metrics_dao = UserEventMetricsDao()

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

        self.assertIsNotNone(metrics.gvcfMd5Received)
        self.assertIsNotNone(metrics.gvcfMd5Path)
        self.assertEqual(metrics.gvcfMd5Path, full_path_md5)
        self.assertEqual(metrics.gvcfMd5Received, 1)

        job_controller.ingest_data_files_into_gc_metrics(file_path, bucket_name)

        metrics = self.metrics_dao.get_metrics_by_member_id(gen_member.id)

        self.assertIsNotNone(metrics.gvcfReceived)
        self.assertIsNotNone(metrics.gvcfPath)
        self.assertEqual(metrics.gvcfPath, full_path)
        self.assertEqual(metrics.gvcfReceived, 1)

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

        test_time = datetime(2021, 7, 9, 14, 1, 1)

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

    def test_informing_loop_ingestion(self):
        loop_decision = 'informing_loop_decision'
        loop_started = 'informing_loop_started'

        participant = self.data_generator.create_database_participant()

        message_broker_record = self.data_generator.create_database_message_broker_record(
            participantId=participant.participantId,
            eventType=loop_decision,
            eventAuthoredTime=clock.CLOCK.now(),
            messageOrigin='example@example.com',
            requestBody={'module_type': 'hdr', 'decision_value': 'yes'},
            requestTime=clock.CLOCK.now(),
            responseError='',
            responseCode='200',
            responseTime=clock.CLOCK.now()
        )

        for key, value in message_broker_record.requestBody.items():
            self.data_generator.create_database_message_broker_event_data(
                participantId=message_broker_record.participantId,
                messageRecordId=message_broker_record.id,
                eventType=message_broker_record.eventType,
                eventAuthoredTime=message_broker_record.eventAuthoredTime,
                fieldName=key,
                valueString=value
            )

        with GenomicJobController(GenomicJob.INGEST_INFORMING_LOOP) as controller:
            controller.ingest_informing_loop_records(
                message_record_id=message_broker_record.id,
                loop_type=loop_decision
            )

        decision_genomic_record = self.informing_loop_dao.get(1)

        self.assertIsNotNone(decision_genomic_record)
        self.assertIsNotNone(decision_genomic_record.event_type)
        self.assertIsNotNone(decision_genomic_record.module_type)
        self.assertIsNotNone(decision_genomic_record.decision_value)

        self.assertEqual(decision_genomic_record.message_record_id, message_broker_record.id)
        self.assertEqual(decision_genomic_record.participant_id, message_broker_record.participantId)
        self.assertEqual(decision_genomic_record.event_type, loop_decision)
        self.assertEqual(decision_genomic_record.module_type, 'hdr')
        self.assertEqual(decision_genomic_record.decision_value, 'yes')

        message_broker_record_two = self.data_generator.create_database_message_broker_record(
            participantId=participant.participantId,
            eventType=loop_started,
            eventAuthoredTime=clock.CLOCK.now(),
            messageOrigin='example@example.com',
            requestBody={'module_type': 'hdr'},
            requestTime=clock.CLOCK.now(),
            responseError='',
            responseCode='200',
            responseTime=clock.CLOCK.now()
        )

        for key, value in message_broker_record_two.requestBody.items():
            self.data_generator.create_database_message_broker_event_data(
                participantId=message_broker_record_two.participantId,
                messageRecordId=message_broker_record_two.id,
                eventType=message_broker_record_two.eventType,
                eventAuthoredTime=message_broker_record_two.eventAuthoredTime,
                fieldName=key,
                valueString=value
            )

        with GenomicJobController(GenomicJob.INGEST_INFORMING_LOOP) as controller:
            controller.ingest_informing_loop_records(
                message_record_id=message_broker_record_two.id,
                loop_type=loop_started
            )

        started_genomic_record = self.informing_loop_dao.get(2)

        self.assertIsNotNone(started_genomic_record)
        self.assertIsNotNone(started_genomic_record.event_type)
        self.assertIsNotNone(started_genomic_record.module_type)
        self.assertIsNone(started_genomic_record.decision_value)

        self.assertEqual(started_genomic_record.message_record_id, message_broker_record_two.id)
        self.assertEqual(started_genomic_record.participant_id, message_broker_record_two.participantId)
        self.assertEqual(started_genomic_record.event_type, loop_started)
        self.assertEqual(started_genomic_record.module_type, 'hdr')

    def test_updating_members_blocklists(self):

        gen_set = self.data_generator.create_database_genomic_set(
            genomicSetName=".",
            genomicSetCriteria=".",
            genomicSetVersion=1
        )

        for i in range(4):
            self.data_generator.create_database_genomic_set_member(
                genomicSetId=gen_set.id,
                biobankId="100153482",
                sampleId="21042005280",
                genomeType="aou_wgs",
                genomicWorkflowState=GenomicWorkflowState.AW0,
                ai_an='Y' if i & 2 == 0 else 'N'
            )

        with GenomicJobController(GenomicJob.UPDATE_MEMBERS_BLOCKLISTS) as controller:
            controller.update_members_blocklists()

        current_members = self.member_dao.get_all()

        self.assertTrue(all(
            obj.blockResearch == 1 and obj.blockResearchReason is not None
            for obj in current_members if obj.ai_an == 'Y')
        )

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
            self.assertTrue(obj.run_id == job_run_id for obj in file_metrics)

