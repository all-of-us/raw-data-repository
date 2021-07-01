import mock

from rdr_service import clock
from rdr_service.dao.genomics_dao import GenomicGCValidationMetricsDao, GenomicIncidentDao, GenomicInformingLoopDao
from rdr_service.dao.message_broker_dao import MessageBrokenEventDataDao
from rdr_service.genomic_enums import GenomicIncidentCode, GenomicJob, GenomicWorkflowState, GenomicSubProcessResult
from rdr_service.genomic.genomic_job_controller import GenomicIncident, GenomicJobController
from tests.helpers.unittest_base import BaseTestCase


class GenomicJobControllerTest(BaseTestCase):
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
        metrics_dao = GenomicGCValidationMetricsDao()
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

        job_controller.ingest_data_files(file_path_md5, bucket_name)

        metrics = metrics_dao.get_metrics_by_member_id(gen_member.id)

        self.assertIsNotNone(metrics.gvcfMd5Received)
        self.assertIsNotNone(metrics.gvcfMd5Path)
        self.assertEqual(metrics.gvcfMd5Path, full_path_md5)
        self.assertEqual(metrics.gvcfMd5Received, 1)

        job_controller.ingest_data_files(file_path, bucket_name)

        metrics = metrics_dao.get_metrics_by_member_id(gen_member.id)

        self.assertIsNotNone(metrics.gvcfReceived)
        self.assertIsNotNone(metrics.gvcfPath)
        self.assertEqual(metrics.gvcfPath, full_path)
        self.assertEqual(metrics.gvcfReceived, 1)

    def test_gvcf_files_ingestion_create_incident(self):
        incident_dao = GenomicIncidentDao()
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
            controller.ingest_data_files(file_path, bucket_name)

        incident = incident_dao.get(1)
        self.assertIsNotNone(incident)
        self.assertEqual(incident.code, GenomicIncidentCode.UNABLE_TO_FIND_METRIC.name)
        self.assertEqual(incident.data_file_path, file_path)
        self.assertEqual(incident.message, 'INGEST_DATA_FILES: Cannot find '
                                           'genomics metric record for sample id: '
                                           '21042005280')

    def test_informing_loop_ingestion(self):

        informing_loop_dao = GenomicInformingLoopDao()
        event_data_dao = MessageBrokenEventDataDao()

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

        loop_decision_records = event_data_dao.get_informing_loop(
            message_broker_record.id,
            loop_decision
        )

        with GenomicJobController(GenomicJob.INGEST_INFORMING_LOOP) as controller:
            controller.ingest_informing_loop_records(
                loop_type=loop_decision,
                records=loop_decision_records
            )

        decision_genomic_record = informing_loop_dao.get(1)

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

        loop_started_records = event_data_dao.get_informing_loop(
            message_broker_record_two.id,
            loop_started
        )

        with GenomicJobController(GenomicJob.INGEST_INFORMING_LOOP) as controller:
            controller.ingest_informing_loop_records(
                loop_type=loop_started,
                records=loop_started_records
            )

        started_genomic_record = informing_loop_dao.get(2)

        self.assertIsNotNone(started_genomic_record)
        self.assertIsNotNone(started_genomic_record.event_type)
        self.assertIsNotNone(started_genomic_record.module_type)
        self.assertIsNone(started_genomic_record.decision_value)

        self.assertEqual(started_genomic_record.message_record_id, message_broker_record_two.id)
        self.assertEqual(started_genomic_record.participant_id, message_broker_record_two.participantId)
        self.assertEqual(started_genomic_record.event_type, loop_started)
        self.assertEqual(started_genomic_record.module_type, 'hdr')

