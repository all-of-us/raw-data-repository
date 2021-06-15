import mock

from rdr_service import clock
from rdr_service.dao.genomics_dao import GenomicGCValidationMetricsDao, GenomicIncidentDao
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
        self.assertEqual(incident.slack_notification, 1)
        self.assertIsNotNone(incident.slack_notification_date)
        self.assertEqual(incident.code, GenomicIncidentCode.UNABLE_TO_FIND_METRIC.name)
        self.assertEqual(incident.data_file_path, file_path)
        self.assertEqual(incident.message, 'Cannot find genomics metric record for sample id: 21042005280')


