# Tests for the Genomics Data Quality Pipeline
import mock, datetime, pytz

from rdr_service import clock, config
from rdr_service.api_util import open_cloud_file
from rdr_service.dao.genomics_dao import GenomicIncidentDao
from rdr_service.genomic_enums import GenomicJob, GenomicSubProcessStatus, GenomicSubProcessResult, \
    GenomicManifestTypes, GenomicIncidentCode
from tests.helpers.unittest_base import BaseTestCase
from rdr_service.genomic.genomic_job_controller import DataQualityJobController
from rdr_service.genomic.genomic_data_quality_components import ReportingComponent


class GenomicDataQualityJobControllerTest(BaseTestCase):
    def setUp(self, with_data=True, with_consent_codes=False) -> None:
        super().setUp()

    @mock.patch('rdr_service.genomic.genomic_job_controller.genomic_job_run_update')
    @mock.patch('rdr_service.genomic.genomic_job_controller.bq_genomic_job_run_update')
    @mock.patch('rdr_service.dao.genomics_dao.GenomicJobRunDao.insert_run_record')
    @mock.patch('rdr_service.dao.genomics_dao.GenomicJobRunDao.update_run_record')
    def test_data_quality_job_controller_creation(self, job_update_mock, job_insert_mock,
                                                  bq_update_mock, resource_update_mock):
        new_run = mock.Mock()
        new_run.id = 1
        job_insert_mock.return_value = new_run

        # Test context manager works correctly
        with DataQualityJobController(GenomicJob.DAILY_SUMMARY_REPORT_JOB_RUNS):
            pass

        job_insert_mock.assert_called_with(GenomicJob.DAILY_SUMMARY_REPORT_JOB_RUNS)
        job_update_mock.assert_called_with(new_run.id,
                                           GenomicSubProcessResult.UNSET,
                                           GenomicSubProcessStatus.COMPLETED)

        bq_update_mock.assert_called()
        resource_update_mock.assert_called()

    @mock.patch('rdr_service.genomic.genomic_job_controller.DataQualityJobController.get_report')
    def test_controller_job_registry(self, report_job_mock):
        with DataQualityJobController(GenomicJob.DAILY_SUMMARY_REPORT_JOB_RUNS) as controller:
            controller.execute_workflow()

        report_job_mock.assert_called_once()


class GenomicDataQualityComponentTest(BaseTestCase):
    def setUp(self, with_data=True, with_consent_codes=False) -> None:
        super().setUp()

        self.fake_time = datetime.datetime(2021, 2, 1, 0, 0, 0, 0, tzinfo=pytz.timezone("UTC"))

    def test_reporting_component_get_report_def_from_date(self):
        rc = ReportingComponent()

        with clock.FakeClock(self.fake_time):

            query_module = 'rdr_service.genomic.genomic_data.GenomicQueryClass.dq_report_runs_summary'

            with mock.patch(query_module) as query_def:

                query_def.return_value = ("", {})

                # Report defs for reports
                report_def_d = rc.set_report_def(level="SUMMARY", target="RUNS", time_frame="D")
                report_def_w = rc.set_report_def(level="SUMMARY", target="RUNS", time_frame="W")

        exp_from_date_d = self.fake_time - datetime.timedelta(days=1)
        exp_from_date_w = self.fake_time - datetime.timedelta(days=7)

        self.assertEqual(exp_from_date_d, report_def_d.from_date)
        self.assertEqual(exp_from_date_w, report_def_w.from_date)

    @staticmethod
    def test_reporting_component_get_report_def_query():
        rc = ReportingComponent()

        # Report defs to test (QUERY, LEVEL, TARGET, TIME_FRAME)
        test_definitions = (
            ("dq_report_runs_summary", {"level": "SUMMARY", "target": "RUNS", "time_frame": "D"}),
        )

        for test_def in test_definitions:

            query_class = 'rdr_service.genomic.genomic_data.GenomicQueryClass'
            query_class += f".{test_def[0]}"

            with mock.patch(query_class) as query_mock:

                query_mock.return_value = ("", {})
                rc.set_report_def(**test_def[1])

                query_mock.assert_called()

    def test_reporting_component_summary_runs_query(self):

        # set up genomic job runs for report
        def_fields = ("jobId", "startTime", "runResult")
        run_defs = (
            (GenomicJob.METRICS_INGESTION,  self.fake_time, GenomicSubProcessResult.SUCCESS),
            (GenomicJob.METRICS_INGESTION, self.fake_time, GenomicSubProcessResult.SUCCESS),
            (GenomicJob.METRICS_INGESTION, self.fake_time, GenomicSubProcessResult.ERROR),
            (GenomicJob.AW1_MANIFEST, self.fake_time, GenomicSubProcessResult.SUCCESS),
            (GenomicJob.AW1_MANIFEST, self.fake_time, GenomicSubProcessResult.ERROR),
            (GenomicJob.AW1_MANIFEST, self.fake_time, GenomicSubProcessResult.ERROR),
        )

        for run_def in run_defs:
            def_dict = dict(zip(def_fields, run_def))
            self.data_generator.create_database_genomic_job_run(**def_dict)

        # Generate report with ReportingComponent
        rc = ReportingComponent()

        report_ran_time = self.fake_time + datetime.timedelta(hours=6)

        rc.set_report_def(level="SUMMARY", target="RUNS", time_frame="D")

        with clock.FakeClock(report_ran_time):
            report_data = rc.get_report_data()

        # Get the genomic_job_run records
        for row in report_data:
            if row['job_id'] == 1:
                self.assertEqual(0, row['UNSET'])
                self.assertEqual(2, row['SUCCESS'])
                self.assertEqual(1, row['ERROR'])

            if row['job_id'] == 8:
                self.assertEqual(0, row['UNSET'])
                self.assertEqual(1, row['SUCCESS'])
                self.assertEqual(2, row['ERROR'])

            self.assertEqual(0, row['NO_FILES'])
            self.assertEqual(0, row['INVALID_FILE_NAME'])
            self.assertEqual(0, row['INVALID_FILE_STRUCTURE'])


class GenomicDataQualityReportTest(BaseTestCase):
    def setUp(self, with_data=False, with_consent_codes=False) -> None:
        super().setUp()
        self.incident_dao = GenomicIncidentDao()

    def test_daily_ingestion_summary(self):
        # Set up test data
        bucket_name = "test-bucket"
        aw1_file_name = "AW1_wgs_sample_manifests/RDR_AoU_SEQ_PKG-2104-026571.csv"
        aw1_manifest_path = f"{bucket_name}/{aw1_file_name}"

        aw2_file_name = "AW2_wgs_data_manifests/RDR_AoU_SEQ_DataManifest_04092021.csv"
        aw2_manifest_path = f"{bucket_name}/{aw2_file_name}"

        # Create AW1 job_run
        aw1_job_run = self.data_generator.create_database_genomic_job_run(
            jobId=GenomicJob.AW1_MANIFEST,
            startTime=clock.CLOCK.now(),
            runResult=GenomicSubProcessResult.SUCCESS
        )

        # Create AW2 job_run
        aw2_job_run = self.data_generator.create_database_genomic_job_run(
            jobId=GenomicJob.METRICS_INGESTION,
            startTime=clock.CLOCK.now(),
            runResult=GenomicSubProcessResult.SUCCESS
        )

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
        self.data_generator.create_database_genomic_file_processed(
            runId=aw1_job_run.id,
            startTime=clock.CLOCK.now(),
            genomicManifestFileId=aw1_manifest_file.id,
            filePath=f"/{aw1_manifest_path}",
            bucketName=bucket_name,
            fileName=aw1_file_name,
        )

        # Create AW2 file_processed
        self.data_generator.create_database_genomic_file_processed(
            runId=aw2_job_run.id,
            startTime=clock.CLOCK.now(),
            genomicManifestFileId=aw2_manifest_file.id,
            filePath=f"/{aw2_manifest_path}",
            bucketName=bucket_name,
            fileName=aw2_file_name,
        )

        with DataQualityJobController(GenomicJob.DAILY_SUMMARY_REPORT_INGESTIONS) as controller:
            report_output = controller.execute_workflow()

        expected_report = "```Daily Ingestions Summary\n"
        expected_report += "record_count    ingested_count    incident_count    "
        expected_report += "file_type    gc_site_id    genome_type    file_path\n"
        expected_report += "1    0    0    aw1    rdr    aou_wgs    "
        expected_report += f"{aw1_manifest_path}\n"
        expected_report += "1    0    0    aw2    rdr    aou_wgs    "
        expected_report += f"{aw2_manifest_path}"
        expected_report += "\n```"

        self.assertEqual(expected_report, report_output)

    @mock.patch('rdr_service.services.slack_utils.SlackMessageHandler.send_message_to_webhook')
    @mock.patch('rdr_service.genomic.genomic_data_quality_components.ReportingComponent.get_report_data')
    @mock.patch('rdr_service.genomic.genomic_data_quality_components.ReportingComponent.format_report')
    def test_report_slack_integration(self, format_mock, report_data_mock, slack_handler_mock):

        # Mock the generated report
        expected_report = "record_count    ingested_count    incident_count    "
        expected_report += "file_type    gc_site_id    genome_type    file_path\n"
        expected_report += "1    0    0    aw1    rdr    aou_wgs    "
        expected_report += "test-bucket/AW1_wgs_sample_manifests/RDR_AoU_SEQ_PKG-2104-026571.csv"
        expected_report += "\n"

        report_data_mock.return_value = None  # skip running the report query
        format_mock.return_value = expected_report

        # Run the workflow
        with DataQualityJobController(GenomicJob.DAILY_SUMMARY_REPORT_INGESTIONS) as controller:
            controller.execute_workflow(slack=True)

        # Test the slack API was called correctly
        slack_handler_mock.assert_called_with(message_data={'text': expected_report})

    def test_daily_ingestion_summary_no_files(self):
        with DataQualityJobController(GenomicJob.DAILY_SUMMARY_REPORT_INGESTIONS) as controller:
            report_output = controller.execute_workflow()

        expected_report = "No data to display for Daily Ingestions Summary"

        self.assertEqual(expected_report, report_output)

    @mock.patch('rdr_service.genomic.genomic_data_quality_components.ReportingComponent.format_report')
    def test_daily_ingestion_summary_long_report(self, format_mock):

        format_mock.return_value = "test\n" * 30

        with DataQualityJobController(GenomicJob.DAILY_SUMMARY_REPORT_INGESTIONS) as controller:
            report_output = controller.execute_workflow(slack=True)

        expected_report = "test\n" * 30

        with open_cloud_file(report_output, 'r') as report_file:
            report_file_data = report_file.read()

        self.assertEqual(expected_report, report_file_data)

    def test_daily_incident_report(self):
        # timeframes
        time_1 = datetime.datetime(2021, 5, 13, 0, 0, 0, 0)
        time_2 = time_1 - datetime.timedelta(days=2)

        # Set up test data
        # Create AW1 job_run
        aw1_job_run = self.data_generator.create_database_genomic_job_run(
            jobId=GenomicJob.AW1_MANIFEST,
            startTime=time_1,
            runResult=GenomicSubProcessResult.SUCCESS
        )

        with clock.FakeClock(time_1):
            # Incident included in report
            self.data_generator.create_database_genomic_incident(
                code=GenomicIncidentCode.UNABLE_TO_FIND_MEMBER.name,
                message='test message',
                source_job_run_id=aw1_job_run.id,
                biobank_id="10001",
                sample_id="20001",
                collection_tube_id="30001",
            )

        with clock.FakeClock(time_2):
            # Incident excluded from report
            self.data_generator.create_database_genomic_incident(
                code=GenomicIncidentCode.UNABLE_TO_FIND_MEMBER.name,
                message='test message 2',
                source_job_run_id=aw1_job_run.id,
                biobank_id="10002",
                sample_id="20002",
                collection_tube_id="30002",
            )

        with clock.FakeClock(time_1):
            with DataQualityJobController(GenomicJob.DAILY_SUMMARY_REPORT_INCIDENTS) as controller:
                report_output = controller.execute_workflow()

        expected_report = "```Daily Incidents Summary\n"
        expected_report += "code    created    biobank_id    genomic_set_member_id    " \
                           "source_job_run_id    source_file_processed_id\n"
        expected_report += "UNABLE_TO_FIND_MEMBER    2021-05-13 00:00:00    10001    None    1    None"
        expected_report += "\n```"

        self.assertEqual(expected_report, report_output)

    # def test_daily_resolved_manifests_report(self):
    #     file_name = 'test_file_name.csv'
    #     bucket_name = 'test_bucket'
    #     sub_folder = 'test_subfolder'
    #
    #     from_date = clock.CLOCK.now() - datetime.timedelta(days=1)
    #
    #     with clock.FakeClock(from_date):
    #         for _ in range(5):
    #             gen_job_run = self.data_generator.create_database_genomic_job_run(
    #                 jobId=GenomicJob.METRICS_INGESTION,
    #                 startTime=clock.CLOCK.now(),
    #                 runResult=GenomicSubProcessResult.SUCCESS
    #             )
    #
    #             gen_processed_file = self.data_generator.create_database_genomic_file_processed(
    #                 runId=gen_job_run.id,
    #                 startTime=clock.CLOCK.now(),
    #                 filePath=f"{bucket_name}/{sub_folder}/{file_name}",
    #                 bucketName=bucket_name,
    #                 fileName=file_name,
    #             )
    #
    #             self.data_generator.create_database_genomic_incident(
    #                 source_job_run_id=gen_job_run.id,
    #                 source_file_processed_id=gen_processed_file.id,
    #                 code=GenomicIncidentCode.FILE_VALIDATION_INVALID_FILE_NAME.name,
    #                 message=f"{gen_job_run.jobId}: File name {file_name} has failed validation.",
    #             )
    #
    #     self.incident_dao.batch_update_incident_fields(
    #         [obj.id for obj in self.incident_dao.get_all()],
    #         _type='resolved'
    #     )
    #
    #     with clock.FakeClock(from_date):
    #         with DataQualityJobController(GenomicJob.DAILY_SUMMARY_VALIDATION_FAILS_RESOLVED) as controller:
    #             report_output = controller.execute_workflow()
    #
    #     print('Darryl')

    @mock.patch('rdr_service.services.email_service.EmailService.send_email')
    def test_send_daily_validation_emails(self, email_mock):

        job_id = 'METRICS_INGESTION'
        file_name = 'test_file_name'
        bucket_name = 'test_bucket'
        sub_folder = 'test_subfolder'

        current_incidents_for_emails = self.incident_dao.get_new_ingestion_incidents()

        with DataQualityJobController(GenomicJob.DAILY_SEND_VALIDATION_EMAILS) as controller:
            controller.execute_workflow()

        self.assertEqual(email_mock.call_count, len(current_incidents_for_emails))

        today = clock.CLOCK.now()

        from_date = today - datetime.timedelta(days=1)
        from_date = from_date + datetime.timedelta(hours=6)
        from_date = from_date.replace(microsecond=0)

        gen_job_run_one = self.data_generator.create_database_genomic_job_run(
            jobId=GenomicJob.METRICS_INGESTION,
            startTime=clock.CLOCK.now(),
            runResult=GenomicSubProcessResult.SUCCESS
        )

        gen_processed_file_one = self.data_generator.create_database_genomic_file_processed(
            runId=gen_job_run_one.id,
            startTime=clock.CLOCK.now(),
            filePath=f"{bucket_name}/{sub_folder}/{file_name}",
            bucketName=bucket_name,
            fileName=file_name,
        )

        with clock.FakeClock(from_date):
            self.data_generator.create_database_genomic_incident(
                source_job_run_id=gen_job_run_one.id,
                source_file_processed_id=gen_processed_file_one.id,
                code=GenomicIncidentCode.FILE_VALIDATION_INVALID_FILE_NAME.name,
                message=f"{job_id}: File name {file_name} has failed validation due to an"
                        f" incorrect file name.",
                submitted_gc_site_id='bcm'
            )

        gen_job_run_two = self.data_generator.create_database_genomic_job_run(
            jobId=GenomicJob.METRICS_INGESTION,
            startTime=clock.CLOCK.now(),
            runResult=GenomicSubProcessResult.SUCCESS
        )

        gen_processed_file_two = self.data_generator.create_database_genomic_file_processed(
            runId=gen_job_run_two.id,
            startTime=clock.CLOCK.now(),
            filePath=f"{bucket_name}/{sub_folder}/{file_name}",
            bucketName=bucket_name,
            fileName=file_name,
        )

        with clock.FakeClock(from_date):
            self.data_generator.create_database_genomic_incident(
                source_job_run_id=gen_job_run_two.id,
                source_file_processed_id=gen_processed_file_two.id,
                code=GenomicIncidentCode.FILE_VALIDATION_FAILED_STRUCTURE.name,
                message=f"{job_id}: File structure of BCM_AoU_SEQ_DataManifest_02262021_008v2.csv is not valid. "
                        f"Missing fields: ['mappedreadspct', 'samplesource']",
                submitted_gc_site_id='jh'
            )

        current_incidents_for_emails = self.incident_dao.get_new_ingestion_incidents()

        self.assertEqual(len(current_incidents_for_emails), 2)

        with DataQualityJobController(GenomicJob.DAILY_SEND_VALIDATION_EMAILS) as controller:
            controller.execute_workflow()

        email_config = config.getSettingJson(config.GENOMIC_DAILY_VALIDATION_EMAILS)

        self.assertEqual(email_mock.call_count, len(current_incidents_for_emails))

        call_args = email_mock.call_args_list
        self.assertEqual(len(call_args), len(current_incidents_for_emails))

        config_recipients = [obj for obj in email_config['recipients'].values()]

        self.assertEqual(len(config_recipients), len(set([obj.submitted_gc_site_id for obj in
                                                          current_incidents_for_emails])))

        for call_arg in call_args:
            recipient_called_list = call_arg.args[0].recipients
            plain_text = call_arg.args[0].plain_text_content

            self.assertTrue(recipient_called_list in config_recipients)
            self.assertTrue(job_id not in plain_text)
            self.assertEqual('no-reply@pmi-ops.org', call_arg.args[0].from_email)
            self.assertEqual('All of Us GC/DRC Manifest Ingestion Failure', call_arg.args[0].subject)

        current_mock_count = email_mock.call_count

        all_incidents = self.incident_dao.get_all()

        self.assertTrue(all(obj.email_notification_sent == 1 and obj.email_notification_sent_date is not None for obj
                            in all_incidents))

        for incident in all_incidents:
            incident.email_notification_sent = 0
            self.incident_dao.update(incident)

        current_incidents_for_emails = self.incident_dao.get_new_ingestion_incidents()

        # should be reset back to 2
        self.assertEqual(len(current_incidents_for_emails), 2)

        email_config = {
            "genomic_daily_validation_emails": {
                "send_emails": 0
            }
        }
        config.override_setting(config.GENOMIC_DAILY_VALIDATION_EMAILS, email_config)

        with DataQualityJobController(GenomicJob.DAILY_SEND_VALIDATION_EMAILS) as controller:
            controller.execute_workflow()

        self.assertEqual(email_mock.call_count, current_mock_count)

