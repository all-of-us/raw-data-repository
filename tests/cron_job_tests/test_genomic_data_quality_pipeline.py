# Tests for the Genomics Data Quality Pipeline
import mock, datetime, pytz

from rdr_service import clock
from rdr_service.genomic_enums import GenomicJob, GenomicSubProcessStatus, GenomicSubProcessResult
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
                report_def_d = rc.get_report_def("SUMMARY", "RUNS", "D")
                report_def_w = rc.get_report_def("SUMMARY", "RUNS", "W")

        exp_from_date_d = self.fake_time - datetime.timedelta(days=1)
        exp_from_date_w = self.fake_time - datetime.timedelta(days=7)

        self.assertEqual(exp_from_date_d, report_def_d.from_date)
        self.assertEqual(exp_from_date_w, report_def_w.from_date)

    def test_reporting_component_get_report_def_query(self):
        rc = ReportingComponent()

        # Report defs to test (QUERY, LEVEL, TARGET, TIME_FRAME)
        test_definitions = (
            ("dq_report_runs_summary", "SUMMARY", "RUNS", "D"),
        )

        for test_def in test_definitions:

            query_class = 'rdr_service.genomic.genomic_data.GenomicQueryClass'
            query_class += f".{test_def[0]}"

            with mock.patch(query_class) as query_mock:

                query_mock.return_value = ("", {})
                rc.get_report_def(*test_def[1:])

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

        with clock.FakeClock(report_ran_time):
            report_data = rc.generate_report("SUMMARY", "RUNS", "D")

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




