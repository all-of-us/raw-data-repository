import http.client
import mock

from tests.helpers.unittest_base import BaseTestCase

from rdr_service import config


class OfflineAppTest(BaseTestCase):
    def setUp(self):
        super(OfflineAppTest, self).setUp()

        from rdr_service.offline.main import app, OFFLINE_PREFIX
        self.offline_test_client = app.test_client()
        self.url_prefix = OFFLINE_PREFIX

    def send_cron_request(self, path, expected_status=http.client.OK):
        return self.send_get(
            path,
            test_client=self.offline_test_client,
            prefix=self.url_prefix,
            headers={'X-Appengine-Cron': True},
            expected_status=expected_status
        )

    def test_offline_http_exceptions_get_logged(self):
        """Make sure HTTPException are logged when thrown"""

        # Need to raise an HTTPException on a cron call, picking an arbitrary one that is easy to mock
        with mock.patch('rdr_service.offline.main.check_enrollment') as mock_cron_call, \
                mock.patch('rdr_service.app_util.check_cron', return_value=True),\
                mock.patch('rdr_service.services.gcp_logging.logging') as mock_logging:
            def throw_exception():
                raise Exception('This exception message should appear in the logs')
            mock_cron_call.side_effect = throw_exception

            # Call to trigger the exception
            self.send_get(
                'EnrollmentStatusCheck',
                test_client=self.offline_test_client,
                prefix=self.url_prefix,
                expected_status=None
            )

            error_log_call = mock_logging.error.call_args
            self.assertIsNotNone(error_log_call, 'An error log should have been made')

            traceback = error_log_call.args[0]
            self.assertIn('throw_exception', traceback, "Traceback should show where the error was raised")

    @mock.patch('rdr_service.offline.requests_log_migrator.RequestsLogMigrator.migrate_latest_requests_logs')
    def test_request_log_migrator_route(self, mock_migrate_call):
        test_db_name = 'test_db_name'
        self.send_cron_request(f'MigrateRequestsLog/{test_db_name}')
        mock_migrate_call.assert_called()

    @mock.patch('rdr_service.services.data_quality.DataQualityChecker.run_data_quality_checks')
    def test_data_quality_check_route(self, mock_checker):
        self.send_cron_request(f'DataQualityChecks')
        mock_checker.assert_called()

    @mock.patch('rdr_service.offline.main.genomic_pipeline')
    def test_genomics_config_disables_jobs(self, pipeline_mock):
        self.temporarily_override_config_setting(config.GENOMIC_CRON_JOBS, {})
        self.send_cron_request('GenomicAW3ArrayWorkflow', expected_status=500)
        pipeline_mock.aw3_array_manifest_workflow.assert_not_called()

        # Enable the cron job and call it again to be sure that it would have worked if it was enabled
        # If this check fails, then the assumptions made in this test are wrong and the test needs to be changed.
        # This test assumes that the only reason the AW3 job would not run and call the pipeline function is if
        # is disabled.
        self.temporarily_override_config_setting(config.GENOMIC_CRON_JOBS, {
            'aw3_array_manifest_workflow': 1
        })
        self.send_cron_request('GenomicAW3ArrayWorkflow')
        pipeline_mock.aw3_array_manifest_workflow.assert_called()

