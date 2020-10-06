from google.cloud.logging_v2.gapic import enums
import logging
import mock

import rdr_service.services.gcp_logging as gcp_logging
from tests.helpers.unittest_base import BaseTestCase


class GCPLoggingTest(BaseTestCase):
    def test_published_severity_level(self):
        """Ensure that the severity level used is the highest of the individual logs being published"""

        with mock.patch('rdr_service.services.gcp_logging.os') as mock_os,\
                mock.patch('rdr_service.services.gcp_logging.gcp_logging_v2') as mock_gcp_logging:
            # Trick the logger into thinking it's on the server and should initialize
            mock_os.environ = {
                'GAE_ENV': 'TEST'
            }

            # Initialize to have the log handler start buffering logs
            gcp_logging.initialize_logging()

            # Make some logs
            logging.info('test info message')
            logging.error('error')
            logging.warning('warning')
            logging.info('one last info')

            # Force the logs to 'publish' to the mock object
            gcp_logging.flush_request_logs()

            # Check that the highest severity of the logs was used for the published entry
            mock_final_log_entry_call = mock_gcp_logging.types.log_entry_pb2.LogEntry
            _, kwargs = mock_final_log_entry_call.call_args
            logged_severity = kwargs.get('severity')
            self.assertEqual(enums.LogSeverity.ERROR, logged_severity)
