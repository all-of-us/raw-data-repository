import mock
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
