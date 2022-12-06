import mock
from werkzeug.exceptions import TooManyRequests

from tests.helpers.unittest_base import BaseTestCase


class FlaskAppConfigTest(BaseTestCase):
    def __init__(self, *args, **kwargs):
        super(FlaskAppConfigTest, self).__init__(*args, **kwargs)
        self.uses_database = False

    def setUp(self, with_data=True, with_consent_codes=False) -> None:
        super(FlaskAppConfigTest, self).setUp(with_data, with_consent_codes)

        self.api_patches = [
            mock.patch('rdr_service.api.participant_api.ParticipantApi.get', return_value='success'),
            mock.patch('rdr_service.api.deceased_report_api.DeceasedReportApi.post', return_value='success')
        ]
        for patcher in self.api_patches:
            patcher.start()

    def tearDown(self):
        super(FlaskAppConfigTest, self).tearDown()
        for patcher in self.api_patches:
            patcher.stop()

    def test_api_requests_limited(self):
        """Integration test to ensure that the API requests are rate limited"""

        did_reach_rate_limit = False
        for _ in range(110):
            response = self.send_get('Participant', expected_status=None)
            if response.status_code == TooManyRequests.code:
                did_reach_rate_limit = True
                break

        self.assertTrue(did_reach_rate_limit)

    def test_limits_are_per_account(self):
        """Make sure that each service account is tracked separately"""
        url = 'Participant'

        # Hit the rate limit for the default service account for the test
        self._reach_rate_limit(url=url)

        # Make another request with a different service account, expecting it to work
        with mock.patch('rdr_service.app_util.config.LOCAL_AUTH_USER', 'another@me.com'):
            self.send_get(url, expected_status=200)

    def test_distinct_endpoint_limits(self):
        """Make sure that when a limit is reached on one endpoint, another is still available"""

        # Hit the rate limit for the Participant endpoint
        self._reach_rate_limit(url='Participant')

        # Expect a request on another endpoint to succeed
        self.send_post('Participant/P123123123/Observation', expected_status=200)

    def _reach_rate_limit(self, url):
        has_hit_limit = False
        for _ in range(110):
            response = self.send_get(url, expected_status=None)
            if response.status_code == TooManyRequests.code:
                has_hit_limit = True
                break

        if not has_hit_limit:
            self.fail('Unable to hit rate limit for setting up test')
