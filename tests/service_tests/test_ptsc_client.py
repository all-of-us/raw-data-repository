import mock

from rdr_service.services.ptsc_client import PtscClient
from tests.helpers.unittest_base import BaseTestCase


@mock.patch('rdr_service.services.ptsc_client.requests')
class PtscClientTest(BaseTestCase):
    def __init__(self, *args, **kwargs):
        super(PtscClientTest, self).__init__(*args, **kwargs)
        self.uses_database = False

    def setUp(self, *args, **kwargs):
        super(PtscClientTest, self).setUp(*args, **kwargs)

        self.auth_url = 'http://test_auth.com'
        self.request_url = 'http://test_request.com'
        self.client_id = 'client_id_val'
        self.secret = 'client_secret_val'
        self.ptsc_client = PtscClient(
            auth_url=self.auth_url,
            request_url=self.request_url,
            client_id=self.client_id,
            client_secret=self.secret
        )

    def test_retrieving_access_token(self, requests_mock):
        def return_access_token_response(**_):
            json_content = {
                'access_token': 'test_token'
            }
            mock_response = mock.MagicMock()
            mock_response.json.return_value = json_content
            return mock_response
        requests_mock.post.side_effect = return_access_token_response

        token = self.ptsc_client.get_access_token()
        self.assertEqual('test_token', token)

    def test_refresh_token(self, requests_mock):
        """
        The token expires after 5 minutes, if we get a 401 for an expired token then we should
        get a new one and try again once.
        """
        def get_response(**_):
            mock_response = mock.MagicMock()
            mock_response.status_code = 401
            mock_response.content = 'error content'
            return mock_response
        requests_mock.get.side_effect = get_response

        with self.assertRaises(Exception) as expected_exception:
            self.ptsc_client.make_request('test')

        self.assertEqual('got status code 401. Message: error content', str(expected_exception.exception))
        self.assertEqual(2, requests_mock.get.call_count)

