from datetime import datetime
import mock

from rdr_service.services.redcap_client import RedcapClient
from tests.helpers.unittest_base import BaseTestCase


class CodesManagementTest(BaseTestCase):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.uses_database = False

    def setUp(self, **kwargs):
        super(CodesManagementTest, self).setUp(**kwargs)
        self.redcap = RedcapClient()
        self.project_api_token = 123456

    def _get_last_request_args(self, mock_requests):
        self.assertTrue(mock_requests.post.called)
        return mock_requests.post.call_args

    @mock.patch('rdr_service.services.redcap_client.requests')
    def test_generic_post_structure(self, mock_requests):
        """Make sure requests are made to the right URL and with the expected headers and format and auth parameters"""

        # Calling arbitrary method to trigger a request
        self.redcap.get_data_dictionary(self.project_api_token)
        args, kwargs = self._get_last_request_args(mock_requests)

        # Check that the request was made to redcap's api
        self.assertEqual('https://redcap.pmi-ops.org/api/', args[0])

        # Check that the expected parameters were posted (auth and formats)
        request_data = kwargs['data']
        self.assertEqual(self.project_api_token, request_data['token'])
        self.assertEqual('json', request_data['format'])
        self.assertEqual('json', request_data['returnFormat'])

        # Check that the request was made with the expected headers
        # https://precisionmedicineinitiative.atlassian.net/browse/PD-5404
        headers = kwargs['headers']
        self.assertDictEqual({
            'User-Agent': 'RDR code sync tool',
            'Accept': None,
            'Connection': None
        }, headers)

    @mock.patch('rdr_service.services.redcap_client.requests')
    def test_data_dictionary_request(self, mock_requests):
        """Make sure requests for the data dictionary are made with the expected content parameter"""

        self.redcap.get_data_dictionary(self.project_api_token)
        _, kwargs = self._get_last_request_args(mock_requests)

        request_data = kwargs['data']
        self.assertEqual('metadata', request_data['content'])

    @mock.patch('rdr_service.services.redcap_client.requests')
    def test_records_request(self, mock_requests):
        """Make sure requests for records (responses) for the survey are made with expected parameters"""

        self.redcap.get_records(self.project_api_token, datetime(2020, 3, 4, hour=23, minute=8, second=37))
        _, kwargs = self._get_last_request_args(mock_requests)

        request_data = kwargs['data']
        self.assertEqual('record', request_data['content'])
        self.assertEqual('2020-03-04 23:08:37', request_data['dateRangeBegin'])
        self.assertTrue(request_data['exportSurveyFields'])
