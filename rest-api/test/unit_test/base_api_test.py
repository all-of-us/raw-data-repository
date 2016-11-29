"""Tests for participant."""

from base_api import BaseApi
from mock import MagicMock, patch

from test.unit_test.unit_test_util import TestBase

class BaseApiTest(TestBase):
  """Base API test"""

  @patch('base_api.api_util')
  @patch('base_api.request')
  @patch('base_api.config.getSetting')
  def test_fake_dates(self, mock_config, mock_request, mock_api_util):
    """Ensure that fake dates are parsed when the header is set"""
    mock_request.headers = {'x-pretend-date': 'True'}
    mock_request.get_json.return_value = {}
    mock_config.return_value = 'True'
    mock_api_util.parse_date.return_value='parsed-date'
    mock_api_util.get_oauth_id.return_value='client-id'

    mock_dao = MagicMock()
    mock_dao.from_json.return_value = 'entity'
    mock_dao.to_json.return_value = {'entity': 'as-json'}

    sut = BaseApi(mock_dao)
    sut.post()
    mock_dao.insert.assert_called_with('entity', date='parsed-date', client_id='client-id')

  @patch('base_api.api_util')
  @patch('base_api.request')
  @patch('base_api.config.getSetting')
  def test_fake_dates_disabled(self, mock_config, mock_request, mock_api_util):
    """Ensure that fake dates are parsed when the header is set"""
    mock_request.headers = {}
    mock_request.get_json.return_value = {}
    mock_config.return_value = 'True'
    mock_api_util.parse_date.return_value='parsed-date'
    mock_api_util.get_oauth_id.return_value='client-id'

    mock_dao = MagicMock()
    mock_dao.from_json.return_value = 'entity'
    mock_dao.to_json.return_value = {'entity': 'as-json'}

    sut = BaseApi(mock_dao)
    sut.post()
    mock_dao.insert.assert_called_with('entity', date=None, client_id='client-id')
