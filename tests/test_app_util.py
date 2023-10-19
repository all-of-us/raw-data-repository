from datetime import datetime
from flask import Flask
import requests
import unittest

import mock
from werkzeug.exceptions import Forbidden, Unauthorized

from rdr_service import app_util, clock, config
from rdr_service.participant_enums import get_bucketed_age
from tests.helpers.unittest_base import BaseTestCase


@app_util.auth_required("foo")
def foo_role(x):
    return x + 1


@app_util.auth_required(["foo", "bar"])
def foo_bar_role(x):
    return x + 1


@app_util.auth_required_cron
def cron_required(x):
    return x + 1


@app_util.nonprod
def not_in_prod():
    pass


class AppUtilTest(BaseTestCase):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.uses_database = False

    def setUp(self):
        super().setUp()

        self.user_info = {
            "example@example.com": {
                "roles": ["role1", "role2"],
                "allow_list_ip_ranges": {"ip6": ["1234:5678::/32"], "ip4": ["123.210.0.1/16"]},
                "clientId": "example"
            }
        }

        # Note that there is a ttl cache on this config value, so it can't be changed during the test.

        from rdr_service.config import LocalFilesystemConfigProvider
        fs = LocalFilesystemConfigProvider()
        fs.store(config.USER_INFO, self.user_info)

    def test_date_header(self):
        response = lambda: None  # Dummy object; functions can have arbitrary attrs set on them.
        setattr(response, "headers", {})

        with clock.FakeClock(datetime(1994, 11, 6, 8, 49, 37)):
            app_util.add_headers(response)

        self.assertEqual(response.headers["Date"], "Sun, 06 Nov 1994 08:49:37 GMT")

    def test_expiry_header(self):
        response = lambda: None  # dummy object
        setattr(response, "headers", {})
        app_util.add_headers(response)

        self.assertEqual(response.headers["Expires"], "Thu, 01 Jan 1970 00:00:00 GMT")

    def test_headers_present(self):
        response = lambda: None  # dummy object
        setattr(response, "headers", {})
        app_util.add_headers(response)

        self.assertEqual(
            set(response.headers.keys()),
            {
                "Date",
                "Expires",
                "Pragma",
                "Cache-control",
                "Content-Disposition",
                "Content-Type",
                "X-Content-Type-Options",
            },
        )

    def test_valid_ip(self):
        allowed_ips = app_util.get_allowed_ips(self.user_info["example@example.com"])
        app_util.enforce_ip_allowed("123.210.0.1", allowed_ips)
        app_util.enforce_ip_allowed("123.210.111.0", allowed_ips)

        app_util.enforce_ip_allowed("1234:5678::", allowed_ips)
        app_util.enforce_ip_allowed("1234:5678:9999::", allowed_ips)

    def test_invalid_ip(self):
        allowed_ips = app_util.get_allowed_ips(self.user_info["example@example.com"])
        with self.assertRaises(Forbidden):
            app_util.enforce_ip_allowed("100.100.0.1", allowed_ips)

        with self.assertRaises(Forbidden):
            app_util.enforce_ip_allowed("5555::", allowed_ips)

    # @patch("rdr_service.app_util.request")
    # def test_auth_required_http_identity_set(self, mock_request):
    def test_auth_required_http_identity_set(self):
        mock_request = mock.MagicMock()
        mock_request.return_value.scheme = "http"
        with mock.patch("rdr_service.app_util.request", mock_request):
            with self.assertRaises(Unauthorized):
                foo_role(1)

    # @mock.patch("rdr_service.app_util.request", spec=app_util.request)
    @mock.patch("rdr_service.app_util.get_oauth_id")
    @mock.patch("rdr_service.app_util.lookup_user_info")
    def test_auth_required_https_identity_set_role_not_matched(self, mock_lookup_user_info, mock_get_oauth_id):
        mock_request = mock.MagicMock()
        mock_request.scheme = "http"
        mock_request.remote_addr = "ip"
        mock_request.headers = {}
        with mock.patch("rdr_service.app_util.request", mock_request):
            mock_get_oauth_id.return_value = "bob@example.com"
            mock_lookup_user_info.return_value = {"place": "holder"}
            with self.assertRaises(Forbidden):
                foo_role(1)
            mock_get_oauth_id.assert_called_with()
            mock_lookup_user_info.assert_called_with(mock_get_oauth_id())

    # @mock.patch("rdr_service.app_util.request")
    @mock.patch("rdr_service.app_util.get_oauth_id")
    @mock.patch("rdr_service.app_util.lookup_user_info")
    def test_auth_required_https_identity_set_role_wrong_match(self, mock_lookup_user_info, mock_get_oauth_id):
        mock_request = mock.MagicMock()
        mock_request.scheme = "https"
        mock_request.remote_addr = "ip"
        mock_request.headers = {}
        with mock.patch("rdr_service.app_util.request", mock_request):
            mock_get_oauth_id.return_value = "bob@example.com"
            mock_lookup_user_info.return_value = {"roles": ["bar"]}
            with self.assertRaises(Forbidden):
                foo_role(1)
            mock_get_oauth_id.assert_called_with()

    # @mock.patch("rdr_service.app_util.request")
    @mock.patch("rdr_service.app_util.get_oauth_id")
    @mock.patch("rdr_service.app_util.lookup_user_info")
    def test_auth_required_https_identity_set_multi_role_not_matched(self, mock_lookup_user_info, mock_get_oauth_id):
        mock_request = mock.MagicMock()
        mock_request.scheme = "https"
        mock_request.remote_addr = "ip"
        mock_request.headers = {}
        with mock.patch("rdr_service.app_util.request", mock_request):
            mock_get_oauth_id.return_value = "bob@example.com"
            mock_lookup_user_info.return_value = {"place": "holder"}

            with self.assertRaises(Forbidden):
                foo_bar_role(1)

            mock_lookup_user_info.return_value = {"roles": ["foo"]}
            self.assertEqual(2, foo_bar_role(1))

    # @mock.patch("rdr_service.app_util.request")
    @mock.patch("rdr_service.app_util.get_oauth_id")
    @mock.patch("rdr_service.app_util.lookup_user_info")
    def test_auth_required_https_identity_set_role_wrong_match(self, mock_lookup_user_info, mock_get_oauth_id):
        mock_request = mock.MagicMock()
        mock_request.scheme = "https"
        mock_request.remote_addr = "ip"
        mock_request.headers = {}
        with mock.patch("rdr_service.app_util.request", mock_request):
            mock_get_oauth_id.return_value = "bob@example.com"
            mock_lookup_user_info.return_value = {"roles": ["baz"]}

            mock_request.headers = {}
            with self.assertRaises(Forbidden):
                foo_bar_role(1)
            mock_get_oauth_id.assert_called_with()
            mock_lookup_user_info.assert_called_with(mock_get_oauth_id())

    # @mock.patch("rdr_service.app_util.request")
    @mock.patch("rdr_service.app_util.get_oauth_id")
    @mock.patch("rdr_service.app_util.lookup_user_info")
    def test_auth_required_https_identity_set_role_match(self, mock_lookup_user_info, mock_get_oauth_id):
        mock_request = mock.MagicMock()
        mock_request.scheme = "http"
        mock_request.remote_addr = "ip"
        mock_request.headers = {}
        with mock.patch("rdr_service.app_util.request", mock_request):
            mock_get_oauth_id.return_value = "bob@example.com"
            mock_lookup_user_info.return_value = {"roles": ["bar"]}
            self.assertEqual(2, foo_bar_role(1))
            mock_get_oauth_id.assert_called_with()
            mock_lookup_user_info.assert_called_with(mock_get_oauth_id())

    # @mock.patch("rdr_service.app_util.request")
    @mock.patch("rdr_service.app_util.get_oauth_id")
    @mock.patch("rdr_service.app_util.lookup_user_info")
    def test_auth_required_ip_ranges(self, mock_lookup_user_info, mock_get_oauth_id):
        mock_request = mock.MagicMock()
        mock_request.scheme = "https"
        mock_request.remote_addr = "10.0.0.1"
        mock_request.headers = {}
        with mock.patch("rdr_service.app_util.request", mock_request):
            mock_get_oauth_id.return_value = "bob@example.com"
            mock_lookup_user_info.return_value = {
                "roles": ["bar"],
                "allow_list_ip_ranges": {"ip4": ["10.0.0.2/32"], "ip6": []},
            }

            with self.assertRaises(Forbidden):
                foo_bar_role(1)

            mock_request.remote_addr = "10.0.0.2"
            self.assertEqual(2, foo_bar_role(1))

    # @mock.patch("rdr_service.app_util.request")
    @mock.patch("rdr_service.app_util.get_oauth_id")
    @mock.patch("rdr_service.app_util.lookup_user_info")
    def test_no_ip6_required(self, mock_lookup_user_info, mock_get_oauth_id):
        mock_request = mock.MagicMock()
        mock_request.scheme = "https"
        mock_request.remote_addr = "10.0.0.1"
        mock_request.headers = {}
        with mock.patch("rdr_service.app_util.request", mock_request):
            mock_get_oauth_id.return_value = "bob@example.com"
            mock_lookup_user_info.return_value = {"roles": ["foo"], "allow_list_ip_ranges": {"ip4": ["10.0.0.2/32"]}}

            mock_request.remote_addr = "10.0.0.2"
            self.assertEqual(2, foo_bar_role(1))

    # @mock.patch("rdr_service.app_util.request")
    @mock.patch("rdr_service.app_util.get_oauth_id")
    @mock.patch("rdr_service.app_util.lookup_user_info")
    def test_auth_required_appid(self, mock_lookup_user_info, mock_get_oauth_id):
        mock_request = mock.MagicMock()
        mock_request.scheme = "https"
        mock_request.remote_addr = "10.0.0.1"
        mock_request.headers = {}
        with mock.patch("rdr_service.app_util.request", mock_request):
            mock_get_oauth_id.return_value = "bob@example.com"

            mock_lookup_user_info.return_value = {"roles": ["bar"], "allow_list_appids": ["must-be-this-id"]}

            with self.assertRaises(Forbidden):
                foo_bar_role(1)

            mock_request.headers = {"X-Appengine-Inbound-Appid": "must-be-this-id"}
            self.assertEqual(2, foo_bar_role(1))

    def test_no_roles_supplied_to_decorator(self):
        with self.assertRaises(TypeError):

            @app_util.auth_required()
            def _():
                pass

        with self.assertRaises(AssertionError):

            @app_util.auth_required(None)
            def _():
                pass

    # @mock.patch("rdr_service.app_util.request", spec=app_util.request)
    @mock.patch("rdr_service.app_util.get_oauth_id")
    def test_check_auth_required_cron(self, mock_get_oauth_id):
        mock_request = mock.MagicMock()
        mock_request.headers = {"X-Appengine-Cron": "true"}
        with mock.patch("rdr_service.app_util.request", mock_request):
            mock_get_oauth_id.return_value = "bob@example.com"
            self.assertEqual(2, cron_required(1))

            mock_request.headers = {}
            with self.assertRaises(Forbidden):
                cron_required(1)

    def test_nonprod(self):
        # The dev config is isntalled by default for tests, reset.
        config.override_setting(config.ALLOW_NONPROD_REQUESTS, False)
        with self.assertRaises(Forbidden):
            not_in_prod()

        config.override_setting(config.ALLOW_NONPROD_REQUESTS, True)
        not_in_prod()

    @mock.patch('rdr_service.app_util.GAE_PROJECT', 'totally_the_server')
    @mock.patch('rdr_service.app_util.requests')
    def test_get_oauth_id_tokeninfo_fallback(self, mock_requests):
        """Make sure the tokeninfo endpoint gets used if userinfo fails"""

        def mock_response(url):
            response = mock.MagicMock()
            if 'userinfo' in url:
                response.status_code = 403
            else:
                response.status_code = 200
                response.json.return_value = {'email': 'fallback_response'}

            return response
        mock_requests.get.side_effect = mock_response

        # Need a request context for app_util to get a token from
        with Flask('test').test_request_context(headers={'Authorization': 'Bearer token'}):
            self.assertEqual('fallback_response', app_util.get_oauth_id())

        mock_requests.get.assert_called_with('https://www.googleapis.com/oauth2/v3/tokeninfo?access_token=token')

    @mock.patch('rdr_service.app_util.GAE_PROJECT', 'totally_the_server')
    @mock.patch('rdr_service.app_util.logging')
    @mock.patch('rdr_service.app_util.requests')
    def test_get_oauth_id_fallback_on_no_email(self, mock_requests, mock_logging):
        """Make sure tokeninfo is used if userinfo doesn't give the email"""

        def mock_response(url):
            response = mock.MagicMock(status_code=200)
            if 'userinfo' in url:
                response.json.return_value = {'other': 'data'}
            else:
                response.json.return_value = {'email': 'fallback_response'}

            return response
        mock_requests.get.side_effect = mock_response

        # Need a request context for app_util to get a token from
        with Flask('test').test_request_context(headers={'Authorization': 'Bearer token'}):
            self.assertEqual('fallback_response', app_util.get_oauth_id())

        mock_requests.get.assert_called_with('https://www.googleapis.com/oauth2/v3/tokeninfo?access_token=token')
        mock_logging.error.assert_called_with('UserInfo endpoint did not return the email')

    @mock.patch('rdr_service.app_util.GAE_PROJECT', 'totally_the_server')
    def test_invalid_token(self):
        """Check that we get an unauthorized error with a bad token"""

        # Need a request context for app_util to get a token from
        with Flask('test').test_request_context(headers={'Authorization': 'Bearer token123'}),\
                self.assertRaises(Unauthorized):
            app_util.get_oauth_id()

    @mock.patch('rdr_service.app_util.GAE_PROJECT', 'definitely_the_server')
    @mock.patch('rdr_service.app_util.requests')
    def test_auth_request_connection_error_retry(self, mock_requests):
        """
        Make sure the authentication logic tries again if there's a connection error
        (we're seeing an occasional bad response from google)
        """

        call_count = 0
        expected_user_email = 'test@me.com'

        def mock_response(_):
            nonlocal call_count
            call_count += 1
            if call_count < 3:
                raise requests.exceptions.ConnectionError(
                    "Auth retry loop should be able to handle a pair of errors and keep going"
                )
            else:
                response = mock.MagicMock()
                response.status_code = 200
                response.json.return_value = {'email': expected_user_email}

                return response
        mock_requests.get.side_effect = mock_response

        # Need a request context for app_util to get a token from
        with Flask('test').test_request_context(headers={'Authorization': 'Bearer token'}):
            self.assertEqual(expected_user_email, app_util.get_oauth_id())

    @mock.patch('rdr_service.app_util.GAE_PROJECT', 'definitely_the_server')
    @mock.patch('rdr_service.app_util.requests')
    def test_caching_user_info(self, mock_requests):
        """Make sure we only call Google's API once per request for getting the user info for the API token."""
        auth_api_response = mock.MagicMock()
        auth_api_response.status_code = 200

        expected_user_email = 'auth@test.com'
        auth_api_response.json.return_value = {'email': expected_user_email}
        mock_requests.get.return_value = auth_api_response
        with Flask('test').test_request_context(headers={'Authorization': 'Bearer token'}):
            self.assertEqual(expected_user_email, app_util.get_oauth_id())
            self.assertEqual(expected_user_email, app_util.get_oauth_id())
            mock_requests.get.assert_called_once()  # There should only be one call to get the user info

        # Make sure another request will make another call to get the new requests user info
        another_email = 'another@test.com'
        auth_api_response.json.return_value = {'email': another_email}
        with Flask('test').test_request_context(headers={'Authorization': 'Bearer token'}):
            self.assertEqual(another_email, app_util.get_oauth_id())

    def test_batch_manager(self):
        processed_objects = []

        def dummy_callback(obj_list):
            processed_objects.extend(obj_list)

        with app_util.BatchManager(batch_size=3, callback=dummy_callback) as batch_manager:
            batch_manager.add(4)
            batch_manager.add(2)

            # Since the batch size hasn't been reached, make sure nothing's been processed yet
            self.assertEqual([], processed_objects)

            # Add one more to make a complete batch and trigger the batch processing
            batch_manager.add(6)
            self.assertEqual([4, 2, 6], processed_objects)

            # Add another and make sure it doesn't get processed on it's own
            batch_manager.add(10)
            self.assertEqual([4, 2, 6], processed_objects)

        # Make sure that the batch processes the remaining items when the context closes
        self.assertEqual([4, 2, 6, 10], processed_objects)

    def test_date_comparison(self):
        """Run checks on the function used for checking timestamp equality"""
        # The same timestamp
        self.assertTrue(app_util.is_datetime_equal(datetime(1994, 11, 6, 8, 49, 37), datetime(1994, 11, 6, 8, 49, 37)))

        # Different of a few hours
        self.assertFalse(app_util.is_datetime_equal(datetime(2021, 10, 6, 13, 57), datetime(2021, 10, 6, 8, 57)))

        # Difference of a few minutes, not allowing for a match when they're off by a number of seconds
        self.assertFalse(app_util.is_datetime_equal(datetime(2021, 10, 6, 13, 57), datetime(2021, 10, 6, 13, 54)))

        # Difference of a few minutes, allowing for a difference between them but not enough for a match
        self.assertFalse(app_util.is_datetime_equal(
            datetime(2021, 10, 6, 13, 57), datetime(2021, 10, 6, 13, 54), difference_allowed_seconds=120
        ))

        # Difference of a 3 minutes, allowing for a difference of 5 minutes
        self.assertTrue(app_util.is_datetime_equal(
            datetime(2021, 10, 6, 13, 57), datetime(2021, 10, 6, 13, 54), difference_allowed_seconds=300
        ))

    def test_age_bucket_calculation(self):
        """Test participant age bucket strings"""
        reference_timestamp = datetime(2022, 3, 15)  # used as the current time for calculations

        three_years_old = datetime(2019, 1, 1)
        age_bucket = get_bucketed_age(date_of_birth=three_years_old, today=reference_timestamp)
        self.assertEqual("0-6", age_bucket)

        seven_years_old = datetime(2014, 3, 16)
        age_bucket = get_bucketed_age(date_of_birth=seven_years_old, today=reference_timestamp)
        self.assertEqual("7-12", age_bucket)

        seven_years_old = datetime(2005, 3, 15)
        age_bucket = get_bucketed_age(date_of_birth=seven_years_old, today=reference_timestamp)
        self.assertEqual("13-17", age_bucket)


if __name__ == "__main__":
    unittest.main()
