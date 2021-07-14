import datetime
from flask import Flask
import requests
import unittest

import mock
from werkzeug.exceptions import Forbidden, Unauthorized

from rdr_service import app_util, clock, config
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
                "whitelisted_ip_ranges": {"ip6": ["1234:5678::/32"], "ip4": ["123.210.0.1/16"]},
                "clientId": "example"
            }
        }

        # Note that there is a ttl cache on this config value, so it can't be changed during the test.

        from rdr_service.config import LocalFilesystemConfigProvider
        fs = LocalFilesystemConfigProvider()
        fs.store(config.USER_INFO, self.user_info)
        #config.insert_config(config.USER_INFO, self.user_info)

    def test_date_header(self):
        response = lambda: None  # Dummy object; functions can have arbitrary attrs set on them.
        setattr(response, "headers", {})

        with clock.FakeClock(datetime.datetime(1994, 11, 6, 8, 49, 37)):
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
        allowed_ips = app_util.get_whitelisted_ips(self.user_info["example@example.com"])
        app_util.enforce_ip_whitelisted("123.210.0.1", allowed_ips)
        app_util.enforce_ip_whitelisted("123.210.111.0", allowed_ips)

        app_util.enforce_ip_whitelisted("1234:5678::", allowed_ips)
        app_util.enforce_ip_whitelisted("1234:5678:9999::", allowed_ips)

    def test_invalid_ip(self):
        allowed_ips = app_util.get_whitelisted_ips(self.user_info["example@example.com"])
        with self.assertRaises(Forbidden):
            app_util.enforce_ip_whitelisted("100.100.0.1", allowed_ips)

        with self.assertRaises(Forbidden):
            app_util.enforce_ip_whitelisted("5555::", allowed_ips)

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
                "whitelisted_ip_ranges": {"ip4": ["10.0.0.2/32"], "ip6": []},
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
            mock_lookup_user_info.return_value = {"roles": ["foo"], "whitelisted_ip_ranges": {"ip4": ["10.0.0.2/32"]}}

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

            mock_lookup_user_info.return_value = {"roles": ["bar"], "whitelisted_appids": ["must-be-this-id"]}

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



if __name__ == "__main__":
    unittest.main()
