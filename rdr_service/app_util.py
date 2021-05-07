import calendar
import datetime
import email.utils
import flask
from flask_limiter import Limiter
from flask_limiter.util import get_remote_address
import logging
from requests.exceptions import RequestException
from time import sleep
import urllib.parse

import netaddr
import pytz
import requests
from flask import request
from werkzeug.exceptions import Forbidden, Unauthorized, GatewayTimeout

from rdr_service import clock, config
from rdr_service.api import base_api
from rdr_service.config import GAE_PROJECT

_GMT = pytz.timezone("GMT")
SCOPE = "https://www.googleapis.com/auth/userinfo.email"

GLOBAL_CLIENT_ID_KEY = 'oauth_client_id'


def handle_database_disconnect(err):
    """Intended to catch DBAPIError's thrown during a request cycle and transform them into 503's.
  If the DBAPIError does not represent an invalidated connection, reraise the error.

  Usage: app.register_error_handler(DBAPIError, handle_database_disconnect)
  """
    if err.connection_invalidated:
        return "DB connection lost, please retry", 503
    raise err


def auth_required_cron(func):
    """A decorator that ensures that the user is a cron job."""

    def wrapped(*args, **kwargs):
        check_cron()
        return func(*args, **kwargs)

    return wrapped


def task_auth_required(func):
    """A decorator that ensures that the user is a task job."""

    def wrapped(*args, **kwargs):
        if GAE_PROJECT == "localhost" or (
            request.headers.get("X-Appengine-Taskname") and "AppEngine-Google" in request.headers.get("User-Agent", "")
        ):
            logging.info("App Engine task request ALLOWED for task endpoint.")
            return func(*args, **kwargs)
        logging.info("User {} NOT ALLOWED for task endpoint".format(get_oauth_id()))
        raise Forbidden()

    return wrapped


def nonprod(func):
    """The decorated function may never run in environments without config.ALLOW_NONPROD_REQUESTS."""

    def wrapped(*args, **kwargs):
        if not config.getSettingJson(config.ALLOW_NONPROD_REQUESTS, False):
            raise Forbidden("Request not allowed in production environment (according to config).")
        return func(*args, **kwargs)

    return wrapped


def check_auth(role_whitelist):
    """Raises Unauthorized or Forbidden if the current user is not allowed."""
    user_email, user_info = get_validated_user_info()
    if set(user_info.get("roles", [])) & set(role_whitelist):
        return

    logging.warning(f"User {user_email} has roles {user_info.get('roles')}, but {role_whitelist} is required")
    raise Forbidden()


def get_auth_token():
    header = request.headers.get("Authorization", '')
    try:
        return header.split(' ', 1)[1]
    except IndexError:
        raise ValueError(f"Invalid Authorization Header: {header}")


def get_token_info_response(token, use_tokeninfo=False):
    verification_endpoint = 'userinfo'
    if use_tokeninfo:
        verification_endpoint = 'tokeninfo'

    google_tokeninfo_url = 'https://www.googleapis.com/oauth2/v3/' + verification_endpoint
    qargs = urllib.parse.urlencode({'access_token': token})
    response = requests.get(f"{google_tokeninfo_url}?{qargs}")
    return response


def get_oauth_id():
    """Returns user email ID if OAUTH token present, or None."""
    '''
    NOTES: 2019-08-15 by tanner and mikey
    currently verifies that the provided token
    is legitimate via google API.
    - performance
        - could be validated locally instead of with API
    '''
    if flask.g and GLOBAL_CLIENT_ID_KEY in flask.g:
        return getattr(flask.g, GLOBAL_CLIENT_ID_KEY)

    retries = 5
    use_tokeninfo_endpoint = False

    while retries:
        retries -= 1

        if GAE_PROJECT == 'localhost':  # NOTE: 2019-08-15 mimic devappserver.py behavior
            return config.LOCAL_AUTH_USER

        try:
            token = get_auth_token()
        except ValueError as e:
            logging.info(f"Invalid Authorization Token: {e}")
            return None
        else:
            try:
                response = get_token_info_response(token, use_tokeninfo=use_tokeninfo_endpoint)
            except RequestException as e:  # Catching any connection or decoding errors that could be thrown
                logging.warning(f'Error validating token: {e}')
            else:
                if response.status_code == 200:
                    data = response.json()

                    if use_tokeninfo_endpoint:  # UserInfo doesn't return expiry info :(
                        token_expiry_seconds = data.get('expires_in')
                        logging.info(f'Token expiring in {token_expiry_seconds} seconds')

                    user_email = data.get('email')
                    if user_email is None:
                        logging.error('UserInfo endpoint did not return the email')
                        use_tokeninfo_endpoint = True
                    else:
                        if flask.g:
                            setattr(flask.g, GLOBAL_CLIENT_ID_KEY, user_email)
                        return user_email
                else:
                    logging.info(f"Oauth failure: {response.content} (status: {response.status_code})")

                    if response.status_code in [400, 401]:  # tokeninfo returns 400
                        raise Unauthorized
                    elif not use_tokeninfo_endpoint:
                        logging.error("UserInfo failed, falling back on Tokeninfo")
                        use_tokeninfo_endpoint = True

        sleep(0.25)
        logging.info('Retrying authentication call to Google after failure.')

    raise GatewayTimeout('Google authentication services is not available, try again later.')


def check_cron():
    """Raises Forbidden if the current user is not a cron job."""
    if request.headers.get("X-Appengine-Cron"):
        logging.info("Appengine-Cron ALLOWED for cron endpoint.")
        return
    logging.info("User {} NOT ALLOWED for cron endpoint".format(get_oauth_id()))
    raise Forbidden()


def lookup_user_info(user_email):
    return config.getSettingJson(config.USER_INFO, {}).get(user_email)


def get_account_origin_id():
    """
    Returns the clientId value set in the config for the user.
    :return: Client Id
    """
    auth_email = get_oauth_id()
    user_info = lookup_user_info(auth_email)
    client_id = user_info.get('clientId', None)
    from rdr_service.api_util import DEV_MAIL
    if not client_id:
        if auth_email == DEV_MAIL:
            client_id = "example"  # TODO: This is a hack because something sets up configs different
            # when running all tests and it doesnt have the clientId key.
    return client_id


def is_self_request():
    return (
        request.remote_addr is None
        and config.getSettingJson(config.ALLOW_NONPROD_REQUESTS, False)
        and not request.headers.get("unauthenticated")
    )


def get_whitelisted_ips(user_info):
    if not user_info.get("whitelisted_ip_ranges"):
        return None
    return [
        netaddr.IPNetwork(rng)
        for rng in user_info["whitelisted_ip_ranges"].get("ip6", [])
        + user_info["whitelisted_ip_ranges"].get("ip4", [])
    ]


def enforce_ip_whitelisted(request_ip, whitelisted_ips):
    if whitelisted_ips == None:  # No whitelist means "don't apply restrictions"
        return
    logging.info("IP RANGES ALLOWED: {}".format(whitelisted_ips))
    ip = netaddr.IPAddress(request_ip)
    if not bool([True for rng in whitelisted_ips if ip in rng]):
        logging.info("IP {} NOT ALLOWED".format(ip))
        raise Forbidden("Client IP not whitelisted: {}".format(ip))
    logging.info("IP {} ALLOWED".format(ip))


def get_whitelisted_appids(user_info):
    return user_info.get("whitelisted_appids")


def enforce_appid_whitelisted(request_app_id, whitelisted_appids):
    if not whitelisted_appids:  # No whitelist means "don't apply restrictions"
        return
    if request_app_id:
        if request_app_id in whitelisted_appids:
            logging.info("APP ID {} ALLOWED".format(request_app_id))
            return
        else:
            logging.info("APP ID {} NOT FOUND IN {}".format(request_app_id, whitelisted_appids))
    else:
        logging.info("NO APP ID FOUND WHEN REQUIRED TO BE ONE OF: {}".format(whitelisted_appids))
    raise Forbidden()


def add_headers(response):
    """Add uniform headers to all API responses.

  All responses are JSON, so we tag them as such at the app level to provide uniform protection
  against content-sniffing-based attacks.
  """
    response.headers["Content-Disposition"] = 'attachment; filename="f.txt"'
    response.headers["X-Content-Type-Options"] = "nosniff"
    response.headers["Content-Type"] = "application/json; charset=utf-8"  # override to add charset
    response.headers["Date"] = email.utils.formatdate(
        calendar.timegm(pytz.utc.localize(clock.CLOCK.now()).astimezone(_GMT).timetuple()), usegmt=True
    )
    response.headers["Pragma"] = "no-cache"
    response.headers["Cache-control"] = "no-cache, must-revalidate"
    # Expire at some date in the past: the epoch.
    response.headers["Expires"] = email.utils.formatdate(0.0, usegmt=True)
    return response


def request_logging():
    """Some uniform logging of request characteristics before any checks are applied."""
    logging.info("Request protocol: HTTPS={}".format(request.environ.get("HTTPS")))


def auth_required(role_whitelist):
    """A decorator that keeps the function from being called without auth.
  role_whitelist can be a string or list of strings specifying one or
  more roles that are allowed to call the function. """

    assert role_whitelist, "Can't call `auth_required` with empty role_whitelist."

    if not isinstance(role_whitelist, list):
        role_whitelist = [role_whitelist]

    def auth_required_wrapper(func):
        def wrapped(*args, **kwargs):
            appid = GAE_PROJECT
            request.log_record = base_api.log_api_request()
            # Only enforce HTTPS and auth for external requests; requests made for data generation
            # are allowed through (when enabled).
            acceptable_hosts = ("None", "testbed-test", "testapp", "localhost", "127.0.0.1")
            # logging.info(str(request.headers))
            if not is_self_request():
                if request.scheme.lower() != "https" and appid not in acceptable_hosts:
                    raise Unauthorized(f"HTTPS is required for {appid}", www_authenticate='Bearer realm="rdr"')
                check_auth(role_whitelist)
            request.logged = False
            result = func(*args, **kwargs)
            if request.logged is False:
                try:
                    base_api.log_api_request(log=request.log_record)
                except RuntimeError:
                    # Unittests don't always setup a valid flask request context.
                    pass
            return result

        return wrapped

    return auth_required_wrapper


def restrict_to_gae_project(allowed_project_list):
    """
    A decorator for restricting access of a method
    to a particular Google App Engine Project
    :param project_list: list of GAE ids, i.e. 'all-of-us-rdr-stable', etc.
    :return: function result or Forbidden
    """
    def restriction_function_wrapper(func):
        def inner(*args, **kwargs):
            app_id = GAE_PROJECT

            # Check app_id against the registered environments
            if app_id in allowed_project_list:
                result = func(*args, **kwargs)
            else:
                raise Forbidden(f'This operation is forbidden on {app_id}')

            return result

        return inner

    return restriction_function_wrapper


def get_validated_user_info():
    """Returns a valid (user email, user info), or raises Unauthorized or Forbidden."""
    user_email = get_oauth_id()

    # Allow clients to simulate an unauthentiated request (for testing)
    # because we haven't found another way to create an unauthenticated request
    # when using dev_appserver. When client tests are checking to ensure that an
    # unauthenticated requests gets rejected, they helpfully add this header.
    # The `application_id` check ensures this feature only works in dev_appserver.
    if request.headers.get("unauthenticated") and GAE_PROJECT == 'localhost':
        user_email = None
    if user_email is None:
        raise Unauthorized("No OAuth user found.")

    user_info = lookup_user_info(user_email)
    if user_info:
        if 'X-Appengine-User-Ip' in request.headers:
            addr = request.headers.get('X-Appengine-User-Ip')
        else:
            addr = request.remote_addr
        enforce_ip_whitelisted(addr, get_whitelisted_ips(user_info))
        # TODO: Probably need to remove appid whitelisted if testing in staging works out. 11-6-2019
        enforce_appid_whitelisted(request.headers.get("X-Appengine-Inbound-Appid"), get_whitelisted_appids(user_info))
        logging.info(f"User {user_email} ALLOWED")
        return (user_email, user_info)

    logging.info(f"User {user_email} NOT ALLOWED")
    raise Forbidden()


class ObjectView(object):
    """access dict attributes as an object"""

    def __init__(self, d):
        self.__dict__ = d


class ObjDict(dict):
    """Subclass dict to treat new dicts like objects"""

    def __getattr__(self, name):
        if name in self:
            return self[name]
        else:
            raise AttributeError("No such attribute: " + name)

    def __setattr__(self, name, value):
        self[name] = value

    def __delattr__(self, name):
        if name in self:
            del self[name]
        else:
            raise AttributeError("No such attribute: " + name)


def datetime_as_naive_utc(value):
    if not isinstance(value, datetime.datetime):
        raise TypeError("datetime_as_naive_utc() only works on datetime.datetime values")
    if value.tzinfo is None:
        return value
    else:
        return value.astimezone(pytz.UTC).replace(tzinfo=None)


def is_care_evo_and_not_prod():
    return GAE_PROJECT != "all-of-us-rdr-prod" and get_account_origin_id() == "careevolution"


def install_rate_limiting(app):
    cache_location = config.getSettingJson('cache_storage_location', default='memory://')
    default_rate_limit = config.getSettingJson('default_rate_limit', '15/second')
    Limiter(
        app,
        key_func=lambda: get_oauth_id() or get_remote_address(),
        default_limits=[default_rate_limit],
        storage_uri=cache_location,
        in_memory_fallback_enabled=True  # Use local memory if cache not found (throws an error otherwise)
    )
