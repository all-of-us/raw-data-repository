import email.utils
import logging
import pytz
import time
import netaddr

from google.appengine.api import app_identity
from google.appengine.api import oauth

from flask import request
from werkzeug.exceptions import Forbidden, Unauthorized

import clock
import config


_GMT = pytz.timezone('GMT')
SCOPE = 'https://www.googleapis.com/auth/userinfo.email'


def handle_database_disconnect(err):
  """Intended to catch DBAPIError's thrown during a request cycle and transform them into 503's.
  If the DBAPIError does not represent an invalidated connection, reraise the error.

  Usage: app.register_error_handler(DBAPIError, handle_database_disconnect)
  """
  if err.connection_invalidated:
    return 'DB connection lost, please retry', 503
  raise err


def auth_required_cron(func):
  """A decorator that ensures that the user is a cron job."""
  def wrapped(*args, **kwargs):
    check_cron()
    return func(*args, **kwargs)
  return wrapped


def nonprod(func):
  """The decorated function may never run in environments without config.ALLOW_NONPROD_REQUESTS."""
  def wrapped(*args, **kwargs):
    if not config.getSettingJson(config.ALLOW_NONPROD_REQUESTS, False):
      raise Forbidden('Request not allowed in production environment (according to config).')
    return func(*args, **kwargs)
  return wrapped


def check_auth(role_whitelist):
  """Raises Unauthorized or Forbidden if the current user is not allowed."""
  user_email, user_info = get_validated_user_info()

  if set(user_info.get('roles', [])) & set(role_whitelist):
    return

  logging.info('User {} has roles {}, but {} is required'.format(
     user_email,
     user_info.get('roles'),
     role_whitelist))
  raise Forbidden()


def get_oauth_id():
  """Returns user email ID if OAUTH token present, or None."""
  try:
    user_email = oauth.get_current_user(SCOPE).email()
  except oauth.Error as e:
    user_email = None
    logging.error('OAuth failure: {}'.format(e))
  return user_email


def check_cron():
  """Raises Forbidden if the current user is not a cron job."""
  if request.headers.get('X-Appengine-Cron'):
    logging.info('Appengine-Cron ALLOWED for cron endpoint.')
    return
  logging.info('User {} NOT ALLOWED for cron endpoint'.format(
      get_oauth_id()))
  raise Forbidden()


def lookup_user_info(user_email):
  return config.getSettingJson(config.USER_INFO, {}).get(user_email)


def _is_self_request():
  return (request.remote_addr is None
      and config.getSettingJson(config.ALLOW_NONPROD_REQUESTS, False)
      and not request.headers.get('unauthenticated'))


def get_validated_user_info():
  """Returns a valid (user email, user info), or raises Unauthorized or Forbidden."""
  user_email = get_oauth_id()
  # Allow clients to simulate an unauthentiated request (for testing)
  # becaues we haven't found another way to create an unauthenticated request
  # when using dev_appserver. When client tests are checking to ensure that an
  # unauthenticated requests gets rejected, they helpfully add this header.
  # The `application_id` check ensures this feature only works in dev_appserver.
  if request.headers.get('unauthenticated') and app_identity.get_application_id() == 'None':
    user_email = None
  if user_email is None:
    raise Unauthorized('No OAuth user found.')

  user_info = lookup_user_info(user_email)
  if user_info:
    enforce_ip_whitelisted(request.remote_addr, get_whitelisted_ips(user_info))
    enforce_appid_whitelisted(request.headers.get('X-Appengine-Inbound-Appid'),
                              get_whitelisted_appids(user_info))
    logging.info('User %r ALLOWED', user_email)
    return (user_email, user_info)

  logging.info('User %r NOT ALLOWED' % user_email)
  raise Forbidden()


def get_whitelisted_ips(user_info):
  if not user_info.get('whitelisted_ip_ranges'):
    return None
  return [netaddr.IPNetwork(rng)
          for rng in user_info['whitelisted_ip_ranges'].get('ip6', []) +
          user_info['whitelisted_ip_ranges'].get('ip4', [])]


def enforce_ip_whitelisted(request_ip, whitelisted_ips):
  if whitelisted_ips == None: # No whitelist means "don't apply restrictions"
    return
  logging.info('IP RANGES ALLOWED: {}'.format(whitelisted_ips))
  ip = netaddr.IPAddress(request_ip)
  if not bool([True for rng in whitelisted_ips if ip in rng]):
    logging.info('IP {} NOT ALLOWED'.format(ip))
    raise Forbidden('Client IP not whitelisted: {}'.format(ip))
  logging.info('IP {} ALLOWED'.format(ip))


def get_whitelisted_appids(user_info):
  return user_info.get('whitelisted_appids')


def enforce_appid_whitelisted(request_app_id, whitelisted_appids):
  if not whitelisted_appids:  # No whitelist means "don't apply restrictions"
    return
  if request_app_id:
    if request_app_id in whitelisted_appids:
      logging.info('APP ID {} ALLOWED'.format(request_app_id))
      return
    else:
      logging.info('APP ID {} NOT FOUND IN {}'.format(request_app_id, whitelisted_appids))
  else:
    logging.info('NO APP ID FOUND WHEN REQUIRED TO BE ONE OF: {}'.format(whitelisted_appids))
  raise Forbidden()


def add_headers(response):
  """Add uniform headers to all API responses.

  All responses are JSON, so we tag them as such at the app level to provide uniform protection
  against content-sniffing-based attacks.
  """
  response.headers['Content-Disposition'] = 'attachment; filename="f.txt"'
  response.headers['X-Content-Type-Options'] = 'nosniff'
  response.headers['Content-Type'] = 'application/json; charset=utf-8'  # override to add charset
  response.headers['Date'] = email.utils.formatdate(
      time.mktime(pytz.utc.localize(clock.CLOCK.now()).astimezone(_GMT).timetuple()),
      usegmt=True)
  response.headers['Pragma'] = 'no-cache'
  response.headers['Cache-control'] = 'no-cache, must-revalidate'
  # Expire at some date in the past: the epoch.
  response.headers['Expires'] = email.utils.formatdate(0.0, usegmt=True)
  return response


def request_logging():
  """Some uniform logging of request characteristics before any checks are applied."""
  logging.info('Request protocol: HTTPS={}'.format(request.environ.get('HTTPS')))


def auth_required(role_whitelist):
  """A decorator that keeps the function from being called without auth.
  role_whitelist can be a string or list of strings specifying one or
  more roles that are allowed to call the function. """

  assert role_whitelist, "Can't call `auth_required` with empty role_whitelist."

  if type(role_whitelist) != list:
    role_whitelist = [role_whitelist]

  def auth_required_wrapper(func):
    def wrapped(*args, **kwargs):
      appid = app_identity.get_application_id()
      # Only enforce HTTPS and auth for external requests; requests made for data generation
      # are allowed through (when enabled).
      if not _is_self_request():
        if request.scheme.lower() != 'https' and appid not in ('None', 'testbed-test', 'testapp'):
          raise Unauthorized('HTTPS is required for %r' % appid)
        check_auth(role_whitelist)
      return func(*args, **kwargs)
    return wrapped
  return auth_required_wrapper


def get_validated_user_info():
  """Returns a valid (user email, user info), or raises Unauthorized or Forbidden."""
  user_email = get_oauth_id()

  # Allow clients to simulate an unauthentiated request (for testing)
  # becaues we haven't found another way to create an unauthenticated request
  # when using dev_appserver. When client tests are checking to ensure that an
  # unauthenticated requests gets rejected, they helpfully add this header.
  # The `application_id` check ensures this feature only works in dev_appserver.
  if request.headers.get('unauthenticated') and app_identity.get_application_id() == 'None':
    user_email = None
  if user_email is None:
    raise Unauthorized('No OAuth user found.')

  user_info = lookup_user_info(user_email)
  if user_info:
    enforce_ip_whitelisted(request.remote_addr, get_whitelisted_ips(user_info))
    enforce_appid_whitelisted(request.headers.get('X-Appengine-Inbound-Appid'),
                              get_whitelisted_appids(user_info))
    logging.info('User %r ALLOWED', user_email)
    return (user_email, user_info)

  logging.info('User %r NOT ALLOWED' % user_email)
  raise Forbidden()
