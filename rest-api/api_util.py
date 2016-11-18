"""Utilities used by the API definition.
"""
import datetime
import logging
import netaddr
import string

import config

from google.appengine.api import app_identity
from google.appengine.api import users
from google.appengine.ext import ndb

from flask import request
from protorpc import message_types
from protorpc import messages
from dateutil.parser import parse
from google.appengine.api import oauth
from werkzeug.exceptions import Unauthorized, BadRequest

SCOPE = 'https://www.googleapis.com/auth/userinfo.email'
EPOCH = datetime.datetime.utcfromtimestamp(0)

# Role constants; used with allowed_roles in auth_required below.
PTC = "ptc"
HEALTHPRO = "healthpro"
PTC_AND_HEALTHPRO = [PTC, HEALTHPRO]

"""A decorator that keeps the function from being called without auth.
   allowed_roles can be a string or list of strings specifying one or
   more roles that are allowed to call the function. """
def auth_required(allowed_roles=None):
  def auth_required_wrapper(func):
    def wrapped(self, *args, **kwargs):
      is_dev_appserver = app_identity.get_application_id() == "None"
      if request.scheme.lower() != 'https' and not is_dev_appserver:
        raise Unauthorized('HTTPS is required')
      allowed_roles_list = allowed_roles
      if allowed_roles and not type(allowed_roles) is list:
        allowed_roles_list = [allowed_roles]
      check_auth(allowed_roles_list)
      return func(self, *args, **kwargs)
    return wrapped
  return auth_required_wrapper

def auth_required_cron_or_admin(func):
  """A decorator that ensures that the user is an admin or cron job."""
  def wrapped(self, *args, **kwargs):
    check_auth_cron_or_admin()
    return func(self, *args, **kwargs)
  return wrapped

def check_auth(allowed_roles):
  user = None
  try:
    user = oauth.get_current_user(SCOPE)
  except oauth.Error as e:
    logging.error('OAuth failure: {}'.format(e))
  ip = request.remote_addr
  user_info = check_user_info(user, ip)
  if allowed_roles:
    if not user_info.get('roles'):
      logging.info('No roles found for user {}'.format(user.email()))
      raise Unauthorized('Forbidden.')
    found_role = False
    for role in user_info['roles']:
      if role in allowed_roles:
        found_role = True
        break
    if not found_role:
      logging.info('No matching role found in {} for user {}'
                   .format(allowed_roles, user.email()))
      raise Unauthorized('Forbidden.')

def get_client_id():
  return oauth.get_current_user(SCOPE).email()

def check_auth_cron_or_admin():
  """Returns true if the current user is a cron job or an admin.

  Only members of the cloud project can be admin users:
  https://cloud.google.com/appengine/docs/python/users/adminusers

  Cron jobs also appear as admin users.
  """
  return users.is_current_user_admin()

def check_user_info(user, ip_string):
  user_email = 'None'
  # We haven't found a way to tell if a dev_appserver request is
  # unauthenticated, so, when the client tests are checking to ensure that an
  # unauthenticated requests gets rejected it helpfully adds this header.
  if user and not request.headers.get('unauthenticated', None):
    user_email = user.email()
    user_info = lookup_user_info(user_email)
    if user_info:
      enforce_ip_whitelisted(ip_string, allowed_ips(user_info))
      enforce_app_id(user_info.get('allowed_app_ids'),
                     request.headers.get('X-Appengine-Inbound-Appid', None))
      logging.info('User {} ALLOWED'.format(user_email))
      return user_info
  logging.info('User {} NOT ALLOWED'.format(user_email))
  raise Unauthorized('Forbidden.')

def enforce_ip_whitelisted(ip_string, allowed_ip_config):
  if not allowed_ip_config:
    return
  logging.info('IP RANGES ALLOWED: {}'.format(allowed_ip_config))
  ip = netaddr.IPAddress(ip_string)
  if not bool([True for rng in allowed_ip_config if ip in rng]):
    logging.info('IP {} NOT ALLOWED'.format(ip))
    raise Unauthorized('Client IP not whitelisted: {}'.format(ip))
  logging.info('IP {} ALLOWED'.format(ip))

def enforce_app_id(allowed_app_ids, app_id):
  if not allowed_app_ids:
    return
  if app_id:
    if app_id in allowed_app_ids:
      logging.info('APP ID {} ALLOWED'.format(app_id))
      return
    else:
      logging.info('APP ID {} NOT FOUND IN {}'.format(app_id, allowed_app_ids))
  else:
    logging.info('NO APP ID FOUND WHEN REQUIRED TO BE ONE OF: {}' + allowed_app_ids)
  raise Unauthorized("User is not in roles: {}".format(allowed_app_ids))

def update_model(old_model, new_model):
  """Updates a model.
  For all fields that are set in new_model, copy them into old_model.

  Args:
    old_model: The ndb model object retrieved from the datastore.
    new_model_dict: A json object containing the new values.
  """

  for k, v in new_model.to_dict().iteritems():
    if type(getattr(type(new_model), k)) != ndb.ComputedProperty and v is not None:
      setattr(old_model, k, v)

class DateHolder(messages.Message):
  date = message_types.DateTimeField(1)

def parse_date(date_str, format=None, date_only=False):
  """Parses JSON dates.

  Args:
    format: If specified, use this date format, otherwise uses the proto
      converter's date handling logic.
   date_only: If specified, and true, will raise an exception if the parsed
     timestamp isn't midnight.
  """
  if format:
    return datetime.datetime.strptime(date_str, format)
  else:
    date_obj = parse(date_str)
    if date_obj.utcoffset():
      date_obj = date_obj.replace(tzinfo=None) - date_obj.utcoffset()
    else:
      date_obj = date_obj.replace(tzinfo=None)
    if date_only:
      if (date_obj != datetime.datetime.combine(date_obj.date(),
                                                datetime.datetime.min.time())):
        raise BadRequest('Date contains non zero time fields')
    return date_obj


def parse_json_date(obj, field_name, format=None):
  """Converts a field of a dictionary from a string to a datetime."""
  if field_name in obj:
    obj[field_name] = parse_date(obj[field_name], format)

def format_json_date(obj, field_name, format=None):
  """Converts a field of a dictionary from a datetime to a string."""
  if field_name in obj:
    if obj[field_name] is None:
      del obj[field_name]
    else:
      if format:
        obj[field_name] = obj[field_name].strftime(format)
      else:
        obj[field_name] = obj[field_name].isoformat()

def unix_time_millis(dt):
  return int((dt - EPOCH).total_seconds() * 1000)

def parse_json_enum(obj, field_name, enum):
  """Converts a field of a dictionary from a string to an enum."""
  if field_name in obj and obj[field_name] is not None:
    obj[field_name] = enum(obj[field_name])


def format_json_enum(obj, field_name):
  """Converts a field of a dictionary from a enum to an string."""
  if field_name in obj and obj[field_name] is not None:
    obj[field_name] = str(obj[field_name])

def remove_field(dict_, field_name):
  """Removes a field from the dict if it exists."""
  if field_name in dict_:
    del dict_[field_name]

def searchable_representation(str_):
  """Takes a string, and returns a searchable representation.

  The string is lowercased and punctuation is removed.
  """
  if not str_:
    return str_

  str_ = str(str_)
  return str_.lower().translate(None, string.punctuation)

def lookup_user_info(user):
  user_info_dict = config.getSettingJson(config.USER_INFO, default={})
  return user_info_dict.get(user)

def allowed_ips(user_info):
  if not user_info.get('ip_ranges'):
    return None
  return [netaddr.IPNetwork(rng)
          for rng in user_info['ip_ranges']['ip6'] + user_info['ip_ranges']['ip4']]
