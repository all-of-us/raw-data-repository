"""Utilities used by the API definition.
"""

import cachetools
import datetime
import json
import logging
import netaddr
import string

import config

from google.appengine.api import users
from google.appengine.ext import ndb

from flask import request
from protorpc import message_types
from protorpc import messages
from dateutil.parser import parse
from google.appengine.api import oauth
from werkzeug.exceptions import Unauthorized, BadRequest

SCOPE = 'https://www.googleapis.com/auth/userinfo.email'
_ALLOWED_USERS_SINGLETON_KEY = "allowed_users"
_IP_CONFIG_SINGLETON_KEY = "ip_config"
EPOCH = datetime.datetime.utcfromtimestamp(0)

def auth_required(func):
  """A decorator that keeps the function from being called without auth."""
  def wrapped(self, *args, **kwargs):
    if request.scheme.lower() != 'https' and config.getSetting(config.ALLOW_INSECURE) != 'True':
      raise Unauthorized('HTTPS is required')
    check_auth()
    return func(self, *args, **kwargs)
  return wrapped

def auth_required_cron_or_admin(func):
  """A decorator that ensures that the user is an admin or cron job."""
  def wrapped(self, *args, **kwargs):
    check_auth_cron_or_admin()
    return func(self, *args, **kwargs)
  return wrapped

def check_auth():
  user = oauth.get_current_user(SCOPE)
  enforce_user_whitelisted(user)
  ip = request.remote_addr
  enforce_ip_whitelisted(ip)

def get_client_id():
  return oauth.get_current_user(SCOPE).email()

def check_auth_cron_or_admin():
  """Returns true if the current user is a cron job or an admin.

  Only members of the cloud project can be admin users:
  https://cloud.google.com/appengine/docs/python/users/adminusers

  Cron jobs also appear as admin users.
  """
  return users.is_current_user_admin()

def enforce_user_whitelisted(user):
  user_email = 'None'
  if user:
    user_email = user.email()
    if user.email() in CONFIG_CACHE[_ALLOWED_USERS_SINGLETON_KEY]:
      logging.info('User {} ALLOWED'.format(user_email))
      return
  logging.info('User {} NOT ALLOWED'.format(user_email))
  raise Unauthorized('Forbidden.')

def enforce_ip_whitelisted(ip_string):
  allowed_ip_config = CONFIG_CACHE[_IP_CONFIG_SINGLETON_KEY]
  logging.info('IP RANGES ALLOWED: {}'.format(allowed_ip_config))
  ip = netaddr.IPAddress(ip_string)
  if not bool([True for rng in allowed_ip_config if ip in rng]):
    logging.info('IP {} NOT ALLOWED'.format(ip))
    raise Unauthorized('Client IP not whitelisted: {}'.format(ip))
  logging.info('IP {} ALLOWED'.format(ip))

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

def _get_config(key):
  if key == _ALLOWED_USERS_SINGLETON_KEY:
    return frozenset(config.getSettingList(config.ALLOWED_USER))
  elif key == _IP_CONFIG_SINGLETON_KEY:
    ip_ranges = json.loads(config.getSetting(config.ALLOWED_IP))
    return [netaddr.IPNetwork(rng)
            for rng in ip_ranges['ip6'] + ip_ranges['ip4']]
  else:
    logging.error('Unexpected config cache key: {}'.format(key))

CONFIG_CACHE = cachetools.TTLCache(2, ttl=60, missing=_get_config)
