"""Utilities used by the API definition.
"""

import config
import datetime


from protorpc import message_types
from protorpc import protojson
from protorpc import messages
from google.appengine.api import oauth
from werkzeug.exceptions import Unauthorized, BadRequest

def check_auth():
  user = oauth.get_current_user()
  return is_user_whitelisted(user)

def is_user_whitelisted(user):
  if user and user.email() in config.getSettingList(config.ALLOWED_USER):
    return

  raise Unauthorized('Forbidden.')


def update_model(old_model, new_model):
  """Updates a model.
  For all fields that are set in new_model, copy them into old_model.

  Args:
    old_model: The ndb model object retrieved from the datastore.
    new_model_dict: A json object containing the new values.
  """

  for k, v in new_model.to_dict().iteritems():
    if v is not None:
      setattr(old_model, k, v)

class DateHolder(messages.Message):
  date = message_types.DateTimeField(1)

def parse_date(date_str, date_only=False):
  """Parses JSON dates.

  Uses the proto converter's date handling logic.
  """
  json_str = '{{"date": "{}"}}'.format(date_str)
  holder = protojson.decode_message(DateHolder, json_str)

  date_obj = holder.date
  if date_only:
    if (date_obj != datetime.datetime.combine(date_obj.date(),
                                              datetime.datetime.min.time())):
      raise BadRequest('Date contains non zero time fields')
  return date_obj
