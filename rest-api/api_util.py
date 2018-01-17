"""Utilities used by the API definition, and authentication/authorization/roles."""

import datetime
import string

from dateutil.parser import parse
from protorpc import message_types
from protorpc import messages
from werkzeug.exceptions import BadRequest

from code_constants import UNSET, UNMAPPED


# Role constants
PTC = "ptc"
HEALTHPRO = "healthpro"
STOREFRONT = "storefront"
EXPORTER = "exporter"
PTC_AND_HEALTHPRO = [PTC, HEALTHPRO]
ALL_ROLES = [PTC, HEALTHPRO, STOREFRONT, EXPORTER]

EPOCH = datetime.datetime.utcfromtimestamp(0)

class DateHolder(messages.Message):
  date = message_types.DateTimeField(1)


def parse_date(date_str, date_format=None, date_only=False):
  """Parses JSON dates.

  Args:
    date_format: If specified, use this date format, otherwise uses the proto
      converter's date handling logic.
   date_only: If specified, and true, will raise an exception if the parsed
     timestamp isn't midnight.
  """
  if date_format:
    return datetime.datetime.strptime(date_str, date_format)
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


def parse_json_date(obj, field_name, date_format=None):
  """Converts a field of a dictionary from a string to a datetime."""
  if field_name in obj:
    obj[field_name] = parse_date(obj[field_name], date_format)

def format_json_date(obj, field_name, date_format=None):
  """Converts a field of a dictionary from a datetime to a string."""
  if field_name in obj:
    if obj[field_name] is None:
      del obj[field_name]
    else:
      if date_format:
        obj[field_name] = obj[field_name].strftime(date_format)
      else:
        obj[field_name] = obj[field_name].isoformat()


def format_json_code(obj, code_dao, field_name):
  field_without_id = field_name[0:len(field_name) - 2]
  value = obj.get(field_name)
  if value:
    code = code_dao.get(value)
    if code.mapped:
      obj[field_without_id] = code.value
    else:
      obj[field_without_id] = UNMAPPED
    del obj[field_name]
  else:
    obj[field_without_id] = UNSET

def format_json_hpo(obj, hpo_dao, field_name):
  if obj[field_name]:
    obj[field_name] = hpo_dao.get(obj[field_name]).name
  else:
    obj[field_name] = UNSET

def format_json_org(obj, organization_dao, field_name):
  if obj[field_name]:
    obj[field_name] = organization_dao.get(obj[field_name]).externalId
  else:
    obj[field_name] = UNSET

def format_json_site(obj, site_dao, field_name):
  site_id = obj.get(field_name + 'Id')
  if site_id is not None:
    obj[field_name] = site_dao.get(site_id).googleGroup
    del obj[field_name + 'Id']
  else:
    obj[field_name] = UNSET

def unix_time_millis(dt):
  return int((dt - EPOCH).total_seconds() * 1000)

def parse_json_enum(obj, field_name, enum_cls):
  """Converts a field of a dictionary from a string to an enum."""
  if field_name in obj and obj[field_name] is not None:
    obj[field_name] = enum_cls(obj[field_name])

def format_json_enum(obj, field_name):
  """Converts a field of a dictionary from a enum to an string."""
  if field_name in obj and obj[field_name] is not None:
    obj[field_name] = str(obj[field_name])
  else:
    obj[field_name] = UNSET

def get_site_id_from_google_group(self, obj):
  if 'site' in obj:
    site = self.site_dao.get_by_google_group(obj['site'])
    if site.siteId:
      obj['site'] = site.siteId
  else:
    pass

def get_awardee_id_from_name(self, obj):
  if 'awardee' in obj:
    awardee = self.hpo_dao.get_by_name(obj['awardee'])
    if awardee.hpoId:
      obj['awardee'] = awardee.hpoId
  else:
    pass

def get_organization_id_from_external_id(self, obj):
  if 'organization' in obj:
    organization = self.organization_dao.get_by_external_id(obj['organization'])
    if organization.organizationId:
      obj['organization'] = organization.organizationId
  else:
    pass

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
