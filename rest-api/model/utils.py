import config
from dateutil.tz import tzutc
from query import PropertyType
from sqlalchemy.types import SmallInteger, TypeDecorator, DateTime
from werkzeug.exceptions import BadRequest
from werkzeug.routing import BaseConverter, ValidationError

_PROPERTY_TYPE_MAP = {
  'String': PropertyType.STRING,
  'Date': PropertyType.DATE,
  'DateTime': PropertyType.DATETIME,
  'UTCDateTime': PropertyType.DATETIME,
  'Enum': PropertyType.ENUM,
  'Integer': PropertyType.INTEGER,
  'SmallInteger': PropertyType.INTEGER
}

class Enum(TypeDecorator):
  """A type for a SQLAlchemy column based on a protomsg Enum provided in the constructor"""
  impl = SmallInteger

  def __init__(self, enum_type):
    super(Enum, self).__init__()
    self.enum_type = enum_type

  def __repr__(self):
    return "Enum(%s)" % self.enum_type.__name__

  def process_bind_param(self, value, dialect):  # pylint: disable=unused-argument
    return int(value) if value else None

  def process_result_value(self, value, dialect):  # pylint: disable=unused-argument
    return self.enum_type(value) if value else None

class UTCDateTime(TypeDecorator):
  impl = DateTime

  def process_bind_param(self, value, engine):
    #pylint: disable=unused-argument
    if value is not None and value.tzinfo:
      return value.astimezone(tzutc()).replace(tzinfo=None)
    return value

def to_client_participant_id(participant_id):
  return 'P%d' % participant_id

def from_client_participant_id(participant_id):
  if not participant_id.startswith('P'):
    raise BadRequest("Invalid participant ID: %s" % participant_id)
  try:
    return int(participant_id[1:])
  except ValueError:
    raise BadRequest("Invalid participant ID: %s" % participant_id)

def get_biobank_id_prefix():
  return str(config.getSetting(config.BIOBANK_ID_PREFIX, 'Z'))

def to_client_biobank_id(biobank_id):
  return '%s%d' % (get_biobank_id_prefix(), biobank_id)

def from_client_biobank_id(biobank_id):
  if not biobank_id.startswith(get_biobank_id_prefix()):
    raise BadRequest("Invalid biobank ID: %s" % biobank_id)
  try:
    return int(biobank_id[1:])
  except ValueError:
    raise BadRequest("Invalid biobank ID: %s" % biobank_id)

class ParticipantIdConverter(BaseConverter):

  def to_python(self, value):
    try:
      return from_client_participant_id(value)
    except BadRequest as ex:
      raise ValidationError(ex.description)

  def to_url(self, value):
    # Assume the client has already converted this.
    return value

def get_property_type(prop):
  prop_property = getattr(prop, "property", None)
  if not prop_property:
    return None
  columns = getattr(prop_property, "columns", None)
  if not columns:
    return None
  property_classname = columns[0].type.__class__.__name__
  property_type = _PROPERTY_TYPE_MAP.get(property_classname)
  if not property_type:
    return None
  return property_type
