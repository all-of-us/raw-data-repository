from sqlalchemy.types import SmallInteger, TypeDecorator
from werkzeug.exceptions import BadRequest
from werkzeug.routing import BaseConverter

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

def to_client_participant_id(participant_id):
  return 'P%d' % participant_id

def from_client_participant_id(participant_id):
  if not participant_id.startswith('P'):
    raise BadRequest("Invalid participant ID: %s" % participant_id)
  try:
    return int(participant_id[1:])
  except ValueError:
    raise BadRequest("Invalid participant ID: %s" % participant_id)

def to_client_biobank_id(biobank_id):
  return 'B%d' % biobank_id

def from_client_biobank_id(biobank_id):
  if not biobank_id.startswith('B'):
    raise BadRequest("Invalid biobank ID: %s" % biobank_id)
  try:
    return int(biobank_id[1:])
  except ValueError:
    raise BadRequest("Invalid biobank ID: %s" % biobank_id)
    
def ParticipantIdConverter(BaseConverter):
  
  def to_python(self, value):
    return from_client_participant_id(value)

  def to_url(self, value):
    return to_client_participant_id(value)
