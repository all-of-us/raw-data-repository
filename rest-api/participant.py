'''The definition of the participant object and DB marshalling.
'''
import db
import uuid

from protorpc import message_types
from protorpc import messages

# For now, the participant fields map directly to the db columns, so do a simple
# mapping.
PARTICIPANT_COLUMNS = [
    'id',
    'name',
    'address',
    'date_of_birth',
    'enrollment_status',
    'physical_exam_status',
]


class PhysicalExamStatus(messages.Enum):
  """The state of the participant's physical exam"""
  NONE = 0
  SCHEDULED = 1
  COMPLETED = 2
  RESULT_READY = 3


class EnrollmentStatus(messages.Enum):
  """The state of the participant"""
  NONE = 0
  INTERESTED = 1
  CONSENTED = 2
  ENGAGED = 3


class Participant(messages.Message):
  """The participant resource definition"""
  id = messages.StringField(1)
  name = messages.StringField(2)
  address = messages.StringField(3)
  date_of_birth = message_types.DateTimeField(4)
  enrollment_status = messages.EnumField(EnrollmentStatus, 5, default='NONE')
  physical_exam_status = messages.EnumField(
      PhysicalExamStatus, 6, default='NONE')


class ParticipantCollection(messages.Message):
  """Collection of Participants."""
  items = messages.MessageField(Participant, 1, repeated=True)


def GetParticipant(id):
  """Retrieves a participant by id."""
  conn = db.GetConn()
  try:
    return conn.GetObject(Participant, id)
  finally:
    conn.Release()

def InsertParticipant(participant):
  """Creates a participant."""
  participant.id = str(uuid.uuid4())
  conn = db.GetConn()
  try:
    obj = conn.InsertObject(Participant, participant)
    conn.Commit()
    return obj
  finally:
    conn.Release()

def UpdateParticipant(participant):
  """Sets only the specified fields on the participant.

  All other fields are untouched.
  """
  conn = db.GetConn()
  try:
    obj = conn.InsertObject(Participant, participant, update=True)
    conn.Commit()
    return obj
  finally:
    conn.Release()

def ListParticipants():
  conn = db.GetConn()
  try:
    return conn.ListObjects(Participant)
  finally:
    conn.Release()

db.RegisterType(Participant, 'participant', PARTICIPANT_COLUMNS)
