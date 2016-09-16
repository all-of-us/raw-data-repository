'''The definition of the participant object and DB marshalling.
'''
from data_access_object import DataAccessObject
from protorpc import message_types
from protorpc import messages

KEY_COLUMNS = ('participant_id',)

# For now, the participant fields map directly to the db columns, so do a simple
# mapping.
COLUMNS = KEY_COLUMNS + (
    'name',
    'address',
    'date_of_birth',
    'enrollment_status',
    'physical_exam_status',
)


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
  participant_id = messages.StringField(1)
  name = messages.StringField(2)
  address = messages.StringField(3)
  date_of_birth = message_types.DateTimeField(4)
  enrollment_status = messages.EnumField(EnrollmentStatus, 5, default='NONE')
  physical_exam_status = messages.EnumField(
      PhysicalExamStatus, 6, default='NONE')


class ParticipantCollection(messages.Message):
  """Collection of Participants."""
  items = messages.MessageField(Participant, 1, repeated=True)


class ParticipantDao(DataAccessObject):
  def __init__(self):
    super(ParticipantDao, self).__init__(resource=Participant,
                                      table='participant',
                                      columns=COLUMNS,
                                      key_columns=KEY_COLUMNS)


DAO = ParticipantDao()
