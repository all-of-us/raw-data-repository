'''The definition of the participant object and DB marshalling.
'''
from data_access_object import DataAccessObject
from protorpc import message_types
from protorpc import messages

KEY_COLUMNS = ('drc_internal_id',)

# For now, the participant fields map directly to the db columns, so do a simple
# mapping.
COLUMNS = KEY_COLUMNS + (
    'participant_id',
    'biobank_id',
    'first_name',
    'middle_name',
    'last_name',
    'zip_code',
    'date_of_birth',
    'membership_tier',
    'physical_exam_status',
    'sign_up_time',
    'consent_time',
    'hpo_id',
    'recruitment_source',
)


class PhysicalExamStatus(messages.Enum):
  """The state of the participant's physical exam"""
  NONE = 0
  SCHEDULED = 1
  COMPLETED = 2
  RESULT_READY = 3


class MembershipTier(messages.Enum):
  """The state of the participant"""
  NONE = 0
  INTERESTED = 1
  CONSENTED = 2
  ENGAGED = 3

class GenderIdentity(messages.Enum):
  """The gender identity of the participant."""
  NONE = 0
  FEMALE = 1
  MALE = 2
  NEITHER = 3
  OTHER = 4
  PREFER_NOT_TO_SAY = 5

class RecruitmentSource(messages.Enum):
  NONE = 0
  HPO = 1
  DIRECT_VOLUNTEER = 2


class Participant(messages.Message):
  """The participant resource definition"""
  participant_id = messages.StringField(1)
  drc_internal_id = messages.StringField(2)
  biobank_id = messages.StringField(3)
  first_name = messages.StringField(4)
  middle_name = messages.StringField(5)
  last_name = messages.StringField(6)
  zip_code = messages.StringField(7)
  date_of_birth = message_types.DateTimeField(8)
  gender_identity = messages.EnumField(GenderIdentity, 9, default='NONE')
  membership_tier = messages.EnumField(MembershipTier, 10, default='NONE')
  physical_exam_status = messages.EnumField(
      PhysicalExamStatus, 11, default='NONE')
  sign_up_time = message_types.DateTimeField(12)
  consent_time = message_types.DateTimeField(13)
  hpo_id = messages.StringField(14)
  recruitment_source = messages.EnumField(RecruitmentSource, 16, default='NONE')

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
