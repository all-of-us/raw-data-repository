'''The definition of the participant object and DB marshalling.
'''

import pprint

from data_access_object import DataAccessObject
from protorpc import message_types
from protorpc import messages
from google.appengine.ext import ndb
from endpoints_proto_datastore.ndb import EndpointsModel


class PhysicalExamStatus(messages.Enum):
  """The state of the participant's physical exam"""
  SCHEDULED = 1
  COMPLETED = 2
  RESULT_READY = 3


class MembershipTier(messages.Enum):
  """The state of the participant"""
  INTERESTED = 1
  CONSENTED = 2
  ENGAGED = 3

class GenderIdentity(messages.Enum):
  """The gender identity of the participant."""
  FEMALE = 1
  MALE = 2
  NEITHER = 3
  OTHER = 4
  PREFER_NOT_TO_SAY = 5

class RecruitmentSource(messages.Enum):
  HPO = 1
  DIRECT_VOLUNTEER = 2


class Participant(EndpointsModel):
  """The participant resource definition"""
  participant_id = ndb.StringProperty()
  drc_internal_id = ndb.StringProperty()
  biobank_id = ndb.StringProperty()
  first_name = ndb.StringProperty()
  middle_name = ndb.StringProperty()
  last_name = ndb.StringProperty()
  zip_code = ndb.StringProperty()
  date_of_birth = ndb.DateProperty()
  gender_identity = ndb.StringProperty()
  membership_tier = ndb.StringProperty()
  physical_exam_status = ndb.StringProperty()
  sign_up_time = ndb.DateTimeProperty()
  consent_time = ndb.DateTimeProperty()
  hpo_id = ndb.StringProperty()
  recruitment_source = ndb.StringProperty()


def update(model):
  # Get the existing entity from the datastore by the drc_internal_id.  We just
  # need the key so we can update it.
  participant = get(model.drc_internal_id)
  # This fills in all unset fields in entity with thier corresponding values
  # from participant.
  model.EntityKeySet(participant.entityKey)
  # Write back the merged entity.
  model.put()
  return model

def get(drc_internal_id):
  query = Participant.query(Participant.drc_internal_id == drc_internal_id)
  iterator = query.iter()
  if not iterator.has_next():
    raise endpoints.NotFoundException(
        'Participant with id {} not found.'.format(drc_internal_id))
  participant = iterator.next()
  if iterator.has_next():
    raise endpoints.InternalServerErrorException(
        'More that one participant with id {} found.'.format(drc_internal_id))
  return participant

def list(model):
  query = Participant.query(
      Participant.last_name == model.last_name,
      Participant.date_of_birth == model.date_of_birth)
  if model.first_name:
    query = query.filter(Participant.first_name == model.first_name)

  return Participant.ToMessageCollection(query.fetch())


# class ParticipantCollection(messages.Message):
#   """Collection of Participants."""
#   items = messages.MessageField(Participant, 1, repeated=True)


# class ParticipantDao(DataAccessObject):
#   def __init__(self):
#     super(ParticipantDao, self).__init__(resource=Participant,
#                                       table='participant',
#                                       columns=COLUMNS,
#                                       key_columns=KEY_COLUMNS)


#DAO = ParticipantDao()
