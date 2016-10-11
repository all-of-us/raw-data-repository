'''The definition of the participant object and DB marshalling.
'''

import api_util

import data_access_object
import identifier

from protorpc import messages
from google.appengine.ext import ndb
from google.appengine.ext.ndb import msgprop

DATE_OF_BIRTH_FORMAT = '%Y-%m-%d'

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


class Participant(ndb.Model):
  """The participant resource definition"""
  participant_id = ndb.StringProperty()
  biobank_id = ndb.StringProperty()
  first_name = ndb.StringProperty()
  first_name_lower = ndb.ComputedProperty(
      lambda self: self.first_name and self.first_name.lower())
  middle_name = ndb.StringProperty()
  last_name = ndb.StringProperty()
  last_name_lower = ndb.ComputedProperty(
      lambda self: self.last_name and self.last_name.lower())
  zip_code = ndb.StringProperty()
  date_of_birth = ndb.DateProperty()
  gender_identity = msgprop.EnumProperty(GenderIdentity)
  membership_tier = msgprop.EnumProperty(MembershipTier)
  physical_exam_status = msgprop.EnumProperty(PhysicalExamStatus)
  sign_up_time = ndb.DateTimeProperty()
  consent_time = ndb.DateTimeProperty()
  hpo_id = ndb.StringProperty()
  recruitment_source = msgprop.EnumProperty(RecruitmentSource)


class ParticipantDAO(data_access_object.DataAccessObject):
  def __init__(self):
    super(ParticipantDAO, self).__init__(Participant)

  def properties_from_json(self, dict_, ancestor_id, id_):
    if id_:
      dict_['participant_id'] = id_

    api_util.parse_json_date(dict_, 'date_of_birth', DATE_OF_BIRTH_FORMAT)
    api_util.parse_json_date(dict_, 'sign_up_time')
    api_util.parse_json_date(dict_, 'consent_time')
    api_util.parse_json_enum(dict_, 'gender_identity', GenderIdentity)
    api_util.parse_json_enum(dict_, 'membership_tier', MembershipTier)
    api_util.parse_json_enum(dict_, 'physical_exam_status', PhysicalExamStatus)
    api_util.parse_json_enum(dict_, 'recruitment_source', RecruitmentSource)
    return dict_

  def properties_to_json(self, dict_):
    api_util.format_json_date(dict_, 'date_of_birth', DATE_OF_BIRTH_FORMAT)
    api_util.format_json_date(dict_, 'sign_up_time')
    api_util.format_json_date(dict_, 'consent_time')
    api_util.format_json_enum(dict_, 'gender_identity')
    api_util.format_json_enum(dict_, 'membership_tier')
    api_util.format_json_enum(dict_, 'physical_exam_status')
    api_util.format_json_enum(dict_, 'recruitment_source')
    api_util.remove_field(dict_, 'first_name_lower')
    api_util.remove_field(dict_, 'last_name_lower')
    return dict_


  def list(self, first_name, last_name, dob_string, zip_code):
    date_of_birth = api_util.parse_date(dob_string, DATE_OF_BIRTH_FORMAT)
    query = Participant.query(Participant.last_name_lower == last_name.lower(),
                              Participant.date_of_birth == date_of_birth)
    if first_name:
      query = query.filter(Participant.first_name_lower == first_name.lower())

    if zip_code:
      query = query.filter(Participant.zip_code == zip_code)

    items = []
    for p in query.fetch():
      items.append(self.to_json(p))
    return {"items": items}

  def allocate_id(self):
    id = identifier.get_id()
    return '{:x}'.format(id).zfill(9)

DAO = ParticipantDAO()
