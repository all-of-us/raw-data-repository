'''The definition of the participant object and DB marshalling.
'''

import api_util
import datetime
import copy

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
  drc_internal_id = ndb.StringProperty()
  biobank_id = ndb.StringProperty()
  first_name = ndb.StringProperty()
  middle_name = ndb.StringProperty()
  last_name = ndb.StringProperty()
  zip_code = ndb.StringProperty()
  date_of_birth = ndb.DateProperty()
  gender_identity = msgprop.EnumProperty(GenderIdentity)
  membership_tier = msgprop.EnumProperty(MembershipTier)
  physical_exam_status = msgprop.EnumProperty(PhysicalExamStatus)
  sign_up_time = ndb.DateTimeProperty()
  consent_time = ndb.DateTimeProperty()
  hpo_id = ndb.StringProperty()
  recruitment_source = msgprop.EnumProperty(RecruitmentSource)

def from_json(dict, id=None):
  dict = copy.deepcopy(dict)

  if id:
    dict['drc_internal_id'] = id

  p=Participant(id=id)
  api_util.parse_json_date(dict, 'date_of_birth', DATE_OF_BIRTH_FORMAT)
  api_util.parse_json_date(dict, 'sign_up_time')
  api_util.parse_json_date(dict, 'consent_time')
  api_util.parse_json_enum(dict, 'gender_identity', GenderIdentity)
  api_util.parse_json_enum(dict, 'membership_tier', MembershipTier)
  api_util.parse_json_enum(dict, 'physical_exam_status', PhysicalExamStatus)
  api_util.parse_json_enum(dict, 'recruitment_source', RecruitmentSource)

  p.populate(**dict)
  return p

def to_json(p):
  dict = p.to_dict()
  dict = copy.deepcopy(dict)
  api_util.format_json_date(dict, 'date_of_birth', DATE_OF_BIRTH_FORMAT)
  api_util.format_json_date(dict, 'sign_up_time')
  api_util.format_json_date(dict, 'consent_time')
  api_util.format_json_enum(dict, 'gender_identity')
  api_util.format_json_enum(dict, 'membership_tier')
  api_util.format_json_enum(dict, 'physical_exam_status')
  api_util.format_json_enum(dict, 'recruitment_source')

  return dict

def list(first_name, last_name, dob_string, zip_code):
  date_of_birth = api_util.parse_date(dob_string, DATE_OF_BIRTH_FORMAT)
  query = Participant.query(Participant.last_name == last_name,
                            Participant.date_of_birth == date_of_birth)
  if first_name:
    query = query.filter(Participant.first_name == first_name)

  if zip_code:
    query = query.filter(Participant.zip_code == zip_code)

  items = []
  for p in query.fetch():
    items.append(to_json(p))
  return {"items": items}
