'''The definition of the participant object and DB marshalling.
'''

import api_util
import datetime
import copy

from protorpc import messages
from google.appengine.ext import ndb


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
  gender_identity = ndb.StringProperty()
  membership_tier = ndb.StringProperty()
  physical_exam_status = ndb.StringProperty()
  sign_up_time = ndb.DateTimeProperty()
  consent_time = ndb.DateTimeProperty()
  hpo_id = ndb.StringProperty()
  recruitment_source = ndb.StringProperty()

def from_json(json, id=None):
  json = copy.deepcopy(json)

  if id:
    json['drc_internal_id'] = id

  p=Participant(id=id)

  if 'date_of_birth' in json:
    json['date_of_birth'] = _parse_date_of_birth(json['date_of_birth'])
  if 'sign_up_time' in json:
    json['sign_up_time'] = api_util.parse_date(json['sign_up_time'])
  if 'consent_time' in json:
    json['consent_time'] = api_util.parse_date(json['consent_time'])
  p.populate(**json)
  return p

def to_json(p):
  dict = p.to_dict()
  dict = copy.deepcopy(dict)
  if dict['date_of_birth']:
    dict['date_of_birth'] = dict['date_of_birth'].strftime(DATE_OF_BIRTH_FORMAT)
  if dict['sign_up_time']:
    dict['sign_up_time'] = dict['sign_up_time'].isoformat()
  if dict['consent_time']:
    dict['consent_time'] = dict['consent_time'].isoformat()
  return dict

def list(first_name, last_name, dob_string):
  date_of_birth = _parse_date_of_birth(dob_string)
  query = Participant.query(Participant.last_name == last_name,
                            Participant.date_of_birth == date_of_birth)
  if first_name:
    query = query.filter(Participant.first_name == first_name)

  items = []
  for p in query.fetch():
    items.append(to_json(p))
  return {"items": items}


def _parse_date_of_birth(dob_string):
  return datetime.datetime.strptime(dob_string, DATE_OF_BIRTH_FORMAT)
