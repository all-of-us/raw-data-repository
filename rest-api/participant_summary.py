'''The definition of the participant summary object and DB marshalling.
'''

import api_util
import data_access_object

from datetime import datetime
from dateutil.relativedelta import relativedelta
from protorpc import messages
from participant import Participant
from google.appengine.ext import ndb
from google.appengine.ext.ndb import msgprop

DATE_OF_BIRTH_FORMAT = '%Y-%m-%d'
SINGLETON_SUMMARY_ID = '1'

class PhysicalEvaluationStatus(messages.Enum):
  """The state of the participant's physical evaluation"""
  SCHEDULED = 1
  COMPLETED = 2
  RESULT_READY = 3


class MembershipTier(messages.Enum):
  """The state of the participant"""
  REGISTERED = 1
  VOLUNTEER = 2
  FULL_PARTICIPANT = 3
  ENROLLEE = 4
  # Note that these are out of order; ENROLEE was added after FULL_PARTICIPANT.

class GenderIdentity(messages.Enum):
  """The gender identity of the participant."""
  FEMALE = 1
  MALE = 2
  FEMALE_TO_MALE_TRANSGENDER = 3
  MALE_TO_FEMALE_TRANSGENDER = 4
  INTERSEX = 5
  OTHER = 6
  PREFER_NOT_TO_SAY = 7

# The lower bounds of the age buckets.
_AGE_LB = [0, 18, 26, 36, 46, 56, 66, 76, 86]
AGE_BUCKETS = ['{}-{}'.format(b, e) for b, e in zip(_AGE_LB, [a - 1 for a in _AGE_LB[1:]] + [''])]

def get_bucketed_age(date_of_birth, today):
  age = relativedelta(today, date_of_birth).years
  for begin, end in zip(_AGE_LB, [a - 1 for a in _AGE_LB[1:]] + ['']):
    if (age >= begin) and (not end or age <= end):
      return str(begin) + '-' + str(end)

class ParticipantSummary(ndb.Model):
  """The participant summary resource definition"""
  participantId = ndb.StringProperty()
  biobankId = ndb.StringProperty()
  firstName = ndb.StringProperty()
  firstNameSearch = ndb.ComputedProperty(
      lambda self: api_util.searchable_representation(self.firstName))
  middleName = ndb.StringProperty()
  lastName = ndb.StringProperty()
  lastNameSearch = ndb.ComputedProperty(
      lambda self: api_util.searchable_representation(self.lastName))
  zipCode = ndb.StringProperty()
  dateOfBirth = ndb.DateProperty()
  genderIdentity = msgprop.EnumProperty(GenderIdentity)
  membershipTier = msgprop.EnumProperty(MembershipTier)
  physicalEvaluationStatus = msgprop.EnumProperty(PhysicalEvaluationStatus)
  signUpTime = ndb.DateTimeProperty()
  consentTime = ndb.DateTimeProperty()
  hpoId = ndb.StringProperty()

class ParticipantSummaryDAO(data_access_object.DataAccessObject):
  def __init__(self):
    super(ParticipantSummaryDAO, self).__init__(ParticipantSummary, Participant,
                                                keep_history = False)

  def properties_from_json(self, dict_, ancestor_id, id_):
    if id_:
      dict_['participantId'] = id_
    api_util.parse_json_date(dict_, 'dateOfBirth', DATE_OF_BIRTH_FORMAT)
    api_util.parse_json_date(dict_, 'signUpTime')
    api_util.parse_json_date(dict_, 'consentTime')
    api_util.parse_json_enum(dict_, 'genderIdentity', GenderIdentity)
    api_util.parse_json_enum(dict_, 'membershipTier', MembershipTier)
    api_util.parse_json_enum(dict_, 'physicalEvaluationStatus', PhysicalEvaluationStatus)
    api_util.parse_json_enum(dict_, 'recruitmentSource', RecruitmentSource)
    return dict_

  def properties_to_json(self, dict_):
    api_util.format_json_date(dict_, 'dateOfBirth', DATE_OF_BIRTH_FORMAT)
    api_util.format_json_date(dict_, 'signUpTime')
    api_util.format_json_date(dict_, 'consentTime')
    api_util.format_json_enum(dict_, 'genderIdentity')
    api_util.format_json_enum(dict_, 'membershipTier')
    api_util.format_json_enum(dict_, 'physicalEvaluationStatus')
    api_util.format_json_enum(dict_, 'recruitmentSource')
    api_util.remove_field(dict_, 'firstNameSearch')
    api_util.remove_field(dict_, 'lastNameSearch')
    return dict_

  def list(self, first_name, last_name, dob_string, zip_code):
    date_of_birth = api_util.parse_date(dob_string, DATE_OF_BIRTH_FORMAT)
    query = ParticipantSummary.query(
        ParticipantSummary.lastNameSearch == api_util.searchable_representation(last_name),
        ParticipantSummary.dateOfBirth == date_of_birth)
    if first_name:
      query = query.filter(
          ParticipantSummary.firstNameSearch == api_util.searchable_representation(first_name))

    if zip_code:
      query = query.filter(ParticipantSummary.zipCode == zip_code)

    items = []
    for p in query.fetch():
      items.append(self.to_json(p))
    return {"items": items}

DAO = ParticipantSummaryDAO()
