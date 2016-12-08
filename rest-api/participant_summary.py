'''The definition of the participant summary object and DB marshalling.
'''

import api_util
import data_access_object

from protorpc import messages
from google.appengine.ext import ndb
from google.appengine.ext.ndb import msgprop

DATE_OF_BIRTH_FORMAT = '%Y-%m-%d'

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

class RecruitmentSource(messages.Enum):
  HPO = 1
  DIRECT_VOLUNTEER = 2


# Valid values for the HPO, not currently enforced.
HPO_VALUES = (
    'pitt',        # Pitt/UPMC
    'columbia',    # Columbia University Medical Center
    'illinois',    # Illinois Precision Medicine Consortium
    'az_tucson',   # University of Arizona, Tucson
    'comm_health', # Community Health Center
    'san_ysidro',  # San Ysidro health Center, Inc.
    'cherokee',    # Cherokee Health Systems
    'eau_claire',  # Eau Claire Cooperative Health Centers, Inc
    'hrhcare',     # HRHCare (Hudson River Healthcare)
    'jackson',     # Jackson-Hinds Comprehensive Health Center
    'geisinger',   # Geisinger Health System
    'cal_pmc',     # California Precision Medicine Consortium
    'ne_pmc',      # New England Precision Medicine Consortium
    'trans_am',    # Trans-American Consortium for the Health Care Systems Research Network
    'va',          # Veterans Affairs
)


class ParticipantSummary(ndb.Model):
  """The participant summary resource definition"""
  participant_id = ndb.StringProperty()
  biobank_id = ndb.StringProperty()
  first_name = ndb.StringProperty()
  first_name_search = ndb.ComputedProperty(
      lambda self: api_util.searchable_representation(self.first_name))
  middle_name = ndb.StringProperty()
  last_name = ndb.StringProperty()
  last_name_search = ndb.ComputedProperty(
      lambda self: api_util.searchable_representation(self.last_name))
  zip_code = ndb.StringProperty()
  date_of_birth = ndb.DateProperty()
  gender_identity = msgprop.EnumProperty(GenderIdentity)
  membership_tier = msgprop.EnumProperty(MembershipTier)
  physical_evaluation_status = msgprop.EnumProperty(PhysicalEvaluationStatus)
  sign_up_time = ndb.DateTimeProperty()
  consent_time = ndb.DateTimeProperty()
  hpo_id = ndb.StringProperty()
  recruitment_source = msgprop.EnumProperty(RecruitmentSource)
  last_modified = ndb.DateTimeProperty(auto_now=True)

class ParticipantSummaryDAO(data_access_object.DataAccessObject):
  def __init__(self):
    super(ParticipantSummaryDAO, self).__init__(Participant)

  def properties_from_json(self, dict_, ancestor_id, id_):
    if id_:
      dict_['participant_id'] = id_
    api_util.parse_json_date(dict_, 'date_of_birth', DATE_OF_BIRTH_FORMAT)
    api_util.parse_json_date(dict_, 'sign_up_time')
    api_util.parse_json_date(dict_, 'consent_time')
    api_util.parse_json_enum(dict_, 'gender_identity', GenderIdentity)
    api_util.parse_json_enum(dict_, 'membership_tier', MembershipTier)
    api_util.parse_json_enum(dict_, 'physical_evaluation_status', PhysicalEvaluationStatus)
    api_util.parse_json_enum(dict_, 'recruitment_source', RecruitmentSource)
    return dict_

  def properties_to_json(self, dict_):
    api_util.format_json_date(dict_, 'date_of_birth', DATE_OF_BIRTH_FORMAT)
    api_util.format_json_date(dict_, 'sign_up_time')
    api_util.format_json_date(dict_, 'consent_time')
    api_util.format_json_enum(dict_, 'gender_identity')
    api_util.format_json_enum(dict_, 'membership_tier')
    api_util.format_json_enum(dict_, 'physical_evaluation_status')
    api_util.format_json_enum(dict_, 'recruitment_source')
    api_util.remove_field(dict_, 'first_name_search')
    api_util.remove_field(dict_, 'last_name_search')
    return dict_


  def list(self, first_name, last_name, dob_string, zip_code):
    date_of_birth = api_util.parse_date(dob_string, DATE_OF_BIRTH_FORMAT)
    query = ParticipantSummary.query(
        ParticipantSummary.last_name_search == api_util.searchable_representation(last_name),
        ParticipantSummary.date_of_birth == date_of_birth)
    if first_name:
      query = query.filter(
          ParticipantSummary.first_name_search == api_util.searchable_representation(first_name))

    if zip_code:
      query = query.filter(ParticipantSummary.zip_code == zip_code)

    items = []
    for p in query.fetch():
      items.append(self.to_json(p))
    return {"items": items}