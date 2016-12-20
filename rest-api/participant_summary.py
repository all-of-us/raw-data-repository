'''The definition of the participant summary object and DB marshalling.
'''

import api_util
import data_access_object
import extraction

from dateutil.relativedelta import relativedelta
from protorpc import messages
from participant import Participant
from google.appengine.ext import ndb
from google.appengine.ext.ndb import msgprop

DATE_OF_BIRTH_FORMAT = '%Y-%m-%d'
SINGLETON_SUMMARY_ID = '1'

class PhysicalEvaluationStatus(messages.Enum):
  """The state of the participant's physical evaluation"""
  UNSET = 0
  SCHEDULED = 1
  COMPLETED = 2
  RESULT_READY = 3
  
class QuestionnaireStatus(messages.Enum):
  """The status of a given questionnaire for this participant"""
  UNSET = 0
  COMPLETED = 1

class MembershipTier(messages.Enum):
  """The state of the participant"""
  UNSET = 0
  SKIPPED = 1
  UNMAPPED = 2
  REGISTERED = 3
  VOLUNTEER = 4
  FULL_PARTICIPANT = 5
  ENROLLEE = 6
  # Note that these are out of order; ENROLEE was added after FULL_PARTICIPANT.

class GenderIdentity(messages.Enum):
  """The gender identity of the participant."""
  UNSET = 0
  SKIPPED = 1
  UNMAPPED = 2  
  FEMALE = 3
  MALE = 4
  FEMALE_TO_MALE_TRANSGENDER = 5
  MALE_TO_FEMALE_TRANSGENDER = 6
  INTERSEX = 7
  OTHER = 8
  PREFER_NOT_TO_SAY = 9
 
class Ethnicity(messages.Enum):
  """The ethnicity of the participant.""" 
  UNSET = 0
  SKIPPED = 1
  UNMAPPED = 2
  HISPANIC = 3
  NON_HISPANIC = 4  
  PREFER_NOT_TO_SAY = 5

class Race(messages.Enum):
  UNSET = 0
  SKIPPED = 1
  UNMAPPED = 2
  AMERICAN_INDIAN_OR_ALASKA_NATIVE = 3
  BLACK_OR_AFRICAN_AMERICAN = 4
  ASIAN = 5    
  NATIVE_HAWAIIAN_OR_OTHER_PACIFIC_ISLANDER = 6
  WHITE = 7
  OTHER_RACE = 8  
  PREFER_NOT_TO_SAY = 9

# The lower bounds of the age buckets.
_AGE_LB = [0, 18, 26, 36, 46, 56, 66, 76, 86]
AGE_BUCKETS = ['{}-{}'.format(b, e) for b, e in zip(_AGE_LB, [a - 1 for a in _AGE_LB[1:]] + [''])]

def extract_bucketed_age(participant_hist_obj):
  if participant_hist_obj.date_of_birth:
    bucketed_age = get_bucketed_age(participant_hist_obj.date_of_birth, participant_hist_obj.date)
    if bucketed_age:
      return extraction.ExtractionResult(bucketed_age, True)
  return extraction.ExtractionResult(None, False)
  
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
  genderIdentity = msgprop.EnumProperty(GenderIdentity, default=GenderIdentity.UNSET)
  membershipTier = msgprop.EnumProperty(MembershipTier, default=MembershipTier.UNSET)
  race = msgprop.EnumProperty(Race, default=Race.UNSET)
  ethnicity = msgprop.EnumProperty(Ethnicity, default=Ethnicity.UNSET)
  physicalEvaluationStatus = msgprop.EnumProperty(PhysicalEvaluationStatus, default=PhysicalEvaluationStatus.UNSET)
  signUpTime = ndb.DateTimeProperty()
  consentTime = ndb.DateTimeProperty()
  hpoId = ndb.StringProperty(default='UNSET')
  consentForStudyEnrollment = msgprop.EnumProperty(QuestionnaireStatus, default=QuestionnaireStatus.UNSET)
  consentForElectronicHealthRecords = msgprop.EnumProperty(QuestionnaireStatus, default=QuestionnaireStatus.UNSET)
  questionnaireOnOverallHealth = msgprop.EnumProperty(QuestionnaireStatus, default=QuestionnaireStatus.UNSET)
  questionnaireOnPersonalHabits = msgprop.EnumProperty(QuestionnaireStatus, default=QuestionnaireStatus.UNSET)
  questionnaireOnSociodemographics = msgprop.EnumProperty(QuestionnaireStatus, default=QuestionnaireStatus.UNSET)
  questionnaireOnHealthcareAccess = msgprop.EnumProperty(QuestionnaireStatus, default=QuestionnaireStatus.UNSET)
  questionnaireOnMedicalHistory = msgprop.EnumProperty(QuestionnaireStatus, default=QuestionnaireStatus.UNSET)
  questionnaireOnMedications = msgprop.EnumProperty(QuestionnaireStatus, default=QuestionnaireStatus.UNSET)
  questionnaireOnFamilyHealth = msgprop.EnumProperty(QuestionnaireStatus, default=QuestionnaireStatus.UNSET)

class ParticipantSummaryDAO(data_access_object.DataAccessObject):
  def __init__(self):
    super(ParticipantSummaryDAO, self).__init__(ParticipantSummary, Participant,
                                                keep_history=False)

  def properties_to_json(self, dict_):
    api_util.format_json_date(dict_, 'dateOfBirth', DATE_OF_BIRTH_FORMAT)
    api_util.format_json_date(dict_, 'signUpTime')
    api_util.format_json_date(dict_, 'consentTime')
    api_util.format_json_enum(dict_, 'genderIdentity')
    api_util.format_json_enum(dict_, 'race')
    api_util.format_json_enum(dict_, 'ethnicity')
    api_util.format_json_enum(dict_, 'membershipTier')
    api_util.format_json_enum(dict_, 'physicalEvaluationStatus')
    api_util.format_json_enum(dict_, 'consentForStudyEnrollment')
    api_util.format_json_enum(dict_, 'consentForElectronicHealthRecords')
    api_util.format_json_enum(dict_, 'questionnaireOnOverallHealth')
    api_util.format_json_enum(dict_, 'questionnaireOnPersonalHabits')        
    api_util.format_json_enum(dict_, 'questionnaireOnSociodemographics')
    api_util.format_json_enum(dict_, 'questionnaireOnHealthcareAccess')
    api_util.format_json_enum(dict_, 'questionnaireOnMedicalHistory')
    api_util.format_json_enum(dict_, 'questionnaireOnMedications')
    api_util.format_json_enum(dict_, 'questionnaireOnFamilyHealth')
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
    
  def get_summary_for_participant(self, participant_id):
    return self.load_if_present(SINGLETON_SUMMARY_ID, participant_id)
  

DAO = ParticipantSummaryDAO()
