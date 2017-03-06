from protorpc import messages
from dateutil.relativedelta import relativedelta

# These are handled specially in code; others will be inserted into the database and handled
# dynamically.
UNSET_HPO_ID = 0

# TODO(danrodney): get rid of this
class HPOId(messages.Enum):
  """The ID of the HPO the participant signed up with"""
  UNSET = 0
  UNMAPPED = 1
  PITT = 2
  COLUMBIA = 3
  ILLINOIS = 4
  AZ_TUCSON = 5
  COMM_HEALTH = 6
  SAN_YSIDRO = 7
  CHEROKEE = 8
  EAU_CLAIRE = 9
  HRHCARE = 10
  JACKSON = 11
  GEISINGER = 12
  CAL_PMC = 13
  NE_PMC = 14
  TRANS_AM = 15
  VA = 16

class PhysicalMeasurementsStatus(messages.Enum):
  """The state of the participant's physical measurements"""
  UNSET = 0
  SCHEDULED = 1
  COMPLETED = 2
  RESULT_READY = 3

class QuestionnaireStatus(messages.Enum):
  """The status of a given questionnaire for this participant"""
  UNSET = 0
  SUBMITTED = 1

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
  import extraction
  if participant_hist_obj.date_of_birth:
    bucketed_age = get_bucketed_age(participant_hist_obj.date_of_birth, participant_hist_obj.date)
    if bucketed_age:
      return extraction.ExtractionResult(bucketed_age, True)
  return extraction.ExtractionResult(None, False)

def get_bucketed_age(date_of_birth, today):
  if not date_of_birth:
    return 'UNSET'
  age = relativedelta(today, date_of_birth).years
  for begin, end in zip(_AGE_LB, [age_lb - 1 for age_lb in _AGE_LB[1:]] + ['']):
    if (age >= begin) and (not end or age <= end):
      return str(begin) + '-' + str(end)
