from code_constants import UNSET
from protorpc import messages
from dateutil.relativedelta import relativedelta

# These are handled specially in code; others will be inserted into the database and handled
# dynamically.
UNSET_HPO_ID = 0

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

# These race values are derived from one or more answers to the race/ethnicity question
# in questionnaire responses. 
class Race(messages.Enum):
  UNSET = 0
  SKIPPED = 1
  UNMAPPED = 2
  AMERICAN_INDIAN_OR_ALASKA_NATIVE = 3
  BLACK_OR_AFRICAN_AMERICAN = 4
  ASIAN = 5
  NATIVE_HAWAIIAN_OR_OTHER_PACIFIC_ISLANDER = 6
  WHITE = 7
  HISPANIC_LATINO_OR_SPANISH = 8
  MIDDLE_EASTERN_OR_NORTH_AFRICAN = 9
  HLS_AND_WHITE = 10
  HLS_AND_BLACK = 11
  HLS_AND_ONE_OTHER_RACE = 12
  HLS_AND_MORE_THAN_ONE_OTHER_RACE = 13
  MORE_THAN_ONE_RACE = 14
  OTHER_RACE = 15
  PREFER_NOT_TO_SAY = 16

ANSWER_CODE_TO_RACE = {
  'WhatRaceEthnicity_AIAN': Race.AMERICAN_INDIAN_OR_ALASKA_NATIVE,
  'WhatRaceEthnicity_Asian': Race.ASIAN,
  'WhatRaceEthnicity_Black': Race.BLACK_OR_AFRICAN_AMERICAN,
  'WhatRaceEthnicity_MENA': Race.MIDDLE_EASTERN_OR_NORTH_AFRICAN,
  'WhatRaceEthnicity_NHPI': Race.NATIVE_HAWAIIAN_OR_OTHER_PACIFIC_ISLANDER,
  'WhatRaceEthnicity_White': Race.WHITE,
  'WhatRaceEthnicity_Hispanic': Race.HISPANIC_LATINO_OR_SPANISH,
  'WhatRaceEthnicity_FreeText': Race.OTHER_RACE,
  'PMI_Skip': Race.SKIPPED,
  'PMI_PreferNotToAnswer': Race.PREFER_NOT_TO_SAY,
  'PMI_Other': Race.OTHER_RACE,
  'PMI_FreeText': Race.OTHER_RACE,
  'PMI_Unanswered': Race.UNSET 
}
  
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
    return UNSET
  age = relativedelta(today, date_of_birth).years
  for begin, end in zip(_AGE_LB, [age_lb - 1 for age_lb in _AGE_LB[1:]] + ['']):
    if (age >= begin) and (not end or age <= end):
      return str(begin) + '-' + str(end)        

def _map_single_race(self, code):
  race_value = ANSWER_CODE_TO_RACE.get(code.value)
  if race_value:
    return race_value
  return ANSWER_CODE_TO_RACE.get(code.parent)

# See: https://docs.google.com/document/d/1Z1rGULWVlmSIAO38ACjMnz0aMuua3sKqFZXjGqw3gqQ      
def get_race(race_codes):
  if not race_codes:
    return None
  if len(race_codes) == 1:
    return _map_single_race(race_codes[0])
  else:
    hispanic = False
    all_races = set([_map_single_race(race_code) for race_code in race_codes])
    if Race.HISPANIC_LATINO_OR_SPANISH in all_races:
      if len(all_races) > 2:
        return Race.HLS_AND_MORE_THAN_ONE_OTHER_RACE
      if Race.WHITE in all_races:
        return Race.HLS_AND_WHITE
      if Race.BLACK in all_races:
        return Race.HLS_AND_BLACK
      return Race.HLS_AND_ONE_OTHER_RACE
    else:
      return Race.MORE_THAN_ONE_RACE
      