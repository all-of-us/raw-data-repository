import concepts
import data_access_object
import extraction
import participant_summary
import fhirclient.models.questionnaireresponse

from census_regions import census_regions
from extraction import UNMAPPED, SKIPPED
from google.appengine.ext import ndb
from participant import Participant
from participant_summary import GenderIdentity, MembershipTier
from questionnaire import DAO as questionnaireDAO
from questionnaire import QuestionnaireExtractor

class QuestionnaireResponse(ndb.Model):
  """The questionnaire response."""
  resource = ndb.JsonProperty()
  last_modified = ndb.DateTimeProperty(auto_now=True)

class QuestionnaireResponseDAO(data_access_object.DataAccessObject):
  def __init__(self):
    super(QuestionnaireResponseDAO, self).__init__(QuestionnaireResponse,
                                                   Participant)

  def properties_to_json(self, m):
    return m['resource']

  def properties_from_json(self, dict_, ancestor_id, id_):
    model = fhirclient.models.questionnaireresponse.QuestionnaireResponse(dict_)
    model.id = id_
    return {
        "resource": model.as_json()
    }

  @ndb.transactional
  def store(self, model, date=None, client_id=None):
    super(QuestionnaireResponseDAO, self).store(model, date, client_id)
    participant_id = model.resource['subject']['reference'].split('/')[1]
    gender_identity_result = extract_field(model, concepts.GENDER_IDENTITY)
    if gender_identity_result.extracted:
      participant_summary_obj = participant_summary.DAO.get_summary_for_participant(participant_id)
      # If the gender identity on the participant doesn't match, update it
      if participant_summary_obj.genderIdentity != gender_identity_result.value:
        participant_summary_obj.genderIdentity = gender_identity_result.value
        participant_summary.DAO.store(participant_summary_obj, date, client_id)

DAO = QuestionnaireResponseDAO()


_ETHNICITY_MAPPING = {
    concepts.HISPANIC: 'hispanic',
    concepts.NON_HISPANIC: 'non_hispanic',
    concepts.ASKED_BUT_NO_ANSWER: 'asked_but_no_answer',
}
_RACE_MAPPING = {
    concepts.AMERICAN_INDIAN_OR_ALASKA_NATIVE: 'american_indian_or_alaska_native',
    concepts.BLACK_OR_AFRICAN_AMERICAN: 'black_or_african_american',
    concepts.ASIAN: 'asian',
    concepts.NATIVE_HAWAIIAN_OR_OTHER_PACIFIC_ISLANDER: 'native_hawaiian_or_other_pacific_islander',
    concepts.WHITE: 'white',
    concepts.OTHER_RACE: 'other_race',
    concepts.ASKED_BUT_NO_ANSWER: 'asked_but_no_answer',
}
_GENDER_IDENTITY_MAPPING = {
    concepts.FEMALE: GenderIdentity.FEMALE,
    concepts.FEMALE_TO_MALE_TRANSGENDER: GenderIdentity.FEMALE_TO_MALE_TRANSGENDER,
    concepts.MALE: GenderIdentity.MALE,
    concepts.MALE_TO_FEMALE_TRANSGENDER: GenderIdentity.MALE_TO_FEMALE_TRANSGENDER,
    concepts.INTERSEX: GenderIdentity.INTERSEX,
    concepts.OTHER: GenderIdentity.OTHER,
    concepts.PREFER_NOT_TO_SAY: GenderIdentity.PREFER_NOT_TO_SAY,
}

_MEMBERSHIP_TIER_MAPPING = {
    concepts.REGISTERED: MembershipTier.REGISTERED,
    concepts.VOLUNTEER: MembershipTier.VOLUNTEER,
    concepts.FULL_PARTICIPANT: MembershipTier.FULL_PARTICIPANT,
    concepts.ENROLLEE: MembershipTier.ENROLLEE
}

# In concepts, it's keyed by the state abbreviation, we need to flip them.
_STATE_MAPPING = {v:k for k, v in concepts.STATES_BY_ABBREV.iteritems()}


class QuestionnaireResponseExtractor(extraction.FhirExtractor):
  CONFIGS = {
      concepts.ETHNICITY: _ETHNICITY_MAPPING,
      concepts.RACE: _RACE_MAPPING,
      concepts.GENDER_IDENTITY: _GENDER_IDENTITY_MAPPING,
      concepts.STATE_OF_RESIDENCE: _STATE_MAPPING,
      concepts.MEMBERSHIP_TIER: _MEMBERSHIP_TIER_MAPPING,
  }

  def extract_questionnaire_id(self):
    source_questionnaire = self.r_fhir.questionnaire.reference
    return source_questionnaire.split('Questionnaire/')[-1]

  def extract_id(self):
    return self.r_fhir.id

  def extract_answer(self, link_id, concept):
    config = self.CONFIGS[concept]
    qs = extraction.get_questions_by_link_id(self.r_fhir, link_id)
    if len(qs) == 1 and len(qs[0].answer) == 1:
      value = extraction.extract_value(qs[0].answer[0])
      concept = value.extract_concept()
      if concept:
        return config.get(concept, UNMAPPED)        
    return SKIPPED

  def extract_link_ids(self, concept):
    questionnaire_id = self.extract_questionnaire_id()
    questionnaire = questionnaireDAO.load_if_present(questionnaire_id)
    if not questionnaire:
      raise ValueError('Invalid Questionnaire id "{0}".'.format(questionnaire_id))

    questionnaire_extractor = QuestionnaireExtractor(questionnaire.resource)
    return questionnaire_extractor.extract_link_id_for_concept(concept)

def extract_race(qr_hist_obj):
  """Returns ExtractionResult for race answer from questionnaire response."""
  return extract_field(qr_hist_obj.obj, concepts.RACE)

def extract_date_of_birth(qr_hist_obj):
  """Returns ExtractionResult for date of birth answer from questionnaire response."""
  return extract_field(qr_hist_obj.obj, concepts.DATE_OF_BIRTH)

def extract_gender_identity(qr_hist_obj):
  """Returns ExtractionResult for gender identity answer from questionnaire response."""
  return extract_field(qr_hist_obj.obj, concepts.GENDER_IDENTITY)

def extract_membership_tier(qr_hist_obj):
  """Returns ExtractionResult for membership tier answer from questionnaire response."""
  return extract_field(qr_hist_obj.obj, concepts.MEMBERSHIP_TIER)

def extract_first_name(qr_hist_obj):
  """Returns ExtractionResult for first name answer from questionnaire response."""
  return extract_field(qr_hist_obj.obj, concepts.FIRST_NAME)

def extract_middle_name(qr_hist_obj):
  """Returns ExtractionResult for middle name answer from questionnaire response."""
  return extract_field(qr_hist_obj.obj, concepts.MIDDLE_NAME)

def extract_last_name(qr_hist_obj):
  """Returns ExtractionResult for last name answer from questionnaire response."""
  return extract_field(qr_hist_obj.obj, concepts.LAST_NAME)


def submission_statuses():
  """Enumerates the questionnaire response submission values"""
  return set(["SUBMITTED"])

def races():
  """Enumerates the race values"""
  return set(_RACE_MAPPING.values())

def extract_ethnicity(qr_hist_obj):
  """Returns ExtractionResult for ethnicity from questionnaire response."""
  return extract_field(qr_hist_obj.obj, concepts.ETHNICITY)

def ethnicities():
  """Enumerates the ethnicity values."""
  return set(_ETHNICITY_MAPPING.values())

def extract_state_of_residence(qr_hist_obj):
  """Returns ExtractionResult for state of residence answer from questionnaire response."""
  return extract_field(qr_hist_obj.obj, concepts.STATE_OF_RESIDENCE)

def states():
  """Enumerates the states."""
  return set(_STATE_MAPPING.values())

def extract_census_region(qr_hist_obj):
  """Returns ExtractionResult for census region from questionnaire response."""
  state_result = extract_state_of_residence(qr_hist_obj)
  if state_result.extracted:
    census_region = census_regions.get(state_result.value)
    if census_region:
      return extraction.ExtractionResult(census_region, True)
  return extraction.ExtractionResult(None, False)

def regions():
  """Enumerates the census regions."""
  return set(census_regions.values())

@ndb.non_transactional
def extract_field(obj, concept):
  """Returns ExtractionResult for concept answer from questionnaire response."""
  response_extractor = QuestionnaireResponseExtractor(obj.resource)
  link_ids = response_extractor.extract_link_ids(concept)
  
  if not len(link_ids) == 1:
    return extraction.ExtractionResult(None, False)  # Failed to extract answer
  return extraction.ExtractionResult(response_extractor.extract_answer(link_ids[0], concept))

@ndb.non_transactional
def extract_concept_presence(concept):

  def extract(history_obj):
    response_extractor = QuestionnaireResponseExtractor(history_obj.obj.resource)
    link_ids = response_extractor.extract_link_ids(concept)

    if not len(link_ids) == 1:
      return extraction.ExtractionResult(None, False)  # Failed to extract answer

    return extraction.ExtractionResult('SUBMITTED')

  return extract

def extract_age(qr_hist_obj, age_func):
  """Returns ExtractionResult with the bucketed participant age on that date."""
  today = qr_hist_obj.date
  date_of_birth = extract_date_of_birth(qr_hist_obj)
  if not date_of_birth:
    return extraction.ExtractionResult(None)  # DOB was not provided: set None
  return extraction.ExtractionResult(age_func(date_of_birth, today))

def extract_bucketed_age(qr_hist_obj):
  return extract_age(qr_hist_obj, participant_summary.get_bucketed_age)
