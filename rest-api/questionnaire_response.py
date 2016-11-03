from collections import namedtuple

import concepts
import data_access_object
import extraction
import participant
import fhirclient.models.questionnaireresponse

from census_regions import census_regions
from extraction import UNMAPPED
from google.appengine.ext import ndb
from participant import Participant
from participant import GenderIdentity
from questionnaire import DAO as questionnaireDAO
from questionnaire import QuestionnaireExtractor

class QuestionnaireResponse(ndb.Model):
  """The questionnaire response."""
  resource = ndb.JsonProperty()

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
      participant_obj = participant.DAO.load(participant_id)
      # If the gender identity on the participant doesn't match, update it
      if participant_obj.gender_identity != gender_identity_result.value:
        participant_obj.gender_identity = gender_identity_result.value
        participant.DAO.store(participant_obj, date, client_id)

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
# In concepts, it's keyed by the state abbreviation, we need to flip them.
_STATE_MAPPING = {v:k for k, v in concepts.STATES_BY_ABBREV.iteritems()}


class QuestionnaireResponseExtractor(extraction.FhirExtractor):
  CONFIGS = {
      concepts.ETHNICITY: _ETHNICITY_MAPPING,
      concepts.RACE: _RACE_MAPPING,
      concepts.GENDER_IDENTITY: _GENDER_IDENTITY_MAPPING,
      concepts.STATE_OF_RESIDENCE: _STATE_MAPPING,
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
      return concept and config.get(concept, UNMAPPED)

def extract_race(qr_hist_obj):
  """Returns ExtractionResult for race answer from questionnaire response."""
  return extract_field(qr_hist_obj.obj, concepts.RACE)

def races():
  """Enumerates the race values"""
  return set(_RACE_MAPPING.values())

def extract_ethnicity(qr_hist_obj):
  """Returns ExtractionResult for ethnicity from questionnaire response."""
  return extract_field(qr_hist_obj.obj, concepts.ETHNICITY)

def ethnicities():
  """Enumerates the ethnicity values."""
  return set(_ETHNICITY_MAPPING.values())

def extract_gender_identity(qr_hist_obj):
  """Returns ExtractionResult for gender identity answer from questionnaire response."""
  return extract_field(qr_hist_obj.obj, concepts.GENDER_IDENTITY)

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
  questionnaire_id = response_extractor.extract_questionnaire_id()
  questionnaire = questionnaireDAO.load_if_present(questionnaire_id)
  if not questionnaire:
    raise ValueError(
        'Invalid Questionnaire id {0} in Response {1}'.format(
            questionnaire_id, response_extractor.extract_id()))
  questionnaire_extractor = QuestionnaireExtractor(questionnaire.resource)
  link_ids = questionnaire_extractor.extract_link_id_for_concept(concept)

  if not len(link_ids) == 1:
    return extraction.ExtractionResult(None, False)  # Failed to extract answer
  # Questionnaire unambiguously asked the desired question.
  return extraction.ExtractionResult(
      response_extractor.extract_answer(link_ids[0], concept))
