import concepts
import data_access_object
import extraction
import fhirclient.models.questionnaireresponse

from census_regions import census_regions
from extraction import UNMAPPED, SKIPPED
from google.appengine.ext import ndb
from participant import Participant
from participant_summary import GenderIdentity, MembershipTier, Ethnicity, Race, DAO as summaryDAO
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
    new_history = self.make_history(model, date, client_id)
    import field_config.participant_summary_config
    summaryDAO().update_with_incoming_data(
            participant_id,
            new_history,
            field_config.participant_summary_config.CONFIG)


_DAO = QuestionnaireResponseDAO()
def DAO():
  return _DAO


_ETHNICITY_MAPPING = {
    concepts.HISPANIC: Ethnicity.HISPANIC,
    concepts.NON_HISPANIC: Ethnicity.NON_HISPANIC,
    concepts.ASKED_BUT_NO_ANSWER: Ethnicity.PREFER_NOT_TO_SAY
}
_RACE_MAPPING = {
    concepts.AMERICAN_INDIAN_OR_ALASKA_NATIVE: Race.AMERICAN_INDIAN_OR_ALASKA_NATIVE,
    concepts.BLACK_OR_AFRICAN_AMERICAN: Race.BLACK_OR_AFRICAN_AMERICAN,
    concepts.ASIAN: Race.ASIAN,
    concepts.NATIVE_HAWAIIAN_OR_OTHER_PACIFIC_ISLANDER:
        Race.NATIVE_HAWAIIAN_OR_OTHER_PACIFIC_ISLANDER,
    concepts.WHITE: Race.WHITE,
    concepts.OTHER_RACE: Race.OTHER_RACE,
    concepts.ASKED_BUT_NO_ANSWER: Race.PREFER_NOT_TO_SAY
}
_GENDER_IDENTITY_MAPPING = {
    concepts.FEMALE: GenderIdentity.FEMALE,
    concepts.FEMALE_TO_MALE_TRANSGENDER: GenderIdentity.FEMALE_TO_MALE_TRANSGENDER,
    concepts.MALE: GenderIdentity.MALE,
    concepts.MALE_TO_FEMALE_TRANSGENDER: GenderIdentity.MALE_TO_FEMALE_TRANSGENDER,
    concepts.INTERSEX: GenderIdentity.INTERSEX,
    concepts.OTHER_GENDER: GenderIdentity.OTHER,
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

  def extract_answer(self, link_id, concept, expected_type):
    qs = extraction.get_questions_by_link_id(self.r_fhir, link_id)
    if len(qs) == 1 and len(qs[0].answer) == 1:
      value = extraction.extract_value(qs[0].answer[0])
      if expected_type == extraction.VALUE_CODING:
        config = self.CONFIGS[concept]
        return config.get(value.extract_concept(), UNMAPPED)
      elif expected_type == extraction.VALUE_STRING:
        return value.extract_string()
    return SKIPPED

  def extract_link_ids(self, concept):
    questionnaire_id = self.extract_questionnaire_id()
    questionnaire = questionnaireDAO().load_if_present(questionnaire_id)
    if not questionnaire:
      raise ValueError('Invalid Questionnaire id "{0}".'.format(questionnaire_id))

    questionnaire_extractor = QuestionnaireExtractor(questionnaire.resource)
    return questionnaire_extractor.extract_link_id_for_concept(concept)

def submission_statuses():
  """Enumerates the questionnaire response submission values"""
  return set(["SUBMITTED"])

def races():
  """Enumerates the race values"""
  return set(_RACE_MAPPING.values())

def ethnicities():
  """Enumerates the ethnicity values."""
  return set(_ETHNICITY_MAPPING.values())

def extract_state_of_residence(qr_hist_obj):
  """Returns ExtractionResult for state of residence answer from questionnaire response."""
  return extract_field(qr_hist_obj.obj, concepts.STATE_OF_RESIDENCE)

def extract_date_of_birth(qr_hist_obj):
  """Returns ExtractionResult for date of birth answer from questionnaire response."""
  return extract_field(qr_hist_obj.obj, concepts.DATE_OF_BIRTH, extraction.VALUE_STRING)

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

def extractor_for(concept, expected_type=extraction.VALUE_CODING):
  def ret(qr_hist_obj):
    return extract_field(qr_hist_obj.obj, concept, expected_type)
  return ret

@ndb.non_transactional
def extract_field(obj, concept, expected_type=extraction.VALUE_CODING):
  """Returns ExtractionResult for concept answer from questionnaire response."""
  response_extractor = QuestionnaireResponseExtractor(obj.resource)
  link_ids = response_extractor.extract_link_ids(concept)

  if not len(link_ids) == 1:
    return extraction.ExtractionResult(None, False)  # Failed to extract answer
  return extraction.ExtractionResult(
          response_extractor.extract_answer(link_ids[0], concept, expected_type))

def extract_concept_presence(concept):
  def extract(history_obj):
    response_extractor = QuestionnaireResponseExtractor(history_obj.obj.resource)
    link_ids = response_extractor.extract_link_ids(concept)

    if not len(link_ids) == 1:
      return extraction.ExtractionResult(None, False)  # Failed to extract answer

    return extraction.ExtractionResult('SUBMITTED')

  return extract

def extract_concept_date(concept):
  def extract(history_obj):
    response_extractor = QuestionnaireResponseExtractor(history_obj.obj.resource)
    link_ids = response_extractor.extract_link_ids(concept)

    if not len(link_ids) == 1:
      return extraction.ExtractionResult(None, False)  # Failed to extract answer

    return extraction.ExtractionResult(history_obj.date)

  return extract
