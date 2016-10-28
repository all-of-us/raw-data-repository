from collections import namedtuple

import data_access_object
import extraction
import participant
import fhirclient.models.questionnaireresponse
from google.appengine.ext import ndb
from participant import Participant
from participant import GenderIdentity
from questionnaire import DAO as questionnaireDAO
from questionnaire import QuestionnaireExtractor
from werkzeug.exceptions import NotFound

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
    gender_identity_result = extract_field(model, extraction.GENDER_IDENTITY_CONCEPT)
    if gender_identity_result.extracted:
      participant_obj = participant.DAO.load(participant_id)
      # If the gender identity on the participant doesn't match, update it
      if participant_obj.gender_identity != gender_identity_result.value:
        participant_obj.gender_identity = gender_identity_result.value
        participant.DAO.store(participant_obj, date, client_id)

DAO = QuestionnaireResponseDAO()

class QuestionnaireResponseExtractor(extraction.FhirExtractor):
  Config = namedtuple('Config', ['field', 'mapping'])

  _ETHNICITY_MAPPING = {
        extraction.Concept('http://hl7.org/fhir/v3/Ethnicity',
                           '2135-2'): 'hispanic',
        extraction.Concept('http://hl7.org/fhir/v3/Ethnicity',
                           '2186-5'): 'non_hispanic',
        extraction.Concept('http://hl7.org/fhir/v3/NullFlavor',
                           'ASKU'): 'asked_but_no_answer',
  }
  _RACE_MAPPING = {
        extraction.Concept('http://hl7.org/fhir/v3/Race',
                           '1002-5'): 'american_indian_or_alaska_native',
        extraction.Concept('http://hl7.org/fhir/v3/Race',
                           '2054-5'): 'black_or_african_american',
        extraction.Concept('http://hl7.org/fhir/v3/Race',
                           '2028-9'): 'asian',
        extraction.Concept('http://hl7.org/fhir/v3/Race',
                           '2076-8'): 'native_hawaiian_or_other_pacific_islander',
        extraction.Concept('http://hl7.org/fhir/v3/Race',
                           '2106-3'): 'white',
        extraction.Concept('http://hl7.org/fhir/v3/Race',
                           '2131-1'): 'other_race',
        extraction.Concept('http://hl7.org/fhir/v3/NullFlavor',
                           'ASKU'): 'asked_but_no_answer',
  }
  _GENDER_IDENTITY_MAPPING = {
        extraction.Concept('http://terminology.pmi-ops.org/ppi/gender-identity',
                           'female'): GenderIdentity.FEMALE,
        extraction.Concept('http://terminology.pmi-ops.org/ppi/gender-identity',
                           'female-to-male-transgender'): GenderIdentity.FEMALE_TO_MALE_TRANSGENDER,                              
        extraction.Concept('http://terminology.pmi-ops.org/ppi/gender-identity',
                           'male'): GenderIdentity.MALE,                              
        extraction.Concept('http://terminology.pmi-ops.org/ppi/gender-identity',
                           'male-to-female-transgender'): GenderIdentity.MALE_TO_FEMALE_TRANSGENDER,
        extraction.Concept('http://terminology.pmi-ops.org/ppi/gender-identity',
                           'intersex'): GenderIdentity.INTERSEX,                              
        extraction.Concept('http://terminology.pmi-ops.org/ppi/gender-identity',
                           'other'): GenderIdentity.OTHER,
        extraction.Concept('http://hl7.org/fhir/v3/NullFlavor',
                          'ASKU'): GenderIdentity.PREFER_NOT_TO_SAY,
  }
  
  CONFIGS = {
      extraction.ETHNICITY_CONCEPT: Config('valueCoding', _ETHNICITY_MAPPING),
      extraction.RACE_CONCEPT: Config('valueCoding', _RACE_MAPPING),
      extraction.GENDER_IDENTITY_CONCEPT: Config('valueCoding', _GENDER_IDENTITY_MAPPING)
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
      answer = qs[0].answer[0]
      value = getattr(answer, config.field, None)
      if value:
        return config.mapping.get(
            extraction.Concept(value.system, value.code),
            extraction.UNMAPPED)

def extract_race(qr_hist_obj):
  """Returns ExtractionResult for race answer from questionnaire response."""
  return extract_field(qr_hist_obj.obj, extraction.RACE_CONCEPT)

def extract_ethnicity(qr_hist_obj):
  """Returns ExtractionResult for ethnicity from questionnaire response."""
  return extract_field(qr_hist_obj.obj, extraction.ETHNICITY_CONCEPT)

def extract_gender_identity(qr_hist_obj):
  """Returns ExtractionResult for gender identity answer from questionnaire response."""  
  return extract_field(qr_hist_obj.obj, extraction.GENDER_IDENTITY_CONCEPT)

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
