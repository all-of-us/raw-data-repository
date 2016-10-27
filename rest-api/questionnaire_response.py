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

# Taken from http://www2.census.gov/geo/docs/maps-data/maps/reg_div.txt
census_regions = {
  'CT': 'NORTHEAST',
  'ME': 'NORTHEAST',
  'MA': 'NORTHEAST',
  'NH': 'NORTHEAST',
  'RI': 'NORTHEAST',
  'VT': 'NORTHEAST',
  'NJ': 'NORTHEAST',
  'NY': 'NORTHEAST',
  'PA': 'NORTHEAST',
  'IL': 'MIDWEST',
  'IN': 'MIDWEST',
  'MI': 'MIDWEST',
  'OH': 'MIDWEST',
  'WI': 'MIDWEST',
  'IA': 'MIDWEST',
  'KS': 'MIDWEST',
  'MN': 'MIDWEST',
  'MO': 'MIDWEST',
  'NE': 'MIDWEST',
  'ND': 'MIDWEST',
  'SD': 'MIDWEST',
  'DE': 'SOUTH',
  'DC': 'SOUTH',
  'FL': 'SOUTH',
  'GA': 'SOUTH',
  'MD': 'SOUTH',
  'NC': 'SOUTH',
  'SC': 'SOUTH',
  'VA': 'SOUTH',
  'WV': 'SOUTH',
  'AL': 'SOUTH',
  'KY': 'SOUTH',
  'MS': 'SOUTH',
  'TN': 'SOUTH',
  'AR': 'SOUTH',
  'LA': 'SOUTH',
  'OK': 'SOUTH',
  'TX': 'SOUTH',
  'AZ': 'WEST',
  'CO': 'WEST',
  'ID': 'WEST',
  'MT': 'WEST',
  'NV': 'WEST',
  'NM': 'WEST',
  'UT': 'WEST',
  'WY': 'WEST',
  'AL': 'WEST',
  'CA': 'WEST',
  'HI': 'WEST',
  'OR': 'WEST',
  'WA': 'WEST' }  

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
  
  _STATE_MAPPING = {
        extraction.Concept('http://terminology.pmi-ops.org/ppi/state',
                           'AL'): 'AL',
        extraction.Concept('http://terminology.pmi-ops.org/ppi/state',
                           'AK'): 'AK',
        extraction.Concept('http://terminology.pmi-ops.org/ppi/state',
                           'AZ'): 'AZ',
        extraction.Concept('http://terminology.pmi-ops.org/ppi/state',
                           'AR'): 'AR',                    
        extraction.Concept('http://terminology.pmi-ops.org/ppi/state',
                           'CA'): 'CA',                    
        extraction.Concept('http://terminology.pmi-ops.org/ppi/state',
                           'CO'): 'CO',                    
        extraction.Concept('http://terminology.pmi-ops.org/ppi/state',
                           'CT'): 'CT',                    
        extraction.Concept('http://terminology.pmi-ops.org/ppi/state',
                           'DE'): 'DE',                    
        extraction.Concept('http://terminology.pmi-ops.org/ppi/state',
                           'FL'): 'FL',                    
        extraction.Concept('http://terminology.pmi-ops.org/ppi/state',
                           'GA'): 'GA',                    
        extraction.Concept('http://terminology.pmi-ops.org/ppi/state',
                           'HI'): 'HI',                    
        extraction.Concept('http://terminology.pmi-ops.org/ppi/state',
                           'ID'): 'ID',                    
        extraction.Concept('http://terminology.pmi-ops.org/ppi/state',
                           'IL'): 'IL',                    
        extraction.Concept('http://terminology.pmi-ops.org/ppi/state',
                           'IN'): 'IN',                    
        extraction.Concept('http://terminology.pmi-ops.org/ppi/state',
                           'IA'): 'IA',                    
        extraction.Concept('http://terminology.pmi-ops.org/ppi/state',
                           'KS'): 'KS',                    
        extraction.Concept('http://terminology.pmi-ops.org/ppi/state',
                           'KY'): 'KY',                    
        extraction.Concept('http://terminology.pmi-ops.org/ppi/state',
                           'LA'): 'LA',                    
        extraction.Concept('http://terminology.pmi-ops.org/ppi/state',
                           'ME'): 'ME',                    
        extraction.Concept('http://terminology.pmi-ops.org/ppi/state',
                           'MD'): 'MD',                    
        extraction.Concept('http://terminology.pmi-ops.org/ppi/state',
                           'MA'): 'MA',                    
        extraction.Concept('http://terminology.pmi-ops.org/ppi/state',
                           'MI'): 'MI',                    
        extraction.Concept('http://terminology.pmi-ops.org/ppi/state',
                           'MN'): 'MN',                    
        extraction.Concept('http://terminology.pmi-ops.org/ppi/state',
                           'MS'): 'MS',                    
        extraction.Concept('http://terminology.pmi-ops.org/ppi/state',
                           'MO'): 'MO',                    
        extraction.Concept('http://terminology.pmi-ops.org/ppi/state',
                           'MT'): 'MT',                    
        extraction.Concept('http://terminology.pmi-ops.org/ppi/state',
                           'NE'): 'NE',                    
        extraction.Concept('http://terminology.pmi-ops.org/ppi/state',
                           'NV'): 'NV',                    
        extraction.Concept('http://terminology.pmi-ops.org/ppi/state',
                           'NH'): 'NH',                    
        extraction.Concept('http://terminology.pmi-ops.org/ppi/state',
                           'NJ'): 'NJ',                    
        extraction.Concept('http://terminology.pmi-ops.org/ppi/state',
                           'NM'): 'NM',                    
        extraction.Concept('http://terminology.pmi-ops.org/ppi/state',
                           'NY'): 'NY',                    
        extraction.Concept('http://terminology.pmi-ops.org/ppi/state',
                           'NC'): 'NC',                    
        extraction.Concept('http://terminology.pmi-ops.org/ppi/state',
                           'ND'): 'ND',                    
        extraction.Concept('http://terminology.pmi-ops.org/ppi/state',
                           'OH'): 'OH',                    
        extraction.Concept('http://terminology.pmi-ops.org/ppi/state',
                           'OK'): 'OK',                    
        extraction.Concept('http://terminology.pmi-ops.org/ppi/state',
                           'OR'): 'OR',                    
        extraction.Concept('http://terminology.pmi-ops.org/ppi/state',
                           'PA'): 'PA',                    
        extraction.Concept('http://terminology.pmi-ops.org/ppi/state',
                           'RI'): 'RI',                    
        extraction.Concept('http://terminology.pmi-ops.org/ppi/state',
                           'SC'): 'SC',                    
        extraction.Concept('http://terminology.pmi-ops.org/ppi/state',
                           'SD'): 'SD',                    
        extraction.Concept('http://terminology.pmi-ops.org/ppi/state',
                           'TN'): 'TN',                    
        extraction.Concept('http://terminology.pmi-ops.org/ppi/state',
                           'TX'): 'TX',                    
        extraction.Concept('http://terminology.pmi-ops.org/ppi/state',
                           'UT'): 'UT',                    
        extraction.Concept('http://terminology.pmi-ops.org/ppi/state',
                           'VT'): 'VT',                    
        extraction.Concept('http://terminology.pmi-ops.org/ppi/state',
                           'VA'): 'VA',                    
        extraction.Concept('http://terminology.pmi-ops.org/ppi/state',
                           'WA'): 'WA',                    
        extraction.Concept('http://terminology.pmi-ops.org/ppi/state',
                           'WV'): 'WV',                    
        extraction.Concept('http://terminology.pmi-ops.org/ppi/state',
                           'WI'): 'WI',                    
        extraction.Concept('http://terminology.pmi-ops.org/ppi/state',
                           'WY'): 'WY',
        extraction.Concept('http://terminology.pmi-ops.org/ppi/state',
                           'ZZ'): 'ZZ'
  }
  
  CONFIGS = {
      extraction.ETHNICITY_CONCEPT: Config('valueCoding', _ETHNICITY_MAPPING),
      extraction.RACE_CONCEPT: Config('valueCoding', _RACE_MAPPING),
      extraction.GENDER_IDENTITY_CONCEPT: Config('valueCoding', _GENDER_IDENTITY_MAPPING),
      extraction.STATE_OF_RESIDENCE_CONCEPT: Config('valueCoding', _STATE_MAPPING)
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

def extract_state_of_residence(qr_hist_obj):
  """Returns ExtractionResult for state of residence answer from questionnaire response."""  
  return extract_field(qr_hist_obj.obj, extraction.STATE_OF_RESIDENCE_CONCEPT)

def extract_census_region(qr_hist_obj):
  """Returns ExtractionResult for census region from questionnaire response."""  
  state_result = extract_state_of_residence(qr_hist_obj)
  if state_result.extracted:
    census_region = census_regions.get(state_result.value)
    if census_region:
      return extraction.ExtractionResult(census_region, True)
  return extraction.ExtractionResult(None, False)  

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
