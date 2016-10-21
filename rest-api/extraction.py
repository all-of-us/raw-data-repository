"""Various functions for extracting fields from FHIR documents"""

from fhirclient.models.fhirelementfactory import FHIRElementFactory
from collections import namedtuple

Concept = namedtuple('Concept', ['system', 'code'])

LOINC = 'http://loinc.org'
ETHNICITY_CONCEPT = Concept(LOINC, '69490-1')
RACE_CONCEPT = Concept(LOINC, '72826-1')


def get_questions_by_link_id(qr, target_link_id):
  ret = []
  if hasattr(qr, 'linkId') and qr.linkId == target_link_id:
    ret += [qr]
  for prop in ('question', 'group', 'answer'):
    if hasattr(qr, prop):
      ret += [v
              for q in _as_list(getattr(qr, prop))
              for v in get_questions_by_link_id(q, target_link_id)]
  return ret

class FhirExtractor(object):
  def __init__(self, resource):
    self.r_fhir = FHIRElementFactory.instantiate(resource['resourceType'], resource)


class QuestionnaireExtractor(FhirExtractor):
  def extract_link_id_for_concept(self, concept):
    assert isinstance(concept, Concept)
    ret = self.extract_link_id_for_concept_(self.r_fhir.group, concept)
    assert len(ret) == 1, \
           '{} questions match concept {}'.format(len(ret), concept)
    return ret[0]

  def extract_link_id_for_concept_(self, qr, concept):
    # Sometimes concept is an existing attr with a value of None.
    for node in qr.concept or []:
      if concept == Concept(node.system, node.code):
        return [qr.linkId]

    ret = []
    for prop in ('question', 'group'):
      if getattr(qr, prop, None):
        ret += [v
                for q in _as_list(getattr(qr, prop))
                for v in self.extract_link_id_for_concept_(q, concept)]
    return ret


class QuestionnaireResponseExtractor(FhirExtractor):
  Config = namedtuple('Config', ['field', 'mapping'])

  _ETHNICITY_MAPPING = {
        Concept('http://hl7.org/fhir/v3/Ethnicity', '2135-2'): 'hispanic',
        Concept('http://hl7.org/fhir/v3/Ethnicity', '2186-5'): 'non_hispanic',
        Concept('http://hl7.org/fhir/v3/NullFlavor', 'ASKU'): 'asked_but_no_answer',
  }
  _RACE_MAPPING = {
        Concept('http://hl7.org/fhir/v3/Race', '1002-5'): 'american_indian_or_alaska_native',
        Concept('http://hl7.org/fhir/v3/Race', '2054-5'): 'black_or_african_american',
        Concept('http://hl7.org/fhir/v3/Race', '2028-9'): 'asian',
        Concept('http://hl7.org/fhir/v3/Race', '2076-8'): 'native_hawaiian_or_other_pacific_islander',
        Concept('http://hl7.org/fhir/v3/Race', '2106-3'): 'white',
        Concept('http://hl7.org/fhir/v3/Race', '2131-1'): 'other_race',
        Concept('http://hl7.org/fhir/v3/NullFlavor', 'ASKU'): 'asked_but_no_answer',
  }
  CONFIGS = {
      ETHNICITY_CONCEPT: Config('valueCoding', _ETHNICITY_MAPPING),
      RACE_CONCEPT: Config('valueCoding', _RACE_MAPPING),
  }

  def extract_questionnaire_id(self):
    source_questionnaire = self.r_fhir.questionnaire.reference
    return source_questionnaire.split('Questionnaire/')[-1]

  def extract_answer(self, link_id, concept):
    config = self.CONFIGS[concept]
    qs = get_questions_by_link_id(self.r_fhir, link_id)
    if len(qs) == 1 and len(qs[0].answer) == 1:
      answer = qs[0].answer[0]
      value = getattr(answer, config.field, None)
      if value:
        return config.mapping[Concept(value.system, value.code)]



def _as_list(v):
  if type(v) != list:
    return [v]
  return v
