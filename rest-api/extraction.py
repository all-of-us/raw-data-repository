"""Various functions for extracting fields from FHIR documents"""

from collections import namedtuple
from fhirclient.models.fhirelementfactory import FHIRElementFactory

UNMAPPED = 'UNMAPPED'

# An ExtractionResult.value may be a valid value, None, or UNMAPPED. Use
# None if the value is being (re)set to None, and UNMAPPED if the value is
# being (re)set to an invalid value. By contrast, setting
# ExtractionResult.extracted to False means that the information available
# doesn't provide the desired field. For instance, if trying to extract an
# Ethnicity result from a questionnaire that didn't ask about Ethnicity. In
# this case the value is ignored (it should be set to None for
# consistency).

class ExtractionResult(object):
  def __init__(self, value, extracted=True):
    self.value = value
    self.extracted = extracted

def simple_field_extractor(field_name):
  """Returns a function that successfully extracts the named field."""
  return lambda hist: ExtractionResult(getattr(hist.obj, field_name))

Concept = namedtuple('Concept', ['system', 'code'])

LOINC = 'http://loinc.org'
ETHNICITY_CONCEPT = Concept(LOINC, '69490-1')
RACE_CONCEPT = Concept(LOINC, '72826-1')
GENDER_IDENTITY_CONCEPT = Concept(LOINC, '76691-5')
STATE_OF_RESIDENCE_CONCEPT = Concept(LOINC, '46499-0')

def get_questions_by_link_id(qr, target_link_id):
  ret = []
  if hasattr(qr, 'linkId') and qr.linkId == target_link_id:
    ret += [qr]
  for prop in ('question', 'group', 'answer'):
    if hasattr(qr, prop):
      ret += [v
              for q in as_list(getattr(qr, prop))
              for v in get_questions_by_link_id(q, target_link_id)]
  return ret

def as_list(v):
  if type(v) != list:
    return [v]
  return v

class FhirExtractor(object):
  def __init__(self, resource):
    self.r_fhir = FHIRElementFactory.instantiate(resource['resourceType'],
                                                 resource)
