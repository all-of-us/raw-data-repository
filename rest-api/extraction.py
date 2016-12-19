"""Various functions for extracting fields from FHIR documents"""

from concepts import Concept

from fhirclient.models.fhirelementfactory import FHIRElementFactory
from werkzeug.exceptions import BadRequest

UNSET = 'UNSET'
SKIPPED = 'SKIPPED'
UNMAPPED = 'UNMAPPED'
BASE_VALUES = set([UNSET, SKIPPED, UNMAPPED])

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
    self.r_fhir = FHIRElementFactory.instantiate(resource['resourceType'], resource)


class Value(object):
  def __init__(self, value, value_type):
    self.value = value
    self.value_type = value_type

  def extract_quantity(self):
    """Extracts the numeric quantity from a value with a quantity node"""
    assert self.value_type == 'valueQuantity'
    return self.value.value

  def extract_concept(self):
    """Extracts a concept from a value with a valueCoding type."""
    assert self.value_type == 'valueCoding'
    return extract_concept(self.value)

  def extract_units(self):
    """Returns the units (as a Concept) that this value is represented in."""
    return Concept(system=self.value.system, code=self.value.code)

# Fields that values can be found in.
VALUE_PROPS = (
    'valueAttachment',
    'valueCodeableConcept',
    'valueCoding',
    'valueDateTime',
    'valuePeriod',
    'valueRange',
    'valueRatio',
    'valueSampledData',
    'valueString',
    'valueTime',
    'valueQuantity',
)

def extract_concept(node):
  """Extracts a valueCodeableConcept."""
  return Concept(system=str(node.system), code=str(node.code))

def extract_value(node):
  """Extracts the value from a set of value[x] fields

  Returns:
    A extraction.Value object. Value.value is the extracted value.
    Value.value_type is one of the valid FHIR field names in which
    a value can reside.

  Raises:
    BadRequest: If a value is specified in more than one field.
  """
  ret = None
  for prop in VALUE_PROPS:
    attr = getattr(node, prop, None)
    if attr:
      if ret:
        raise BadRequest('{} has multiple values'.format(node.resource_name))
      ret = Value(attr, prop)
  return ret
