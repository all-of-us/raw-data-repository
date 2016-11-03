"""Data and functions used to validate incoming data."""

from collections import namedtuple
import extraction

from werkzeug.exceptions import BadRequest

# A FieldValidation object defines a set of validations for a field.
#   display_name: Used in error messages.
#   concept: An extraction.Concept
#   funcs: A list of functions to apply to the value.
#   required: If True, will raise a BadRequest if the field is not present.
#
#   Three values will be passed to the validation function:
#    value: An extraction.Value value for the field being validated.
#    display_name: The display name for this value, to be used in error messages.
#    value_dict: A dictionary of extraction.Concept to extraction.Value, for all the
#      other values. Used to enforce rules like field A must be less than field B.
FieldValidation = namedtuple('FieldValidation', ['concept', 'display_name', 'funcs', 'required'])

def validate_fields(field_validations, value_dict):
  """Validates the passed values based on the field definitions

  Args:
    field_validations: A list of FieldValidation objects containing the validation rules.
    value_dict: A dictionary of extraction.Concept to extraction.Value

  Raises:
    BadRequest: If a value fails validation.
  """
  for field in field_validations:
    value = value_dict.get(field.concept, None)
    if not value:
      if field.required:
        raise BadRequest('{} ({}:{}) is required. Not found in evaluation.'.format(
            field.display_name, field.concept.system, field.concept.code))

    for val_func in field.funcs:
      val_func(value, field.display_name, value_dict)

def within_range(low, high):
  """Returns a function that verifies that a given value is within the range (Exclusive)."""
  def validate(val, display_name, _):
    val = val.extract_quantity()
    if low < val < high:
      return
    raise BadRequest('{} of {} is outside acceptable range ({} - {})'.format(
        display_name, val, low, high))

  return validate

def lessthan(other_field):
  """Returns a function that verifies that this field is less than the specified field"""
  def validate(val, display_name, value_dict):
    val = val.extract_quantity()
    other_value = value_dict[other_field.concept].extract_quantity()
    if val < other_value:
      return
    raise BadRequest('{} of {} is not less than {} which is {}'.format(
        display_name, val, other_field.display_name, other_value))
  return validate

def has_units(units_concept):
  """Returns a function that verifies that this field uses the specified units."""
  def validate(val, display_name, _):
    val_units = val.extract_units()
    if units_concept != val_units:
      raise BadRequest('Expecting units "{}" for {} but found "{}".'.format(
          units_concept, display_name, val_units))
  return validate
