"""Tests for validation."""
import unittest

from fhirclient.models.quantity import Quantity
from test.unit_test.unit_test_util import TestBase
from concepts import Concept, UNIT_KG, UNIT_MM_HG
from extraction import Value
from werkzeug.exceptions import BadRequest

from field_validation import FieldValidation, has_units, validate_fields, lessthan, within_range

CONCEPT_A = Concept("http://foo.com/system", "concept_a_code")
CONCEPT_B = Concept("http://foo.com/system", "concept_b_code")

FIELD_A = FieldValidation(CONCEPT_A, "Concept A", [within_range(0, 300)], required=True)
FIELD_REQUIRED = FieldValidation(CONCEPT_B, "Concept B Required", [], required=True)
FIELD_NOT_REQUIRED = FieldValidation(CONCEPT_B, "Concept B Not Required", [], required=False)
FIELD_LESSTHAN = FieldValidation(CONCEPT_B, "B less than A", [lessthan(FIELD_A)], required=True)
FIELD_MM_HG = FieldValidation(CONCEPT_A, "Unit MM_HG", [has_units(UNIT_MM_HG)], required=True)
FIELD_KG = FieldValidation(CONCEPT_A, "Unit KG", [has_units(UNIT_KG)], required=True)

class ValidationTest(TestBase):

  def test_validate_fieldsrange(self):
    value_dict = {CONCEPT_A: Value(_make_qty(30), 'valueQuantity')}
    validate_fields([FIELD_A], value_dict)

  def test_validate_fields_outside_range_low(self):
    value_dict = {CONCEPT_A: Value(_make_qty(0), 'valueQuantity')}
    with self.assertRaises(BadRequest):
      validate_fields([FIELD_A], value_dict)

  def test_validate_fields_outside_range_high(self):
    value_dict = {CONCEPT_A: Value(_make_qty(300), 'valueQuantity')}
    with self.assertRaises(BadRequest):
      validate_fields([FIELD_A], value_dict)

  def test_validate_fields_required(self):
    value_dict = {CONCEPT_A: Value(_make_qty(30), 'valueQuantity')}
    with self.assertRaises(BadRequest):
      validate_fields([FIELD_REQUIRED], value_dict)

  def test_validate_fields_not_required(self):
    value_dict = {CONCEPT_A: Value(_make_qty(30), 'valueQuantity')}
    validate_fields([FIELD_NOT_REQUIRED], value_dict)

  def test_validate_fields_not_lessthan(self):
    value_dict = {
        CONCEPT_A: Value(_make_qty(30), 'valueQuantity'),
        CONCEPT_B: Value(_make_qty(30), 'valueQuantity'),
    }
    with self.assertRaises(BadRequest):
      validate_fields([FIELD_LESSTHAN], value_dict)

  def test_validate_fields_lessthan(self):
    value_dict = {
        CONCEPT_A: Value(_make_qty(30), 'valueQuantity'),
        CONCEPT_B: Value(_make_qty(20), 'valueQuantity'),
    }
    validate_fields([FIELD_LESSTHAN], value_dict)

  def test_validate_fields_units(self):
    value_dict = {CONCEPT_A: Value(_make_qty(30), 'valueQuantity')}
    validate_fields([FIELD_MM_HG], value_dict)

  def test_validate_fields_wrong_units(self):
    value_dict = {CONCEPT_A: Value(_make_qty(30), 'valueQuantity')}
    with self.assertRaises(BadRequest):
      validate_fields([FIELD_KG], value_dict)


def _make_qty(qty, ):
  q = {
      "code": "mm[Hg]",
      "system": "http://unitsofmeasure.org",
      "unit": "mmHg",
      "value": qty,
  }
  return Quantity(q)

if __name__ == '__main__':
  unittest.main()
