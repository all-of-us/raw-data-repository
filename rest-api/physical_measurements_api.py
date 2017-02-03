import api_util
import base_api
import concepts
import config
import datetime
import measurements
import logging
import sync_log

from api_util import HEALTHPRO, PTC, PTC_AND_HEALTHPRO
from field_validation import FieldValidation, has_units, lessthan, within_range
from query import OrderBy
from werkzeug.exceptions import BadRequest


_SYSTOLIC_BP = FieldValidation(
    concepts.SYSTOLIC_BP,
    'systolic blood pressure',
    [within_range(0, 300), has_units(concepts.UNIT_MM_HG)],
    required=True)
_DIASTOLIC_BP = FieldValidation(
    concepts.DIASTOLIC_BP,
    'diastolic blood pressure',
    [within_range(0, 300), has_units(concepts.UNIT_MM_HG), lessthan(_SYSTOLIC_BP)],
    required=True)
_HEART_RATE = FieldValidation(
    concepts.HEART_RATE,
    'heart rate',
    [within_range(0, 300), has_units(concepts.UNIT_PER_MIN)],
    required=True)
_WEIGHT = FieldValidation(
    concepts.WEIGHT,
    'weight',
    [within_range(0, 1000), has_units(concepts.UNIT_KG)],
    required=True)
_BMI = FieldValidation(concepts.BMI,
    'body mass index',
    [has_units(concepts.UNIT_KG_M2)],
    required=True)
_HIP_CIRCUMFERENCE = FieldValidation(
    concepts.HIP_CIRCUMFERENCE,
    'hip circumference',
    [within_range(0, 300), has_units(concepts.UNIT_CM)],
    required=True)
_WAIST_CIRCUMFERENCE = FieldValidation(
    concepts.WAIST_CIRCUMFERENCE,
    'waist circumerence',
    [within_range(0, 300), has_units(concepts.UNIT_CM)],
    required=True)

_MEASUREMENTS_ORDER = OrderBy("last_modified", True)


class PhysicalMeasurementsAPI(base_api.BaseApi):
  def __init__(self):
    super(PhysicalMeasurementsAPI, self).__init__(measurements.DAO())

  @api_util.auth_required(PTC_AND_HEALTHPRO)
  def get(self, id_=None, a_id=None):
    return super(PhysicalMeasurementsAPI, self).get(id_, a_id)

  @api_util.auth_required(HEALTHPRO)
  def post(self, a_id=None):
    return super(PhysicalMeasurementsAPI, self).post(a_id)

  @api_util.auth_required(HEALTHPRO)
  def put(self, id_, a_id=None):
    return super(PhysicalMeasurementsAPI, self).put(id_, a_id)

  @api_util.auth_required(HEALTHPRO)
  def patch(self, id_, a_id=None):
    return super(PhysicalMeasurementsAPI, self).patch(id_, a_id)

  @api_util.auth_required(PTC_AND_HEALTHPRO)
  def list(self, a_id):
    return super(PhysicalMeasurementsAPI, self).query("id", _MEASUREMENTS_ORDER, a_id)

  def validate_object(self, e, a_id=None):
    field_validators = [
        _SYSTOLIC_BP,
        _DIASTOLIC_BP,
        _HEART_RATE,
        _WEIGHT,
        _BMI,
        _HIP_CIRCUMFERENCE,
        _WAIST_CIRCUMFERENCE,
    ]
    extractor = measurements.PhysicalMeasurementsExtractor(e.resource)
    value_dict = {f.concept: extractor.extract_value(f.concept) for f in field_validators}
    field_validation.validate_fields(field_validators, value_dict)


@api_util.auth_required(PTC)
def sync_physical_measurements():
  max_results = config.getSetting(config.MEASUREMENTS_ENTITIES_PER_SYNC, 100)
  return base_api.sync(sync_log.PHYSICAL_MEASUREMENTS, max_results)
