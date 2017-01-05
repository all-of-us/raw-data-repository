"""The API definition file for the participants API.

This defines the APIs and the handlers for the APIs.
"""

import api_util
import base_api
import concepts
import evaluation
import participant
import participant_summary
import field_validation

from api_util import HEALTHPRO, PTC, PTC_AND_HEALTHPRO
from field_validation import FieldValidation, has_units, lessthan, within_range
from flask import request
from werkzeug.exceptions import BadRequest

SYSTOLIC_BP = FieldValidation(concepts.SYSTOLIC_BP,
                              'systolic blood pressure',
                              [within_range(0, 300), has_units(concepts.UNIT_MM_HG)],
                              required=True)
DIASTOLIC_BP = FieldValidation(
    concepts.DIASTOLIC_BP,
    'diastolic blood pressure',
    [within_range(0, 300), has_units(concepts.UNIT_MM_HG), lessthan(SYSTOLIC_BP)],
    required=True)
HEART_RATE = FieldValidation(concepts.HEART_RATE,
                             'heart rate',
                             [within_range(0, 300), has_units(concepts.UNIT_PER_MIN)],
                             required=True)
WEIGHT = FieldValidation(concepts.WEIGHT,
                         'weight',
                         [within_range(0, 1000), has_units(concepts.UNIT_KG)],
                         required=True)
BMI = FieldValidation(concepts.BMI,
                      'body mass index',
                      [has_units(concepts.UNIT_KG_M2)],
                      required=True)
HIP_CIRCUMFERENCE = FieldValidation(concepts.HIP_CIRCUMFERENCE,
                                    'hip circumference',
                                    [within_range(0, 300), has_units(concepts.UNIT_CM)],
                                    required=True)
WAIST_CIRCUMFERENCE = FieldValidation(concepts.WAIST_CIRCUMFERENCE,
                                      'waist circumerence',
                                      [within_range(0, 300), has_units(concepts.UNIT_CM)],
                                      required=True)



class ParticipantAPI(base_api.BaseApi):

  def __init__(self):
    super(ParticipantAPI, self).__init__(participant.DAO)

  @api_util.auth_required(PTC_AND_HEALTHPRO)
  def get(self, id_=None, a_id=None):
    return super(ParticipantAPI, self).get(id_, a_id)

  @api_util.auth_required(PTC)
  def post(self, a_id=None):
    return super(ParticipantAPI, self).post(a_id)

  @api_util.auth_required(PTC)
  def put(self, id_, a_id=None):
    return super(ParticipantAPI, self).put(id_, a_id)

  @api_util.auth_required(PTC)
  def patch(self, id_, a_id=None):
    return super(ParticipantAPI, self).patch(id_, a_id)

class EvaluationAPI(base_api.BaseApi):

  def __init__(self):
    super(EvaluationAPI, self).__init__(evaluation.DAO)

  @api_util.auth_required(PTC_AND_HEALTHPRO)
  def get(self, id_=None, a_id=None):
    return super(EvaluationAPI, self).get(id_, a_id)

  @api_util.auth_required(HEALTHPRO)
  def post(self, a_id=None):
    return super(EvaluationAPI, self).post(a_id)

  @api_util.auth_required(HEALTHPRO)
  def put(self, id_, a_id=None):
    return super(EvaluationAPI, self).put(id_, a_id)

  @api_util.auth_required(HEALTHPRO)
  def patch(self, id_, a_id=None):
    return super(EvaluationAPI, self).patch(id_, a_id)

  @api_util.auth_required(PTC_AND_HEALTHPRO)
  def list(self, a_id):
    return super(EvaluationAPI, self).query("id", a_id)

  def validate_object(self, e, a_id=None):
    field_validators = [
        SYSTOLIC_BP,
        DIASTOLIC_BP,
        HEART_RATE,
        WEIGHT,
        BMI,
        HIP_CIRCUMFERENCE,
        WAIST_CIRCUMFERENCE,
    ]
    extractor = evaluation.EvaluationExtractor(e.resource)
    value_dict = {f.concept: extractor.extract_value(f.concept) for f in field_validators}
    field_validation.validate_fields(field_validators, value_dict)

def _check_existence(extractor, system, code, name):
  value = extractor.extract_value(concepts.Concept(system, code))
  if not value:
    raise BadRequest('Evaluation does not contain a value for {}, ({}:{}).'.format(
        name, system, code))


class ParticipantSummaryAPI(base_api.BaseApi):
  def __init__(self):
    super(ParticipantSummaryAPI, self).__init__(participant_summary.DAO)

  @api_util.auth_required(PTC_AND_HEALTHPRO)
  def get(self, id_=None):
    if id_:
      return super(ParticipantSummaryAPI, self).get(participant_summary.SINGLETON_SUMMARY_ID, id_)
    else:
      if request.args.get('hpoId') or (request.args.get('lastName') and request.args.get('dateOfBirth')):        
        return super(ParticipantSummaryAPI, self).query("participantId")
      else:
        raise BadRequest("Participant summary queries must specify hpoId or both lastName and dateOfBirth")
