"""The API definition file for the participants API.

This defines the APIs and the handlers for the APIs.
"""

import copy
import datetime
import traceback

import api_util
import base_api
import biobank_order
import biobank_sample
import concepts
import evaluation
import offline.metrics_config
import participant
import questionnaire_response
import field_validation

from flask import request
from flask.ext.restful import Resource
from field_validation import FieldValidation, has_units, lessthan, within_range

from werkzeug.exceptions import BadRequest, InternalServerError


METRICS_CONFIG = offline.metrics_config.METRICS_CONFIGS['Participant']


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

  @api_util.auth_required
  def list(self, a_id=None):
    # In order to do a query, at least the last name and the birthdate must be
    # specified.
    last_name = request.args.get('last_name', None)
    date_of_birth = request.args.get('date_of_birth', None)
    first_name = request.args.get('first_name', None)
    zip_code = request.args.get('zip_code', None)
    if not last_name or not date_of_birth:
      raise BadRequest(
          'Last name and date of birth must be specified.')
    return participant.DAO.list(first_name, last_name, date_of_birth, zip_code)

  def validate_object(self, p, a_id=None):
    if not p.sign_up_time:
      p.sign_up_time = datetime.datetime.now()

class EvaluationAPI(base_api.BaseApi):
  def __init__(self):
    super(EvaluationAPI, self).__init__(evaluation.DAO)

  @api_util.auth_required
  def list(self, a_id):
    return evaluation.DAO.list(a_id)

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


class ParticipantSummaryAPI(Resource):

  @api_util.auth_required
  def get(self, id_, date=None):
    # Use the current date by default for the history objects.  This is required to make
    # the age calculations for the participants work.
    date = date or datetime.datetime.now()
    pt = participant.DAO.load(id_)

    child_daos = [
        questionnaire_response.DAO,
        evaluation.DAO,
        biobank_order.DAO,
        biobank_sample.DAO,
    ]

    # Fetch all child objects, then wrap them in history objects so we can use the
    # metrics pipeline config to extract the summary.
    hists = [dao.history_model(parent=o.key, date=date, obj=o)
             for dao in child_daos
             for o in dao.children(pt)]
    hists.extend([participant.DAO.history_model(parent=pt.key, date=date, obj=pt)])

    summary = {'Participant.' + k: v for k, v in METRICS_CONFIG['initial_state'].iteritems()}
    for hist in hists:
      for field in METRICS_CONFIG['fields'][hist.key.kind()]:
        try:
          result = field.func(hist)
          if result.extracted:
            summary['Participant.' + field.name] = str(result.value)
        except Exception as _:
          raise InternalServerError('Exception extracting field {0}: {1}'.format(
              field.name, traceback.format_exc()))

      for field in METRICS_CONFIG['summary_fields']:
        try:
          result = field.func(summary)
          if result.extracted:
            summary['Participant.' + field.name] = str(result.value)
        except Exception as _:
          raise InternalServerError('Exception extracting summary field {0}: {1}'.format(
              field.name, traceback.format_exc()))

    return summary
