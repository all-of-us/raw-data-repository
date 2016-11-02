"""The API definition file for the participants API.

This defines the APIs and the handlers for the APIs.
"""

import datetime

import api_util
import base_api
import biobank_order
import evaluation
import offline.metrics_config
import participant

from flask import request
from flask.ext.restful import Resource
from protorpc import messages
from questionnaire_response import DAO as response_DAO
from werkzeug.exceptions import BadRequest, InternalServerError

METRICS_CONFIG = offline.metrics_config.METRICS_CONFIGS['Participant']


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


class ParticipantSummaryAPI(Resource):

  @api_util.auth_required
  def get(self, id_, date=None):
    date = date or datetime.datetime.now()
    pt = participant.DAO.load(id_)

    hists = sorted([response_DAO.last_history(r) for r in response_DAO.children(pt)] +
                   [evaluation.DAO.last_history(e) for e in evaluation.DAO.children(pt)] +
                   [biobank_order.DAO.last_history(b) for b in biobank_order.DAO.children(pt)] +
                   [participant.DAO.last_history(pt)],
                   key=lambda o: o.date)

    summary = {}
    for hist in hists:
      for field in METRICS_CONFIG['fields'][hist.key.kind()]:
        try:
          result = field.func(hist)
          if result.extracted:
            summary['Participant.' + field.name] = str(result.value)
        except Exception as ex:
          raise InternalServerError('Exception extracting field {0}: {1}'.format(field.name, ex))

    return summary
