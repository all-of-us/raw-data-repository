import functools

from dao.metrics_ehr_service import INTERVALS, MetricsEhrService

from flask import request
from flask_restful import Resource
from werkzeug.exceptions import BadRequest

from api_util import HEALTHPRO, parse_date
import app_util


DATE_FORMAT = '%Y-%m-%d'


class MetricsEhrApi(Resource):

  @app_util.auth_required(HEALTHPRO)
  def get(self):
    validators = {
      'start_date': self._parse_date,
      'end_date': self._parse_date,
      'site_ids': self._parse_list,
      'interval': functools.partial(self._parse_choice, INTERVALS)
    }
    params = self._parse_input(validators, request.get_json())
    return MetricsEhrService().get_metrics(**params)

  @staticmethod
  def _parse_input(validators, params):
    return {
      key: validators[key](params, key)
      for key
      in validators
    }

  @staticmethod
  def _parse_date(params, key):
    try:
      return parse_date(params[key])
    except ValueError:
      raise BadRequest('Invalid {key} date: {value}'.format(
        key=key,
        value=params[key]
      ))
    except KeyError:
      raise BadRequest('Missing {key}'.format(key=key))

  @staticmethod
  def _parse_list(params, key):
    try:
      return list(params[key]) if isinstance(params[key], (list, tuple)) else [params[key]]
    except KeyError:
      return []

  @staticmethod
  def _parse_choice(choices, params, key):
    try:
      if params[key] in choices:
        return params[key]
      raise BadRequest('Invalid choice for {key}. Must be one of {choices}'.format(
        key=key,
        choices=', '.join(choices)
      ))
    except KeyError:
      raise BadRequest('Missing {key}'.format(key=key))
