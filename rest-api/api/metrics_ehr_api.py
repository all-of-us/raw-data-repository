import functools

from dao.calendar_dao import INTERVALS
from dao.hpo_dao import HPODao
from dao.metrics_ehr_service import MetricsEhrService

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
      'awardee_names': self._parse_comma_separated,
      'interval': functools.partial(self._parse_choice, INTERVALS)
    }
    valid_arguments = self._parse_input(validators, {
      'start_date': request.args.get('start_date'),
      'end_date': request.args.get('end_date'),
      'awardee_names': request.args.get('awardee'),  # NOTE: non-ideal name to match existing APIs
      'interval': request.args.get('interval'),
    })
    return MetricsEhrService().get_metrics(
      start_date=valid_arguments['start_date'],
      end_date=valid_arguments['end_date'],
      interval=valid_arguments['interval'],
      hpo_ids=self._get_hpo_ids_from_awardee_names(valid_arguments['awardee_names']),
    )

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
  def _parse_comma_separated(params, key):
    try:
      return params[key].split(',')
    except (KeyError, AttributeError):
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

  @staticmethod
  def _get_hpo_ids_from_awardee_names(awardee_names):
    dao = HPODao()
    try:
      return [
        dao.get_by_name(name).hpoId
        for name in awardee_names
      ]
    except AttributeError:
      raise BadRequest('Invalid awardees {value}'.format(value=','.join(awardee_names)))
