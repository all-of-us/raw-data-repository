import functools

from dao.calendar_dao import INTERVALS
from dao.metrics_ehr_service import MetricsEhrService
from dao.organization_dao import OrganizationDao

from flask import request
from flask_restful import Resource
from werkzeug.exceptions import BadRequest

from api_util import HEALTHPRO, parse_date
import app_util


DATE_FORMAT = '%Y-%m-%d'


class MetricsEhrApiBaseResource(Resource):

  def parse_input(self):
    validators = {
      'start_date': self._parse_date,
      'end_date': self._parse_date,
      'organizations': self._parse_comma_separated,
      'interval': functools.partial(self._parse_choice, INTERVALS)
    }
    return self._parse_input(validators, {
      'start_date': request.args.get('start_date'),
      'end_date': request.args.get('end_date'),
      'organizations': request.args.get('organization'),
      'interval': request.args.get('interval'),
    })

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
      return params[key].upper().split(',')
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
  def _get_organization_ids_from_organizations(organizations):
    dao = OrganizationDao()
    try:
      return [
        dao.get_by_external_id(name).organizationId
        for name in organizations
      ]
    except AttributeError:
      raise BadRequest('Invalid organization {value}'.format(value=','.join(organizations)))


class MetricsEhrApi(MetricsEhrApiBaseResource):
  """
  A combined view of:
  - Participant EHR Consented vs EHR Received Over Time
  - Organizations Active Over Time
  - Organization Participant Status Counts At Specific Time (end_date)
  """

  @app_util.auth_required(HEALTHPRO)
  def get(self):
    valid_arguments = self.parse_input()
    org_ids = self._get_organization_ids_from_organizations(valid_arguments['organizations'])
    return MetricsEhrService().get_metrics(
      start_date=valid_arguments['start_date'],
      end_date=valid_arguments['end_date'],
      interval=valid_arguments['interval'],
      organization_ids=org_ids
    )


class ParticipantEhrMetricsOverTimeApi(MetricsEhrApiBaseResource):
  """
  Participant EHR Consented vs EHR Received Over Time
  """

  @app_util.auth_required(HEALTHPRO)
  def get(self):
    valid_arguments = self.parse_input()
    org_ids = self._get_organization_ids_from_organizations(valid_arguments['organizations'])
    return MetricsEhrService().get_participant_ehr_metrics_over_time_data(
      start_date=valid_arguments['start_date'],
      end_date=valid_arguments['end_date'],
      interval=valid_arguments['interval'],
      organization_ids=org_ids
    )


class OrganizationsActiveMetricsOverTimeApi(MetricsEhrApiBaseResource):
  """
  Organizations Active Over Time
  """

  @app_util.auth_required(HEALTHPRO)
  def get(self):
    valid_arguments = self.parse_input()
    org_ids = self._get_organization_ids_from_organizations(valid_arguments['organizations'])
    return MetricsEhrService().get_organizations_active_over_time_data(
      start_date=valid_arguments['start_date'],
      end_date=valid_arguments['end_date'],
      interval=valid_arguments['interval'],
      organization_ids=org_ids
    )


class OrganizationMetricsApi(MetricsEhrApiBaseResource):
  """
  Organization Participant Status Counts At Specific Time (end_date)
  """

  @app_util.auth_required(HEALTHPRO)
  def get(self):
    valid_arguments = self.parse_input()
    org_ids = self._get_organization_ids_from_organizations(valid_arguments['organizations'])
    return MetricsEhrService().get_organization_metrics_data(
      end_date=valid_arguments['end_date'],
      organization_ids=org_ids
    )
