from flask import request
from flask_restful import Resource
from werkzeug.exceptions import BadRequest

from rdr_service import app_util, clock
from rdr_service.api_util import HEALTHPRO, parse_date
from rdr_service.dao.metrics_ehr_service import MetricsEhrService
from rdr_service.dao.organization_dao import OrganizationDao


class MetricsEhrApiBaseResource(Resource):
    def get_input_validators(self):
        return {"organization": self._make_parse_with_default(self._parse_organizations, [])}

    def parse_input(self):
        return self._parse_input(self.get_input_validators(), request.args)

    def _parse_organizations(self, params, key):
        return self._get_organization_ids_from_organizations(self._parse_comma_separated(params, key))

    @staticmethod
    def _make_parse_with_default(parser, default=None):
        def wrapped(params, key):
            if key not in params:
                return default
            else:
                return parser(params, key)

        return wrapped

    @staticmethod
    def _parse_input(validators, params):
        return {key: validators[key](params, key) for key in validators}

    @staticmethod
    def _parse_date(params, key):
        try:
            return parse_date(params[key])
        except ValueError:
            raise BadRequest("Invalid {key} date: {value}".format(key=key, value=params[key]))
        except KeyError:
            raise BadRequest("Missing {key}".format(key=key))

    @staticmethod
    def _parse_comma_separated(params, key):
        return params[key].split(",")

    @staticmethod
    def _get_organization_ids_from_organizations(organizations):
        dao = OrganizationDao()
        try:
            return [dao.get_by_external_id(name.upper()).organizationId for name in organizations]
        except AttributeError:
            raise BadRequest("Invalid organization {value}".format(value=",".join(organizations)))


class MetricsEhrApi(MetricsEhrApiBaseResource):
    """
  A combined view of:
  - Participant EHR Consented vs EHR Received
  - Organization Participant Status Counts
  """

    @app_util.auth_required(HEALTHPRO)
    def get(self):
        valid_arguments = self.parse_input()
        return MetricsEhrService().get_current_metrics(organization_ids=valid_arguments["organization"])


class ParticipantEhrMetricsOverTimeApi(MetricsEhrApiBaseResource):
    """
  Participant EHR Consented vs EHR Received
  """

    @app_util.auth_required(HEALTHPRO)
    def get(self):
        valid_arguments = self.parse_input()
        return MetricsEhrService().get_current_ehr_data(organization_ids=valid_arguments["organization"])


class OrganizationMetricsApi(MetricsEhrApiBaseResource):
    """
  Organization Participant Status Counts as of date
  """

    def get_input_validators(self):
        return dict(
            super(OrganizationMetricsApi, self).get_input_validators(),
            end_date=self._make_parse_with_default(self._parse_date, clock.CLOCK.now()),
        )

    @app_util.auth_required(HEALTHPRO)
    def get(self):
        valid_arguments = self.parse_input()
        return MetricsEhrService().get_organization_metrics_data(
            end_date=valid_arguments["end_date"], organization_ids=valid_arguments["organization"]
        )
