from flask import request
from flask_restful import Resource
from werkzeug.exceptions import BadRequest
from rdr_service.api_util import STOREFRONT_AND_REDCAP
from rdr_service.app_util import auth_required
from rdr_service.dao.workbench_dao import WorkbenchWorkspaceDao
from rdr_service.participant_enums import WorkbenchWorkspaceStatus

DEFAULT_SEQUESTRATION_WINDOWS = 23


class ResearchProjectsDirectoryApi(Resource):
    def __init__(self):
        self.workspace_dao = WorkbenchWorkspaceDao()

    @auth_required(STOREFRONT_AND_REDCAP)
    def get(self):
        params = {
            'status': request.args.get('status'),
            'sequest_hour': request.args.get('sequest_hour')
        }

        filters = self.validate_params(params)
        results = self.get_filtered_results(**filters)

        return results

    def get_filtered_results(self, status, sequest_hour):
        """Queries DB, returns results in format consumed by front-end
        :param status: Workspace status
        :param sequest_hour: sequestration time window
        :return: Filtered results
        """

        return self.workspace_dao.get_workspaces_with_user_detail(status, sequest_hour)

    def validate_params(self, params):
        filters = {}
        if params['status']:
            try:
                filters['status'] = WorkbenchWorkspaceStatus(params['status'])
            except TypeError:
                raise BadRequest(f"Invalid parameter status: {params['status']}")
        else:
            filters['status'] = None

        if params['sequest_hour']:
            try:
                filters['sequest_hour'] = int(params['sequest_hour'])
            except TypeError:
                raise BadRequest(f"Invalid parameter sequest_hour: {params['sequest_hour']}")
        else:
            filters['sequest_hour'] = DEFAULT_SEQUESTRATION_WINDOWS

        return filters
