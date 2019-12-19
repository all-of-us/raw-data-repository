from flask import request
from flask_restful import Resource
from werkzeug.exceptions import BadRequest
from rdr_service.api_util import STOREFRONT
from rdr_service.app_util import auth_required
from rdr_service.dao.workbench_dao import WorkbenchWorkspaceDao
from rdr_service.participant_enums import WorkbenchWorkspaceStatus


class ResearchProjectsDirectoryApi(Resource):
    def __init__(self):
        self.workspace_dao = WorkbenchWorkspaceDao()

    @auth_required(STOREFRONT)
    def get(self):
        params = {
            'status': request.args.get('status')
        }

        filters = self.validate_params(params)
        results = self.get_filtered_results(**filters)

        return results

    def get_filtered_results(self, status):
        """Queries DB, returns results in format consumed by front-end
        :param status: Workspace status
        :return: Filtered results
        """

        return self.workspace_dao.get_workspaces_with_user_detail(status)

    def validate_params(self, params):
        filters = {}
        if params['status']:
            try:
                filters['status'] = WorkbenchWorkspaceStatus(params['status'])
            except TypeError:
                raise BadRequest(f"Invalid parameter status: {params['status']}")
        else:
            filters['status'] = None

        return filters
