from flask import request
from rdr_service.api.base_api import BaseApi
from werkzeug.exceptions import BadRequest
from rdr_service.api_util import REDCAP
from rdr_service.app_util import auth_required
from rdr_service.dao.workbench_dao import WorkbenchWorkspaceDao, WorkbenchWorkspaceAuditDao


class RedcapWorkbenchAuditApi(BaseApi):
    def __init__(self):
        super().__init__(WorkbenchWorkspaceAuditDao())
        self.workspace_dao = WorkbenchWorkspaceDao()

    @auth_required(REDCAP)
    def post(self):
        return super().post()

    @auth_required(REDCAP)
    def get(self):
        params = {
            'last_snapshot_id': request.args.get('last_snapshot_id'),
            'snapshot_id': request.args.get('snapshot_id'),
            'workspace_id': request.args.get('workspace_id')
        }

        filters = self.validate_params(params)
        results = self.get_filtered_results(**filters)

        return results

    def get_filtered_results(self, **filters):
        """Queries DB, returns results in format consumed by front-end
        :param filters: query parameters for filtering the data
        :return: Filtered results
        """

        return self.workspace_dao.get_redcap_audit_workspaces(**filters)

    def validate_params(self, params):
        filters = {}
        for key in params:
            try:
                filters[key] = int(params[key]) if params[key] is not None else None
            except TypeError:
                raise BadRequest(f"Invalid parameter {key}: {params[key]}")

        return filters
