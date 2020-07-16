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
            'snapshot_id': request.args.get('snapshot_id')
        }

        filters = self.validate_params(params)
        results = self.get_filtered_results(**filters)

        return results

    def get_filtered_results(self, last_snapshot_id, snapshot_id):
        """Queries DB, returns results in format consumed by front-end
        :param last_snapshot_id: indicate the last max snapshot id which has been synced
        :param snapshot_id: indicate the snapshot to be synced
        :return: Filtered results
        """

        return self.workspace_dao.get_redcap_audit_workspaces(last_snapshot_id, snapshot_id)

    def validate_params(self, params):
        filters = {}
        if params['last_snapshot_id']:
            try:
                filters['last_snapshot_id'] = int(params['last_snapshot_id'])
            except TypeError:
                raise BadRequest(f"Invalid parameter last_snapshot_id: {params['last_snapshot_id']}")
        else:
            filters['last_snapshot_id'] = None

        if params['snapshot_id']:
            try:
                filters['snapshot_id'] = int(params['snapshot_id'])
            except TypeError:
                raise BadRequest(f"Invalid parameter snapshot_id: {params['snapshot_id']}")
        else:
            filters['snapshot_id'] = None

        return filters
