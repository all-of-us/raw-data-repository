from flask import request
from rdr_service.api.base_api import BaseApi
from werkzeug.exceptions import BadRequest
from rdr_service.api_util import REDCAP_AND_RDR
from rdr_service.app_util import auth_required
from rdr_service.dao.workbench_dao import WorkbenchResearcherDao, WorkbenchWorkspaceAuditDao


class BaseRedcapApi(BaseApi):
    def __init__(self):
        super().__init__(WorkbenchWorkspaceAuditDao())
        self.get_filters = None

    @auth_required(REDCAP_AND_RDR)
    def get(self):
        get_param_config = {
            'redcapworkbenchauditapi': {
                'last_snapshot_id': request.args.get('last_snapshot_id'),
                'snapshot_id': request.args.get('snapshot_id'),
                'workspace_id': request.args.get('workspace_id')
            },
            'redcapresearcherauditapi': {
                'last_snapshot_id': request.args.get('last_snapshot_id'),
                'snapshot_id': request.args.get('snapshot_id'),
                'user_source_id': request.args.get('user_source_id')
            }

        }
        self.get_filters = self.validate_params(
            get_param_config[self.__class__.__name__.lower()]
        )

    @auth_required(REDCAP_AND_RDR)
    def post(self):
        return super().post()

    @staticmethod
    def validate_params(params):
        filters = {}
        for key in params:
            try:
                filters[key] = int(params[key]) if params[key] is not None \
                                                   and params[key].isnumeric() \
                                                   else None
            except TypeError:
                raise BadRequest(f"Invalid parameter {key}: {params[key]}")

        return filters


class RedcapWorkbenchAuditApi(BaseRedcapApi):
    def __init__(self):
        super().__init__()
        self.dao = WorkbenchWorkspaceAuditDao()

    def get(self):
        super(RedcapWorkbenchAuditApi, self).get()
        return self.dao.workspace_dao.get_redcap_audit_workspaces(**self.get_filters)


class RedcapResearcherAuditApi(BaseRedcapApi):
    def __init__(self):
        super().__init__()
        self.dao = WorkbenchResearcherDao()

    def get(self):
        super(RedcapResearcherAuditApi, self).get()
        return self.dao.get_redcap_audit_researchers(**self.get_filters)

