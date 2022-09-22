from flask import request
from rdr_service.api.base_api import BaseApi
from werkzeug.exceptions import BadRequest
from rdr_service.api_util import REDCAP_AND_RDR
from rdr_service.app_util import auth_required
from rdr_service.config import GAE_PROJECT
from rdr_service.dao.bq_workbench_dao import rebuild_bq_audit
from rdr_service.dao.workbench_dao import WorkbenchResearcherDao, WorkbenchWorkspaceAuditDao
from rdr_service.services.system_utils import list_chunks


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

    def _do_insert(self, m):
        audit_records = super()._do_insert(m)
        # Generate tasks to build PDR records.
        if GAE_PROJECT == 'localhost':
            rebuild_bq_audit(audit_records)
        else:
            ids = list()
            for obj in audit_records:
                ids.append(obj.id)

            if len(ids) > 0:
                for chunk in list_chunks(ids, chunk_size=250):
                    payload = {'table': 'audit', 'ids': chunk}
                    self._task.execute('rebuild_research_workbench_table_records_task', payload=payload,
                                 in_seconds=30, queue='resource-rebuild')
        return audit_records

class RedcapResearcherAuditApi(BaseRedcapApi):
    def __init__(self):
        super().__init__()
        self.dao = WorkbenchResearcherDao()

    def get(self):
        super(RedcapResearcherAuditApi, self).get()
        return self.dao.get_redcap_audit_researchers(**self.get_filters)

