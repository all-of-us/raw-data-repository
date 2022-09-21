from rdr_service import app_util
from rdr_service import clock
from flask import request
from rdr_service.config import GAE_PROJECT
from rdr_service.api.base_api import BaseApi
from rdr_service.api_util import WORKBENCH_AND_REDCAP_AND_RDR
from rdr_service.dao.bq_workbench_dao import rebuild_bq_workpaces
from rdr_service.dao.workbench_dao import WorkbenchWorkspaceDao, WorkbenchResearcherDao
from rdr_service.dao.metadata_dao import WORKBENCH_LAST_SYNC_KEY, MetadataDao


class WorkbenchWorkspaceApi(BaseApi):
    def __init__(self):
        super().__init__(WorkbenchWorkspaceDao(), get_returns_children=True)

    @app_util.auth_required(WORKBENCH_AND_REDCAP_AND_RDR)
    def post(self):
        now = clock.CLOCK.now()
        metadata_dao = MetadataDao()
        metadata_dao.upsert(WORKBENCH_LAST_SYNC_KEY, date_value=now)
        backfill_arg = request.args.get('backfill')
        is_backfill = True if backfill_arg and backfill_arg.lower() == 'true' else False
        self.dao.is_backfill = is_backfill
        return super().post()

    def _do_insert(self, m):
        workspaces = super()._do_insert(m)

        if GAE_PROJECT == 'localhost':
            rebuild_bq_workpaces(workspaces)
        else:
            workspaces_payload = {'table': 'workspace', 'ids': []}
            workspace_users_payload = {'table': 'workspace_user', 'ids': []}
            for obj in workspaces:
                workspaces_payload['ids'].append(obj.id)
                if obj.workbenchWorkspaceUser:
                    for user in obj.workbenchWorkspaceUser:
                        workspace_users_payload['ids'].append(user.id)

            if len(workspaces_payload['ids']) > 0:
                self._task.execute('rebuild_research_workbench_table_records_task', payload=workspaces_payload,
                                   in_seconds=30, queue='resource-rebuild')
            if len(workspace_users_payload['ids']) > 0:
                self._task.execute('rebuild_research_workbench_table_records_task', payload=workspace_users_payload,
                                   in_seconds=30, queue='resource-rebuild')
        return workspaces


class WorkbenchResearcherApi(BaseApi):
    def __init__(self):
        super().__init__(WorkbenchResearcherDao(), get_returns_children=True)

    @app_util.auth_required(WORKBENCH_AND_REDCAP_AND_RDR)
    def post(self):
        backfill_arg = request.args.get('backfill')
        is_backfill = True if backfill_arg and backfill_arg.lower() == 'true' else False
        self.dao.is_backfill = is_backfill
        return super().post()

    def _do_insert(self, m):
        researchers = super()._do_insert(m)
        # Note: Moved PDR rebuild task to: workbench_dao.py:1152 WorkbenchWorkspaceHistoryDao._insert_history() method.
        return researchers
