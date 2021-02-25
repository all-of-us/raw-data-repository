from rdr_service import app_util
from rdr_service import clock
from flask import request
from rdr_service.api.base_api import BaseApi
from rdr_service.api_util import WORKBENCH_AND_REDCAP
from rdr_service.dao.bq_workbench_dao import rebuild_bq_workpaces, rebuild_bq_wb_researchers
from rdr_service.dao.workbench_dao import WorkbenchWorkspaceDao, WorkbenchResearcherDao
from rdr_service.dao.metadata_dao import WORKBENCH_LAST_SYNC_KEY, MetadataDao


class WorkbenchWorkspaceApi(BaseApi):
    def __init__(self):
        super().__init__(WorkbenchWorkspaceDao(), get_returns_children=True)

    @app_util.auth_required(WORKBENCH_AND_REDCAP)
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
        rebuild_bq_workpaces(workspaces)
        return workspaces


class WorkbenchResearcherApi(BaseApi):
    def __init__(self):
        super().__init__(WorkbenchResearcherDao(), get_returns_children=True)

    @app_util.auth_required(WORKBENCH_AND_REDCAP)
    def post(self):
        return super().post()

    def _do_insert(self, m):
        result = super()._do_insert(m)
        rebuild_bq_wb_researchers(result)
        return result
