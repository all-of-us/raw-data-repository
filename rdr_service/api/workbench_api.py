from rdr_service import app_util

from rdr_service.api.base_api import BaseApi
from rdr_service.api_util import WORKBENCH_AND_REDCAP
from rdr_service.dao.bq_workbench_dao import rebuild_bq_workpaces, rebuild_bq_wb_researchers
from rdr_service.dao.workbench_dao import WorkbenchWorkspaceDao, WorkbenchResearcherDao


class WorkbenchWorkspaceApi(BaseApi):
    def __init__(self):
        super().__init__(WorkbenchWorkspaceDao(), get_returns_children=True)

    @app_util.auth_required(WORKBENCH_AND_REDCAP)
    def post(self):
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
