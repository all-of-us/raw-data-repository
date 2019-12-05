from rdr_service import app_util

from rdr_service.api.base_api import BaseApi
from rdr_service.api_util import WORKBENCH
from rdr_service.dao.workbench_dao import WorkbenchWorkspaceDao, WorkbenchResearcherDao


class WorkbenchWorkspaceApi(BaseApi):
    def __init__(self):
        super().__init__(WorkbenchWorkspaceDao(), get_returns_children=True)

    @app_util.auth_required(WORKBENCH)
    def post(self):
        return super().post()


class WorkbenchResearcherApi(BaseApi):
    def __init__(self):
        super().__init__(WorkbenchResearcherDao(), get_returns_children=True)

    @app_util.auth_required(WORKBENCH)
    def post(self):
        return super().post()
