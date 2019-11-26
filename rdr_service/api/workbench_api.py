from rdr_service import app_util

from rdr_service.api.base_api import UpdatableApi
from rdr_service.api_util import PTC
from rdr_service.dao.workbench_workspace_dao import WorkbenchWorkspaceDao


class WorkbenchApi(UpdatableApi):
    def __init__(self):
        super(WorkbenchApi, self).__init__(WorkbenchWorkspaceDao())

    @app_util.auth_required(PTC)
    def post(self):
        return super(WorkbenchApi, self).post()
