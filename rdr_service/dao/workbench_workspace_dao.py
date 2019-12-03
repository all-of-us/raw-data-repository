from rdr_service.dao.base_dao import UpdatableDao
from rdr_service.model.workbench_workspace import (
    WorkbenchWorkspace
)


class WorkbenchWorkspaceDao(UpdatableDao):
    def __init__(self):
        super(WorkbenchWorkspaceDao, self).__init__(WorkbenchWorkspace, order_by_ending=["id"])

    def get_id(self, obj):
        return obj.id

    def from_client_json(self, resource_json):
        # TODO
        return

    def insert(self, workspaces):
        # TODO
        return workspaces

    def to_client_json(self, workspaces):
        # TODO
        return


