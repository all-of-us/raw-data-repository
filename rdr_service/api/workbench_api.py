from rdr_service import app_util

from rdr_service.api.base_api import BaseApi
from rdr_service.api_util import RDR_AND_WORKBENCH
from rdr_service.dao.workbench_dao import WorkbenchWorkspaceDao, WorkbenchResearcherDao, \
    WorkbenchResearcherHistoryDao, WorkbenchWorkspaceHistoryDao


class WorkbenchWorkspaceApi(BaseApi):
    def __init__(self):
        super(WorkbenchWorkspaceApi, self).__init__(WorkbenchWorkspaceDao(), get_returns_children=True)

    @app_util.auth_required(RDR_AND_WORKBENCH)
    def post(self):
        return super(WorkbenchWorkspaceApi, self).post()

    @app_util.auth_required(RDR_AND_WORKBENCH)
    def get(self):
        return super(WorkbenchWorkspaceApi, self)._query(id_field="workspaceId")

    def _make_resource_url(self, json, id_field, participant_id):  # pylint: disable=unused-argument
        from rdr_service import main

        return main.api.url_for(self.__class__, user_id=json[id_field], _external=True)


class WorkbenchWorkspaceHistoryApi(BaseApi):
    def __init__(self):
        super(WorkbenchWorkspaceHistoryApi, self).__init__(WorkbenchWorkspaceHistoryDao(), get_returns_children=True)

    @app_util.auth_required(RDR_AND_WORKBENCH)
    def get(self):
        return super(WorkbenchWorkspaceHistoryApi, self)._query(id_field="workspaceId")

    def _make_resource_url(self, json, id_field, participant_id):  # pylint: disable=unused-argument
        from rdr_service import main

        return main.api.url_for(self.__class__, user_id=json[id_field], _external=True)


class WorkbenchResearcherApi(BaseApi):
    def __init__(self):
        super(WorkbenchResearcherApi, self).__init__(WorkbenchResearcherDao(), get_returns_children=True)

    @app_util.auth_required(RDR_AND_WORKBENCH)
    def post(self):
        return super(WorkbenchResearcherApi, self).post()

    @app_util.auth_required(RDR_AND_WORKBENCH)
    def get(self):
        return super(WorkbenchResearcherApi, self)._query(id_field="userId")

    def _make_resource_url(self, json, id_field, participant_id):  # pylint: disable=unused-argument
        from rdr_service import main

        return main.api.url_for(self.__class__, user_id=json[id_field], _external=True)


class WorkbenchResearcherHistoryApi(BaseApi):
    def __init__(self):
        super(WorkbenchResearcherHistoryApi, self).__init__(WorkbenchResearcherHistoryDao(), get_returns_children=True)

    @app_util.auth_required(RDR_AND_WORKBENCH)
    def get(self):
        return super(WorkbenchResearcherHistoryApi, self)._query(id_field="userId")

    def _make_resource_url(self, json, id_field, participant_id):  # pylint: disable=unused-argument
        from rdr_service import main

        return main.api.url_for(self.__class__, user_id=json[id_field], _external=True)
