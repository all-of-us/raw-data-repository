from flask import request

from rdr_service import app_util
from rdr_service.api.base_api import BaseApi
from rdr_service.dao.site_hierarchy_dao import SiteHierarchyDao
from rdr_service.api_util import CURATION, RDR


class SiteHierarchyApi(BaseApi):
    def __init__(self):
        super().__init__(SiteHierarchyDao())

    @app_util.auth_required([CURATION, RDR])
    def get(self, id_=None, participant_id=None):
        """This will call the list method below"""
        return super().get(id_=id_, participant_id=participant_id)

    @app_util.auth_required([CURATION, RDR])
    def list(self, participant_id=None):
        """Handle a GET request."""
        kwargs = request.args
        return SiteHierarchyDao().handle_list_queries(**kwargs)
