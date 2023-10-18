from flask import request

from rdr_service.api.base_api import BaseApi
from rdr_service.dao.site_hierarchy_dao import SiteHierarchyDao


class SiteHierarchyApi(BaseApi):
    def __init__(self):
        super().__init__(SiteHierarchyDao())

    def list(self, participant_id=None):
        """Handle a GET request."""
        kwargs = request.args
        return SiteHierarchyDao().handle_list_queries(**kwargs)
