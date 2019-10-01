from api.base_api import UpdatableApi
from app_util import auth_required
from api_util import PTC
from dao.organization_hierarchy_sync_dao import OrganizationHierarchySyncDao


class OrganizationHierarchyApi(UpdatableApi):
  def __init__(self):
    super(OrganizationHierarchyApi, self).__init__(OrganizationHierarchySyncDao())

  @auth_required(PTC)
  def put(self):
    return super(OrganizationHierarchyApi, self).put(None, skip_etag=True)
