from dao.cache_all_dao import CacheAllDao
from model.organization import Organization
from singletons import ORGANIZATION_CACHE_INDEX

class OrganizationDao(CacheAllDao):
  def __init__(self):
    super(OrganizationDao, self).__init__(Organization, cache_index=ORGANIZATION_CACHE_INDEX,
                                 cache_ttl_seconds=600, index_field_keys=['externalId'])

  def _validate_update(self, session, obj, existing_obj):
    # Organizations aren't versioned; suppress the normal check here.
    pass

  def get_id(self, obj):
    return obj.organizationId

  def get_by_external_id(self, external_id):
    return self._get_cache().index_maps['externalId'].get(external_id)