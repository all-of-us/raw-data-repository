from dao.cache_all_dao import CacheAllDao
from model.hpo import HPO

class HPODao(CacheAllDao):
  def __init__(self):
    super(HPODao, self).__init__(HPO, cache_ttl_seconds=600, index_field_keys=['name'])

  def _validate_update(self, session, obj, existing_obj):
    # HPOs aren't versioned; suppress the normal check here.
    pass

  def get_id(self, obj):
    return obj.hpoId

  def get_by_name(self, name):
    return self._get_cache().index_maps['name'].get(name)