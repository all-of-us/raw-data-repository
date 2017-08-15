from base_dao import UpdatableDao
from sqlalchemy.orm.session import make_transient

import singletons

class EntityCache(object):
  """A cache of entities of a particular type, indexed by ID (in id_to_entity) and optionally other
   fields (in index_maps).
   """
  def __init__(self, dao, entities, index_field_keys):
    """Constructor taking the DAO, all the entities in the database for this type, and a list of
    field names or tuples of field names to index the entities by."""
    self.id_to_entity = {}
    if index_field_keys:
      self.index_maps = {index_field_key: {} for index_field_key in index_field_keys}
    for entity in entities:
      make_transient(entity)
      self.id_to_entity[dao.get_id(entity)] = entity
      if index_field_keys:
        for index_field_key in index_field_keys:
          if type(index_field_key) is tuple:
            key = tuple(getattr(entity, index_field) for index_field in index_field_key)
          else:
            key = getattr(entity, index_field_key)
          self.index_maps[index_field_key][key] = entity

class CacheAllDao(UpdatableDao):
  """A DAO that loads all values from the database and caches them in memory for some period of time
  when the cache is empty and is being used.
  Used for tables that have relatively few rows and updates and high read usage.

  Updates to rows will invalidate the entire cache on the server where the update occurs, but not
  on other servers.

  cache_index is an index from singletons (e.g. CODE_CACHE_INDEX) provided by subclasses
  to specify a key for the cache. (This is faster than hashing the type name.)

  cache_ttl_seconds is the TTL for entries in the cache in seconds.

  index_field_keys is an optional list for secondary indexes; elements in it can either by
  individual field names or tuples of field names. Cached objects will be keyed by those fields.
  
  See BaseDao for documentation on order_by_ending.
  """

  def __init__(self, model_type, cache_index, cache_ttl_seconds, index_field_keys=None,
               order_by_ending=None):
    super(CacheAllDao, self).__init__(model_type, order_by_ending)
    self.index_field_keys = index_field_keys
    self.cache_index = cache_index
    self.cache_ttl_seconds = cache_ttl_seconds

  def _load_cache(self):
    with self.session() as session:
      all_entities = session.query(self.model_type).all()
    return EntityCache(self, all_entities, self.index_field_keys)

  def _get_cache(self):
    return singletons.get(self.cache_index, (lambda: self._load_cache()), self.cache_ttl_seconds)

  def get_with_session(self, session, obj_id, **kwargs):
    #pylint: disable=unused-argument
    return self._get_cache().id_to_entity.get(obj_id)

  def get(self, obj_id):
    return self._get_cache().id_to_entity.get(obj_id)

  def _invalidate_cache(self):
    singletons.invalidate(self.cache_index)

  def insert_with_session(self, session, obj):
    super(CacheAllDao, self).insert_with_session(session, obj)
    self._invalidate_cache()

  def update_with_session(self, session, obj):
    super(CacheAllDao, self).update_with_session(session, obj)
    self._invalidate_cache()

  def get_with_ids(self, ids):
    if ids is None:
      return []
    return [self.get(id_) for id_ in ids]

  def get_all(self):
    return self._get_cache().id_to_entity.values()
