import threading
from clock import CLOCK
from datetime import timedelta

singletons_lock = threading.RLock()
# We use a list with indexes, rather than a dict, to eliminate the performance hit of hashing.
singletons_map = {}

CODE_CACHE_INDEX = 0
HPO_CACHE_INDEX = 1
SITE_CACHE_INDEX = 2
SQL_DATABASE_INDEX = 3
ORGANIZATION_CACHE_INDEX = 4

def reset_for_tests():
  with singletons_lock:
    singletons_map.clear()

def _get(cache_index):
  existing_pair = singletons_map.get(cache_index)
  if existing_pair and (existing_pair[1] is None or existing_pair[1] >= CLOCK.now()):
    return existing_pair[0]
  return None

def get(cache_index, constructor, cache_ttl_seconds=None):
  """Get a cache with a specified index from the list above. If not initialized, use
  constructor to initialize it; if cache_ttl_seconds is set, reload it after that period."""
  # First try without a lock
  result = _get(cache_index)
  if result:
    return result

  # Then grab the lock and try again
  with singletons_lock:
    result = _get(cache_index)
    if result:
      return result
    else:
      new_instance = constructor()
      expiration_time = None
      if cache_ttl_seconds is not None:
        expiration_time = CLOCK.now() + timedelta(seconds=cache_ttl_seconds)
      singletons_map[cache_index] = (new_instance, expiration_time)
      return new_instance

def invalidate(cache_index):
  with singletons_lock:
    singletons_map[cache_index] = None
