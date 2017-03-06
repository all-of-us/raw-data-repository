import cachetools
import threading

cache_lock = threading.RLock()
cache_map = {}

singletons_lock = threading.RLock()
singletons_map = {}

def reset_for_tests():
  with singletons_lock:
    singletons_map.clear()
  with cache_lock:
    cache_map.clear()

def get(constructor):
  # First try without a lock (usually should return something)
  existing_instance = singletons_map.get(constructor.__name__)
  if existing_instance:
    return existing_instance
  # Then grab the lock and try again
  with singletons_lock:
    existing_instance = singletons_map.get(constructor.__name__)
    if existing_instance:
      return existing_instance
    else:
      new_instance = constructor()
      singletons_map[constructor.__name__] = new_instance
      return new_instance

def get_cache(cache_type, ttl_seconds, get_method=None):
  existing_cache = cache_map.get(cache_type.__name__)
  if existing_cache:
    return existing_cache
  with cache_lock:
    new_cache = cachetools.TTLCache(1, ttl=ttl_seconds, missing=get_method)
    new_cache.lock = threading.RLock()
    cache_map[cache_type.__name__] = new_cache
    return new_cache
