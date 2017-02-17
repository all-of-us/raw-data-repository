import threading

singletons_lock = threading.RLock()
singletons_map = {}

def reset_for_tests():
  with singletons_lock:
    singletons_map.clear()

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