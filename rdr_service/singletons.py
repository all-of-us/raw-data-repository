import threading
from datetime import datetime, timedelta
import redis
import logging
import pytz
import os

from rdr_service.clock import CLOCK
from rdr_service.services.gcp_config import RdrEnvironment

singletons_lock = threading.RLock()
singletons_map = {}

CODE_CACHE_INDEX = 0
HPO_CACHE_INDEX = 1
SITE_CACHE_INDEX = 2
SQL_DATABASE_INDEX = 3
ORGANIZATION_CACHE_INDEX = 4
GENERIC_SQL_DATABASE_INDEX = 5
MAIN_CONFIG_INDEX = 6
DB_CONFIG_INDEX = 7
BACKUP_SQL_DATABASE_INDEX = 8
ALEMBIC_SQL_DATABASE_INDEX = 9
READ_UNCOMMITTED_DATABASE_INDEX = 10
BASICS_PROFILE_UPDATE_CODES_CACHE_INDEX = 11

# Those values could be modified on one App Engine instance without notifying other instances.
# We use Google MemoryStore service to sync the refresh status across instances.
REFRESH_STATUS_CHECK_LIST = [
    CODE_CACHE_INDEX, HPO_CACHE_INDEX, SITE_CACHE_INDEX, ORGANIZATION_CACHE_INDEX
]

REDIS_NAME_PREFIX = 'update_cache_index_'
REDIS_ENVS = [RdrEnvironment.SANDBOX.value, RdrEnvironment.STABLE.value, RdrEnvironment.PROD.value]
REDIS_HOST = '10.105.0.4'
REDIS_PORT = 6378
REDIS_AUTH = 'bdfc8a34-3800-4ae0-9c2d-58eafb9948be'

_DATE_FORMAT = "%Y-%m-%dT%H:%M:%SZ"

# Get project name and credentials
if os.getenv('GAE_ENV', '').startswith('standard'):
    # Production in the standard environment
    import google.auth
    GAE_CREDENTIALS, GAE_PROJECT = google.auth.default()
    GAE_VERSION_ID = os.environ.get('GAE_VERSION')
else:
    GAE_CREDENTIALS = 'local@localhost.net'
    GAE_PROJECT = 'localhost'
    GAE_VERSION_ID = 'develop'


def reset_for_tests():
    with singletons_lock:
        singletons_map.clear()


def _get(cache_index, cache_ttl_seconds=None):
    existing_pair = singletons_map.get(cache_index)
    if existing_pair and (existing_pair[1] is None or existing_pair[1] >= CLOCK.now()):
        # check if value has been updated in other instances, if yes, return None to trigger refresh local cache value
        logging.warning('get cache_index:' + str(cache_index))
        logging.warning('get GAE_PROJECT:' + str(GAE_PROJECT))
        if cache_index in REFRESH_STATUS_CHECK_LIST and GAE_PROJECT in REDIS_ENVS:
            logging.info(f"reading updated time in Redis for cache index {str(cache_index)}")
            redis_client = redis.StrictRedis(host=REDIS_HOST, port=REDIS_PORT)
            last_updated_time_str = redis_client.get(REDIS_NAME_PREFIX + str(cache_index))
            if last_updated_time_str and cache_ttl_seconds and existing_pair[1]:
                last_updated_time = datetime.strptime(last_updated_time_str, _DATE_FORMAT)
                if last_updated_time < existing_pair[1] - timedelta(seconds=cache_ttl_seconds):
                    logging.info(f"cache index {str(cache_index)} not refreshed in other instances, nothing to do")
                    return existing_pair[0]
                else:
                    logging.info(f"cache index {str(cache_index)} refreshed in other instances, need to refresh")
                    return None
        return existing_pair[0]
    return None


def get(cache_index, constructor, cache_ttl_seconds=None, **kwargs):
    """Get a cache with a specified index from the list above. If not initialized, use
  constructor to initialize it; if cache_ttl_seconds is set, reload it after that period."""
    # First try without a lock
    result = _get(cache_index, cache_ttl_seconds)
    if result:
        return result

    # Then grab the lock and try again
    with singletons_lock:
        result = _get(cache_index, cache_ttl_seconds)
        if result:
            return result
        else:
            new_instance = constructor(**kwargs)
            expiration_time = None
            if cache_ttl_seconds is not None:
                expiration_time = CLOCK.now() + timedelta(seconds=cache_ttl_seconds)
            singletons_map[cache_index] = (new_instance, expiration_time)
            return new_instance


def invalidate(cache_index):
    with singletons_lock:
        singletons_map[cache_index] = None
        # this method will be called when value updated or cache need to be refreshed
        # set lasted update time to Redis, so other instances can know and refresh their caches
        logging.warning('invalidate cache_index:' + str(cache_index))
        logging.warning('invalidate GAE_PROJECT:' + str(GAE_PROJECT))
        if cache_index in REFRESH_STATUS_CHECK_LIST and GAE_PROJECT in REDIS_ENVS:
            logging.info(f"setting updated time in Redis for cache index {str(cache_index)}")
            redis_client = redis.StrictRedis(host=REDIS_HOST, port=REDIS_PORT)
            updated_time_str = _format_datetime(CLOCK.now())
            redis_client.set(name=REDIS_NAME_PREFIX + str(cache_index), value=updated_time_str, ex=1000)


def _format_datetime(dt):
    aware_dt = dt if dt.tzinfo is None else pytz.utc.localize(dt)
    return aware_dt.strftime(_DATE_FORMAT)
