# import multiprocessing
import os

_port = 8080 # local dev/testing.

if os.getenv('GAE_ENV', '').startswith('standard'):
    _port = os.environ.get('PORT', 8081)

bind = "0.0.0.0:{0}".format(_port)
workers = 2
threads = 2
timeout = 60
log_level = "debug"
# Do not use "gevent" for worker class, doesn't work on App Engine.
# worker_class = "gevent"
raw_env = [
    "RDR_CONFIG_PROVIDER={0}".format(os.environ.get('RDR_CONFIG_PROVIDER', None)),
    "RDR_STORAGE_PROVIDER={0}".format(os.environ.get('RDR_STORAGE_PROVIDER', None)),
]
# workers = multiprocessing.cpu_count() * 2 + 1
# threads = multiprocessing.cpu_count() * 2 + 1