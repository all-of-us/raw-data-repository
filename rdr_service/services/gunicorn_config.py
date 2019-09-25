# import multiprocessing
import os

_port = 8080 # local dev/testing.

if os.getenv('GAE_ENV', '').startswith('standard'):
    _port = 8081  # default app engine port.
    if 'PORT' in os.environ:
        _port = os.environ['PORT']

bind = "0.0.0.0:{0}".format(_port)
workers = 1
threads = 1

# workers = multiprocessing.cpu_count() * 2 + 1
# threads = multiprocessing.cpu_count() * 2 + 1