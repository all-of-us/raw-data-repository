import multiprocessing
import os
import resource
import sys

_port = 8080 # local dev/testing.
workers = 1
threads = 1

max_requests = 1000
max_requests_jitter = 50

if os.getenv('GAE_ENV', '').startswith('standard'):
    _port = os.environ.get('PORT', 8081)
    workers = multiprocessing.cpu_count() * 2
    threads = multiprocessing.cpu_count() * 2

bind = "0.0.0.0:{0}".format(_port)

timeout = 60
log_level = "debug"
# Do not use "gevent" for worker class, doesn't work on App Engine.
# worker_class = "gevent"
raw_env = [
    "RDR_CONFIG_PROVIDER={0}".format(os.environ.get('RDR_CONFIG_PROVIDER', None)),
    "RDR_STORAGE_PROVIDER={0}".format(os.environ.get('RDR_STORAGE_PROVIDER', None)),
]


# GAE F4 instances allow for up to 1G of memory to be used.
# If we're getting too close to the limit we should close the instance.
# That way we can gracefully limit our memory rather than have Google killing us forcefully
# (and potentially in the middle of handling a request).
def post_request(worker, request, environment, response):
    # Sum up memory used for this process and any children (resulting in memory used by this instance)
    self_mem_bytes = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    children_mem_bytes = resource.getrusage(resource.RUSAGE_CHILDREN).ru_maxrss
    memory_threshold_bytes = 950000000  # 950 megabytes

    if self_mem_bytes + children_mem_bytes > memory_threshold_bytes:
        # Gracefully kill the worker.
        # This is copied from Gunicorn's code for closing out a sync worker after reaching the max_requests limit
        worker.alive = False
