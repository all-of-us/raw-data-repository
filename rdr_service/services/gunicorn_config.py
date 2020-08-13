import multiprocessing
import os
import resource

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
def post_request(worker, *_):
    # Sum up memory used for this process and any children (resulting in memory used by this instance)
    self_mem_bytes = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    children_mem_bytes = resource.getrusage(resource.RUSAGE_CHILDREN).ru_maxrss
    memory_threshold_bytes = 996147200  # 950 megabytes (950 x 1024 x 1024)

    # Gracefully kill the worker that just completed a request if the instance is holding on to too much memory.
    # This unfortunate worker may not be the one using the most memory on the instance but it will help,
    # and hopefully we'll eventually get the one that is.
    # In the future this can be tuned to use the process id of the worker to find how much memory that worker
    # has allocated.
    memory_used_bytes = self_mem_bytes + children_mem_bytes
    if memory_used_bytes > memory_threshold_bytes:

        # This is copied from Gunicorn's code for closing out a sync worker after reaching the max_requests limit
        worker.alive = False

        memory_used_megabytes = round(memory_used_bytes / 1048576, 2)
        # Logs from the worker seem to appear beside the normal app logs, but without log levels attached to them.
        worker.log.info(f"Auto-restarting worker after gunicorn instance {os.getpid()} was"
                        f"found to be using {memory_used_megabytes} megabytes.")
