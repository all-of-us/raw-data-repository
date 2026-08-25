import multiprocessing
import os
import resource

from rdr_service.rdr_thread_worker import RdrThreadWorker


_port = 8080 # local dev/testing.
workers = 1
threads = 1
worker_class = RdrThreadWorker

max_requests = 1000
max_requests_jitter = 50

if os.getenv('GAE_ENV', '').startswith('standard'):
    _port = os.environ.get('PORT', 8081)
    workers = multiprocessing.cpu_count()
    threads = multiprocessing.cpu_count() * 16


bind = "0.0.0.0:{0}".format(_port)

timeout = 700
keepalive = 700
log_level = "debug"
# Do not use "gevent" for worker class, doesn't work on App Engine.
# worker_class = "gevent"
raw_env = [
    "RDR_CONFIG_PROVIDER={0}".format(os.environ.get('RDR_CONFIG_PROVIDER', None)),
    "RDR_STORAGE_PROVIDER={0}".format(os.environ.get('RDR_STORAGE_PROVIDER', None)),
]


# GAE F4_1G / B4_1G instances allow up to 3072 MB of memory to be used
# So each worker needs to stay under a limit to prevent the sum of their used memory from
# exceeding GAE's hard limit. If a worker is using too much, we'll restart it to release memory
# back to the OS. That way we can gracefully limit our memory rather than have Google killing
# us forcefully (and potentially in the middle of handling a request), which shows up as a 503.
def post_request(worker, request, environment, response):  # pylint: disable=unused-argument
    # Sum up memory used for the calling worker
    memory_used_kilobytes = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss

    # The instance's total memory limit depends on the app.yaml instance_class:
    #   F4_1G / B4_1G / B8 = 3072 MB
    # Using an F4_1G (3072 MB) instance and leaving ~500 MB of headroom for in-flight
    # spikes / non-worker overhead, each worker should stay under (2560 MB / workers).
    total_worker_budget_mb = 2560
    memory_threshold_kilobytes = (total_worker_budget_mb * 1024) / max(workers, 1)
    if memory_used_kilobytes > memory_threshold_kilobytes:
        memory_used_megabytes = round(memory_used_kilobytes / 1024, 2)
        # Logs from the worker appear beside the normal app logs, but without log levels attached to them.
        worker.log.info(f"Restarting worker found to be using {memory_used_megabytes} megabytes (pid: {os.getpid()})")

        # This will safely start shutting down a worker: letting it continue with the request it is
        # currently processing, but closing it off from further requests.
        worker.handle_exit(None, None)

# The below function is useful for debugging settings.
# Leaving in but disabled in case future modifications need to be tested.
# def post_fork(server, worker):  # pylint: disable=unused-argument
#     server.log.info(f"Gunicorn Keep-Alive: {server.cfg.keepalive}")
#     server.log.info(f"Gunicorn Timeout: {server.cfg.timeout}")
