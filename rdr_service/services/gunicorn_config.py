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
    workers = multiprocessing.cpu_count()
    threads = workers * 16


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
# So each worker will need to stay under a limit to prevent the sum of their used memory to go over GAE's limit.
# If a worker is using too much, then we'll restart it to release what it's gathered back to the OS.
# That way we can gracefully limit our memory rather than have Google killing us forcefully
# (and potentially in the middle of handling a request).
def post_request(worker, request, environment, response):  # pylint: disable=unused-argument
    # Sum up memory used for the calling worker
    memory_used_kilobytes = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss

    # Our instance is killed if it is using more than 1024 megabytes.
    # To leave enough room to handle memory intensive requests, let's restart each worker if
    # they're using more than their share of memory a total of 1000 megabytes for the server.
    memory_threshold_kilobytes = 1024000 / workers  # 1000 megabytes (1000 x 1024) shared between the number of workers
    if memory_used_kilobytes > memory_threshold_kilobytes:
        memory_used_megabytes = round(memory_used_kilobytes / 1024, 2)
        # Logs from the worker appear beside the normal app logs, but without log levels attached to them.
        worker.log.info(f"Restarting worker found to be using {memory_used_megabytes} megabytes (pid: {os.getpid()})")

        # This will safely start shutting down a worker: letting it continue with the request it is
        # currently processing, but closing it off from further requests (as was seen with thread workers when we
        # set alive to False).
        # WARNING: As of now the parameters sig and frame aren't used by the thread worker (our current default). If
        # we change to another worker type, or if Gunicorn updates the handle_quit code to do something with them,
        # then we may need to pass something in.
        worker.handle_abort(None, None)
