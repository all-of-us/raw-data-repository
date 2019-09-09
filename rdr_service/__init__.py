import inspect
import logging


LOG = logging.getLogger(__name__)


def _log_need_to_fix(name):
    def wrapped(*args, **kwargs):
        stack = inspect.stack()
        caller = inspect.getframeinfo(stack[1][0])
        LOG.warning("NEED TO FIX: {}, {} called {}".format(
            caller.filename,
            caller.lineno,
            name
        ))
    return staticmethod(wrapped)


# TODO: Decide if these are the best places for these.

# Simple non-working replacement for google.appengine.ext.deferred
class deferred:
    def defer(self, *args):
        pass


class taskqueue:
    add = _log_need_to_fix('taskqueue.add')
