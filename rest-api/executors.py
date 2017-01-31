"""Executor utility functions. Can override to change test behavior."""

from google.appengine.ext import deferred

def defer(fn, *args, **kwargs):
  deferred.defer(fn, *args, **kwargs)