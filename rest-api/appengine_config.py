"""Adds Python libraries from outside this directory.

cloud.google.com/appengine/docs/standard/python/tools/using-libraries-python-27
"""

from google.appengine.ext import vendor

vendor.add('lib')
vendor.add('lib-common')
vendor.add('appengine-mapreduce/python/src')
vendor.add('appengine-mapreduce/python/test')


def webapp_add_wsgi_middleware(app):
  from google.appengine.ext.appstats import recording
  app = recording.appstats_wsgi_middleware(app)
  return app

