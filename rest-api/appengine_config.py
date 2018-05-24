"""Adds Python libraries from outside this directory.

cloud.google.com/appengine/docs/standard/python/tools/using-libraries-python-27
"""

from google.appengine.ext import vendor

import cProfile
import cStringIO
import logging
import pstats

vendor.add('lib')
vendor.add('lib-common')
vendor.add('appengine-mapreduce/python/src')
vendor.add('appengine-mapreduce/python/test')


#def webapp_add_wsgi_middleware(app):
#
#  def profiling_wrapper(environ, start_response):
#    profile = cProfile.Profile()
#    response = profile.runcall(app, environ, start_response)
#    stream = cStringIO.StringIO()
#    stats = pstats.Stats(profile, stream=stream)
#    stats.sort_stats('cumulative').print_stats()
#    logging.info('Profile data:\n%s', stream.getvalue())
#    return response
#
#  return profiling_wrapper

def webapp_add_wsgi_middleware(app):
    from google.appengine.ext.appstats import recording
    app = recording.appstats_wsgi_middleware(app)
    return app

