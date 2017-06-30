"""Adds Python libraries from outside this directory.

cloud.google.com/appengine/docs/standard/python/tools/using-libraries-python-27
"""

from google.appengine.ext import vendor

vendor.add('lib')
vendor.add('lib-common')
vendor.add('appengine-mapreduce/python/src')
vendor.add('appengine-mapreduce/python/test')
