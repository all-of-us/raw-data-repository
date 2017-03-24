import copy
import json
import os
import unittest

from client.client import Client

# To run the tests against the test instance instead,
# set environment variable PMI_DRC_RDR_INSTANCE.
_DEFAULT_INSTANCE = 'http://localhost:8080'

_CREDS_FILE = './test-data/test-client-cert.json'
_BASE_PATH = 'rdr/v1'


class BaseClientTest(unittest.TestCase):
  def setUp(self):
    super(BaseClientTest, self).setUp()
    self.maxDiff = None
    instance = os.environ.get('PMI_DRC_RDR_INSTANCE') or _DEFAULT_INSTANCE
    self.client = Client(_BASE_PATH, False, _CREDS_FILE, instance)

  def assertJsonEquals(self, obj_a, obj_b):
    obj_b = copy.deepcopy(obj_b)
    for transient_key in ('etag', 'kind', 'meta'):
      if transient_key in obj_b:
        del obj_b[transient_key]
    self.assertMultiLineEqual(_pretty(obj_a), _pretty(obj_b))


def _pretty(obj):
  return json.dumps(obj, sort_keys=True, indent=4, separators=(',', ': '))
