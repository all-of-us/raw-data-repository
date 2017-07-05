import copy
import json
import os
import unittest

from client import Client
from main_util import configure_logging

# To run the tests against the test instance instead,
# set environment variable PMI_DRC_RDR_INSTANCE.
_DEFAULT_INSTANCE = 'http://localhost:8080'

_OFFLINE_BASE_PATH = 'offline'


class BaseClientTest(unittest.TestCase):
  def setUp(self):
    super(BaseClientTest, self).setUp()
    configure_logging()
    self.maxDiff = None
    instance = os.environ.get('PMI_DRC_RDR_INSTANCE') or _DEFAULT_INSTANCE
    creds_file = os.environ.get('TESTING_CREDS_FILE')
    self.client = Client(parse_cli=False, default_instance=instance, creds_file=creds_file)
    self.offline_client = Client(
        base_path=_OFFLINE_BASE_PATH,
        parse_cli=False,
        default_instance=instance,
        creds_file=creds_file)

  def assertJsonEquals(self, obj_a, obj_b):
    obj_b = copy.deepcopy(obj_b)
    for transient_key in ('etag', 'kind', 'meta'):
      if transient_key in obj_b:
        del obj_b[transient_key]
    self.assertMultiLineEqual(_pretty(obj_a), _pretty(obj_b))


def _pretty(obj):
  return json.dumps(obj, sort_keys=True, indent=4, separators=(',', ': '))
