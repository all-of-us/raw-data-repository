import StringIO
import csv
import datetime
import random
import time

import clock
import config
import pytz
from cloudstorage import cloudstorage_api  # stubbed by testbed
from code_constants import BIOBANK_TESTS

from offline import genomic_set_file_handler
from test import test_data
from test.unit_test.unit_test_util import CloudStorageSqlTestBase, NdbTestBase, TestBase


_BASELINE_TESTS = list(BIOBANK_TESTS)
_FAKE_BUCKET = 'rdr_fake_bucket'


class GenomicSetFileHandlerTest(CloudStorageSqlTestBase, NdbTestBase):
  def setUp(self):
    super(GenomicSetFileHandlerTest, self).setUp(use_mysql=True)
    NdbTestBase.doSetUp(self)
    TestBase.setup_fake(self)
    # Everything is stored as a list, so override bucket name as a 1-element list.
    config.override_setting(config.GENOMIC_SET_BUCKET_NAME, [_FAKE_BUCKET])

  def _write_cloud_csv(self, file_name, contents_str):
    with cloudstorage_api.open('/%s/%s' % (_FAKE_BUCKET, file_name), mode='w') as cloud_file:
      cloud_file.write(contents_str.encode('utf-8'))

  def test_end_to_end(self):
    samples_file = test_data.open_genomic_set_file()

    input_filename = 'cloud%s.csv' % self._naive_utc_to_naive_central(clock.CLOCK.now()).strftime(
        genomic_set_file_handler.INPUT_CSV_TIME_FORMAT)
    print '---- input_filename: ' + input_filename
    self._write_cloud_csv(input_filename, samples_file)
    genomic_set_file_handler.read_genomic_set_from_bucket()

  def _naive_utc_to_naive_central(self, naive_utc_date):
    utc_date = pytz.utc.localize(naive_utc_date)
    central_date = utc_date.astimezone(pytz.timezone('US/Central'))
    return central_date.replace(tzinfo=None)
