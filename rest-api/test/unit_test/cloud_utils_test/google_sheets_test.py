import json

from test.unit_test.unit_test_util import TestBase

from cloud_utils.google_sheets import GoogleSheetCSVReader
from google.appengine.api import urlfetch_stub
from google.appengine.api import apiproxy_stub_map

class GoogleSheetCSVReaderTest(TestBase):
  EXAMPLE_SHEET_ID = '1ZY6KMtnMZ_5-dv8cqRgVWHOH9j23GcMZTC61UMXisuE' # https://docs.google.com/spreadsheets/d/1ZY6KMtnMZ_5-dv8cqRgVWHOH9j23GcMZTC61UMXisuE/edit?usp=sharing

  def setUp(self):
    # unittesting Google SDK.
    # https://cloud.google.com/appengine/docs/standard/python/tools/localunittesting
    apiproxy_stub_map.apiproxy = apiproxy_stub_map.APIProxyStubMap()
    apiproxy_stub_map.apiproxy.RegisterStub('urlfetch', urlfetch_stub.URLFetchServiceStub())

  def test_reads_google_sheet_by_id(self):
    """should behave like a csv.DictReader"""
    reader = GoogleSheetCSVReader(self.EXAMPLE_SHEET_ID)
    rows = list(reader)
    self.assertEqual(len(rows), 3)
    self.assertEqual(
      json.dumps(rows[0], sort_keys=True),
      json.dumps({'A': '0', 'B': '1', 'C': '2'}, sort_keys=True),
      "should behave like csv.DictReader"
    )
