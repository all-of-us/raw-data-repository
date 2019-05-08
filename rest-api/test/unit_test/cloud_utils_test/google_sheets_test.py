import json

from test.unit_test.unit_test_util import TestbedTestBase
from cloud_utils.google_sheets import GoogleSheetCSVReader

class GoogleSheetCSVReaderTest(TestbedTestBase):
  EXAMPLE_SHEET_ID = '1ZY6KMtnMZ_5-dv8cqRgVWHOH9j23GcMZTC61UMXisuE' # https://docs.google.com/spreadsheets/d/1ZY6KMtnMZ_5-dv8cqRgVWHOH9j23GcMZTC61UMXisuE/edit?usp=sharing

  def setUp(self):
    super(GoogleSheetCSVReaderTest, self).setUp()
    self.testbed.init_urlfetch_stub()

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
