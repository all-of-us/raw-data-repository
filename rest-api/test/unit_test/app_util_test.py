import datetime
import unittest

import app_util
import clock
from test.unit_test.unit_test_util import TestBase


class TestAppUtil(TestBase):
  def test_date_header(self):
    response = lambda: None  # Dummy object; functions can have arbitrary attrs set on them.
    setattr(response, 'headers', {})

    with clock.FakeClock(datetime.datetime(1994, 11, 6, 8, 49, 37)):
      app_util.add_headers(response)

    self.assertEquals(response.headers['Date'], 'Sun, 06 Nov 1994 08:49:37 GMT')

  def test_expiry_header(self):
    response = lambda: None  # dummy object
    setattr(response, 'headers', {})
    app_util.add_headers(response)

    self.assertEqual(response.headers['Expires'], 'Thu, 01 Jan 1970 00:00:00 GMT')

  def test_headers_present(self):
    response = lambda: None  # dummy object
    setattr(response, 'headers', {})
    app_util.add_headers(response)

    self.assertItemsEqual(response.headers.keys(), (
        'Date',
        'Expires',
        'Pragma',
        'Cache-control',
        'Content-Disposition',
        'Content-Type',
        'X-Content-Type-Options',
    ))


if __name__ == '__main__':
  unittest.main()
