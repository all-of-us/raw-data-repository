"""Tests for participant."""

import datetime
import unittest

import participant

class ParticipantTest(unittest.TestCase):
  def test_bucket_age(self):
    testcases = ((18, '18-25'),
                 (19, '18-25'),
                 (25, '18-25'),
                 (26, '26-35'),
                 (85, '76-85'),
                 (86, '86-'),
                 (100, '86-'))
    date_of_birth = datetime.datetime(1940, 8, 21)
    for testcase in testcases:
      response_date = date_of_birth + datetime.timedelta(testcase[0] * 365.25)
      self.assertEqual(testcase[1],
                       participant._bucketed_age(date_of_birth, response_date))


if __name__ == '__main__':
  unittest.main()
