"""Test for the biobank order endpoint."""

import json
import unittest

import test_util

class TestBiobankOrder(unittest.TestCase):
  def setUp(self):
    self.maxDiff = None
    self.client = test_util.get_client('rdr/v1')
    self.participant_id = test_util.create_participant(
        'Eval', 'Johnson', '1969-02-02')


  def test_insert_eval(self):
    evaluation_files = [
        'test-data/biobank_order_1.json',
    ]

    for json_file in evaluation_files:
      with open(json_file) as f:
        biobank_order = json.load(f)
        path = 'Participant/{}/BiobankOrder'.format(self.participant_id)
        test_util.round_trip(self, self.client, path, biobank_order)

if __name__ == '__main__':
  unittest.main()
