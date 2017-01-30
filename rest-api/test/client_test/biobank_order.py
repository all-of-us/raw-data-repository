"""Test for the biobank order endpoint."""

import json
import unittest
from client.client import HttpException

import test_util

class TestBiobankOrder(unittest.TestCase):
  def setUp(self):
    self.maxDiff = None
    self.client = test_util.get_client('rdr/v1')
    self.participant_id = test_util.create_participant(self.client)

  def test_insert_eval(self):
    order_files = [
        'test-data/biobank_order_1.json',
    ]

    for json_file in order_files:
      with open(json_file) as f:
        biobank_order = json.load(f)
        biobank_order['subject'] = biobank_order['subject'].format(self.participant_id)
        if biobank_order.get('identifier'):
          for identifier in biobank_order.get('identifier'):
            identifier['value'] = identifier['value'].format(self.participant_id)
        path = 'Participant/{}/BiobankOrder'.format(self.participant_id)
        test_util.round_trip(self, self.client, path, biobank_order)
        # This should fail because the identifiers are already in use.
        try:
          test_util.round_trip(self, self.client, path, biobank_order)
          raise Exception('Second request should have failed')
        except HttpException as err:
          if "is already in use" not in err.message:
            raise Exception('Unexpected error: {}'.format(err.message))
          pass

if __name__ == '__main__':
  unittest.main()
