"""Test for the evaluation endpoint."""

import json
import unittest
import datetime

import test_util


class TestEvaluation(unittest.TestCase):
  def setUp(self):
    self.maxDiff = None
    self.client = test_util.get_client('rdr/v1')
    self.participant_id = test_util.create_participant(self.client)
    self.when = datetime.datetime.now().isoformat()

  def test_insert_eval(self):
    evaluation_files = [
        'test-data/evaluation-as-fhir.json',
    ]

    for json_file in evaluation_files:
      with open(json_file) as f:
        evaluation = f.read() \
          .replace('$authored_time', self.when) \
          .replace('$participant_id', self.participant_id)

        evaluation = json.loads(evaluation)
        path = 'Participant/{}/PhysicalEvaluation'.format(self.participant_id)
        test_util.round_trip(self, self.client, path, evaluation)

if __name__ == '__main__':
  unittest.main()
