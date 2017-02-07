"""Utilities for loading test data."""

import datetime
import json
import os


def data_path(filename):
  return os.path.join(os.path.dirname(__file__), 'test-data', filename)


def load_measurement_json(participant_id, now=None):
  with open(data_path('measurements-as-fhir.json')) as measurements_file:
    json_text = measurements_file.read()
    json_text = json_text.replace('$participant_id', participant_id)
    json_text = json_text.replace('$authored_time', now or datetime.datetime.now().isoformat())
    return json.loads(json_text)  # deserialize to validate
