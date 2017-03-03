"""Utilities for loading test data."""

import datetime
import json
import os


def data_path(filename):
  return os.path.join(os.path.dirname(__file__), 'test-data', filename)

def primary_provider_link(hpo_name):
  return '[ { "primary": true, "organization": { "reference": "Organization/%s" } } ]' % hpo_name

def load_measurement_json(participant_id, now=None):
  """Loads a PhysicalMeasurement FHIR resource returns it as parsed JSON."""
  with open(data_path('measurements-as-fhir.json')) as measurements_file:
    json_text = measurements_file.read() % {
      'participant_id': participant_id,
      'authored_time': now or datetime.datetime.now().isoformat(),
    }
    return json.loads(json_text)  # deserialize to validate


def load_measurement_json_amendment(participant_id, amended_id, now=None):
  """Loads a PhysicalMeasurement FHIR resource and adds an amendment extension."""
  with open(data_path('measurements-as-fhir-amendment.json')) as amendment_file:
    extension = json.loads(amendment_file.read() % {
          'physical_measurement_id': amended_id,
        })
  with open(data_path('measurements-as-fhir.json')) as measurements_file:
    measurement = json.loads(measurements_file.read() % {
          'participant_id': participant_id,
          'authored_time': now or datetime.datetime.now().isoformat(),
        })
  measurement['entry'][0]['resource'].update(extension)
  return measurement


def load_biobank_order_json(participant_id):
  with open(data_path('biobank_order_1.json')) as f:
    return json.loads(f.read() % {
      'participant_id': participant_id,
    })
