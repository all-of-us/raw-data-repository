"""Utilities used by the tests."""

import copy
import json

from client.client import Client

CREDS_FILE = './test-data/test-client-cert.json'
DEFAULT_INSTANCE = 'http://localhost:8080'
# 'https://pmi-drc-api-test.appspot.com/'

def get_client(base_path):
  return Client(base_path, False, CREDS_FILE, DEFAULT_INSTANCE)

def create_participant(first, last, birthday):
  participant = {
      'first_name': first,
      'last_name': last,
      'date_of_birth': birthday,
  }
  participant_client = Client(
      'rdr/v1', False, CREDS_FILE, DEFAULT_INSTANCE)
  response = participant_client.request_json(
      'Participant', 'POST', participant)
  return response['participant_id']


def round_trip(test, client, path, resource):
  response = client.request_json(path, 'POST', resource)
  q_id = response['id']
  del response['id']
  _compare_json(test, resource, response)

  response = client.request_json('{}/{}'.format(path, q_id), 'GET')
  del response['id']
  _compare_json(test, resource, response)


def _compare_json(test, obj_a, obj_b):
  obj_b = copy.deepcopy(obj_b)
  if 'etag' in obj_b:
    del obj_b['etag']
  if 'kind' in obj_b:
    del obj_b['kind']
  test.assertMultiLineEqual(pretty(obj_a), pretty(obj_b))

def pretty(obj):
  return json.dumps(obj, sort_keys=True, indent=4, separators=(',', ': '))
