"""Utilities used by the tests."""

import copy
import json
import os

from client.client import Client

CREDS_FILE = './test-data/test-client-cert.json'
DEFAULT_INSTANCE = 'http://localhost:8080'

# To run the tests against the test instance instead,
# set environment variable PMI_DRC_RDR_INSTANCE.

def get_client(base_path):
  instance = os.environ.get('PMI_DRC_RDR_INSTANCE') or DEFAULT_INSTANCE
  return Client(base_path, False, CREDS_FILE, instance)

def create_participant(client, first, last, birthday):
  participant = {
      'first_name': first,
      'last_name': last,
      'date_of_birth': birthday,
  }
  response = client.request_json('Participant', 'POST', participant)
  return response['participantId']

def create_questionnaire(client, json_file):
  with open(json_file) as f:
    questionnaire = json.load(f)
    response = client.request_json('Questionnaire', 'POST', questionnaire)
    return response['id']

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
  if 'meta' in obj_b:
    del obj_b['meta']
  test.assertMultiLineEqual(pretty(obj_a), pretty(obj_b))

def pretty(obj):
  return json.dumps(obj, sort_keys=True, indent=4, separators=(',', ': '))
