"""Create fake participant data and load it into an RDR instance using the API client."""

import argparse
import copy
import json
import sys
from client.client import Client


def parse_args(default_instance=None):
  parser = argparse.ArgumentParser()
  parser.add_argument(
      '--count',
      type=int,
      help='The number of example participants to create',
      default=10)

  parser.add_argument(
      '--instance',
      type=str,
      help='The instance to hit, either https://xxx.appspot.com, '
      'or http://localhost:8080',
      default=default_instance)
  return parser.parse_args()


if __name__ == '__main__':
  args = parse_args()
  client = Client('rdr/v1', default_instance=args.instance, parse_cli=False)

  consent_questionnaire = json.load(open('test-data/consent_questionnaire.json'))
  consent_questionnaire_id = client.request_json(
      'Questionnaire', 'POST', consent_questionnaire)['id']

  sociodemographics_questionnaire = json.load(
      open('test-data/sociodemographics_questionnaire.json'))
  sociodemographics_questionnaire_id = client.request_json(
      'Questionnaire', 'POST', sociodemographics_questionnaire)['id']

  vars = {
    'consent_questionnaire_id': consent_questionnaire_id,
    'sociodemographics_questionnaire_id': sociodemographics_questionnaire_id,
  }

  for module in extra_ppi_modules:
    vars[module + '_questionnaire_id'] = client.request_json(
        'Questionnaire',
        'POST',
        {
            "resourceType": "Questionnaire",
            "status": "published",
            "publisher":"fake",
            "group": {
                "concept": [{
                    "system": "http://terminology.pmi-ops.org/CodeSystem/ppi-module",
                    "code": module,
                }],
            },
        })['id']

  for i in range(args.count):
    for request_details in fake_participants.create_fake_participant():
      when = request_details['when']
      endpoint = request_details['endpoint']
      payload_json = json.dumps(request_details['payload'])
      vars.update(request_details.get('vars', {}))
      for k, v in vars.iteritems():
        payload_json = payload_json.replace('$%s' % k, str(v))
        endpoint = endpoint.replace('$%s' % k, str(v))
      payload = json.loads(payload_json)
      response = client.request_json(endpoint, 'POST', payload, headers={'X-Pretend-Date': when})
      for k, f in request_details.get('gather', {}).iteritems():
        vars[k] = f(response)
