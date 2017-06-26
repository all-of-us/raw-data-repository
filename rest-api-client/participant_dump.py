"""Print debugging information about a participant.

Usage: run_client.sh --account $USER@pmi-ops.org --project all-of-us-rdr-prod %(prog)s P12345
"""

import httplib
import json
import logging
import pprint
import subprocess

from client.client import Client
from tools.main_util import configure_logging, get_parser


_FIXME_PROJECT = 'all-of-us-rdr-sandbox'
_SERVER_LOG_FRESHNESS = '90d'


def log_debug_info(client, participant_id, project):
  # basic info: signup time, withdrawal
  logging.info(
      '%s response from GET\n%s',
      participant_id,
      pprint.pformat(client.request_json('Participant/%s' % participant_id)))
  # QuestionnaireResponses available
  # QuestionnaireResponses for this participant

  logging.info(
      'Server logs for %s from the last %s (oldest first)\n%s',
      participant_id,
      _SERVER_LOG_FRESHNESS,
      '\n'.join(['\t' + line for line in _get_app_log_lines(participant_id, project)]))


def _get_app_log_lines(participant_id, project):
  # Alternate format for easy reading on the CLI:
  #    --format="value(timestamp,severity,protoPayload.status,protoPayload.resource)"
  log_json_data = subprocess.check_output([
      'gcloud',
      'beta',
      'logging',
      'read',
      ('resource.type="gae_app"'
       + ' logName="projects/%(project)s/logs/appengine.googleapis.com%%2Frequest_log"'
       + ' "%(participant_id)s"') % {'participant_id': participant_id, 'project': project},
       '--format', 'json',
       '--freshness', _SERVER_LOG_FRESHNESS,
  ])
  log_data = json.loads(log_json_data)
  formatted_lines = []
  for msg in reversed(log_data):
    payload = msg['protoPayload']
    formatted_lines.append('%(startTime)s %(method)s %(status)d %(resource)s' % payload)
    if (payload['status'] != httplib.OK or
        any([line['severity'] != 'INFO' for line in payload['line']])):
      for line in payload['line']:
        formatted_lines.append('\t%(time)s %(severity)s %(logMessage)s' % line)
  return formatted_lines


if __name__ == '__main__':
  configure_logging()
  parser = get_parser()
  parser.add_argument('participant_id', help='P12345 format participant ID to look up.')
  client = Client(parser=parser)
  log_debug_info(client, client.args.participant_id, _FIXME_PROJECT)
