import logging
import pprint
from main_util import get_parser, configure_logging

from client import Client, HttpException, client_log


def get_meta_info(client, participant_id):
  response = client.request_json('Participant/{}'.format(participant_id))
  etag = response['meta']
  pprint.pprint(response)
  return etag


def get_request_body():
  request_body = {
    "withdrawalStatus": "NO_USE"
  }
  return request_body


def main(client):
  reason = client.args.withdrawal_reason
  justify = client.args.withdrawal_justification
  participants = client.args.participants
  print participants, "<<<<<<<<<<<<"
  print type(participants), '<<< type'
  request_body = get_request_body()

  for participant in participants:
    path = 'Participant/' + str(participant)
    etag = get_meta_info(client, participant)
    response = client.request_json(path, 'PUT', body=request_body, headers=etag)
    logging.info(pprint.pformat(response))

    participant_id = response['participantId']
    # Fetch that participant and print it out.
    response = client.request_json('Participant/{}'.format(participant_id))
    logging.info(pprint.pformat(response))


if __name__ == '__main__':
  configure_logging()
  client_log.setLevel(logging.WARN)  # Suppress the log of HTTP requests.
  arg_parser = get_parser()
  arg_parser.add_argument('--dry_run', action='store_true')
  arg_parser.add_argument('--withdrawal_reason', help='withdrawal reason is one of [fraudulent | '
                                                      'duplicate | test]', required=True)
  arg_parser.add_argument('--withdrawal_justification', help='A string justification for '
                                                             'withdrawal', required=True, nargs='+')
  arg_parser.add_argument('--participants', help='List of participants to withdrawal, all having '
                                                 'the same reason and justification',
                           required=True, nargs='+')
  main(Client(parser=arg_parser))
