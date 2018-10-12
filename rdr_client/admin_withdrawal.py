"""
Used to administratively withdrawal a list of participants (all with the same withdrawal reason
and justification. i.e. n list of participants are registered as TEST participants
param: withdrawalreason must be one of FRAUDULENT | TEST | DUPLICATE.
param: withdrawalreasonjustification is a string explanation.
run with run_client e.g.
$run_client.sh --project <PROJECT> --account <ACCOUNT> [--service_account <ACCOUNT>]
admin_withdrawal.py --withdrawal_reason <FRAUDULENT, DUPLICATE, TEST> --participants <P1000, P1001>
"""
import logging
import pprint

from main_util import get_parser, configure_logging

from client import Client, client_log


def make_request_body(client, participant_id):
  reason_list = ['TEST', 'FRAUDULENT', 'DUPLICATE']

  reason = client.args.withdrawal_reason
  justify = " ".join(client.args.withdrawal_justification)
  response = client.request_json('Participant/{}'.format(participant_id))

  if reason not in reason_list:
    raise ValueError('withdrawalReason must be one of {}'.format(reason_list))

  response['withdrawalStatus'] = 'NO_USE'
  response['withdrawalReason'] = reason
  response['withdrawalReasonJustification'] = justify

  return response


def main(client):
  participants = client.args.participants

  for participant in participants:
    path = 'Participant/' + str(participant)
    request_body = make_request_body(client, participant)
    pprint.pformat(request_body)
    if not client.args.dry_run:
      response = client.request_json(path, 'PUT', request_body,
                                     headers={'If-Match': client.last_etag})
      logging.info('\n Participant: {} withdrawn. New info: \n {}'.format(response['participantId'],
                                                                   pprint.pformat(response)))
    else:
      logging.info('Request that would be sent for participant {}: \n {} '.format(
                    participant, pprint.pformat(request_body)))


if __name__ == '__main__':
  configure_logging()
  client_log.setLevel(logging.WARN)  # Suppress the log of HTTP requests.
  arg_parser = get_parser()
  arg_parser.add_argument('--dry_run', action='store_true')
  arg_parser.add_argument('--withdrawal_reason', help='withdrawal reason is one of [fraudulent | '
                                                      'duplicate | test]', required=True)
  arg_parser.add_argument('--withdrawal_justification', help='A string justification for '
                                                             'withdrawal', required=True, nargs='+')
  arg_parser.add_argument('--participants', help='Participants to withdrawal, all having '
                                                 'the same reason and justification. Seperated by '
                                                 'spaces', required=True, nargs='+')
  main(Client(parser=arg_parser))
