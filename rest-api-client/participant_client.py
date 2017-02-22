"""Simple client demonstrating how to create and retrieve a participant"""

import pprint


def main():
  client = Client('rdr/v1')

  response = client.request_json('Participant', 'POST')
  pprint.pprint(response)

  participant_id = response['participantId']
  # Fetch that participant and print it out.
  response = client.request_json('Participant/{}'.format(participant_id))
  pprint.pprint(response)


if __name__ == '__main__':
  main()
