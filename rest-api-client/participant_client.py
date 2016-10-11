"""Simple client demonstrating how to create and retrieve a participant"""

import pprint

from client.client import Client, HttpException

def main():
  client = Client('participant/v1')

  first_name = 'Mister'
  last_name = 'Pants'
  date_of_birth = '1975-08-21'

  # Create a new participant.
  participant = {
      'first_name': first_name,
      'last_name': last_name,
      'date_of_birth': date_of_birth,
  }

  response = client.request_json('participants', 'POST', participant)
  pprint.pprint(response)

  participant_id = response['participant_id']
  # Fetch that participant and print it out.
  response = client.request_json('participants/{}'.format(participant_id))
  pprint.pprint(response)


if __name__ == '__main__':
  main()
