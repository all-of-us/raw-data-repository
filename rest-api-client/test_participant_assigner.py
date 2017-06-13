"""Assigns participants with the specified IDs to the test HPO."""

import argparse
import csv
from client.client import Client, client_log


def main(parser):
  client = Client('rdr/v1', parser=parser)
  num_updates = 0
  with open(client.args.file) as csvfile:
    reader = csv.reader(csvfile)
    for line in reader:
      participant_id = line[0].strip()
      if participant_id:
        client_participant_id = 'P{}'.format(participant_id)
        participant = client.request_json('Participant/{}'.format(client_participant_id))    
        participant['providerLink'] =  [{
            'primary': True,
            'organization': {
              'reference': 'Organization/TEST'
            }
          }]      
        client.request_json('Participant/{}'.format(client_participant_id), 'PUT', participant,
                            headers = {'If-Match': client.last_etag})
        num_updates += 1
  client_log.info('Updated %d participants.', num_updates)

if __name__ == '__main__':  
  parser = argparse.ArgumentParser()
  parser.add_argument('--file', help='File containing the list of participant IDs',
                      required=True)
  main(parser)

