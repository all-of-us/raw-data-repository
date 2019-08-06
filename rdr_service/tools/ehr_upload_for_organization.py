"""
Copies all files under Participant/:id from ptc-uploads-all-of-us-rdr-prod to
awardee bucket with a site subdirectory. i.e. gs://aouxxx/Participant/hpo-site-xxx/P:id
include a "no-site-pairing" subdirectory for thos participants not paired.

Input: csv file with 'pmi_id' and 'paired_site' headers.
Input: awardee bucket for upload. i.e. aouxxx

If you need to create the csv use the following query, export to csv, and add the headers:
select p.participant_id, SUBSTRING_INDEX(s.google_group, '-', -1) from participant p
left join site s on p.site_id = s.site_id
where p.hpo_id = :hpo_id
"""

import csv
import logging
import shlex
import subprocess
from rdr_service.main_util import configure_logging, get_parser

SOURCE_BUCKET = 'ptc-uploads-all-of-us-rdr-prod'


def read_csv(input_file):
  """ Read a csv as dict
      Return participant dict with paired_site """
  participant = {}
  with open(input_file) as csv_file:
    reader = csv.DictReader(csv_file)
    try:
      for row in reader:
        if row['paired_site'] == '' or row['paired_site'] is None:
          # no_site_pairing is a possible bucket name in awardee bucket
          row['paired_site'] = 'no_site_pairing'

        participant[row['pmi_id']] = row['paired_site']
    except KeyError as e:
      print 'Check csv file headers. Error: {}'.format(e)

    return participant


def upload_consents(participant, bucket):
  """ Naively copy (multithreaded) from ptc upload bucket to the awardee bucket under a site name
  (google_group) identifier"""
  for pid, google_group in participant.items():
    gsutil = "gsutil -m cp -r gs://" + SOURCE_BUCKET + "/Participant/P" + str(
      pid) + "/* " + "gs://" + bucket + "/Participant/" + \
             google_group + "/P" + str(pid) + "/"

    run_gsutil(gsutil)


def sync_ehr_consents(csv_file, bucket):
  participant = read_csv(csv_file)
  logging.info('Reading data complete, beginning sync...')
  upload_consents(participant, bucket)


def run_gsutil(gsutil):
  system_call = subprocess.Popen(shlex.split(gsutil))
  system_call.communicate()[0]


if __name__ == '__main__':
  configure_logging()
  parser = get_parser()
  parser.add_argument('--csv_file', help='The csv file to use', required=True)
  parser.add_argument('--input_bucket', help='The awardee bucket to upload consent to',
                      required=True)
  args = parser.parse_args()
  sync_ehr_consents(args.csv_file, args.input_bucket)
