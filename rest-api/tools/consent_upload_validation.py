""" read from csv (assumes participant id column header is [pmi_id] and paired site header is
    [paired_site]).
     Checks PTC-UPLOADS-ALL-OF-US-RDR-PROD bucket and the bucket given in args (e.g. aouXXX)
     Compares PTC uploaded files (consent pdf's and signature png's) and files located in awardee
     bucket, which should be identical. Then writes two csv files for each bucket. One for missing
     files and one for files found in the buckets.
     aou buckets have a different hierarchy than ptc-upload bucket. They are of the type:
     gs://aouXXX/Participant/hpo-site-vapaloalto/Participant
     It is assumed the paired_site csv columns are of the format [vapaloalto] without the
     <hpo-site->
"""
import csv
import datetime
import os
import re
import shlex
import subprocess

from main_util import get_parser, configure_logging


base_bucket = 'ptc-uploads-all-of-us-rdr-prod'
required_files = {'ConsentPII.pdf', 'EHRConsentPII.pdf', 'ConsentPII.png', 'EHRConsentPII.png'}


def read_csv(input_file):
  participant = {}
  with open(input_file, 'r') as csv_file:
    try:
      reader = csv.DictReader(csv_file)
    except UnicodeDecodeError:
      print 'Decode Error'

    try:
      for row in reader:
        if row['paired_site'] == 'NULL' or row['paired_site'] is None:
          # no_site_pairing is a possible bucket name in awardee bucket
          row['paired_site'] = 'no_site_pairing'

        participant[row['pmi_id']] = row['paired_site']
    except KeyError as e:
      print 'Check csv file headers. Error: {}'.format(e)

    return participant


def get_bucket_file_info(participant_ids, bucket, p_dict=None):
  participant_files = {}
  for _id in participant_ids:
    output_list = []
    if bucket == base_bucket:
      gsutil_command = shlex.split(str("gsutil ls gs://" + bucket + '/Participant/' + _id))
    else:
      gsutil_command = shlex.split(str("gsutil ls gs://" + bucket + '/Participant/' +
                                       'hpo-site-' + p_dict[_id] + '/' + _id))
    try:
      output = subprocess.check_output(gsutil_command)
      output_list.extend(output.split())
    except subprocess.CalledProcessError:
      print 'Skipping participant {}: Directory does not exist.'.format(_id)

    participant_files[_id] = output_list

  return participant_files


def _strip_path(f):
  return os.path.basename(f)


def _sanitize_versions(f):
  return re.sub(r'__\d+', '', f)


def remove_path_and_version_info(participant_files):
  for _id, files in participant_files.items():
    strip = map(_strip_path, files)
    no_versions = map(_sanitize_versions, strip)
    participant_files[_id] = no_versions


def get_missing_file_info(participant_files):
  for _id, files in participant_files.items():
    files = set(files)
    missing_files = required_files.difference(files)
    participant_files[_id] = {'files_found': list(files), 'missing_files': list(missing_files)}
    print 'Missing Files for participant {}: {}'.format(_id, list(missing_files))


def write_to_csv(participant_files, descriptor):
  missing_fields = ['pmi_id', 'missing files']
  found_fields = ['pmi_id', 'files found']
  missing_filename = 'missing_files_' + descriptor + '_' + str(datetime.date.today()) + '.csv'
  existing_filename = 'existing_files_' + descriptor + '_' + str(datetime.date.today()) + '.csv'
  print 'Creating csv files...'
  with open(missing_filename, 'w') as missing:
    with open(existing_filename, 'w') as found:
      missing_writer = csv.writer(missing)
      found_writer = csv.writer(found)
      missing_writer.writerow(missing_fields)
      found_writer.writerow(found_fields)

      for _id, files in participant_files.items():
        missing_files = files['missing_files']
        existing_files = files['files_found']
        missing_files.insert(0, _id)
        existing_files.insert(0, _id)
        if len(missing_files) > 1:
          missing_writer.writerow(missing_files)
        if len(existing_files) > 1:
          found_writer.writerow(existing_files)

  print 'Created files: {} | {}'.format(missing_filename, existing_filename)


def main(args):
  participant_dict = read_csv(args.input)
  participant_ids = list(participant_dict.keys())

  participant_files_base_bucket = get_bucket_file_info(participant_ids, base_bucket)
  participant_files_awardee_bucket = get_bucket_file_info(participant_ids, args.bucket,
                                                          participant_dict)

  for files in [participant_files_base_bucket, participant_files_awardee_bucket]:
    # strip path and the [__123] version indicator from filename to facilitate comparison.
    # @TODO[MM]: WE MAY NEED TO KEEP THE VERSION, AS THIS MIGHT MATTER IN SOME CASES.
    remove_path_and_version_info(files)
    get_missing_file_info(files)

  write_to_csv(participant_files_base_bucket, descriptor=base_bucket)
  write_to_csv(participant_files_awardee_bucket, descriptor=args.bucket)


if __name__ == '__main__':
  configure_logging()
  parser = get_parser()
  parser.add_argument('--bucket', help='The source bucket to check against. e.g. aouXXX',
                      required=True)
  parser.add_argument('--input', help='Path to input csv. The csv should at a minimum contain'
                                      'pmi_id and paired_site as headers. pmi_id should have '
                                      'environment indicator as follows [P12345] in column.'
                                      , required=True)

  main(parser.parse_args())
