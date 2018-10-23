""" read from csv (assumes participant id column header is [pmi_id]). """
import csv
import os
import re
import shlex
import subprocess

from main_util import get_parser, configure_logging


base_bucket = 'ptc-uploads-all-of-us-rdr-prod'
required_files = {'ConsentPII.pdf', 'EHRConsentPII.pdf', 'ConsentPII.png', 'EHRConsentPII.png'}


def read_csv(input_file):
 participant_list = []
 with open(input_file, 'r') as csv_file:
   try:
     reader = csv.DictReader(csv_file)
   except UnicodeDecodeError:
     print('Decode Error')

   for row in reader:
     participant_list.append(row['pmi_id'])

   return participant_list


def get_bucket_file_info(participant_ids):
  participant_files = {}
  for _id in participant_ids[:3]:  #@TODO: REMOVE THE INDEXING
    output_list = []
    print('checking Participant: {}'.format(_id))
    gsutil_command = shlex.split(str("gsutil ls gs://" + base_bucket + '/Participant/' + _id))

    try:
      output = subprocess.check_output(gsutil_command)
      output_list.extend(output.split())
    except subprocess.CalledProcessError:
      print 'skipping {}: File not found.'.format(_id)

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
    print('Missing Files for participant {}: {}'.format(_id, list(missing_files)), '\n')


def write_to_csv(participant_files):
  missing_fields = ['pmi_id', 'missing files']
  found_fields = ['pmi_id', 'files found']
  with open('missing_files.csv', 'w') as missing:
    with open('files_found.csv', 'w') as found:
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


def main(args):
  participant_ids = read_csv(args.input)
  participant_files = get_bucket_file_info(participant_ids)
  remove_path_and_version_info(participant_files)
  get_missing_file_info(participant_files)
  write_to_csv(participant_files)
  print participant_files


if __name__ == '__main__':
  configure_logging()
  parser = get_parser()
  parser.add_argument('--bucket', help='The source bucket to check against. i.e. aouXXX',
                      required=True)
  parser.add_argument('--input', help='Path to input csv', required=True)

  main(parser.parse_args())
