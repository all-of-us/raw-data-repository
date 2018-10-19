# Exports the entire contents of database tables to Unicode CSV files stored in GCS.
# Used instead of Cloud SQL export because it handles newlines and null characters properly.
#
# Documentation of permissions management:
# https://docs.google.com/document/d/1vKiu2zcSy97DQTIuSezr030kTyeDthome9XzNy98B6M
#
# Usage: ./run_client.sh --project <PROJECT> --account <ACCOUNT> \
# --service_account exporter@<PROJECT>.iam.gserviceaccount.com export_tables.py \
# --database rdr --tables code,participant --directory test_directory
# [--instance_name <INSTANCE_NAME>]
#
# "directory" indicates a directory inside the GCS bucket to write the files to
#
# If "rdr" is chosen for the database, the data will be written to <ENVIRONMENT>-rdr-export;
# If "cdm" or "voc" are chosen, the data will be written to <ENVIRONMENT>-cdm.
# If specified, "instance_name" must be the name of a database instance that
# the RDR has access to with the same credentials as the main instance.
# (This is used for exporting ETL results from clones of the main RDR instance.)

import logging

from client import Client
from main_util import get_parser, configure_logging

def export_tables(client):
  table_names = client.args.tables.split(',')
  logging.info('Exporting %s from %s to %s' % (table_names, client.args.database,
                                               client.args.directory))
  request_body = {'database': client.args.database,
                  'tables': table_names,
                  'directory': client.args.directory,
                  'deidentify': client.args.deidentify
  }
  if client.args.instance_name:
    request_body['instance_name'] = client.args.instance_name
  response = client.request_json('ExportTables', 'POST', request_body)
  logging.info('Data is being exported to: %s' % response['destination'])

if __name__ == '__main__':
  configure_logging()
  parser = get_parser()
  parser.add_argument('--database', help='The database to export data from', required=True)
  parser.add_argument('--tables', help='A comma-separated list of tables to export',
                      required=True)
  parser.add_argument('--directory',
                      help='A directory to write CSV output to inside the GCS bucket',
                      required=True)
  parser.add_argument('--deidentify', help='Whether to deidentify the exports',
                      action='store_true')
  parser.add_argument('--instance_name',
                      help='Name of the instance to read data from; defaults to rdrmaindb')
  export_tables(Client(parser=parser, base_path='offline'))
