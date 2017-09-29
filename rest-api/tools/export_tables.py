"""Uses Cloud SQL export API to export the contents of one more tables to CSV files.
"""

import logging
import sys
import MySQLdb

from time import sleep
from main_util import get_parser, configure_logging
from googleapiclient import discovery
from oauth2client.service_account import ServiceAccountCredentials

_SCOPE = "https://www.googleapis.com/auth/cloud-platform"
_INSTANCE = "rdrmaindb"

def main(args):
  credentials = ServiceAccountCredentials.from_json_keyfile_name(args.creds_file, [_SCOPE])
  service = discovery.build('sqladmin', 'v1beta4', credentials=credentials)
  table_names = args.tables.split(',')
  for table in table_names:
    uri = args.output_path + '/%s.csv' % table
    logging.info("Exporting %s.%s to %s..." % (args.database, table, uri))
    request_body = {
      "exportContext": {
        "kind": "sql#exportContext",
        "fileType": "CSV",
        "uri": uri,
        "databases": [
          args.database
        ],
        "csvExportOptions": {
          "selectQuery": "select * from %s" % MySQLdb.escape_string(table)
        }
      }
    }
    request = service.instances().export(project=args.project, instance=_INSTANCE,
                                       body=request_body)
    response = request.execute()
    status = response['status']
    if status != 'DONE':
      logging.info("Waiting for export of %s to complete..." % table)
      while status != 'DONE':
        sleep(1)
        request = service.operations().get(project=args.project, operation=response['name'])
        response = request.execute()
        status = response['status']
    if response.get('error'):
      logging.error("Errors in response: " % response)
      sys.exit(-1)
    else:
      logging.info('Done exporting %s.' % table)

if __name__ == '__main__':
  configure_logging()
  parser = get_parser()
  parser.add_argument('--project',
                      help='Project to export data for',
                      required=True)
  parser.add_argument('--creds_file',
                      help='Path to credentials JSON file.',
                      required=True)
  parser.add_argument('--output_path',
                      help='GCS path to write the output files to.',
                      required=True)
  parser.add_argument('--database',
                      help='Name of the database containing the tables to export.',
                      required=True)
  parser.add_argument('--tables',
                      help=('Comma separated list of table names to export; ' +
                            ' all columns will be exported, in the order they are defined.'),
                      required=True)
  main(parser.parse_args())
