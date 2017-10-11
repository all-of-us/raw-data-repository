from google.appengine.api import app_identity
from google.appengine.ext import deferred
from offline.sql_exporter import SqlExporter
from werkzeug.exceptions import BadRequest

class TableExporter(object):
  """API that exports data from our database to UTF-8 CSV files in GCS.

  Used instead of Cloud SQL export because it handles newlines and null characters in a way that
  other CSV clients (e.g. BigQuery, Google Sheets) can actually understand.
  """

  @classmethod
  def _export_csv(cls, bucket_name, database, directory, table_name):
    SqlExporter(bucket_name, use_unicode=True).run_export('%s/%s.csv' % (directory, table_name),
                                                          'SELECT * FROM %s.%s' %
                                                          (database, table_name))

  @staticmethod
  def export_tables(database, tables, directory):
    app_id = app_identity.get_application_id()
    # Determine what GCS bucket to write to based on the environment and database.
    if app_id == "None":
      bucket_name = app_identity.get_default_gcs_bucket_name()
    elif database == 'rdr':
      bucket_name = '%s-rdr-export' % app_id
    elif database == 'cdm' or database == 'voc':
      bucket_name = '%s-cdm' % app_id
    else:
      raise BadRequest("Invalid database: %s" % database)
    for table_name in tables:
      deferred.defer(TableExporter._export_csv, bucket_name, database, directory, table_name)
    return {'destination': 'gs://%s/%s' % (bucket_name, directory)}