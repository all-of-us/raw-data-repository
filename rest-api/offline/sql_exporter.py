import contextlib
import csv
import logging

from dao import database_factory
from cloudstorage import cloudstorage_api
from sqlalchemy import text

# Delimiter used in CSVs written (use this when reading them back out).
DELIMITER = ','
_BATCH_SIZE = 1000

class SqlExportFileWriter(object):
  def __init__(self, dest):
    self.writer = csv.writer(dest, delimiter=DELIMITER)

  def writerows(self, results):
    writer.writerows(results)


class SqlExporter(object):
  """Executes a SQL query, fetches results in batches, and writes output to a CSV in GCS."""
  def __init__(self, bucket_name):
    self._bucket_name = bucket_name

  def run_export(self, file_name, sql, query_params=None):
    with self._open_writer(file_name) as writer:
      self.run_export_with_writer(writer)

  def run_export_with_writer(self, writer, sql, query_params):
    with database_factory.get_database().session() as session:
      self.run_export_with_session(writer, session, sql, query_params=query_params)

  def run_export_with_session(self, writer, session, sql, query_params=None):
    # Each query from AppEngine standard environment must finish in 60 seconds.
    # If we start running into trouble with that, we'll either
    # need to break the SQL up into pages, or (more likely) switch to cloud SQL export.
    cursor = session.execute(text(sql), params=query_params)
    try:
      writer.writerow(cursor.keys())
      results = cursor.fetchmany(_BATCH_SIZE)
      while results:
        writer.writerows(results)
        results = cursor.fetchmany(_BATCH_SIZE)
    finally:
      cursor.close()

  @contextlib.contextmanager
  def _open_writer(self, file_name):
    gcs_path = '/%s/%s' % (self._bucket_name, file_name)
    logging.info('Exporting data to %s...', gcs_path)
    with cloudstorage_api.open(gcs_path, mode='w') as dest:
      writer = SqlExportFileWriter(dest)
      yield writer
    logging.info('Export to %s complete.', gcs_path)
