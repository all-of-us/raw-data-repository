import contextlib
import csv
import logging

from dao import database_factory
from cloudstorage import cloudstorage_api
from sqlalchemy import text

DELIMITER = ','
_BATCH_SIZE = 1000


class SqlExporter(object):
  def __init__(self, bucket_name):
    self._bucket_name = bucket_name

  def run_export(self, file_name, sql, query_params=None):
    with database_factory.get_database().session() as session:
      self.run_export_with_session(file_name, session, sql, query_params=query_params)

  def run_export_with_session(self, file_name, session, sql, query_params=None):
    # Each query from AppEngine standard environment must finish in 60 seconds.
    # If we start running into trouble with that, we'll either
    # need to break the SQL up into pages, or (more likely) switch to cloud SQL export.
    cursor = session.execute(text(sql), params=query_params)
    try:
      gcs_path = '/%s/%s' % (self._bucket_name, file_name)
      logging.info('Exporting data to %s...', gcs_path)
      with self._open_writer(gcs_path) as writer:
        writer.writerow(cursor.keys())
        results = cursor.fetchmany(_BATCH_SIZE)
        while results:
          writer.writerows(results)
          results = cursor.fetchmany(_BATCH_SIZE)
    finally:
      cursor.close()
    logging.info('Export to %s complete.', gcs_path)

  @contextlib.contextmanager
  def _open_writer(self, gcs_path):
    with cloudstorage_api.open(gcs_path, mode='w') as dest:
      writer = csv.writer(dest, delimiter=DELIMITER)
      yield writer
