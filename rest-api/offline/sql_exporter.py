import contextlib
import csv
import logging

from dao import database_factory
from cloudstorage import cloudstorage_api
from sqlalchemy import text
from unicode_csv import UnicodeWriter

# Delimiter used in CSVs written (use this when reading them back out).
DELIMITER = ','
_BATCH_SIZE = 1000

class SqlExportFileWriter(object):
  """Writes rows to a CSV file, optionally filtering on a predicate."""
  def __init__(self, dest, predicate=None, use_unicode=False):
    if use_unicode:
      self._writer = UnicodeWriter(dest, delimiter=DELIMITER)
    else:
      self._writer = csv.writer(dest, delimiter=DELIMITER)
    self._predicate = predicate

  def write_header(self, keys):
    self._writer.writerow(keys)

  def write_rows(self, results):
    if self._predicate:
      results = [result for result in results if self._predicate(result)]
    if results:
      self._writer.writerows(results)

class CompositeSqlExportWriter(object):

  def __init__(self, writers):
    self._writers = writers

  def write_header(self, keys):
    for writer in self._writers:
      writer.write_header(keys)

  def write_rows(self, results):
    for writer in self._writers:
      writer.write_rows(results)

class SqlExporter(object):
  """Executes a SQL query, fetches results in batches, and writes output to a CSV in GCS."""
  def __init__(self, bucket_name, use_unicode=False):
    self._bucket_name = bucket_name
    self._use_unicode = use_unicode

  def run_export(self, file_name, sql, query_params=None, backup=False, transformf=None,
    db_connection_string=None):
    with self.open_writer(file_name) as writer:
      self.run_export_with_writer(writer, sql, query_params, backup=backup, transformf=transformf,
                                  db_connection_string=db_connection_string)

  def run_export_with_writer(self, writer, sql, query_params, backup=False, transformf=None,
    db_connection_string=None):
    with database_factory.make_server_cursor_database(backup,
                                                      db_connection_string).session() as session:
      self.run_export_with_session(writer, session, sql,
                                   query_params=query_params, transformf=transformf)

  def run_export_with_session(self, writer, session, sql, query_params=None,
                              transformf=None):
    # Each query from AppEngine standard environment must finish in 60 seconds.
    # If we start running into trouble with that, we'll either
    # need to break the SQL up into pages, or (more likely) switch to cloud SQL export.
    cursor = session.execute(text(sql), params=query_params)
    try:
      writer.write_header(cursor.keys())
      results = cursor.fetchmany(_BATCH_SIZE)
      while results:
        if transformf:
          # Note: transformf accepts an iterable and returns an iterable, the output of this call
          # may no longer be a row proxy after this point.
          results = [transformf(r) for r in results]
        writer.write_rows(results)
        results = cursor.fetchmany(_BATCH_SIZE)
    finally:
      cursor.close()

  @contextlib.contextmanager
  def open_writer(self, file_name, predicate=None):
    gcs_path = '/%s/%s' % (self._bucket_name, file_name)
    logging.info('Exporting data to %s...', gcs_path)
    with cloudstorage_api.open(gcs_path, mode='w') as dest:
      writer = SqlExportFileWriter(dest, predicate, use_unicode=self._use_unicode)
      yield writer
    logging.info('Export to %s complete.', gcs_path)
