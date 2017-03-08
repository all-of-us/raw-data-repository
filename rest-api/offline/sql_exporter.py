import csv
import logging

from dao import database_factory
from cloudstorage import cloudstorage_api

BATCH_SIZE = 1000

class SqlExporter(object):

  def __init__(self, bucket_name):
    self.database = database_factory.get_database()
    self.bucket_name = bucket_name

  def run_export(self, file_name, sql):
    # Each query from AppEngine standard environment must finish in 60 seconds.
    # If we start running into trouble with that, we'll either
    # need to break the SQL up into pages, or (more likely) switch to cloud SQL export.
    cursor = self.database.get_engine().execute(sql)
    try:
      filename = '/%s/%s' % (self.bucket_name, file_name)
      logging.info('Exporting data to %s...', filename)
      with cloudstorage_api.open(filename, mode='w') as dest:
        writer = csv.writer(dest, delimiter=',')
        writer.writerow(cursor.keys())
        results = cursor.fetchmany(BATCH_SIZE)
        while results:
          writer.writerows(results)
          results = cursor.fetchmany(BATCH_SIZE)
    finally:
      cursor.close()
    logging.info('Export to %s complete.', filename)
