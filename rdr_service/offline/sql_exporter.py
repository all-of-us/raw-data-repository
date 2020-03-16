import contextlib
import csv
import logging
import tempfile
from sqlalchemy import text

from rdr_service.api_util import open_cloud_file
from rdr_service.dao import database_factory

# Delimiter used in CSVs written (use this when reading them back out).
DELIMITER = ","
_BATCH_SIZE = 1000


class SqlExportFileWriter(object):
    """Writes rows to a CSV file, optionally filtering on a predicate."""

    def __init__(self, dest, predicate=None):
        self._writer = csv.writer(dest, delimiter=DELIMITER)
        self._predicate = predicate

    def write_header(self, keys):
        self._writer.writerow(keys)

    def write_rows(self, results):
        if self._predicate:
            results = [result for result in results if self._predicate(result)]
        if results:
            self._writer.writerows(results)


class SqlExporter(object):
    """Executes a SQL query, fetches results in batches, and writes output to a CSV in GCS."""

    def __init__(self, bucket_name):
        self._bucket_name = bucket_name


    def run_export(self, file_name, sql, query_params=None, backup=False, transformf=None, instance_name=None, predicate=None):
        with tempfile.NamedTemporaryFile(mode='w+') as tmp_file:
            writer = SqlExportFileWriter(tmp_file, predicate=predicate)
            # write data to temp file
            self.run_export_with_writer(
                writer, sql, query_params, backup=backup, transformf=transformf, instance_name=instance_name
            )
            tmp_file.seek(0)
            gcs_path = "/%s/%s" % (self._bucket_name, file_name)
            # Logging does not expand in GCloud, so I'm trying this out.
            message = f"Exporting data to {gcs_path}"
            logging.info(message)
            with open_cloud_file(gcs_path, mode='w') as cloud_file:
                data = tmp_file.read(4096)
                while data:
                    cloud_file.write(data)
                    data = tmp_file.readlines(4096)
            #     tmp_file.write(cloud_file)
            #     tmp_file.seek(0)
            #     while data:
            #         # write to the bucket
            #         cloud_file.write_rows(data)
            #         data = tmp_file.read(4096)
            #
            #     tmp_file.seek(0)
            #     self.run_export_with_writer(
            #         cloud_file, sql, query_params, backup=backup, transformf=transformf, instance_name=instance_name
            #     )

    def run_export_with_writer(self, writer, sql, query_params, backup=False, transformf=None, instance_name=None):
        with database_factory.make_server_cursor_database(backup, instance_name).session() as session:
            self.run_export_with_session(writer, session, sql, query_params=query_params, transformf=transformf)

    def run_export_with_session(self, writer, session, sql, query_params=None, transformf=None):
        # Each query from AppEngine standard environment must finish in 60 seconds.
        # If we start running into trouble with that, we'll either
        # need to break the SQL up into pages, or (more likely) switch to cloud SQL export.
        cursor = session.execute(text(sql), params=query_params)
        try:
            fields = list(cursor.keys())
            writer.write_header(fields)
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

    # @contextlib.contextmanager
    # def open_cloud_writer(self, file_name, predicate=None):
    #     gcs_path = "/%s/%s" % (self._bucket_name, file_name)
    #     # Logging does not expand in GCloud, so I'm trying this out.
    #     message = f"Exporting data to {gcs_path}"
    #     logging.info(message)
    #     with open_cloud_file(gcs_path, mode='w') as dest:
    #         writer = SqlExportFileWriter(dest, predicate)
    #         yield writer
    #         message = f"Export to {gcs_path} complete."
    #         logging.info(message)
