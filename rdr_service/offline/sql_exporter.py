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

    def run_export(self, file_name, sql, query_params=None, backup=False, transformf=None, instance_name=None,
                   predicate=None):
        tmp_file_name = self.write_temp_export_file(sql, query_params, backup, transformf, instance_name, predicate)
        self.upload_export_file(tmp_file_name, file_name, predicate)

    def write_temp_export_file(self, sql, query_params, backup, transformf, instance_name, predicate):
        with tempfile.NamedTemporaryFile(mode='w+', delete=False) as tmp_file:
            tmp_file_name = tmp_file.name

            logging.info(f"Exporting to temporary file: {tmp_file_name}")

            sql_writer = SqlExportFileWriter(tmp_file, predicate=predicate)
            # write data to temp file
            self.run_export_with_writer(
                sql_writer, sql, query_params, backup=backup, transformf=transformf, instance_name=instance_name
            )

        return tmp_file_name

    def upload_export_file(self, tmp_file_name, file_name, predicate):
        logging.info(f"Opening {tmp_file_name} for export.")
        with open(tmp_file_name) as tmp_file:
            csv_reader = csv.reader(tmp_file)

            logging.info("Exporting temp file with cloud writer...")
            with self.open_cloud_writer(file_name, predicate) as cloud_writer:
                headers = next(csv_reader)
                cloud_writer.write_header(headers)

                logging.info("Writing rows to cloud file.")
                for row in csv_reader:
                    cloud_writer.write_rows([row])

    def run_export_with_writer(self, writer, sql, query_params, backup=False, transformf=None, instance_name=None):
        with database_factory.make_server_cursor_database(backup, instance_name).session() as session:
            self.run_export_with_session(writer, session, sql, query_params=query_params, transformf=transformf)

    def run_export_with_session(self, writer, session, sql, query_params=None, transformf=None):
        if isinstance(sql, str):
            cursor = session.execute(text(sql), params=query_params)
        else:
            if query_params is not None:
                # This function is embedded in a few layers, adding this as protection and to help in understanding
                # the use of the function
                raise Exception('Unexpected query_params when using Sqlalchemy query')
            cursor = session.execute(sql)

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

    @contextlib.contextmanager
    def open_cloud_writer(self, file_name, predicate=None):
        gcs_path = "/%s/%s" % (self._bucket_name, file_name)
        # Logging does not expand in GCloud, so I'm trying this out.
        message = f"Exporting data to {gcs_path}"
        logging.info(message)
        with open_cloud_file(gcs_path, mode='w') as dest:
            writer = SqlExportFileWriter(dest, predicate)
            yield writer
            message = f"Export to {gcs_path} complete."
            logging.info(message)
