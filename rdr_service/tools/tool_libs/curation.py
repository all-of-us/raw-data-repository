#! /bin/env python
#
# Template for RDR tool python program.
#

import argparse
import csv
import logging
import sys

from rdr_service.services.system_utils import setup_logging, setup_i18n, print_progress_bar
from rdr_service.storage import GoogleCloudStorageProvider
from rdr_service.tools.tool_libs import GCPProcessContext, GCPEnvConfigObject

_logger = logging.getLogger("rdr_logger")

# Tool_cmd and tool_desc name are required.
# Remember to add/update bash completion in 'tool_lib/tools.bash'
tool_cmd = "curation"
tool_desc = "Support tool for Curation ETL process"


EXPORT_BATCH_SIZE = 10000

# TODO: Rewrite the Curation ETL bash scripts into multiple Classes here.

class CurationExportClass(object):
    """
    Export the data from the Curation ETL process.
    """
    tables = ['care_site', 'condition_era', 'condition_occurrence', 'cost', 'death',
              'device_exposure', 'dose_era', 'drug_era', 'drug_exposure', 'fact_relationship',
              'location', 'measurement', 'observation', 'observation_period', 'payer_plan_period',
              'person', 'procedure_occurrence', 'provider', 'visit_occurrence']

    db_conn = None

    def __init__(self, args, gcp_env: GCPEnvConfigObject):
        """
        :param args: command line arguments.
        :param gcp_env: gcp environment information, see: gcp_initialize().
        """
        self.args = args
        self.gcp_env = gcp_env

    def get_table_count(self, table):
        """
        Get the number of records in the table.
        :param table: table name.
        :return: integer
        """
        cursor = self.db_conn.cursor()
        cursor.execute(f"select count(1) from cdm.{table}")
        total = cursor.fetchone()[0]
        cursor.close()
        return total

    def run_query(self, sql):
        """
        Run a query and return results
        :param sql: sql statement to execute
        :return: results from query
        """
        cursor = self.db_conn.cursor()
        cursor.execute(sql)
        results = cursor.fetchall()
        cursor.close()
        return results

    def get_field_names(self, table, exclude=None):
        """
        Run a query and get the field list.
        :param table: table name
        :param exclude: list of excluded fields names.
        :return: list of field names
        """
        if not exclude:
            exclude = []
        cursor = self.db_conn.cursor()
        cursor.execute(f"select * from cdm.{table} limit 1")
        cursor.fetchone()
        fields = [f[0] for f in cursor.description if f[0] not in exclude]
        cursor.close()
        return fields

    def clean_data(self, data):
        """
        Clean the data field if string
        :param data: data value
        :return: cleaned data value.
        """
        if isinstance(data, str):
            # We found some records in the Observation table that had multiple "\0" characters in them.
            return data.replace('\0', '').strip()
        return data

    def export_table(self, table):
        """
        Export table to cloud bucket
        :param table: Table name
        :return:
        """
        cloud_file = f'{self.args.export_path}/{table}.csv'

        total = self.get_table_count(table)
        fields = self.get_field_names(table, ['id'])
        field_list = ', '.join(fields)
        count = 0
        storage_provider = GoogleCloudStorageProvider()

        sql = f'SELECT {field_list} FROM {table}'

        with storage_provider.open(cloud_file, 'wt') as h:

            # Write out the header row of the CSV file.
            writer = csv.DictWriter(h, fieldnames=fields, quoting=csv.QUOTE_NONNUMERIC)
            writer.writeheader()
            # If no data records, just return.
            if total == 0:
                _logger.info(f' {table:20} : 0/0 100.0% complete')
                return

            _logger.info(f'executing sql for table {table}')
            cursor = self.db_conn.cursor()
            cursor.execute(sql)
            results = cursor.fetchmany(size=EXPORT_BATCH_SIZE)
            while results:
                for row in results:
                    # merge field names and data into dict.
                    data_dict = dict(zip(fields, [self.clean_data(d) for d in row]))
                    writer.writerow(data_dict)
                    count += 1

                results = cursor.fetchmany(size=EXPORT_BATCH_SIZE)

                print_progress_bar(
                    count, total, prefix=f" {table:20} : {count}/{total}:", suffix="complete", bar_length=60)

            cursor.close()
            _logger.info(f'uploading {table} export to GCS bucket')

    def run(self):
        """
        Main program process
        :return: Exit code value
        """
        self.gcp_env.activate_sql_proxy(replica=True)
        # Because there are no models for the data stored in the 'cdm' database, we'll
        # just use a standard MySQLDB connection.
        self.db_conn = self.gcp_env.make_mysqldb_connection(user='alembic', database='cdm')

        if self.args.table:
            _logger.info(f"Exporting {self.args.table} to {self.args.export_path}...")
            self.export_table(self.args.table)
            return 0

        _logger.info(f"Exporting tables to {self.args.export_path}...")
        for table in self.tables:
            self.export_table(table)

        return 0


def run():
    # Set global debug value and setup application logging.
    setup_logging(
        _logger, tool_cmd, "--debug" in sys.argv, "{0}.log".format(tool_cmd) if "--log-file" in sys.argv else None
    )
    setup_i18n()

    # Setup program arguments.
    parser = argparse.ArgumentParser(prog=tool_cmd, description=tool_desc)
    parser.add_argument("--debug", help="enable debug output", default=False, action="store_true")  # noqa
    parser.add_argument("--log-file", help="write output to a log file", default=False, action="store_true")  # noqa
    parser.add_argument("--project", help="gcp project name", default="localhost")  # noqa
    parser.add_argument("--account", help="pmi-ops account", default=None)  # noqa
    parser.add_argument("--service-account", help="gcp iam service account", default=None)  # noqa
    parser.add_argument("--export-path", help="Bucket path to export to", required=True, type=str)  # noqa
    parser.add_argument("--table", help="Export a specific table", type=str, default=None)  # noqa
    args = parser.parse_args()

    if not args.export_path.startswith('gs://all-of-us-rdr-prod-cdm/'):
        raise NameError("Export path must start with 'gs://all-of-us-rdr-prod-cdm/'.")

    if args.export_path.endswith('/'):  # Remove trailing slash if present.
        args.export_path = args.export_path[5:-1]

    with GCPProcessContext(tool_cmd, args.project, args.account, args.service_account) as gcp_env:
        process = CurationExportClass(args, gcp_env)
        exit_code = process.run()
        return exit_code


# --- Main Program Call ---
if __name__ == "__main__":
    sys.exit(run())
