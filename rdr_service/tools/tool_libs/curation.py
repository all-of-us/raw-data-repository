#! /bin/env python
#
# Template for RDR tool python program.
#

import argparse
import logging
import sys

from rdr_service.services.gcp_utils import gcp_sql_export_csv
from rdr_service.services.system_utils import setup_logging, setup_i18n
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
    tables = ['pid_rid_mapping', 'care_site', 'condition_era', 'condition_occurrence', 'cost', 'death',
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

    def export_table(self, table):
        """
        Export table to cloud bucket
        :param table: Table name
        :return:
        """
        cloud_file = f'gs://{self.args.export_path}/{table}.csv'

        # We have to add a row at the start for the CSV headers, Google hasn't implemented another way yet
        # https://issuetracker.google.com/issues/111342008
        column_names = self.get_field_names(table, ['id'])
        header_string = ','.join([f"'{column_name}'" for column_name in column_names])

        # We need to handle NULLs and convert them to empty strings as gcloud sql has a bug when putting them in a csv
        # https://issuetracker.google.com/issues/64579566
        # NULL characters (\0) can also corrupt the output file, so they're removed.
        # And whitespace was trimmed before so that's moved into the SQL as well
        # Newlines and double-quotes are also replaced with spaces and single-quotes, respectively
        field_list = [f"TRIM(REPLACE(REPLACE(REPLACE(COALESCE({name}, ''), '\\0', ''), '\n', ' '), '\\\"', '\\\''))"
                      for name in column_names]

        # Unions are unordered, so the headers do not always end up at the top of the file.
        # The below format forces the headers to the top of the file
        # This is needed because gcloud export sql doesn't support column headers and
        # Curation would like them in the file for schema validation (ROC-687)
        sql_string = f"""
            SELECT {','.join(column_names)}
            FROM 
            (
                (
                    SELECT
                      1 as sort_col,
                      {header_string}
                ) 
                UNION ALL
                (
                    SELECT 2,
                        {','.join(field_list)}
                    FROM {table}
                )                
            ) a
            ORDER BY a.sort_col ASC
        """

        _logger.info(f'exporting {table}')
        gcp_sql_export_csv(
            self.args.project,
            sql_string,
            cloud_file,
            database='cdm'
        )

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
