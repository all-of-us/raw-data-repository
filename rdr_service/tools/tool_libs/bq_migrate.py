#! /bin/env python
#
# Template for RDR tool python program.
#

import argparse

# pylint: disable=superfluous-parens
# pylint: disable=broad-except
import importlib
import json
import logging
import os
import sys
import tempfile

from rdr_service.model import BQ_SCHEMAS, BQ_VIEWS
from rdr_service.model.bq_base import (
    BQDuplicateFieldException,
    BQException,
    BQInvalidModeException,
    BQInvalidSchemaException,
    BQSchema,
    BQSchemaStructureException,
)
from rdr_service.services.gcp_utils import gcp_bq_command
from rdr_service.services.system_utils import setup_logging, setup_unicode
from rdr_service.tools.tool_libs import GCPProcessContext

_logger = logging.getLogger("rdr_logger")

# tool_cmd and tool_desc name are required.
# Remember to add/update bash completion in 'tool_lib/tools.bash'
tool_cmd = "migrate-bq"
tool_desc = "bigquery schema migration tool"


class BQMigration(object):
    def __init__(self, args, gcp_env):
        """
    :param args: command line arguments.
    :param gcp_env: gcp environment information, see: gcp_initialize().
    """
        self.args = args
        self.gcp_env = gcp_env

    def get_bq_obj_uri(self, obj):
        """
    Build the table URI value.
    :param obj: table name
    :return: string
    """
        return "{0}:{1}.{2}".format(self.args.project, self.args.dataset, obj)

    def create_table(self, table, schema):
        """
    Create a table with the given schema in BigQuery.
    :param table: table name
    :param schema: json string
    :return: True if successful otherwise False
    """
        # bq mk --table --expiration [INTEGER] --description [DESCRIPTION]
        #             --label [KEY:VALUE, KEY:VALUE] [PROJECT_ID]:[DATASET].[TABLE] [SCHEMA]
        tf = tempfile.NamedTemporaryFile(delete=False)
        tf.write(schema)
        tf.close()

        args = "{0} {1}".format(self.get_bq_obj_uri(table), tf.name)
        cflags = "--table --label organization:rdr"
        pcode, so, se = gcp_bq_command("mk", args=args, command_flags=cflags)  # pylint: disable=unused-variable

        os.unlink(tf.name)

        if pcode != 0:
            if "parsing error in row starting at position" in so:
                raise BQInvalidSchemaException(so)
            else:
                raise BQException(se if se else so)

        return True

    def create_view(self, view_name, view_sql, view_desc):
        """
    Create a view
    :param view_name: view name
    :param view_sql: view sql string
    :return: True if successful otherwise False
    """
        self.delete_table(view_name)

        if "{project}" in view_sql:
            view_sql = view_sql.format(project=self.args.project)

        args = "'{0}'".format(self.get_bq_obj_uri(view_name))
        cflags = "--use_legacy_sql=false --label organization:rdr --description '{0}' --view '{1}'".format(
            view_desc, view_sql
        )
        pcode, so, se = gcp_bq_command("mk", args=args, command_flags=cflags)  # pylint: disable=unused-variable

        if pcode != 0:
            raise BQException(se if se else so)

        return True

    def modify_table(self, table, schema):
        """
    Modify the schema of a table in BigQuery.
    :param table: table name
    :param schema: json string
    :return: True if
    """
        tf = tempfile.NamedTemporaryFile(delete=False)
        tf.write(schema)
        tf.close()

        # bq update [PROJECT_ID]:[DATASET].[TABLE] [SCHEMA]
        args = "{0} {1}".format(self.get_bq_obj_uri(table), tf.name)
        pcode, so, se = gcp_bq_command("update", args=args)  # pylint: disable=unused-variable

        os.unlink(tf.name)

        if pcode != 0:
            if "already exists in schema" in so:
                raise BQDuplicateFieldException(so)
            elif "parsing error in row starting at position" in so:
                raise BQInvalidSchemaException(so)
            elif "add required columns to an existing schema" in so:
                raise BQInvalidModeException(so)
            elif "Precondition Failed" in so:
                raise BQSchemaStructureException(so)
            else:
                raise BQException(so)

        return True

    def delete_table(self, table):
        """
    Delete the table from BigQuery
    :param table: table name
    :return: string
    """
        # bq rm --force --table [PROJECT_ID]:[DATASET].[TABLE]
        table_uri = self.get_bq_obj_uri(table)
        pcode, so, se = gcp_bq_command(
            "rm", args=table_uri, command_flags="--force --table"
        )  # pylint: disable=unused-variable

        if pcode != 0:
            raise BQException(se if se else so)

        return so

    def get_table_schema(self, table):
        """
    Retrieve the table schema from BigQuery
    :param table: table name
    :return: string
    """
        # bq show --schema --format=prettyjson [PROJECT_ID]:[DATASET].[TABLE]
        table_uri = self.get_bq_obj_uri(table)
        pcode, so, se = gcp_bq_command(
            "show", args=table_uri, command_flags="--schema --format=prettyjson"
        )  # pylint: disable=unused-variable

        if pcode != 0:
            if "Not found" in so:
                return None
            if "Authorization error" in so:
                _logger.error("** BigQuery returned an authorization error, please check the following: **")
                _logger.error("   * Service account has correct permissions.")
                _logger.error("   * Timezone and time on computer match PMI account settings.")
                # for more suggestions look at:
                #    https://blog.timekit.io/google-oauth-invalid-grant-nightmare-and-how-to-fix-it-9f4efaf1da35
            raise BQException(se if se else so)

        return so

    def run(self):
        """
    Main program process
    :return: Exit code value
    """
        # TODO: Validate dataset name exists in BigQuery
        # Loop through table schemas
        for path, obj_name in BQ_SCHEMAS:
            mod = importlib.import_module(path, obj_name)
            mod_class = getattr(mod, obj_name)
            instance = mod_class()
            schema_name = instance.get_name()
            ls_obj = instance.get_schema()

            if self.args.delete and (self.args.delete.lower() == "all" or schema_name in self.args.delete):
                self.delete_table(schema_name)
                _logger.info("  {0:21}: {1}".format(schema_name, "deleted"))
                continue

            # _logger.info(' schema: {0}'.format(instance.to_json()))
            rs_json = self.get_table_schema(schema_name)

            if not rs_json:
                self.create_table(schema_name, ls_obj.to_json())
                _logger.info("  {0:21}: {1}".format(schema_name, "created"))
            else:
                try:
                    rs_obj = BQSchema(json.loads(rs_json))
                except ValueError:
                    # Something is there in BigQuery for this schema, but it is bad.
                    # If this happens, the table can be reset by deleting it and then creating again it using this tool.
                    _logger.info("  {0:21}: {1}".format(schema_name, "!!! corrupt, needs reset !!!"))
                    continue

                if rs_obj == ls_obj:
                    _logger.info("  {0:21}: {1}".format(schema_name, "unchanged"))
                else:
                    self.modify_table(schema_name, ls_obj.to_json())
                    _logger.info("  {0:21}: {1}".format(schema_name, "updated"))

        # Loop through view schemas
        for path, obj_name, view_name, view_desc in BQ_VIEWS:
            if self.args.delete and (self.args.delete.lower() == "all" or view_name in self.args.delete):
                self.delete_table(view_name)
                _logger.info("  {0:21}: {1}".format(view_name, "deleted"))
                continue

            mod = importlib.import_module(path, obj_name)
            view_sql = getattr(mod, obj_name)
            if self.create_view(view_name, view_sql, view_desc):
                _logger.info("  {0:21}: {1}".format(view_name, "replaced"))

        return 0


def run():
    # Set global debug value and setup application logging.
    setup_logging(
        _logger, tool_cmd, "--debug" in sys.argv, "{0}.log".format(tool_cmd) if "--log-file" in sys.argv else None
    )
    setup_unicode()

    # Setup program arguments.
    parser = argparse.ArgumentParser(prog=tool_cmd, description=tool_desc)
    parser.add_argument("--debug", help="Enable debug output", default=False, action="store_true")  # noqa
    parser.add_argument("--log-file", help="write output to a log file", default=False, action="store_true")  # noqa
    parser.add_argument("--project", help="gcp project name", default="localhost")  # noqa
    parser.add_argument("--account", help="pmi-ops account", default=None)  # noqa
    parser.add_argument("--service-account", help="gcp iam service account", default=None)  # noqa
    parser.add_argument("--dataset", help="bigquery dataset name", required=True)  # noqa
    parser.add_argument(
        "--delete", help="a comma delimited list of schema names or 'all'", default=None, metavar="SCHEMAS"
    )  # noqa
    args = parser.parse_args()

    with GCPProcessContext(tool_cmd, args.project, args.account, args.service_account) as gcp_env:
        process = BQMigration(args, gcp_env)
        exit_code = process.run()
        return exit_code


# --- Main Program Call ---
if __name__ == "__main__":
    sys.exit(run())
