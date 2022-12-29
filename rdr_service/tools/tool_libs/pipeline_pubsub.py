#! /bin/env python
#
# Tool for publishing pipeline pub-sub notifications.
#

import argparse

import logging
from MySQLdb import Connection, Error
from MySQLdb.cursors import DictCursor

import sys
from typing import List

from rdr_service.cloud_utils.gcp_google_pubsub import submit_pipeline_pubsub_msg
from rdr_service.services.system_utils import setup_logging, setup_i18n, JSONObject
from rdr_service.tools.tool_libs import GCPProcessContext, GCPEnvConfigObject

_logger = logging.getLogger("rdr_logger")

# Tool_cmd and tool_desc name are required.
# Remember to add/update bash completion in 'tool_lib/tools.bash'
tool_cmd = "pipeline-pubsub"
tool_desc = "submit pipeline pub-sub messages"


ACTIONS = ['insert', 'update', 'delete', 'upsert']


class MySQLSchemaFieldModel(JSONObject):
    """ MySQL table schema definition values """
    Field: str = None  # Field name
    Type: str = None  # Field type information.
    Null: str = None  # YES or NO if field is nullable.
    Key: str = None  # PRI = Primary Key, UNI = Unique Index, MUL = ?
    Default: str = None
    Extra: str = None  # 'auto_increment'


class ResourcePubSUbClass(object):

    db_conn: Connection = None

    def __init__(self, args, gcp_env: GCPEnvConfigObject):
        """
        :param args: command line arguments.
        :param gcp_env: gcp environment information, see: gcp_initialize().
        """
        self.args = args
        self.gcp_env = gcp_env

    def fetch_table_struct(self, table_key: str) -> [List[MySQLSchemaFieldModel], List[str]]:
        """
        Return the requested table structure
        :param table_key: Table key = "{database}.{table}"
        :return: List of columns and a list of primary key columns
        """
        sql = f'describe {table_key}'
        # Note: the MySQL connection cannot be awaited.
        cursor = self.db_conn.cursor(DictCursor)
        cursor.execute(sql)
        try:
            columns: List[MySQLSchemaFieldModel] = cursor.fetchall()
            # Get a list of primary key columns in mysql schema
            pk_columns = [c['Field'] for c in columns if c['Key'] == 'PRI']
            return columns, pk_columns
        except Error as e:
            _logger.error(e)
        finally:
            cursor.close()

        return None, None

    def send_pubsub_message(self, table_key: str, pk_columns: List[str], pk_values: List[List[str]]):
        """
        Submit a Pipeline Pub/Sub service message.
        :param table_key:
        :param pk_columns: List of table primary key column names
        :param pk_values: List of primary key values. See gcp_google_pubsub._validate_pk_values() for more info.
        :return: Response or none.
        """
        database, table = table_key.split('.')
        resp = submit_pipeline_pubsub_msg(database, table, self.args.action, pk_columns, pk_values,
                                          project=self.gcp_env.project)
        # sample response: {'messageIds': ['6516999682321403']}
        return resp

    def run(self):
        """
        Main program process
        :return: Exit code value
        """
        self.gcp_env.activate_sql_proxy()
        self.db_conn = self.gcp_env.make_mysqldb_connection()

        columns, pk_columns = self.fetch_table_struct(self.args.table_key)

        if not columns or not pk_columns:
            _logger.error(f'Database check using {self.args.table_key} failed.')
            return -1

        # See gcp_google_pubsub._validate_pk_values() for more info.
        pk_values = list()
        pk_values.append(self.args.id)

        resp = self.send_pubsub_message(self.args.table_key, pk_columns, pk_values)
        if 'messageIds' in resp:
            _logger.info(f'successfully published pub/sub message (msg id: {resp["messageIds"]}')
        else:
            _logger.error(resp['error'])

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
    parser.add_argument("--table-key", help="database table lookup id", type=str, required=True)
    parser.add_argument("--action", help="pubsub message action value", choices=ACTIONS, default='insert')
    parser.add_argument("--id", help="comma delimited values for primary key", type=str, required=True)

    args = parser.parse_args()

    # Note: For now, we are only supporting PK values that identify a single record in a table.
    #       In the future we should support '--batch' and '--from-file' options to support multiple
    #       table records.
    args.id = [a.strip() for a in args.id.split(',')]

    with GCPProcessContext(tool_cmd, args.project, args.account, args.service_account) as gcp_env:
        process = ResourcePubSUbClass(args, gcp_env)
        exit_code = process.run()
        return exit_code


# --- Main Program Call ---
if __name__ == "__main__":
    sys.exit(run())
