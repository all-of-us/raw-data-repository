#! /bin/env python
#
# Template for RDR tool python program.
#

import argparse

# pylint: disable=superfluous-parens
# pylint: disable=broad-except
import datetime
import json
import logging
import sys

from rdr_service.services.system_utils import setup_logging, setup_i18n, print_progress_bar
from rdr_service.tools.tool_libs import GCPProcessContext, GCPEnvConfigObject

_logger = logging.getLogger("rdr_logger")

# Tool_cmd and tool_desc name are required.
# Remember to add/update bash completion in 'tool_lib/tools.bash'
tool_cmd = "bq-participant-fix"
tool_desc = "Temporary tool to fix bad pm values"


class ProgramTemplateClass(object):
    def __init__(self, args, gcp_env: GCPEnvConfigObject):
        """
        :param args: command line arguments.
        :param gcp_env: gcp environment information, see: gcp_initialize().
        """
        self.args = args
        self.gcp_env = gcp_env

    def run(self):
        """
        Main program process
        :return: Exit code value
        """
        self.gcp_env.activate_sql_proxy()
        db_conn = self.gcp_env.make_mysqldb_connection()

        project_suffix = self.gcp_env.project.split("-")[-1]
        pdr_project_id = f'aou-pdr-data-{project_suffix}'

        if pdr_project_id not in ('aou-pdr-data-stable', 'aou-pdr-data-prod'):
            _logger.error(f'Project {pdr_project_id} is not supported, aborting.')
            return -1

        # Force resource 'pm_final' value to string and see if it is 'true' or 'false', this excludes fixed records.
        sql = f"""
            select id from bigquery_sync
            where project_id = '{pdr_project_id}' and dataset_id = 'rdr_ops_data_view' and table_id = 'pdr_participant'
                and resource ->> '$.pm[0].pm_final' in ('true', 'false')
            order by modified;
        """

        cursor = db_conn.cursor()
        cursor.execute(sql)
        results = cursor.fetchall()
        ids = [r[0] for r in results]
        pos = 0
        total_ids = len(ids)

        print(f'Processing {len(ids)} records...')

        sql = "select resource from bigquery_sync where id = %s"
        ins_sql = "update bigquery_sync set resource = %s, modified = %s where id = %s"

        for id_ in ids:
            args = [id_]
            cursor.execute(sql, args)
            resource = json.loads(cursor.fetchone()[0])

            for pm in resource['pm']:
                pm['pm_final'] = 1 if pm['pm_final'] else 0

            ins_args = [json.dumps(resource), datetime.datetime.utcnow(), id_]
            cursor.execute(ins_sql, ins_args)
            db_conn.commit()

            pos += 1
            print_progress_bar(
                pos, total_ids, prefix="{0}/{1}:".format(pos, total_ids), suffix="complete"
            )

        cursor.close()

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
    args = parser.parse_args()

    with GCPProcessContext(tool_cmd, args.project, args.account, args.service_account) as gcp_env:
        process = ProgramTemplateClass(args, gcp_env)
        exit_code = process.run()
        return exit_code


# --- Main Program Call ---
if __name__ == "__main__":
    sys.exit(run())
