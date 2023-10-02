#! /bin/env python
#
# Tool for the Benefit of Managing NPH Samples
#
import argparse
import logging
import sys

from rdr_service.config import NPH_SMS_BUCKETS
from rdr_service.dao.study_nph_sms_dao import SmsN0Dao
from rdr_service.services.system_utils import setup_logging, setup_i18n
from rdr_service.tools.tool_libs import GCPProcessContext
from rdr_service.tools.tool_libs.tool_base import ToolBase
from rdr_service.workflow_management.nph.sms_workflows import SmsWorkflow

_logger = logging.getLogger("rdr_logger")

# Tool_cmd and tool_desc name are required.
# Remember to add/update bash completion in 'tool_lib/tools.bash'
tool_cmd = "nph_sms_util"
tool_desc = "tool for loading spot ODS and exporting to data mart."


class SmsUtil(ToolBase):

    def run(self):
        if self.args.project == "all-of-us-rdr-prod":
            _logger.warning("You are about to run this operation on prod.")
            response = input("Continue? (y/n)> ")
            if response != "y":
                _logger.info("Aborted.")
                return 0

        process_map = {
            "FILE_GENERATION": self.run_file_generation,
        }

        self.gcp_env.activate_sql_proxy()

        return process_map[self.args.process]()

    def run_file_generation(self):
        n0_dao = SmsN0Dao()
        new_package_ids = n0_dao.get_n0_package_ids_without_n1()

        if self.gcp_env == "localhost":
            recipients = NPH_SMS_BUCKETS.get('test').keys()
        else:
            recipients = NPH_SMS_BUCKETS.get(self.gcp_env.project.split('-')[-1], 'test').keys()
        for package_id in new_package_ids:
            for recipient in recipients:
                if recipient not in package_id[1]:
                    continue
                generation_data = {
                    "job": "FILE_GENERATION",
                    "file_type": "N1_MC1",
                    "recipient": recipient,
                    "package_id": package_id[0],
                    "env": self.gcp_env.project
                }
                workflow = SmsWorkflow(generation_data)
                workflow.execute_workflow()


def run():
    # Set global debug value and setup application logging.
    setup_logging(
        _logger, tool_cmd, "--debug" in sys.argv, "{0}.log".format(tool_cmd) if "--log-file" in sys.argv else None
    )
    setup_i18n()
    tool_processes = [
        "FILE_GENERATION"
    ]
    # Setup program arguments.
    parser = argparse.ArgumentParser(prog=tool_cmd, description=tool_desc)
    parser.add_argument("--project", help="gcp project name", default="localhost")  # noqa
    parser.add_argument("--account", help="pmi-ops account", default=None)  # noqa
    parser.add_argument("--service-account", help="gcp iam service account", default=None)  # noqa
    parser.add_argument("--process",
                        help="process to run, choose one: {}".format(tool_processes),
                        choices=tool_processes,
                        default=None,
                        required=True,
                        type=str)  # noqa
    parser.add_argument("--file-type",
                        help="file type, i.e. N1_MC1",
                        default=None,
                        required=False,
                        type=str)  # noqa

    args = parser.parse_args()

    with GCPProcessContext(tool_cmd, args.project, args.account, args.service_account) as gcp_env:
        try:
            sms_process = SmsUtil(args, gcp_env)
            exit_code = sms_process.run()
        # pylint: disable=broad-except
        except Exception as e:
            _logger.info(f'Error has occured, {e}. For help use "nph_sms_util --help".')
            exit_code = 1

        return exit_code


# --- Main Program Call ---
if __name__ == "__main__":
    sys.exit(run())
