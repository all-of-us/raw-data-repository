#! /bin/env python
#
# Template for RDR tool python program.
#
import argparse
import logging
import sys

from rdr_service.genomic.genomic_storage_class import GenomicStorageClass
from rdr_service.genomic_enums import GenomicJob
from rdr_service.services.system_utils import setup_logging, setup_i18n

from rdr_service.tools.tool_libs import GCPProcessContext
from rdr_service.tools.tool_libs.tool_base import ToolBase

_logger = logging.getLogger("rdr_logger")

# Tool_cmd and tool_desc name are required.
# Remember to add/update bash completion in 'tool_lib/tools.bash'
tool_cmd = "genomic-storage-update"
tool_desc = "Tool for updating genomic data file storage types"


class GenomicStorageTool(ToolBase):

    def run(self):
        self.gcp_env.activate_sql_proxy()

        storage_job_type = {
            'array': GenomicJob.UPDATE_ARRAY_STORAGE_CLASS,
            'wgs': GenomicJob.UPDATE_WGS_STORAGE_CLASS
        }[self.args.storage_type]

        genomic_storage = GenomicStorageClass(
            storage_job_type=storage_job_type,
            logger=_logger
        )
        genomic_storage.run_storage_update()


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
    parser.add_argument("--storage-type", help="add storage type for file update", choices=['array', 'wgs'],
                        required=True)
    # noqa
    args = parser.parse_args()

    with GCPProcessContext(tool_cmd, args.project, args.account, args.service_account) as gcp_env:
        process = GenomicStorageTool(args, gcp_env)
        exit_code = process.run()
        return exit_code


# --- Main Program Call ---
if __name__ == "__main__":
    sys.exit(run())
