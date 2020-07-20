#! /bin/env python
#
# Tool to manage the RDR readthedocs project via its public API.
#

import argparse

# pylint: disable=superfluous-parens
# pylint: disable=broad-except
import logging
import sys

from json import dumps
from time import sleep

#from rdr_service import config
from rdr_service.services.system_utils import setup_logging, setup_i18n
#from rdr_service.tools.tool_libs import GCPProcessContext, GCPEnvConfigObject
from rdr_service.services.documentation_utils import ReadTheDocsHandler

_logger = logging.getLogger("rdr_logger")

# Tool_cmd and tool_desc name are required.
# Remember to add/update bash completion in 'tool_lib/tools.bash'
tool_cmd = "rdr-docs"
tool_desc = "manage RDR documentation content on readthedocs.org"

class RTDBaseClass(object):
    def __init__(self, args):
        """
        :param args: command line arguments.
        """
        self.args = args
        self._rtd_handler = ReadTheDocsHandler()

class RTDBuildClass(RTDBaseClass):

    def run(self):
        build_id = self._rtd_handler.build_the_docs(self.args.slug)
        _logger.info(f'Documentation build {build_id} started for version {self.args.slug}')
        if not self.args.no_wait:
            state = ""
            json_data = None
            while state.lower() != 'finished':
                sleep(10)
                json_data = self._rtd_handler.get_build_details(build_id)
                if state != json_data['state']['name']:
                    state = json_data['state']['name']
                    _logger.info(f'{state}')

            success = json_data['success']
            if success:
                _logger.info(f'"{self.args.slug}" build {build_id} succeeded')
            else:
                _logger.error(f'"{self.args.slug}" build {build_id} FAILED.')
                _logger.error(dumps(json_data, indent=4))
        return 0

class RTDListClass(RTDBaseClass):

    def run(self):
        if self.args.build:
            _logger.info(dumps(self._rtd_handler.get_build_details(self.args.build), indent=4))
        if self.args.version:
            _logger.info(dumps(self._rtd_handler.get_version_details(self.args.version), indent=4))
        if self.args.default_tag:
            default = self._rtd_handler.get_project_details()['default_branch']
            _logger.info(f'RDR docs latest version using RDR git tag {default}')
        return 0

class RTDUpdateClass(RTDBaseClass):

    def run(self):
        if self.args.latest:
            self._rtd_handler.update_project_to_release(self.args.latest)
            _logger.info(f'RDR docs will now use git tag {self.args.latest} when building latest version')
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

    help_txt = "RDR documentation services.  NOTE: requires RTD_API_TOKEN environment variable or "
    help_txt += "readthedocs_creds entry in current_config.   See prod or stable config for details"
    subparser = parser.add_subparsers(help=help_txt)

    # Trigger documentation build
    build_parser = subparser.add_parser("build", help="Trigger a documentation build on readthedocs.org")
    build_parser.add_argument("--slug", help="version slug (e.g., 'stable', 'latest') to build.  Default is 'stable'",
                              default="stable", type=str, required=True)
    build_parser.add_argument("--no-wait", help="Do not wait for build to complete",
                              default=False, action="store_true")

    # Get item details from ReadTheDocs
    list_parser = subparser.add_parser("list", help="Retrieve details on an item from readthedocs.org")
    list_parser.add_argument("--build", help="Show build details for the specified build id", type=int)
    list_parser.add_argument("--version",
                             help="Show version details for the specified version slug (e.g., 'stable' or 'latest')",
                             type=str)
    list_parser.add_argument("--default-tag",
                             help="Show the current default_branch/tag for RDR 'latest' version", action="store_true")

    # Update project settings in ReadTheDocs
    update_parser = subparser.add_parser("update", help="Update the RDR readthedocs.org project settings")
    update_parser.add_argument("--latest",
                               help="release git tag (X.Y.Z) to set as latest default_branch in readthedocs.org",
                               default=None, type=str)

    args = parser.parse_args()

    if hasattr(args, 'no_wait'):
        process = RTDBuildClass(args)
        exit_code = process.run()
    elif hasattr(args, 'latest'):
        process = RTDUpdateClass(args)
        exit_code = process.run()
    elif hasattr(args, 'build') or hasattr(args, 'version') or hasattr(args, 'default_tag'):
        process = RTDListClass(args)
        exit_code = process.run()
    else:
        _logger.info('Please select a service option to run.  For help, use "rdr-docs --help"')
        exit_code = 1
    return exit_code


# --- Main Program Call ---
if __name__ == "__main__":
    sys.exit(run())
