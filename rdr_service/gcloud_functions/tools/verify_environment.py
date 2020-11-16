#! /bin/env python
#
# Simply verify the environment is valid for running the client apps.
#

import argparse

# pylint: disable=superfluous-parens
# pylint: disable=broad-except
import importlib
import logging
import sys

from django_service.services.gcp_utils import gcp_get_app_access_token, gcp_get_app_host_name
from django_service.services.system_utils import make_api_request
from tools import GCPProcessContext

_logger = logging.getLogger("pdr")

# tool_cmd and tool_desc name are required.
# Remember to add/update bash completion in 'tools.bash'
tool_cmd = "verify"
tool_desc = "test local environment"


class Verify(object):
    def __init__(self, args, gcp_env):
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
        result = 0
        requests_mod = False
        #
        # python modules that need to be installed in the local system
        #
        modules = ["requests", "urllib3"]

        for module in modules:
            try:
                mod = importlib.import_module(module)
                _logger.info("found python module [{0}].".format(mod.__name__))
                if module == "requests":
                    requests_mod = True
            except ImportError:
                _logger.error('missing python [{0}] module, please run "pip --install {0}"'.format(module))
                result = 1

        if self.args.project in ["localhost", "127.0.0.1"]:
            _logger.warning("unable to perform additional testing unless project parameter is set.")
            return result

        # Try making some API calls to to verify OAuth2 token functions.
        if requests_mod:

            host = gcp_get_app_host_name(self.args.project)
            url = "rdr/v1"

            _logger.info("attempting simple api request.")
            code, resp = make_api_request(host, url)

            if code != 200 or "version_id" not in resp:
                _logger.error("simple api request failed")
                return 1

            _logger.info("{0} version is [{1}].".format(host, resp["version_id"]))

            # verify OAuth2 token can be retrieved.
            token = gcp_get_app_access_token()
            if token and token.startswith("ya"):
                _logger.info("verified app authentication token.")

                # TODO: Make an authenticated API call here. What APIs are avail for this?

            else:
                _logger.error("app authentication token verification failed.")
                result = 1

        else:
            _logger.warning("skipping api request tests.")

        return result


def run():
    # Set global debug value and setup application logging.
    GCPProcessContext.setup_logging(tool_cmd)
    parser = GCPProcessContext.get_argparser(tool_cmd, tool_desc)
    args = parser.parse_args()

    with GCPProcessContext(tool_cmd, args) as gcp_env:
        process = Verify(args, gcp_env)
        exit_code = process.run()
        return exit_code


# --- Main Program Call ---
if __name__ == "__main__":
    sys.exit(run())
