#! /bin/env python
#
# This file is subject to the terms and conditions defined in the
# file 'LICENSE', which is part of this source code package.
#
# Template for RDR tool python program.
#

import argparse
import importlib
import logging
import os
import shutil
import sys
import tempfile

from pathlib import Path

from rdr_service.tools.tool_libs import GCPProcessContext, GCPEnvConfigObject
from rdr_service.services.gcp_utils import gcp_gcloud_command
from rdr_service.services.system_utils import setup_logging, setup_i18n

_logger = logging.getLogger("pdr")

# Tool_cmd and tool_desc name are required.
# Remember to add/update bash completion in 'tools.bash'
tool_cmd = "deploy-func"
tool_desc = "Deploy gcloud function"
cloud_functions_dir = "gcloud_functions"


class DeployFunctionClass(object):
    def __init__(self, args, gcp_env: GCPEnvConfigObject, project_path, func_path):
        """
        :param args: command line arguments.
        :param gcp_env: gcp environment information, see: gcp_initialize().
        """
        self.args = args
        self.gcp_env = gcp_env
        self.project_path = project_path
        self.func_path = func_path

    def _get_deploy_args(self):
        """
        Get the deploy trigger arguments from the function main.py file.
        """
        mod = importlib.import_module(f'{cloud_functions_dir}.{self.args.function}.main')
        args = mod.get_deploy_args(self.gcp_env)

        return args

    def _prep_for_deploy(self, tmp_path):
        """
        Copy files required for deployment to temp directory.
        :param tmp_path: string with path to temporary deployment directory.
        """
        # Copy the function directory files.
        shutil.copytree(f'{self.func_path}/', f'{tmp_path}/f/')

        # Copy the requirements.txt file and remove 'aou_cloud' requirement.
        lines = open(f'{self.project_path}/{cloud_functions_dir}/requirements.txt').readlines()
        with open(f'{tmp_path}/f/requirements.txt', 'w') as h:
            for line in lines:
                if line.startswith('#') or 'python-aou-cloud-services' in line:
                    continue
                h.write(line)

        return 0

    def run(self):
        """
        Main program process
        :return: Exit code value
        """

        clr = self.gcp_env.terminal_colors
        # _logger.info(clr.fmt('This is a blue info line.', clr.fg_bright_blue))
        # _logger.info(clr.fmt('This is a custom color line', clr.custom_fg_color(156)))
        _logger.info(clr.fmt('Function Deployment Information:', clr.custom_fg_color(156)))
        _logger.info(clr.fmt(''))
        _logger.info('=' * 90)
        _logger.info('  Target GCP Project    : {0}'.format(clr.fmt(self.gcp_env.project)))
        _logger.info('  Function              : {0}'.format(clr.fmt(self.args.function)))
        _logger.info('=' * 90)

        if not self.args.quiet:
            confirm = input('\nDeploy function (Y/n)? : ')
            if confirm and confirm.lower().strip() != 'y':
                _logger.warning('Aborting deployment.')
                return 1

        trigger_args = ' '.join(self._get_deploy_args())

        # Create a temporary directory to put the function, support code and deploy it.
        tmp_obj = tempfile.TemporaryDirectory(prefix='func_')
        tmp_path = tmp_obj.name

        result = self._prep_for_deploy(tmp_path)

        # Copy the 'aou_cloud' directory.
        import aou_cloud as _aou_cloud
        aou_path = os.path.dirname(_aou_cloud.__file__)
        shutil.copytree(f'{aou_path}', f'{tmp_path}/f/aou_cloud',
                        ignore=shutil.ignore_patterns('__pycache__', 'tools', 'tests'))

        _cwd = os.path.abspath(os.curdir)
        tp = os.path.join(f'{tmp_path}/', 'f')
        os.chdir(tp)

        if result == 0:

            args = f'deploy {trigger_args}'
            # Add debug logging to cloud function.
            if self.args.debug:
                args += ' --set-env-vars FUNC_DEBUG=1'

            # '--quiet' argument prevents gcloud from asking 'allow unauthenticated requests?' question on cli.
            pcode, so, se = gcp_gcloud_command('functions', args, '--runtime python37 --quiet')

            if pcode == 0:
                _logger.info(f'Successfully deployed function {self.args.function}.')
                if self.args.debug:
                    _logger.info(se or so)
            else:
                _logger.error(f'Failed to deploy function {self.args.function}. ({pcode}: {se or so}).')
                result = -1

        os.chdir(_cwd)

        # Clean up temp directory.
        if os.path.exists(tp):
            shutil.rmtree(tp)

        return result


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
    parser.add_argument("--quiet", help="do not ask for user input", default=False, action="store_true")  # noqa
    parser.add_argument('--function', help="gcloud function directory name", required=True)

    args = parser.parse_args()

    project_path = Path(os.path.dirname(sys.argv[0])).parent
    func_path = os.path.join(f'{project_path}/{cloud_functions_dir}', args.function)
    if not os.path.exists(func_path):
        raise FileNotFoundError('GCloud function directory not found.')

    with GCPProcessContext(tool_cmd, args.project, args.account, args.service_account) as gcp_env:
        process = DeployFunctionClass(args, gcp_env, project_path, func_path)
        exit_code = process.run()
        return exit_code


# --- Main Program Call ---
if __name__ == "__main__":
    sys.exit(run())
