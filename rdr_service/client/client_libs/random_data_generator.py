#! /bin/env python
#
# Generate random fake participant data
#

import argparse

# pylint: disable=superfluous-parens
# pylint: disable=broad-except
import csv
import logging
import sys
from time import sleep

from rdr_service.tools.tool_libs import GCPProcessContext
from rdr_service.services.gcp_utils import gcp_get_app_access_token, gcp_get_app_host_name, gcp_make_auth_header
from rdr_service.services.system_utils import make_api_request, setup_logging, setup_i18n

_logger = logging.getLogger("rdr_logger")

tool_cmd = "random-gen"
tool_desc = "random participant data generator"


class RandomGeneratorClass(object):

    MAX_PARTICIPANTS_PER_REQUEST = 25
    MAX_CONSECUTIVE_ERRORS = 5
    SLEEP_TIME_AFTER_ERROR_SECONDS = 3

    _gen_url = "rdr/v1/DataGen"
    _host = None
    _oauth_token = None

    def __init__(self, args, gcp_env):
        self.args = args
        self.gcp_env = gcp_env

        if args:
            self._host = gcp_get_app_host_name(self.args.project)
            if self.args.port:
                self._host = "{0}:{1}".format(self._host, self.args.port)
            else:
                if self._host in ["127.0.0.1", "localhost"]:
                    self._host = "{0}:{1}".format(self._host, 8080)

            if self._host not in ["127.0.0.1", "localhost"]:
                self._oauth_token = gcp_get_app_access_token()

    def generate_fake_data(self):
        total_participants_created = 0

        while total_participants_created < self.args.num_participants:
            participants_for_batch = min(
                self.MAX_PARTICIPANTS_PER_REQUEST, self.args.num_participants - total_participants_created
            )
            request_body = {
                "num_participants": participants_for_batch,
                "include_physical_measurements": self.args.include_physical_measurements,
                "include_biobank_orders": self.args.include_biobank_orders,
            }
            if self.args.hpo:
                request_body["hpo"] = self.args.hpo
            _logger.info("generating batch of [{0}] participants.".format(participants_for_batch))
            num_consecutive_errors = 0
            while num_consecutive_errors <= self.MAX_CONSECUTIVE_ERRORS:
                code, resp = make_api_request(
                    self._host, self._gen_url, req_type="POST", json_data=request_body, headers=gcp_make_auth_header()
                )
                if code == 200:
                    break
                _logger.error("{0} [{1}]".format(code, resp))
                num_consecutive_errors += 1
                sleep(self.SLEEP_TIME_AFTER_ERROR_SECONDS)
            if num_consecutive_errors > self.MAX_CONSECUTIVE_ERRORS:
                raise IOError("more than {0} consecutive errors; bailing out.".format(self.MAX_CONSECUTIVE_ERRORS))

            total_participants_created += participants_for_batch
            _logger.info("total participants created: [{0}].".format(total_participants_created))
        if self.args.create_biobank_samples:
            _logger.info("requesting Biobank sample generation.")
            code, resp = make_api_request(
                self._host,
                self._gen_url,
                req_type="POST",
                json_data={"create_biobank_samples": True},
                headers=gcp_make_auth_header(),
            )
            if code != 200:
                _logger.error("request to generate biobank samples failed.")
            else:
                _logger.info(
                    "biobank samples are being generated asynchronously."
                    " wait until done, then use the cron tab in AppEngine to start the samples pipeline."
                )

    def _read_csv_lines(self, filepath):
        with open(filepath, "r") as f:
            reader = csv.reader(f)
            return [line[0].strip() for line in reader]

    def generate_data_from_file(self):
        reader = self._read_csv_lines(self.args.create_samples_from_file)
        _logger.info("requesting pm&b for participants")
        for item in reader:
            # pylint: disable=unused-variable
            code, resp = make_api_request(
                self._host, self._gen_url, req_type="POST", json_data=item, headers=gcp_make_auth_header()
            )
            if code != 200:
                _logger.error("request failed")

    def run(self):
        """
    Main program process
    :return: Exit code value
    """
        if self.args.create_samples_from_file:
            self.generate_data_from_file()
        else:
            self.generate_fake_data()

        return 0


def run():
    # Set global debug value and setup application logging.
    setup_logging(
        _logger, tool_cmd, "--debug" in sys.argv, "{0}.log".format(tool_cmd) if "--log-file" in sys.argv else None
    )
    setup_i18n()
    exit_code = 1

    # Setup program arguments.
    parser = argparse.ArgumentParser(prog=tool_cmd, description=tool_desc)
    parser.add_argument("--debug", help="Enable debug output", default=False, action="store_true")  # noqa
    parser.add_argument("--log-file", help="write output to a log file", default=False, action="store_true")  # noqa
    parser.add_argument("--project", help="gcp project name", default="localhost")  # noqa
    parser.add_argument("--account", help="pmi-ops account", default=None)  # noqa
    parser.add_argument("--service-account", help="gcp iam service account", default=None)  # noqa
    parser.add_argument("--port", help="alternate ip port to connect to", default=None)  # noqa
    parser.add_argument("--num_participants", type=int, help="The number of participants to create.", default=0)
    parser.add_argument(
        "--include_physical_measurements", action="store_true", help="True if physical measurements should be created"
    )
    parser.add_argument(
        "--include_biobank_orders", action="store_true", help="True if biobank orders should be created"
    )
    parser.add_argument("--hpo", help="The HPO name to assign participants to; defaults to random choice.")
    parser.add_argument(
        "--create_biobank_samples", action="store_true", help="True if biobank samples should be created"
    )
    parser.add_argument(
        "--create_samples_from_file",
        help="Creates PM&B for existing participants from a csv file; requires path"
        ' to file. File is expected to contain a single column of ID"s with a '
        "leading env. identifier. i.e. P",
    )

    args = parser.parse_args()

    if args.num_participants == 0 and not args.create_biobank_samples and not args.create_samples_from_file:
        parser.error("--num_participants must be nonzero unless --create_biobank_samples is true.")
        exit(exit_code)

    with GCPProcessContext(tool_cmd, args.project, args.account, args.service_account) as gcp_env:
        # verify we're not getting pointed to production.
        if gcp_env.project == "all-of-us-rdr-prod":
            _logger.error("using spec generator in production is not allowed.")
            return 1

        process = RandomGeneratorClass(args, gcp_env)
        exit_code = process.run()
        return exit_code


# --- Main Program Call ---
if __name__ == "__main__":
    sys.exit(run())
