#! /bin/env python
#
# Template for RDR tool python program.
#
import argparse
import logging
import re
import sys

from rdr_service.code_constants import EMAIL_QUESTION_CODE as EQC, LOGIN_PHONE_NUMBER_QUESTION_CODE as PNQC
from rdr_service.services.gcp_utils import gcp_make_auth_header
from rdr_service.services.system_utils import make_api_request
from rdr_service.services.system_utils import setup_logging, setup_i18n
from rdr_service.tools.tool_libs import GCPProcessContext

_logger = logging.getLogger("rdr_logger")

# Tool_cmd and tool_desc name are required.
# Remember to add/update bash completion in 'tool_lib/tools.bash'
tool_cmd = "ppi-check"
tool_desc = "check participant ppi data in rdr"


class CheckPPIDataClass(object):
    def __init__(self, args, gcp_env):
        """
        :param args: command line arguments.
        :param gcp_env: gcp environment information, see: gcp_initialize().
        """
        self.args = args
        self.gcp_env = gcp_env

    def check_ppi_data(self):
        """
        Fetch and process spreadsheet, then call CheckPpiData for results
        :param client: Client object
        :param args: program arguments
        """
        # See if we have filter criteria
        if not self.args.email and not self.args.phone:
            do_filter = False
        else:
            do_filter = True

        if not self.args.phone:
            self.args.phone = list()
        if not self.args.email:
            self.args.email = list()

        csv_data = self.fetch_csv_data()
        ppi_data = dict()

        # iterate over each data column, convert them into a dict.
        for column in range(0, len(csv_data[0]) - 1):
            row_dict = self.convert_csv_column_to_dict(csv_data, column)
            email = row_dict[EQC] if EQC in row_dict else None
            phone_no = row_dict[PNQC] if PNQC in row_dict else None

            if do_filter is False or (email in self.args.email or phone_no in self.args.phone):
                # prioritize using email value over phone number for key
                key = email if email else phone_no
                ppi_data[key] = row_dict

        if len(ppi_data) == 0:
            _logger.error("No participants matched filter criteria. aborting.")
            return

        host = f'{self.gcp_env.project}.appspot.com'
        data = {"ppi_data": ppi_data}

        headers = gcp_make_auth_header()
        code, resp = make_api_request(host, '/rdr/v1/CheckPpiData', headers=headers, json_data=data, req_type="POST")

        if code != 200:
            _logger.error(f'API request failed. {code}: {resp}')
            return

        self.log_ppi_results(resp["ppi_results"])

    def fetch_csv_data(self):
        """
        Download a google doc spreadsheet in CSV format
        :return: A list object with rows from spreadsheet
        """
        host = 'docs.google.com'
        path = f'spreadsheets/d/{self.args.sheet_id}/export?format=csv&' + \
               f'id={self.args.sheet_id}s&gid={self.args.sheet_gid}'

        code, resp = make_api_request(host, path, ret_type='text')
        if code != 200:
            _logger.error(f'Error fetching https://{host}{path}. {code}: {resp}')
            return

        resp = resp.replace('\r', '')

        csv_data = list()
        for row in resp.split('\n'):
            csv_data.append(row.split(','))

        return csv_data

    def convert_csv_column_to_dict(self, csv_data, column):
        """
        Return a dictionary object with keys from the first column and values from the specified
        column.
        :param csv_data: File-like CSV text downloaded from Google spreadsheets. (See main doc.)
        :return: dict of fields and values for given column
        """
        results = dict()

        for row in csv_data:
            key = row[0]
            data = row[1:][column]

            if data:
                if key not in results:
                    results[key] = data.strip() if data else ""
                else:
                    # append multiple choice questions
                    results[key] += "|{0}".format(data.strip())

        return results

    def log_ppi_results(self, data):
        """
        Formats and logs the validation results. See CheckPpiDataApi for response format details.
        """
        clr = self.gcp_env.terminal_colors
        _logger.info(clr.fmt(''))
        _logger.info('Results:')
        _logger.info('=' * 110)

        total = 0
        errors = 0
        for email, results in data.items():
            tests_count, errors_count = results["tests_count"], results["errors_count"]
            errors += errors_count
            total += tests_count
            log_lines = [
                clr.fmt(f"  {email}: {tests_count} tests, {errors_count} errors",
                        clr.fg_bright_green if errors_count == 0 else clr.fg_bright_red)
            ]
            for message in results["error_messages"]:
                # Convert braces and unicode indicator to quotes for better readability
                message = re.sub("\['", '"', message)
                message = re.sub("'\]", '"', message)
                while '  ' in message:
                    message = message.replace('  ', ' ')
                log_lines += ["\n      " + message]
            _logger.info("".join(log_lines))
        _logger.info('=' * 110)
        _logger.info(f"Completed {total} tests across {len(data)} participants with {errors} errors.")

    def run(self):
        """
        Main program process
        :return: Exit code value
        """
        self.check_ppi_data()
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

    parser.add_argument("--sheet-id",
                        help='google spreadsheet doc id, after the "/d/" in the URL. the doc must be public.') # noqa
    parser.add_argument("--sheet-gid", help='google spreadsheet sheet id, after "gid=" in the url.')  # noqa
    parser.add_argument("--email", help=("only validate the given e-mail(s). Validate all by default. "
                            "this flag may be repeated to specify multiple e-mails."),action="append")  # noqa
    parser.add_argument("--phone", help=("only validate the given phone number. " 
                            "this flag may be repeated to specify multiple phone numbers."),action="append")  # noqa
    args = parser.parse_args()

    with GCPProcessContext(tool_cmd, args.project, args.account, args.service_account) as gcp_env:
        process = CheckPPIDataClass(args, gcp_env)
        exit_code = process.run()
        return exit_code


# --- Main Program Call ---
if __name__ == "__main__":
    sys.exit(run())
