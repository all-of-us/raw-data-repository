#! /bin/env python
#
# Find COPE survey response counts and breakdowns for each question
#

import argparse
import json
import logging
import random
import sys

from rdr_service.dao import database_factory
from rdr_service.services.gcp_utils import gcp_format_sql_instance, gcp_make_auth_header
from rdr_service.services.system_utils import make_api_request, setup_logging, setup_i18n
from rdr_service.tools.tool_libs import GCPProcessContext

_logger = logging.getLogger("rdr_logger")

# Tool_cmd and tool_desc name are required.
# Remember to add/update bash completion in 'tool_lib/tools.bash'
tool_cmd = "cope-answers"
tool_desc = "create a json text file that gives total answer counts for "

ANSWER_COUNT_SQL = """
    SELECT qc.value question_code, ac.value answer_code, COUNT(DISTINCT qr.participant_id) answer_count
    FROM questionnaire_response_answer qra
    INNER JOIN questionnaire_response qr ON qr.questionnaire_response_id = qra.questionnaire_response_id
    INNER JOIN questionnaire_question qq ON qq.questionnaire_question_id = qra.question_id
    INNER JOIN code qc ON qc.code_id = qq.code_id
    INNER JOIN code ac ON ac.code_id = qra.value_code_id
    INNER JOIN questionnaire_history qh ON qh.questionnaire_id = qr.questionnaire_id
        AND qh.version = qr.questionnaire_version
    WHERE qc.value IN ('msds_11', 'msds_12', 'copect_40_xx15', 'cdc_covid_19_18', 'phq_9_4',
                      'ipaq_1', 'ipaq_3', 'ipaq_5', 'ipaq_7', 'ucla_ls8_9')
        AND qra.end_time IS NULL
        AND qh.last_modified > :cope_month
    GROUP BY qc.value, ac.value;
"""


class CopeAnswersClass(object):
    def __init__(self, args, gcp_env):
        self.args = args
        self.gcp_env = gcp_env

    def _setup_database_connection(self):
        _logger.info("retrieving db configuration...")
        headers = gcp_make_auth_header()
        resp_code, resp_data = make_api_request(
            "{0}.appspot.com".format(self.gcp_env.project), "/rdr/v1/Config/db_config", headers=headers
        )
        if resp_code != 200:
            _logger.error(resp_data)
            _logger.error("failed to retrieve config, aborting.")
            return 1

        passwd = resp_data["rdr_db_password"]
        if not passwd:
            _logger.error("failed to retrieve database user password from config.")
            return 1

        # connect a sql proxy to the current project
        _logger.info("starting google sql proxy...")
        port = random.randint(10000, 65535)
        instances = gcp_format_sql_instance(self.gcp_env.project, port=port)
        proxy_pid = self.gcp_env.activate_sql_proxy(instance=instances, port=port)
        if not proxy_pid:
            _logger.error("activating google sql proxy failed.")
            return 1

    @staticmethod
    def _new_answer_tracker():
        return {
            'total_answers': 0
        }

    @staticmethod
    def _new_question_tracker():
        return {
            'total_answers': 0,
            'answers': {}
        }

    def run(self):
        self._setup_database_connection()
        question_totals = {}
        with database_factory.make_server_cursor_database().session() as session:
            answer_counts = session.execute(ANSWER_COUNT_SQL, params={'cope_month': self.args.cope_month})
            for count in answer_counts:
                # Get the running totals for the question and answer
                # if they don't exist already we'll create them, but they'll need to be saved in the dictionary later
                question_data = question_totals.get(count.question_code, self._new_question_tracker())
                answer_data = question_data['answers'].get(count.answer_code, self._new_answer_tracker())

                question_data['total_answers'] += count.answer_count
                answer_data['total_answers'] += count.answer_count

                # Set the data back on the dictionaries so that we store newly created trackers
                question_data['answers'][count.answer_code] = answer_data
                question_totals[count.question_code] = question_data

        output_file_name = 'results.json'
        with open(output_file_name, 'w') as file:
            file.write(json.dumps(question_totals, indent=4))

        print('SUCCESS: output written to', output_file_name)
        return 0


def run():
    # Set global debug value and setup application logging.
    setup_logging(
        _logger, tool_cmd, "--debug" in sys.argv, "{0}.log".format(tool_cmd) if "--log-file" in sys.argv else None
    )
    setup_i18n()

    # Setup program arguments.
    parser = argparse.ArgumentParser(prog=tool_cmd, description=tool_desc)
    parser.add_argument("--project", help="gcp project name", default="localhost")  # noqa
    parser.add_argument("--account", help="pmi-ops account", default=None)  # noqa
    parser.add_argument("--service-account", help="gcp iam service account", default=None)  # noqa
    parser.add_argument('--cope-month', required=True, help='month of the cope survey (ex. "2020-06" for June COPE)')

    args = parser.parse_args()

    with GCPProcessContext(tool_cmd, args.project, args.account, args.service_account) as gcp_env:
        process = CopeAnswersClass(args, gcp_env)
        exit_code = process.run()
        return exit_code


# --- Main Program Call ---
if __name__ == "__main__":
    sys.exit(run())
