#! /bin/env python
#
# Find COPE survey response counts and breakdowns for each question
#

import json

from rdr_service.dao import database_factory
from rdr_service.tools.tool_libs._tool_base import cli_run, ToolBase

# Tool_cmd and tool_desc name are required.
# Remember to add/update bash completion in 'tool_lib/tools.bash'
tool_cmd = "cope-answers"
tool_desc = "create a json text file that gives total answer counts for the COPE survey of the given month"

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


class CopeAnswersClass(ToolBase):
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
        super(CopeAnswersClass, self).run()

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


def add_additional_arguments(parser):
    parser.add_argument('--cope-month', required=True, help='month of the cope survey (ex. "2020-06" for June COPE)')


def run():
    cli_run(tool_cmd, tool_desc, CopeAnswersClass, add_additional_arguments)
