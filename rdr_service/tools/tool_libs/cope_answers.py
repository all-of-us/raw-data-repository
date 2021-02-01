#! /bin/env python
#
# Find COPE survey response counts and breakdowns for each question
#

import json

from rdr_service import config
from rdr_service.tools.tool_libs.tool_base import cli_run, ToolBase

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
    WHERE qc.value IN :codes
        AND qra.end_time IS NULL
        AND qh.external_id in :external_ids_for_month
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

    def find_external_ids_for_month(self, cope_month):
        server_config = self.get_server_config()

        for external_ids, month in server_config[config.COPE_FORM_ID_MAP].items():
            if cope_month.lower() == month.lower():
                return external_ids.split(',')

    def run(self):
        super(CopeAnswersClass, self).run()

        codes = [code.strip() for code in self.args.codes.split(',')]
        external_ids = self.find_external_ids_for_month(self.args.cope_month)

        question_totals = {}
        with self.get_session() as session:
            answer_counts = session.execute(ANSWER_COUNT_SQL, params={
                'codes': codes,
                'external_ids_for_month': external_ids
            })
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

        output_file_name = f'{self.args.cope_month.capitalize()}CopeSurveyResults.json'
        with open(output_file_name, 'w') as file:
            file.write(json.dumps(question_totals, indent=4))

        print('SUCCESS: output written to', output_file_name)
        return 0


def add_additional_arguments(parser):
    parser.add_argument('--cope-month', required=True, help='month of the cope survey (ex. Oct or June)')
    parser.add_argument('--codes', required=True,
                        help='Codes to aggregate for the COPE survey '
                             'in a quoted and comma separated string (ex. "ab_1, cd2")')


def run():
    cli_run(tool_cmd, tool_desc, CopeAnswersClass, add_additional_arguments)
