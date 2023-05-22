#! /bin/env python
#
# Tool for analyzing participant TheBasics responses to identify partials/duplicates vs. full surveys
#

import datetime

# pylint: disable=superfluous-parens
# pylint: disable=broad-except
import logging
from sqlalchemy.orm import aliased
from sqlalchemy import func

from rdr_service.code_constants import BASICS_PROFILE_UPDATE_QUESTION_CODES
from rdr_service.model.code import Code
from rdr_service.model.questionnaire import Questionnaire, QuestionnaireConcept
from rdr_service.model.questionnaire_response import QuestionnaireResponse, QuestionnaireResponseAnswer
from rdr_service.model.questionnaire import QuestionnaireQuestion
from rdr_service.tools.tool_libs.tool_base import ToolBase, cli_run
from rdr_service.participant_enums import QuestionnaireResponseClassificationType

_logger = logging.getLogger("rdr_logger")

# Tool_cmd and tool_desc name are required.
# Remember to add/update bash completion in 'tool_lib/tools.bash'
tool_cmd = "module-data-analyzer"
tool_desc = "Tool to display and analyze module / questionnaire response data"

class ModuleDataAnalyzer(ToolBase):

    # Will contain a list of question code strings that have free text answer values needing redaction
    redacted_fields = []

    def get_module_questionnaire_ids(self, module: str, session):
        """ Return a list of all questionnaire_id values associated with the module/survey name """

        if not module:
            raise ValueError('Missing module string for questionnaire_id lookup')

        results = session.query(
            Questionnaire
        ).join(
            QuestionnaireConcept, QuestionnaireConcept.questionnaireId == Questionnaire.questionnaireId
        ).join(
            Code, QuestionnaireConcept.codeId == Code.codeId
        ).filter(
            Code.value == module
        )
        q_ids = [r.questionnaireId for r in results.all()]
        if not q_ids:
            raise ValueError(f'No questionnaire_id values found for module {module}')

        return q_ids

    def get_response_as_dict(self, response_id : int, session) -> dict:
        """
        Generate a dict of a module response.  Includes metadata keys/values and an answers nested dict
        with question code keys and answer values.  The answer value is from a COALESCE of the
        QuestionnaireResponseAnswer table's value_* columns for each possible answer datatype
        :param response_id:   Integer questionnaire response id
        :param session:       A DAO session() object, if one has already been instantiated.
        """
        response_dict = {'answers': dict()}
        if not session:
            raise RuntimeError('session object has not been instantiated')

        # Possible for the answer_list query below to return nothing in isolated cases where there were no answers in
        # payload.  So, grab the QuestionnaireResponse row separately as well to extract response metadata
        meta_row = session.query(QuestionnaireResponse)\
                   .filter(QuestionnaireResponse.questionnaireResponseId == response_id).first()

        answer = aliased(Code)

        answer_list = session.query(
            QuestionnaireResponse.questionnaireResponseId,
            QuestionnaireResponse.answerHash,
            QuestionnaireResponse.authored,
            QuestionnaireResponse.externalId,
            QuestionnaireResponseAnswer.valueString,  # used for determining redaction logic
            Code.value.label('question_code_value'),
            func.coalesce(answer.value,
                          QuestionnaireResponseAnswer.valueBoolean,
                          QuestionnaireResponseAnswer.valueDate,
                          QuestionnaireResponseAnswer.valueDateTime,
                          QuestionnaireResponseAnswer.valueDecimal,
                          QuestionnaireResponseAnswer.valueInteger,
                          QuestionnaireResponseAnswer.valueString,
                          QuestionnaireResponseAnswer.valueSystem,
                          QuestionnaireResponseAnswer.valueUri
                          ).label('answer_value')
        ).select_from(
            QuestionnaireResponse
        ).join(
            QuestionnaireResponseAnswer
        ).join(
            QuestionnaireQuestion,
            QuestionnaireResponseAnswer.questionId == QuestionnaireQuestion.questionnaireQuestionId
        ).join(
            Code, QuestionnaireQuestion.codeId == Code.codeId
        ).outerjoin(
            answer, QuestionnaireResponseAnswer.valueCodeId == answer.codeId
        ).filter(
            QuestionnaireResponse.questionnaireResponseId == response_id,
            QuestionnaireResponse.classificationType != QuestionnaireResponseClassificationType.DUPLICATE
        ).order_by(QuestionnaireResponse.authored,
                   QuestionnaireResponse.created
                   ).all()

        # Build nested dict of question code keys/answer values
        for row in answer_list:
            ans = row.answer_value
            if row.question_code_value in response_dict['answers']:
                # Multi-select answer case; concatenate selections
                prev_ans = response_dict['answers'][row.question_code_value]
                ans = ','.join([prev_ans, ans])

            response_dict['answers'][row.question_code_value] = ans

            # Keep track of which question codes/fields had free text answers that should be redacted
            if (row.answer_value and row.valueString == row.answer_value
                   and row.question_code_value not in self.redacted_fields):
                self.redacted_fields.append(row.question_code_value)

        response_dict['answer_count'] = len(response_dict['answers'].keys())
        response_dict['questionnaireResponseId'] = response_id
        response_dict['classificationType'] = meta_row.classificationType if meta_row else None
        response_dict['answerHash'] = meta_row.answerHash if meta_row else None
        response_dict['authored'] = meta_row.authored if meta_row else None
        response_dict['externalId'] = meta_row.externalId if meta_row else None

        return response_dict

    def generate_response_diff(self, curr_response, prior_response=None):
        """
        Inspect two chronologically adjacent module responses dicts to generate a diff-like summary
        :param curr_response:  A dict of question code keys and answer values
        :param prior_response: A dict of question code keys and answer values
        """
        diff_details = list()
        prior_response_keys = prior_response.keys() if prior_response else []
        curr_response_keys = curr_response.keys()
        key_set = set().union(prior_response_keys, curr_response_keys)
        #  Diff Tuple contains (<diff symbol>, <question code/field name>[, <answer value>])
        #  Diff symbols:
        #         +   Field did not exist in prior response; new/added in the current response
        #         =   Field exists in both prior and current response and has the same answer value
        #         !   Field exists in both prior and current response  but answer value changed in the current response
        #         -   Field existed in the prior response but is missing from the current response
        #
        #  The answer value is included in the diff for new (+) or changed (!) answers; omitted if unchanged (=)
        #  If the prior_response is None (first payload), all content will be displayed as a new (+) answer
        for key in sorted(key_set):
            if key in prior_response_keys and key not in curr_response_keys:
                diff_details.append(('-', key))
            else:
                answer = curr_response.get(key)
                # Redact answers to free text fields (PII risk) unless they were skipped, or redaction is disabled
                if key in curr_response_keys and key not in prior_response_keys:
                    answer_output = answer if (answer.lower() == 'pmi_skip' or key not in self.redacted_fields
                                               or self.args.no_redact) else '<redacted>'
                    diff_details.append(('+', key, answer_output))
                elif prior_response[key] != curr_response[key]:
                    answer_output = answer if (answer.lower() == 'pmi_skip' or key not in self.redacted_fields
                                               or self.args.no_redact) else '<redacted>'
                    diff_details.append(('!', key, answer_output))
                else:
                    diff_details.append(('=', key))

        return diff_details

    def output_response_history(self, pid, response_list):
        """
        Write formatted details of the participant's TheBasics data to stdout
        """

        last_answers = None
        last_response_id = None
        for idx in range(len(response_list)):
            rsp = response_list[idx]
            rsp_id = rsp.get('questionnaire_response_id', None)
            payload = str(rsp.get('payload_type', 'UNKNOWN'))
            ext_id = rsp.get('external_id', None)
            authored = datetime.datetime.strftime(rsp.get('authored'), '%Y-%m-%dT%H:%M:%S')
            dup_of = rsp.get('duplicate_of', None)
            reason = rsp.get('reason', '')
            ans_hash = rsp.get('answer_hash')

            curr_answers = response_list[idx].get('answers', None)
            print('\n'.join([f'{"Participant":52}:\tP{pid}',
                             f'{"Questionnaire Response":52}:\t{rsp_id}',
                             f'{"Authored":52}:\t{authored}',
                             f'{"Form entry id (external id)":52}:\t{ext_id}',
                             f'{"Answer hash":52}:\t{ans_hash}',
                             f'{"Payload inspection result":52}:\t{payload}',
                             f'{"Duplicate of":52}:\t{int(dup_of) if dup_of else None}',
                             f'{"Reason":52}:\t{reason}']))

            diff = self.generate_response_diff(curr_answers, last_answers)
            if len(diff):
                if idx > 0:
                    # This is not the first response in the participant's list
                    print(f'\nResponse {rsp_id} content vs. last response {last_response_id}')
                else:
                    print(f'\nResponse {rsp_id} content:')

                for line in diff:
                    if len(line) == 3:
                        # line is tuple: (<diff symbol +! >, <question code>, <answer>)
                        print(f'{line[0]} {line[1]:50}:\t{line[2]}')
                    else:
                        # Line is tuple(<diff symbol =- >, <question code>)
                        print(f'{line[0]} {line[1]}')

            print('\n')
            last_answers = curr_answers
            last_response_id = rsp_id

    def inspect_responses(self, pid, response_list):
        """
        Inspect the entire module response history for a participant
        It will use the QuestionnaireResponseAnswer data for all the received payloads for this participant to
        look for any that should be marked with a specific QuestionnaireResponseClassificationType value, such as
        DUPLICATE or NO_ANSWER_VALUES.  Requires comparing adjacent responses to find subset/superset DUPLICATE cases
        :param pid:   Participant ID
        :param response_list:  List of dicts with summary details about each of the participant's TheBasics responses
        """
        if not len(response_list):
            print(f'No data for participant {pid}')
            return

        last_response_answer_set, last_authored, last_response_type = (None, None, None)
        last_position = 0
        answer_hashes = [r['answer_hash'] for r in response_list]
        has_completed_survey = False  # Track if/when a COMPLETE survey response is detected

        for curr_position in range(len(response_list)):
            curr_response = response_list[curr_position]
            curr_authored, curr_response_type, curr_rsp_id = (curr_response.get('authored', None),
                                                              curr_response.get('payload_type', None),
                                                              curr_response.get('questionnaire_response_id', None))
            # Flag indeterminate ordering for two payloads w/ identical authored timestamps but different classification
            if last_authored and last_authored == curr_authored and last_response_type != curr_response_type:
                curr_response['reason'] = 'Same authored ts as last payload (indeterminate order)'

            if curr_response_type == QuestionnaireResponseClassificationType.COMPLETE:
                # Notable if more than one COMPLETED survey is encountered, or if the first COMPLETE survey was
                # not the first response in the participant's history.  Does not impact classification
                if has_completed_survey:
                    response_list[curr_position]['reason'] = ' '.join([response_list[curr_position]['reason'],
                                                                       'Multiple complete survey payloads'])
                elif curr_position > 0:
                    response_list[curr_position]['reason'] = ' '.join([response_list[curr_position]['reason'],
                                                                       'Partial received before first complete survey'])
                has_completed_survey = True

            answers = curr_response.get('answers')
            # Some outlier cases where the payload had a FHIR doc containing question codes, but no
            # answer data was sent for any of them.  See:  questionnaire_response_ids 101422823 or 999450910
            # These will be ignored when producing diffs between chronologically adjacent authored responses
            if not answers:
                response_list[curr_position]['payload_type'] = QuestionnaireResponseClassificationType.NO_ANSWER_VALUES
                curr_response_answer_set = None
            else:
                # Sets are used here to enable check for subset/superset relationships between response data
                curr_response_answer_set = set(answers.items())
                if last_response_answer_set is not None:
                    # index() will find the first location in the answer_hashes list containing the current response's
                    # answer hash.  If it doesn't match the current response's position, the current response is
                    # a duplicate (in answer content) of the earlier response.  Set classification based on whether
                    # authored timestamp changed
                    matching_hash_idx = answer_hashes.index(curr_response['answer_hash'])
                    if matching_hash_idx != curr_position:
                        if curr_authored == response_list[matching_hash_idx].get('authored'):
                            reclassification = QuestionnaireResponseClassificationType.DUPLICATE
                        else:
                            reclassification = QuestionnaireResponseClassificationType.AUTHORED_TIME_UPDATED

                        dup_rsp_id = response_list[matching_hash_idx].get('questionnaire_response_id')
                        # Update the current response's classification
                        response_list[curr_position]['payload_type'] = reclassification
                        response_list[curr_position]['duplicate_of'] = dup_rsp_id
                        response_list[curr_position]['reason'] = ' '.join([response_list[curr_position]['reason'],
                                                                           'Duplicate answer hash'])

                    # Check for the cascading response signature where last/subset is made a dup of current/superset
                    elif (curr_response_answer_set and curr_response_answer_set.issuperset(last_response_answer_set)
                          and last_position > 0):
                        response_list[last_position]['payload_type'] = \
                            QuestionnaireResponseClassificationType.DUPLICATE
                        response_list[last_position]['duplicate_of'] = curr_rsp_id
                        response_list[last_position]['reason'] = ' '.join([response_list[curr_position-1]['reason'],
                                                                             'Subset of a cascading superset response'])

            last_authored = response_list[curr_position]['authored']
            last_response_type = response_list[curr_position]['payload_type']
            last_response_answer_set = curr_response_answer_set
            last_position = curr_position

        if not has_completed_survey:
            # Flag the last entry with a note that participant has no full survey
            response_list[-1]['reason'] = ' '.join([response_list[-1]['reason'],
                                                    'Participant has no COMPLETE survey responses'])

        print(f'\n===============Results for P{pid}====================\n')
        self.output_response_history(pid, response_list)

    def process_participant_responses(self, pid, responses, session):
        """
        Evaluate a participant's module response history for the specified module (--module argument).
        :param pid:  Participant ID
        :param responses: QuestionnaireResponse result set of module responses for this participant, in chronological
                          order by authored time
        :param session:   session object
        """
        if not len(responses):
            raise (ValueError, f'P{pid}: {self.args.module} response list was empty')

        # Each's pid's responses (one dict per response payload) will be gathered into a list of dicts
        result_details = list()

        # Track if this participant has something other than completed surveys in their history
        has_partial = False
        for response in responses:
            rsp_id = response.questionnaireResponseId
            response_dict = self.get_response_as_dict(rsp_id, session=session)
            if not len(response_dict.keys()):
                print(f'No response data found for participant {pid} response id {rsp_id}')
                continue

            # Extra processing for TheBasics: Distinguish profile update paylaods from full surveys.  Could potentially
            # be extended to recognize ConsentPII profile update payloads that pre-date the profile update API?
            full_survey = True if self.args.module != 'TheBasics' else False
            if not full_survey:
                for field, value in response_dict['answers'].items():
                    if field not in BASICS_PROFILE_UPDATE_QUESTION_CODES and value:
                        # Found a question code/answer other than what's on the profile update question codes list
                        full_survey = True
                        break

            # Default duplicate_of and reason fields to None/empty string, may be revised in next inspection step
            result_details.append({'questionnaire_response_id': response_dict.get('questionnaireResponseId', None),
                                   'authored': response_dict.get('authored', None),
                                   'current_classification': \
                                       str(response_dict.get('classificationType',
                                                             QuestionnaireResponseClassificationType.COMPLETE)),
                                   'answer_hash': response_dict.get('answerHash', None),
                                   'external_id': response_dict.get('externalId', None),
                                   'payload_type': QuestionnaireResponseClassificationType.COMPLETE if full_survey \
                                       else QuestionnaireResponseClassificationType.PROFILE_UPDATE,
                                   'answers': response_dict.get('answers', None),
                                   'duplicate_of': None,
                                   'reason': '',
                                   })
            has_partial = has_partial or not full_survey

        self.inspect_responses(pid, result_details)

    def run(self):
        super(ModuleDataAnalyzer, self).run()
        if self.args.pid:
            participant_id_list = [int(i) for i in self.args.pid.split(',')]
        elif self.args.from_file:
            participant_id_list = self.get_int_ids_from_file(self.args.from_file)

        if not participant_id_list:
            logging.error('Must specify participant ids via either --pid or --from-file argument')
            return 1

        with self.get_session() as session:
            q_ids = self.get_module_questionnaire_ids(self.args.module, session)
            if not q_ids:
                logging.error(f'Unable to find questionnaire_id list for module {self.module}')
                return 1

            # Get all the responses for the specified module for htis participant, in chronological by authored order
            for pid in participant_id_list:
                responses = session.query(
                    QuestionnaireResponse
                ).select_from(
                    QuestionnaireResponse
                ).filter(
                    QuestionnaireResponse.participantId == pid,
                    QuestionnaireResponse.questionnaireId.in_(q_ids)
                ).order_by(
                    QuestionnaireResponse.authored,
                    QuestionnaireResponse.created
                ).all()

                if responses:
                    print(f'{len(responses)} {self.args.module} responses found for participant {pid} ')
                    self.process_participant_responses(pid, responses, session)
                else:
                    _logger.info(f'No {self.args.module} questionnaire_response records found for participant {pid}')

def add_additional_arguments(parser):
    parser.add_argument('--pid', required=False,
                        help="Single participant id or comma-separated list of participant id integer values")
    parser.add_argument('--from-file', required=False,
                        help="file of integer participant id values")
    parser.add_argument('--module', default='TheBasics',
                        help='Module name / code value (e.g., TheBasics, EHRConsentPII, etc.).  Default: TheBasics')
    parser.add_argument("--no-redact",
                        help="Displays unredacted question/answer consent.  CAUTION: can contain PII",
                        default=False, action="store_true"
                        )

def run():
    return cli_run(tool_cmd, tool_desc, ModuleDataAnalyzer, parser_hook=add_additional_arguments, replica=True)
