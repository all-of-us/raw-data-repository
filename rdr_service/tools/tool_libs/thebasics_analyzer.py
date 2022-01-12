#! /bin/env python
#
# Tool for analyzing participant TheBasics responses to identify partials/duplicates vs. full surveys
#

import argparse
import csv
import datetime

# pylint: disable=superfluous-parens
# pylint: disable=broad-except
import logging
import os
import sys
from sqlalchemy.orm import aliased
from sqlalchemy import func

from rdr_service.dao.questionnaire_response_dao import QuestionnaireResponseDao
from rdr_service.model.bq_questionnaires import BQPDRTheBasicsSchema
from rdr_service.model.code import Code
from rdr_service.model.questionnaire import Questionnaire, QuestionnaireConcept
from rdr_service.model.questionnaire_response import QuestionnaireResponse, QuestionnaireResponseAnswer
from rdr_service.model.questionnaire import QuestionnaireQuestion
from rdr_service.services.system_utils import setup_logging, setup_i18n, print_progress_bar
from rdr_service.tools.tool_libs import GCPProcessContext, GCPEnvConfigObject
from rdr_service.participant_enums import QuestionnaireResponsePayloadType

_logger = logging.getLogger("rdr_logger")

# Tool_cmd and tool_desc name are required.
# Remember to add/update bash completion in 'tool_lib/tools.bash'
tool_cmd = "thebasics-analyzer"
tool_desc = "Tool to collect data on participants with partial/multiple TheBasics responses"

# Restrict verbose output of answer data for these fields (PII), by default
REDACTED_FIELDS = BQPDRTheBasicsSchema._force_boolean_fields

# These question codes comprise the list of expected/possible codes that could come in a payload triggered by a
# profile update sent independent of a full TheBasics survey payload
PROFILE_UPDATE_QUESTION_CODES = [
    'PersonOneAddress_PersonOneAddressCity',
    'PersonOneAddress_PersonOneAddressState',
    'PersonOneAddress_PersonOneAddressZipCode',
    'SecondaryContactInfo_PersonOneEmail',
    'SecondaryContactInfo_PersonOneFirstName',
    'SecondaryContactInfo_PersonOneMiddleInitial',
    'SecondaryContactInfo_PersonOneLastName',
    'SecondaryContactInfo_PersonOneAddressOne',
    'SecondaryContactInfo_PersonOneAddressTwo',
    'SecondaryContactInfo_PersonOneTelephone',
    'SecondaryContactInfo_PersonOneRelationship',
    'SecondaryContactInfo_SecondContactsFirstName',
    'SecondaryContactInfo_SecondContactsMiddleInitial',
    'SecondaryContactInfo_SecondContactsLastName',
    'SecondaryContactInfo_SecondContactsAddressOne',
    'SecondaryContactInfo_SecondContactsAddressTwo',
    'SecondContactsAddress_SecondContactCity',
    'SecondContactsAddress_SecondContactZipCode',
    'SecondaryContactInfo_SecondContactsEmail',
    'SecondaryContactInfo_SecondContactsNumber',
    'SecondContactsAddress_SecondContactState',
    'SecondaryContactInfo_SecondContactsRelationship',
    'SocialSecurity_SocialSecurityNumber'
]

# Column headers for TSV export
EXPORT_FIELDS = ['participant_id', 'questionnaire_response_id', 'authored', 'external_id', 'payload_type',
                 'duplicate_of', 'reason']

class TheBasicsAnalyzerClass(object):
    def __init__(self, args, gcp_env: GCPEnvConfigObject, id_list=None):
        """
        :param args: command line arguments.
        :param gcp_env: gcp environment information, see: gcp_initialize().
        """
        self.args = args
        self.gcp_env = gcp_env
        self.id_list = id_list
        self.results = dict()
        self.pids_with_partials = dict()
        self.ro_dao = None

    def get_the_basics_questionnaire_ids(self):
        """ Return a list of all questionnaire_id values associated with TheBasics survey """
        with self.ro_dao.session() as session:
            results = session.query(
                Questionnaire
            ).join(
                QuestionnaireConcept, QuestionnaireConcept.questionnaireId == Questionnaire.questionnaireId
            ).join(
                Code, QuestionnaireConcept.codeId == Code.codeId
            ).filter(
                Code.value == 'TheBasics'
            ).all()
            return [r.questionnaireId for r in results]

    def create_results_tsv(self, fname):
        """
        Generate a tsv file with analysis results.  This export can be fed back into the tool as an import file,
        in order to apply necessary updates to the QuestionnaireResponse table records
        """

        if not len(self.results):
            _logger.warning('No results to export')
            return
        with open(fname, 'wt') as f:
            tsv_writer = csv.writer(f, delimiter='\t')
            tsv_writer.writerow(EXPORT_FIELDS)
            for pid in self.results.keys():
                pid_results = self.results.get(pid)
                if not len(pid_results):
                    continue
                for resp in self.results.get(pid):
                    tsv_writer.writerow(resp)
                # blank row between each pid's results
                tsv_writer.writerow(['' for _ in EXPORT_FIELDS])

    def generate_response_diff(self, curr_response, prior_response=None):
        """
        Inspect two chronologically adjacent TheBasics responses dicts to generate a diff-like summary
        :param curr_response:  A dict of question code keys and answer values
        :param prior_response: A dict of question code keys and answer values
        """
        #  Diff Tuple contains (<diff symbol>, <question code/field name>[, <answer value>])
        #  Diff symbols:
        #         +   Field did not exist in prior response; new/added in the current response
        #         =   Field exists in both prior and current response and has the same answer value
        #         !   Field exists in both prior and current response  but answer value changed in the current response
        #         -   Field existed in the prior response but is missing from the current response
        #
        #  The answer value is included in the diff for new (+) or changed (!) answers; omitted if unchanged (=)
        #  If the prior_response is None (first payload), all content will be displayed as a new (+) answer

        diff_details = list()
        prior_response_keys = prior_response.keys() if prior_response else []
        curr_response_keys = curr_response.keys()
        key_set = set().union(prior_response_keys, curr_response_keys)

        for key in sorted(key_set):
            if key in prior_response_keys and key not in curr_response_keys:
                diff_details.append(('-', key))
            else:
                answer = curr_response.get(key)
                # Redact answers to free text fields (PII) unless they were skipped, or redaction is disabled
                if answer.lower() != 'pmi_skip' and key in REDACTED_FIELDS and not self.args.no_redact:
                    answer = '<redacted>'
                if key in curr_response_keys and key not in prior_response_keys:
                    diff_details.append(('+', key, answer))
                elif prior_response[key] != curr_response[key]:
                    diff_details.append(('!', key, answer))
                else:
                    diff_details.append(('=', key))

        return diff_details

    def get_response_as_dict(self, response_id : int, session=None) -> dict:
        """
        Generate a dict of a TheBasics response.  Includes meta data keys/values and an answers nested dict
        with question code keys and answer values.  The answer value is from a COALESCE of the
        QuestionnaireResponseAnswer table's valueCodeId, valueString, valueBoolean, valueInteger, valueDate,
        and valueDateTime columns
        :param response_id:   Integer questionnaire response id
        :param session:       A DAO session() object, if one has already been instantiated.
        """
        response_dict = {'answers': dict()}
        if not session:
            close_session = True
            session = self.ro_dao.session()
        else:
            close_session = False

        # Possible for the answer_list query below to return nothing in isolated cases where there were no answers in
        # payload.  So, grab the QuestionnaireResponse row separately as well to extract response meta data
        meta_row = session.query(QuestionnaireResponse)\
                   .filter(QuestionnaireResponse.questionnaireResponseId == response_id).first()

        answer = aliased(Code)

        answer_list = session.query(
            QuestionnaireResponse.questionnaireResponseId,
            QuestionnaireResponse.answerHash,
            QuestionnaireResponse.authored,
            QuestionnaireResponse.externalId,
            Code.value.label('question_code_value'),
            func.coalesce(answer.value,
                          QuestionnaireResponseAnswer.valueString,
                          QuestionnaireResponseAnswer.valueBoolean,
                          QuestionnaireResponseAnswer.valueInteger,
                          QuestionnaireResponseAnswer.valueDate,
                          QuestionnaireResponseAnswer.valueDateTime
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
            QuestionnaireResponse.isDuplicate != 1   # TODO:  This column name/value list expected to change
        ).order_by(QuestionnaireResponse.authored,
                   QuestionnaireResponse.created
                   ).all()

        # Build nested dict of question code keys/answer values
        for row in answer_list:
            response_dict['answers'][row.question_code_value] = row.answer_value


        response_dict['answer_count'] = len(response_dict['answers'].keys())
        response_dict['questionnaireResponseId'] = response_id
        response_dict['answerHash'] = meta_row.answerHash if meta_row else None
        response_dict['authored'] = meta_row.authored if meta_row else None
        response_dict['externalId'] = meta_row.externalId if meta_row else None

        if close_session:
            session.close()

        return response_dict

    def output_response_history(self, pid, response_list):
        """
        For --verbose mode, write formatted details of the participant's TheBasics data to stdout
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
                             f'{"External id":52}:\t{ext_id}',
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
        Inspect the entire TheBasics response history for a participant
        It will use the QuestionnaireResponseAnswer data for all the received payloads for this participant to
        look for any that should be marked with a specific QuestionnaireResponsePayloadType value, such as DUPLICATE
        or NO_ANSWER_VALUES.
        :param pid:   Participant ID
        :param response_list:  List of dicts with summary details about each of the participant's TheBasics responses
        """
        if not len(response_list):
            print(f'No data for participant {pid}')
            return

        last_response_answer_set = None
        answer_hashes = [r['answer_hash'] for r in response_list]
        has_completed_survey = False # Track if/when a COMPLETE survey response is detected

        for curr_position in range(len(response_list)):
            curr_response = response_list[curr_position]
            rsp_id = curr_response.get('questionnaire_response_id')
            # COMPLETE payloads don't have their answer data inspected by default; only if more details requested
            # But indicate payloads that could impact RDR/PDR business logic
            if curr_response.get('payload_type') == QuestionnaireResponsePayloadType.COMPLETE:
                if has_completed_survey:
                    response_list[curr_position]['reason'] = 'Multiple complete survey payloads'
                    # Flag if this first completed survey follows some previously appended result
                elif curr_position > 0:
                    response_list[curr_position]['reason'] = 'Partial received before first complete survey'
                has_completed_survey = True
                if not self.args.verbose:
                    continue

            answers = curr_response.get('answers')
            # Some outlier cases where the payload had a FHIR doc containing question codes, but no
            # answer data was sent for any of them.  See:  questionnaire_response_ids 101422823 or 999450910
            # These will be ignored when producing diffs between chronologically adjacent authored responses
            if not answers:
                response_list[curr_position]['payload_type'] = QuestionnaireResponsePayloadType.NO_ANSWER_VALUES
                continue

            # Sets are used here to enable check for subset/superset relationships between responses
            if last_response_answer_set is None:
                # Initialize "last" data to this first response
                last_response_answer_set = set(answers.items())
            else:
                curr_response_answer_set = set (answers.items())
                # index() will find the first location in the answer_hashes list containing the current response's
                # answer hash.  If it doesn't match the current response's position, the current response is a
                # duplicate of an earlier one
                matching_hash_idx = answer_hashes.index(curr_response['answer_hash'])
                if (matching_hash_idx != curr_position and
                        response_list[curr_position]['payload_type'] != QuestionnaireResponsePayloadType.COMPLETE):
                    # Mark this partial payload as a duplicate of the response whose answer hash it matches
                    # TODO:  Determine what to do with COMPLETE payload dups, with/without matching authored?
                    dup_rsp_id = response_list[matching_hash_idx].get('questionnaire_response_id')
                    response_list[curr_position]['payload_type'] = QuestionnaireResponsePayloadType.DUPLICATE
                    response_list[curr_position]['duplicate_of'] = dup_rsp_id
                    response_list[curr_position]['reason'] = 'Duplicate answer hash'

                # Check for the cascading subset/superset multiple response signature.  If the current response
                # is a superset of the last, update the last response's details to mark it as a duplicate of this
                # current response
                elif curr_response_answer_set and curr_response_answer_set.issuperset(last_response_answer_set):
                    response_list[curr_position-1]['payload_type'] = QuestionnaireResponsePayloadType.DUPLICATE
                    response_list[curr_position-1]['duplicate_of'] = rsp_id
                    response_list[curr_position-1]['reason'] = 'Subset of a cascading superset response'

                last_response_answer_set = curr_response_answer_set

        if not has_completed_survey:
            # Flag the last entry with a note that participant has no full survey
            response_list[-1]['reason'] = 'Participant has no COMPLETE survey responses'

        if self.args.verbose:
            print(f'\n===============Results for P{pid}====================\n')
            self.output_response_history(pid, response_list)

        # Updated content for response_list returned
        return response_list

    def process_participant_responses(self, pid, responses, session):
        """
        Evaluate a participant's TheBasics response history.
        :param pid:  Participant ID
        :param responses: QuestionnaireResponse result set of TheBasics responses for this participant, in chronological
                          order by authored time
        """
        if not len(responses):
            raise (ValueError, f'P{pid}: TheBasics response list was empty')

        # Each's pid's results will be saved as a list of dicts (one dict per TheBasics response payload with summary
        # details of a specific questionnaire_response_id's paylaod)
        self.results[pid] = list()
        result_details = list()

        # Track if this participant has something other than completed surveys in their history
        has_partial = False
        for response in responses:
            rsp_id = response.questionnaireResponseId
            response_dict = self.get_response_as_dict(rsp_id, session=session)
            if not len(response_dict.keys()):
                print(f'No response data found for participant {pid} response id {rsp_id}')
                continue
            # A full response is identified by having a populated/"truthy" value for a question code key not in the list
            # of potential profile update question codes.
            full_survey = False
            for field, value in response_dict['answers'].items():
                if field not in PROFILE_UPDATE_QUESTION_CODES and value:
                    full_survey = True
                    break

            # Default duplicate_of and reason fields to None, may be revised in next inspection step
            result_details.append({ 'questionnaire_response_id': response_dict.get('questionnaireResponseId', None),
                                    'authored': response_dict.get('authored', None),
                                    'answer_hash': response_dict.get('answerHash', None),
                                    'external_id' : response_dict.get('externalId', None),
                                    'payload_type': QuestionnaireResponsePayloadType.COMPLETE if full_survey\
                                                    else QuestionnaireResponsePayloadType.PROFILE_UPDATE,
                                    'answers': response_dict.get('answers', None),
                                    'duplicate_of': None,
                                    'reason': None,
            })
            has_partial = has_partial or not full_survey

        # Participants with just a single, full survey TheBasics response won't need additional inspection
        if has_partial or len(result_details) > 1:
            prev_complete = False  # Has a full survey been found yet for this pid?
            # Inspection can flag duplicates and update contents of the result_details list items/dicts
            result_details = self.inspect_responses(pid, result_details)
            for rec in result_details:
                reason = rec['reason']
                if rec['payload_type'] == str(QuestionnaireResponsePayloadType.COMPLETE):
                    if prev_complete:
                        reason = 'Multiple full survey responses'
                    # Flag if this first completed survey follows some previously appended result
                    elif len(self.results[pid]):
                        reason = 'Partial received before complete survey'
                    prev_complete = True
                # Build the summary list of result values for this pid/response
                self.results[pid].append([pid, rec['questionnaire_response_id'],
                                          rec['authored'].strftime("%Y-%m-%d %H:%M:%S"),
                                          rec['external_id'], str(rec['payload_type']),
                                          rec['duplicate_of'], reason
                                          ])
    def run(self):
        """
        Main program process
        :return: Exit code value
        """
        if not (self.args.import_results or self.id_list):
            _logger.error('Nothing to process')
            return 1

        self.gcp_env.activate_sql_proxy(replica=True)

        if self.args.import_results:
            #TODO:  Write method to ingest export results file and do database updates, once the
            # QuestionnaireResponse model is updated
            pass
        else:
            self.ro_dao = QuestionnaireResponseDao()
            basics_ids = self.get_the_basics_questionnaire_ids()
            processed_pid_count = 0
            num_pids = len(self.id_list)
            with self.ro_dao.session() as session:
                for pid in self.id_list:
                    responses = session.query(
                        QuestionnaireResponse
                    ).select_from(
                        QuestionnaireResponse
                    ).filter(
                        QuestionnaireResponse.participantId == pid,
                        QuestionnaireResponse.questionnaireId.in_(basics_ids)
                    ).order_by(
                        QuestionnaireResponse.authored,
                        QuestionnaireResponse.created
                    ).all()

                    if responses:
                        self.process_participant_responses(pid, responses, session)

                    if not self.args.verbose:
                        print_progress_bar(
                            processed_pid_count, num_pids, prefix="{0}/{1}:".format(processed_pid_count, num_pids),
                            suffix="pids processed")
                    processed_pid_count += 1

            if self.args.export_to:
                print(f'\nExporting results to {self.args.export_to}...')
                self.create_results_tsv(self.args.export_to)

def get_id_list(fname):
    """
    :param fname:  The filename passed with the --from-file argument
    :return: A list of integers, or None on missing/empty fname
    """
    filename = os.path.expanduser(fname)
    if not os.path.exists(filename):
        _logger.error(f"File '{fname}' not found.")
        return None

    # read ids from file.
    ids = open(os.path.expanduser(fname)).readlines()
    # convert ids from a list of strings to a list of integers.
    ids = [int(i) for i in ids if i.strip()]
    return ids if len(ids) else None


def run():
    # Set global debug value and setup application logging.
    setup_logging(
        _logger, tool_cmd, "--debug" in sys.argv, "{0}.log".format(tool_cmd) if "--log-file" in sys.argv else None
    )
    setup_i18n()

    # Setup program arguments.
    parser = argparse.ArgumentParser(prog=tool_cmd, description=tool_desc)
    parser.add_argument("--debug", help="enable debug output", default=False, action="store_true")  # noqa
    parser.add_argument("--project", help="gcp project name", default="localhost")  # noqa
    parser.add_argument("--account", help="pmi-ops account", default=None)  # noqa
    parser.add_argument("--service-account", help="gcp iam service account", default=None)  # noqa
    parser.add_argument("--id", help="Analyze TheBasics data for a single participant_id",
                        type=int, default=None)
    parser.add_argument("--from-file", help="Analyze TheBasics data for a list of participant ids in the file",
                        metavar='FILE', type=str, default=None)
    parser.add_argument("--verbose",
                        help="Display participant question/answer content to stdout. Free text redacted by default",
                        default=False, action="store_true")
    parser.add_argument("--no-redact",
                        help="If --verbose, displays unredacted question/answer consent.  CAUTION: can contain PII",
                        default=False, action="store_true"
                        )

    parser.add_argument("--import-results", help="import results from a previous export and use to update RDR data")
    parser.add_argument("--export-to", help="Export results to a tsv file", metavar='OUTPUT_TSV_FILE',
                        type=str, default=None)

    args = parser.parse_args()
    with GCPProcessContext(tool_cmd, args.project, args.account, args.service_account) as gcp_env:
        ids = None
        if hasattr(args, 'import_results') and not args.import_results:
            if hasattr(args, 'from_file') and args.from_file:
                ids = get_id_list(args.from_file)
            else:
                ids = list([int(args.id) if args.id else None])
        process = TheBasicsAnalyzerClass(args, gcp_env, ids)

        exit_code = process.run()
        return exit_code


# --- Main Program Call ---
if __name__ == "__main__":
    sys.exit(run())
