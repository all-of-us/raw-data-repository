#! /bin/env python
#
# Tool for analyzing participant TheBasics responses to identify partials/duplicates vs. full surveys
#

import argparse
import csv
import datetime
import pandas

# pylint: disable=superfluous-parens
# pylint: disable=broad-except
import logging
import os
import sys
from sqlalchemy.orm import aliased
from sqlalchemy import func, update

from rdr_service.code_constants import BASICS_PROFILE_UPDATE_QUESTION_CODES
from rdr_service.dao.questionnaire_response_dao import QuestionnaireResponseDao
from rdr_service.model.bq_questionnaires import BQPDRTheBasicsSchema
from rdr_service.model.code import Code
from rdr_service.model.questionnaire import Questionnaire, QuestionnaireConcept
from rdr_service.model.questionnaire_response import QuestionnaireResponse, QuestionnaireResponseAnswer
from rdr_service.model.questionnaire import QuestionnaireQuestion
from rdr_service.services.system_utils import setup_logging, setup_i18n, print_progress_bar, list_chunks
from rdr_service.tools.tool_libs import GCPProcessContext, GCPEnvConfigObject
from rdr_service.participant_enums import QuestionnaireResponseClassificationType

_logger = logging.getLogger("rdr_logger")

# Tool_cmd and tool_desc name are required.
# Remember to add/update bash completion in 'tool_lib/tools.bash'
tool_cmd = "thebasics-analyzer"
tool_desc = "Tool to collect data on participants with partial/multiple TheBasics responses"

# Restrict verbose output of answer data for these fields (PII), by default
REDACTED_FIELDS = BQPDRTheBasicsSchema._force_boolean_fields

# Column headers for TSV export
EXPORT_FIELDS = ['participant_id', 'questionnaire_response_id', 'current_classification', 'authored', 'external_id',
                 'payload_type', 'duplicate_of', 'reason']

# Rows in a tool export results file marked as COMPLETE (default classification) will not need updating
# NOTE:  Currently only expect DUPLICATE, PROFILE_UPDATE, and NO_ANSWER_VALUES to be assigned by the tool
CLASSIFICATION_UPDATE_VALUES = [str(QuestionnaireResponseClassificationType.DUPLICATE),
                                str(QuestionnaireResponseClassificationType.PROFILE_UPDATE),
                                str(QuestionnaireResponseClassificationType.NO_ANSWER_VALUES),
                                str(QuestionnaireResponseClassificationType.AUTHORED_TIME_UPDATED),
                                str(QuestionnaireResponseClassificationType.PARTIAL)
                                ]


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

    def add_results_to_tsv(self, pid, pid_results):
        """ Add the results generated for a specific participant to the TSV export file """

        # Participants who didn't have a TheBasics response, or only had a single complete survey response, are skipped
        if not len(pid_results) or (len(pid_results) == 1 and 'COMPLETE' in pid_results[0]):
            return

        with open(self.args.export_to, 'a') as f:
            tsv_writer = csv.writer(f, delimiter='\t')
            for rec in pid_results:
                row_values = [pid, rec['questionnaire_response_id'],
                              rec['current_classification'],
                              rec['authored'].strftime("%Y-%m-%d %H:%M:%S") if rec['authored'] else None,
                               rec['external_id'], rec['payload_type'], rec['duplicate_of'], rec['reason']]
                tsv_writer.writerow(row_values)
            # if blank row wanted between each pid's results:
            # tsv_writer.writerow(['' for _ in EXPORT_FIELDS])
        return

    def generate_response_diff(self, curr_response, prior_response=None):
        """
        Inspect two chronologically adjacent TheBasics responses dicts to generate a diff-like summary
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
                # Redact answers to free text fields (PII) unless they were skipped, or redaction is disabled
                if key in curr_response_keys and key not in prior_response_keys:
                    answer_output = answer if (answer.lower() == 'pmi_skip' or key not in REDACTED_FIELDS
                                               or self.args.no_redact) else '<redacted>'
                    diff_details.append(('+', key, answer_output))
                elif prior_response[key] != curr_response[key]:
                    answer_output = answer if (answer.lower() == 'pmi_skip' or key not in REDACTED_FIELDS
                                               or self.args.no_redact) else '<redacted>'
                    diff_details.append(('!', key, answer_output))
                else:
                    diff_details.append(('=', key))

        return diff_details

    def get_response_as_dict(self, response_id : int, session=None) -> dict:
        """
        Generate a dict of a TheBasics response.  Includes meta data keys/values and an answers nested dict
        with question code keys and answer values.  The answer value is from a COALESCE of the
        QuestionnaireResponseAnswer table's value_* columns for each possible answer datatype
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
            QuestionnaireResponse.questionnaireResponseId == response_id
            # QuestionnaireResponse.classificationType != QuestionnaireResponseClassificationType.DUPLICATE
        ).order_by(QuestionnaireResponse.authored,
                   QuestionnaireResponse.created
                   ).all()

        # Build nested dict of question code keys/answer values
        for row in answer_list:
            response_dict['answers'][row.question_code_value] = row.answer_value

        response_dict['answer_count'] = len(response_dict['answers'].keys())
        response_dict['questionnaireResponseId'] = response_id
        response_dict['classificationType'] = meta_row.classificationType if meta_row else None
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
            curr_classification = rsp.get('current_classification')

            curr_answers = response_list[idx].get('answers', None)
            print('\n'.join([f'{"Participant":52}:\tP{pid}',
                             f'{"Questionnaire Response":52}:\t{rsp_id}',
                             f'{"Authored":52}:\t{authored}',
                             f'{"External id":52}:\t{ext_id}',
                             f'{"Answer hash":52}:\t{ans_hash}',
                             f'{"Current classification":52}\t{curr_classification}',
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
                        if curr_authored == response_list[matching_hash_idx].get('authored') :
                            reclassification = QuestionnaireResponseClassificationType.DUPLICATE
                        elif curr_response_type == QuestionnaireResponseClassificationType.COMPLETE:
                            reclassification = QuestionnaireResponseClassificationType.AUTHORED_TIME_UPDATED
                        else:
                            reclassification = QuestionnaireResponseClassificationType.DUPLICATE

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

        if self.args.verbose:
            print(f'\n===============Results for P{pid}====================\n')
            self.output_response_history(pid, response_list)

        if self.args.export_to:
            self.add_results_to_tsv(pid, response_list)

        return response_list

    def process_participant_responses(self, pid, responses, session):
        """
        Evaluate a participant's TheBasics response history.
        :param pid:  Participant ID
        :param responses: QuestionnaireResponse result set of TheBasics responses for this participant, in chronological
                          order by authored time
        :param session:   session object
        """
        if not len(responses):
            raise (ValueError, f'P{pid}: TheBasics response list was empty')

        # Each's pid's responses (one dict per TheBasics response payload) will be gathered into a list of dicts
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
                if field not in BASICS_PROFILE_UPDATE_QUESTION_CODES and value:
                    full_survey = True
                    break

            # Default duplicate_of and reason fields to None/empty string, may be revised in next inspection step
            result_details.append({ 'questionnaire_response_id': response_dict.get('questionnaireResponseId', None),
                                    'authored': response_dict.get('authored', None),
                                    'current_classification':\
                                        str(response_dict.get('classificationType',
                                                              QuestionnaireResponseClassificationType.COMPLETE)),
                                    'answer_hash': response_dict.get('answerHash', None),
                                    'external_id' : response_dict.get('externalId', None),
                                    'payload_type': QuestionnaireResponseClassificationType.COMPLETE if full_survey\
                                                    else QuestionnaireResponseClassificationType.PROFILE_UPDATE,
                                    'answers': response_dict.get('answers', None),
                                    'duplicate_of': None,
                                    'reason': '',
            })
            has_partial = has_partial or not full_survey

        # Participants with just a single, full survey TheBasics response won't need additional inspection (unless
        # verbose mode was requested). Inspection can flag duplicates including cascading subset/superset response cases
        if has_partial or len(result_details) > 1 or self.args.verbose:
            self.inspect_responses(pid, result_details)

    def update_db_records_from_tsv(self):
        """
        Ingest a previously created thebasics-analyzer tool export TSV file and perform related DB updates
        Among other fields, each TSV row has a questionnaire_response_id and the payload_type (classification) to
        be assigned for that response record in the QuestionnaireResponse table
        """
        data_file = self.args.import_results
        if data_file:
            # Import the TSV data into a pandas dataframe
            df = pandas.read_csv(data_file, sep="\t")
            dao = QuestionnaireResponseDao()
            with dao.session() as session:
                print(f'Updating QuestionnaireResponse records from results file {data_file}...')
                for classification in CLASSIFICATION_UPDATE_VALUES:
                    value_dict = {QuestionnaireResponse.classificationType:
                                      QuestionnaireResponseClassificationType(classification)}
                    # Filter the matching dataframe rows that were assigned this classification and extract the
                    # questionnaire_response_id values from those rows into a list.
                    result_df = df.loc[df['payload_type'] == classification, 'questionnaire_response_id']
                    response_ids = [val for index, val in result_df.items()]
                    processed = 0
                    ids_to_update = len(response_ids)
                    if ids_to_update:
                        print(f'{ids_to_update} records will be classified as {classification}')
                        # list_chunks() yields sublist chunks up to a max size from the specified list
                        for id_batch in list_chunks(response_ids, 1000):
                            query = (
                                update(QuestionnaireResponse)
                                .values(value_dict)
                                .where(QuestionnaireResponse.questionnaireResponseId.in_(id_batch))
                            )
                            session.execute(query)
                            session.commit()
                            processed += len(id_batch)
                            print_progress_bar(processed, ids_to_update,
                                               prefix="{0}/{1}:".format(processed, ids_to_update),
                                               suffix="records updated")
                    else:
                        print(f'No records of classification {classification} in import file')
        else:
            _logger.error('No import file specified')

    def run(self):
        """
        Main program process
        :return: Exit code value
        """
        if not (self.args.import_results or self.id_list):
            _logger.error('Nothing to process')
            return 1

        # TODO:  For now, to perform DB updates, the records to be updated must be imported from a previous export
        # Updates will not occur automatically during the analysis / processing of participant responses
        if self.args.import_results:
            # Uses the main database to perform writes/updates
            self.gcp_env.activate_sql_proxy(replica=False)
            self.update_db_records_from_tsv()
        else:
            # Write out the header row to a fresh/truncated export file, if export was specified
            if self.args.export_to:
                with open(self.args.export_to, 'w') as f:
                    tsv_writer = csv.writer(f, delimiter='\t')
                    tsv_writer.writerow(EXPORT_FIELDS)

            # operations other than import use the read-only replica
            self.gcp_env.activate_sql_proxy(replica=True)
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
                        # QuestionnaireResponse.classificationType != QuestionnaireResponseClassificationType.DUPLICATE,
                        QuestionnaireResponse.questionnaireId.in_(basics_ids)
                    ).order_by(
                        QuestionnaireResponse.authored,
                        QuestionnaireResponse.created
                    ).all()

                    if responses:
                        self.process_participant_responses(pid, responses, session)

                    processed_pid_count += 1
                    if not self.args.verbose:
                        print_progress_bar(
                            processed_pid_count, num_pids, prefix="{0}/{1}:".format(processed_pid_count, num_pids),
                            suffix="pids processed")


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
