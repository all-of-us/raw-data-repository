#! /bin/env python
#
# Tool for analyzing participant TheBasics responses to identify partials/duplicates vs. full surveys
#

import argparse
import copy
import csv
import pprint

# pylint: disable=superfluous-parens
# pylint: disable=broad-except
import logging
import os
import sys
from sqlalchemy.orm import aliased
from sqlalchemy import func

from rdr_service.dao.questionnaire_response_dao import QuestionnaireResponseDao
from rdr_service.model.bigquery_sync import BigQuerySync
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
tool_desc = "One-off tool to collect data on participants whose TheBasics data may require remediation to correct"

REDACTED_FIELDS = BQPDRTheBasicsSchema._force_boolean_fields

# Fields/keys from a bigquery_sync resource dict that are not question code fields.  Excluded from response analysis
NON_QUESTION_CODE_FIELDS = [
   'id', 'created', 'modified', 'authored', 'language', 'participant_id', 'questionnaire_response_id',
    'questionnaire_id', 'external_id', 'status', 'status_id'
]

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

    @staticmethod
    def compare_response_dicts(curr_response, prior_response=None):
        """
        Inspect two chronologically adjacent TheBasics responses dicts to generate a diff-like summary.  Consists of
        tuples of the field (question code) and an indicator on whether it existed / whether the answer value changed
        from a prior (chronologically adjacent) response.
        """
        #  Diff Tuple is a (<diff symbol>, <question code/field name>)
        #  Diff symbols:
        #         +   Field did not exist in prior response and is new in the current response
        #         =   Field exists in both prior and current response and has the same answer value in both
        #         !   Field exists in both prior and current response  but answer value changed in the current response
        #         -   Field existed in the prior response but is missing from the current response
        #
        #    Example:  when so-called "cascading" responses for a profile update exist where each subsequent
        #    response contains the same question/answer values from a previous response plus a new question/answer
        #    value, the diff data looks like:
        #                 First questionnaire_response_id 783438096 contains only the secondary contact's first name
        #                 [('+', 'SecondaryContactInfo_SecondContactsFirstName')]
        #
        #                 Diff of response id 794746324 vs. 783438096 (now has middle initial):
        #                 [('=', 'SecondaryContactInfo_SecondContactsFirstName'),
        #                  ('+', 'SecondaryContactInfo_SecondContactsMiddleInitial')]
        #
        #                 Diff of response id 802561481 vs. 794746324 (now has last name value):
        #                 [('=', 'SecondaryContactInfo_SecondContactsFirstName'),
        #                  ('+', 'SecondaryContactInfo_SecondContactsLastName'),
        #                  ('=', 'SecondaryContactInfo_SecondContactsMiddleInitial')]

        diff_details = list()
        prior_response_keys = prior_response.keys() if prior_response else []
        curr_response_keys = curr_response.keys()
        key_set = set().union(prior_response_keys, curr_response_keys)

        for key in sorted(key_set):
            if key in prior_response_keys and key not in curr_response_keys:
                diff_details.append(('-', key))
            elif key in curr_response_keys and key not in prior_response_keys:
                diff_details.append(('+', key))
            elif prior_response[key] != curr_response[key]:
                diff_details.append(('!', key))
            else:
                diff_details.append(('=', key))

        return diff_details

    def basics_pretty_print(self, response_dict):
        """
        Pretty print the basics response dict.  Redact free text responses unless --no-redact was specified
        """
        if not self.args.no_redact:
            for key in response_dict.keys():
                if key in REDACTED_FIELDS and response_dict[key] and response_dict[key].lower() != 'pmi_skip':
                    response_dict[key] = '*redacted*'

        pprint.pprint(response_dict)

    def inspect_responses(self, pid, response_list):
        """
        Inspect the entire TheBasics response history for a participant
        It will use the QuestionnaireResponseAnswer data for all the received payloads for this participant to
        look for any that should be marked with a specific QuestionnaireResponsePayloadType value, such as DUPLICATE
        or NO_ANSWER_VALUES.  It can also output diff details if requested
        :param pid:   Participant ID
        :param response_list:  List of dicts with summary details about each of the participant's TheBasics responses
        """
        answer = aliased(Code)
        show_details = self.args.verbose or self.args.show_diffs   # extra output to stdout requested
        with self.ro_dao.session() as session:
            last_response_dict = None
            # Sets are used here to enable check for subset/superset relationships between responses
            last_response_answer_set = None
            curr_response_answer_set = None
            # Make a list of all the QuestionnaireResponse answer_hash values, for finding duplicates
            answer_hashes = [r['answer_hash'] for r in response_list]
            if self.args.verbose:
                print(f'\nP{pid}:\n')

            for curr_position in range(len(response_list)):
                curr_response = response_list[curr_position]
                rsp_id = curr_response.get('questionnaire_response_id')
                # COMPLETE payloads don't have their answer data inspected by default; only if more details requested
                if curr_response.get('payload_type') == QuestionnaireResponsePayloadType.COMPLETE and not show_details:
                    continue

                response_dict = dict()
                # Retrieve all the question/answer values for this response
                answer_list = session.query(
                    QuestionnaireResponse.questionnaireResponseId,
                    QuestionnaireResponse.answerHash,
                    QuestionnaireResponse.authored,
                    Code.value.label('question_code_value'),
                    Code.codeId,
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
                    QuestionnaireResponse.questionnaireResponseId == rsp_id,
                    QuestionnaireResponse.isDuplicate == 0
                ).order_by(QuestionnaireResponse.authored,
                           QuestionnaireResponse.created
                ).all()

                # Some outlier cases where the payload had a FHIR doc containing question codes, but no
                # answer data was sent for any of them.  See:  questionnaire_response_ids 101422823 or 999450910
                # These will be ignored when producing diffs between chronologically adjacent authored responses
                if not answer_list:
                    response_list[curr_position]['payload_type'] = QuestionnaireResponsePayloadType.NO_ANSWER_VALUES
                    if show_details:
                        pprint.pprint(response_list[curr_position])
                        print('\n')
                    continue

                if last_response_dict is None:
                    # Initialize "last" data dict to this (first) response
                    last_response_id = response_list[0].get('questionnaire_response_id')
                    last_response_dict = dict()
                    for row in answer_list:
                        last_response_dict[row.question_code_value] = row.answer_value

                    last_response_answer_set = set(last_response_dict.items())
                    if show_details:
                        pprint.pprint({k: response_list[curr_position][k] for k in ['questionnaire_response_id',
                                                                          'authored', 'answer_hash']})
                        pprint.pprint(self.compare_response_dicts(last_response_dict, None))
                        if self.args.verbose:
                            self.basics_pretty_print(last_response_dict)
                        print('\n')
                else:
                    for row in answer_list:
                        response_dict[row.question_code_value] = row.answer_value
                        curr_response_answer_set = set(response_dict.items())

                    # index() will find the first location in the answer_hashes list containing this response's
                    # answer hash.  If it doesn't match the current response's position, the current response is a
                    # duplicate of an earlier one
                    matching_hash_idx = answer_hashes.index(row.answerHash)
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

                    if show_details:
                        pprint.pprint({k: response_list[curr_position][k] for k in ['questionnaire_response_id',
                                                                          'authored', 'answer_hash']})
                        diff = self.compare_response_dicts(response_dict, last_response_dict)
                        if len(diff):
                            print(f'Response {rsp_id} changes vs. last response  {last_response_id}:')
                            pprint.pprint(diff)
                        else:
                            print(f'Response {rsp_id} has same content as response {last_response_id}')

                        if self.args.verbose:
                            # Verbose also outputs all the question/answer data
                            self.basics_pretty_print(response_dict)

                        print('\n')

                    last_response_answer_set = curr_response_answer_set
                    last_response_dict = copy.deepcopy(response_dict)
                    last_response_id = rsp_id

            if show_details:
                print(f'\nResults for participant {pid}:\n=======================================\n')
                pprint.pprint(response_list)
                print('\n')

        return response_list

    def process_participant_responses(self, pid, responses, session):
        """
        Evaluate a participant's TheBasics response history.  This uses the generated data for the BigQuery
        pdr_mod_thebasics table (created when TheBasics responses are received by the RDR) to evaluate which
        question and answer codes were part of the response in order to assess the type of payload received (i.e.,
        partial/profile update vs. full survey)

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

        # Using the JSON resource data/dict from the generated PDR record to determine full vs. partial responses
        # Example resource field for a pdr_mod_the_basics record:
        #            {
        #             "status": "COMPLETED",
        #             "created": "2021-12-08T20:48:49",
        #             "authored": "2021-12-08T20:48:49",
        #             "language": null,
        #             "status_id": 1,
        #             "external_id": "Vibrent_FORM_ID_284",
        #             "participant_id": 765887715,
        #             "Disability_Deaf": null,
        #             "Disability_Blind": null,
        #             "questionnaire_id": 2,
        #             "AIAN_AIANSpecific": null,
        #             "MENA_MENASpecific": null,
        #             "NHPI_NHPISpecific": null,
        #             "TheBasics_Birthplace": null,
        #             "Gender_GenderIdentity": "GenderIdentity_Woman",
        #             "HomeOwn_CurrentHomeOwn": null,
        #             "questionnaire_response_id": 823960215,
        #             (... more keys correspond to all TheBasics question codes and their answer values...)
        #             "RaceEthnicityNoneOfThese_RaceEthnicityFreeTextBox": 0
        #           }
        #
        # A full response is identified by having a populated/"truthy" value for a question code key not in the list
        # of potential profile update question codes.   Example above would be identified as a full response based
        # on the Gender_GenderIdentity question code key having a populated value.
        for response in responses:
            rsp_id = response.questionnaireResponseId
            pdr_resource_rec = session.query(
                BigQuerySync
            ).filter(BigQuerySync.tableId == 'pdr_mod_thebasics',
                BigQuerySync.pk_id == rsp_id
            ).first()

            if pdr_resource_rec:
                data = pdr_resource_rec.resource
                full_survey = False
                for field, value in data.items():
                    # Ignore metadata fields in the resource data such as participant_id, authored, created, etc.
                    if field in NON_QUESTION_CODE_FIELDS:
                        continue
                    # Look for a "truthy" response value for any question code not in the list of profile update codes
                    if field not in PROFILE_UPDATE_QUESTION_CODES and value:
                        full_survey = True
                        break
            else:
                print(f'No bigquery_sync pdr_mod_thebasics record for response id/pk_id {rsp_id}')

            # Default duplicate_of and reason fields to None for now, may be revised in next inspection step
            result_details.append({ 'questionnaire_response_id': response.questionnaireResponseId,
                                    'authored': response.authored,
                                    'answer_hash': response.answerHash,
                                    'payload_type': QuestionnaireResponsePayloadType.COMPLETE if full_survey\
                                                    else QuestionnaireResponsePayloadType.PROFILE_UPDATE,
                                    'duplicate_of': None,
                                    'reason': None,
                                    'external_id': response.externalId,
            })
            has_partial = has_partial or not full_survey

        # Participants with only a single, full survey TheBasics response won't need additional inspection
        if has_partial or len(result_details) > 1:
            prev_complete = False  # Has a full survey been found yet for this pid?
            # Inspection can flag duplicates and update contents of the result_details list items/dicts
            result_details = self.inspect_responses(pid, result_details)
            for rec in result_details:
                reason = rec['reason']
                if rec['payload_type'] == QuestionnaireResponsePayloadType.COMPLETE:
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

                    if not self.args.verbose and not self.args.show_diffs:
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
    parser.add_argument("--show-diffs",
                        help="Display which question/answer content changed between participant responses to stdout",
                        default=False, action="store_true")
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
