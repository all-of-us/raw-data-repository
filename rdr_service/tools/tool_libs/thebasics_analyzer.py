#! /bin/env python
#
# Template for RDR tool python program.
#

import argparse
import copy
import pprint

# pylint: disable=superfluous-parens
# pylint: disable=broad-except
import logging
import os
import sys
# from dateutil.relativedelta import relativedelta
from sqlalchemy.orm import aliased
from sqlalchemy import func

from rdr_service.dao.questionnaire_response_dao import QuestionnaireResponseDao
from rdr_service.model.bigquery_sync import BigQuerySync
from rdr_service.model.code import Code
from rdr_service.model.questionnaire import Questionnaire, QuestionnaireConcept
from rdr_service.model.questionnaire_response import QuestionnaireResponse, QuestionnaireResponseAnswer
from rdr_service.model.questionnaire import QuestionnaireQuestion
from rdr_service.services.system_utils import setup_logging, setup_i18n
from rdr_service.tools.tool_libs import GCPProcessContext, GCPEnvConfigObject

_logger = logging.getLogger("rdr_logger")

# Tool_cmd and tool_desc name are required.
# Remember to add/update bash completion in 'tool_lib/tools.bash'
tool_cmd = "thebasics-analyzer"
tool_desc = "One-off tool to collect data on TheBasics partial/multiple survey responses"

# Fields/keys from a bigquery_sync resource dict that are not question code fields/should not be part of analysis
NON_QUESTION_CODE_FIELDS = [
   'id', 'created', 'modified', 'authored', 'language', 'participant_id', 'questionnaire_response_id',
    'questionnaire_id', 'external_id', 'status', 'status_id'
]

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

class TheBasicsAnalyzerClass(object):
    def __init__(self, args, gcp_env: GCPEnvConfigObject, id_list=None):
        """
        :param args: command line arguments.
        :param gcp_env: gcp environment information, see: gcp_initialize().
        """
        self.args = args
        self.gcp_env = gcp_env
        self.id_list = id_list
        self.results = {}
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

    def add_pid_response_history(self, pid, response_history, issue_list):
        """
        Adds the pid's response details to one of the class's issue lists
        Prints out the history if the --verbose option is in effect
        """
        # Sort the response history by authored date
        history = sorted(response_history, key=lambda d: d['authored'])
        issue_list.append({pid: history})
        if self.args.verbose:
            formatted_details = pprint.pformat(history)
            print(f'PID {pid} TheBasics history\n{formatted_details}')

    def check_for_duplicates(self, pid):
        """
        This will process the entire TheBasics history for a participant (called for participants who had partials)
        It will use the QuestionnaireResponseAnswer data for all the partial responses for this participant to look
        for any that can be marked as duplicates.
        """
        answer = aliased(Code)
        partials = full_responses = None
        pid_data = self.pids_with_partials[pid]
        if pid_data:
            partials = [rsp['questionnaire_response_id'] for rsp in pid_data if not rsp['full_survey']]
            full_responses = [rsp['questionnaire_response_id'] for rsp in pid_data if rsp['full_survey']]
        # print(f'Participant {pid} TheBasics responses:\n')
        print(f'P{pid} TheBasics full survey questionnaire responses:\n{full_responses}')
        print(f'P{pid} TheBasics partial payload questionnaire responses:\n{partials}')

        with self.ro_dao.session() as session:
            last_response_dict = None
            response_answer_set = None
            last_qr_response_id = partials[0]
            for response_id in partials:
                response_dict = dict()
                answer_list = session.query(
                    QuestionnaireResponse.questionnaireResponseId,
                    QuestionnaireResponse.authored,
                    Code.value.label('question_code_value'),
                    Code.codeId,
                    func.coalesce(answer.value, QuestionnaireResponseAnswer.valueString,
                                  QuestionnaireResponseAnswer.valueBoolean, QuestionnaireResponseAnswer.valueInteger,
                                  QuestionnaireResponseAnswer.valueDate, QuestionnaireResponseAnswer.valueDateTime
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
                    QuestionnaireResponse.isDuplicate == 0
                ).order_by(QuestionnaireResponse.authored,
                           QuestionnaireResponse.created
                ).all()

                if not answer_list:
                    print(f'P{pid}: No answer values present for TheBasics response {response_id}')
                    continue

                if last_response_dict is None:
                    last_response_dict = dict()
                    for row in answer_list:
                        last_response_dict[row.question_code_value] = row.answer_value
                    last_answer_set = set(last_response_dict.items())
                    print(f'First partial response id: {response_id}')
                else:
                    for row in answer_list:
                        response_dict[row.question_code_value] = row.answer_value
                    response_answer_set = set(response_dict.items())

                    if response_answer_set and response_answer_set.issuperset(last_answer_set):
                        print(f'\tResponse {last_qr_response_id} is a duplicate/subset of {response_id}')

                    last_answer_set = copy.deepcopy(response_answer_set)
                    last_qr_response_id = response_id

    def process_participant_responses(self, pid, responses, session):
        if not len(responses):
            raise (ValueError, f'P{pid}: TheBasics response list was empty')

        result_details = list()
        has_partial = False
        for response in responses:
            pdr_resource_rec = session.query(
                BigQuerySync
            ).filter(BigQuerySync.tableId == 'pdr_mod_thebasics',
                BigQuerySync.pk_id == response.questionnaireResponseId
            ).first()
            if pdr_resource_rec:
                data = pdr_resource_rec.resource
                full_survey = False
                for field, value in data.items():
                    if field in NON_QUESTION_CODE_FIELDS:
                        continue
                    if field not in PROFILE_UPDATE_QUESTION_CODES and value:
                        full_survey = True
                        break

            result_details.append(
                {'authored': response.authored,
                 'questionnaire_response_id': response.questionnaireResponseId,
                 'full_survey': full_survey,
                 'external_id': response.externalId
                 }
             )

            has_partial = has_partial or not full_survey
        # For now, only analyze pids with known partial TheBasics payloads.
        # TODO:  Also analyze pids with multiple full survey payloads
        if has_partial:
            print('\n====================================================================================')
            self.pids_with_partials[pid] = result_details
            self.check_for_duplicates(pid)


    def run(self):
        """
        Main program process
        :return: Exit code value
        """
        if not len(self.id_list):
            _logger.error('No ids to process')
            return 1

        self.gcp_env.activate_sql_proxy(replica=True)
        self.ro_dao = QuestionnaireResponseDao()
        basics_ids = self.get_the_basics_questionnaire_ids()
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


def get_id_list(fname):
    """
    Shared helper routine for tool classes that allow input from a file of integer ids (participant ids or
    id values from a specific table).
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
    parser.add_argument("--log-file", help="write output to a log file", default=False, action="store_true")  # noqa
    parser.add_argument("--project", help="gcp project name", default="localhost")  # noqa
    parser.add_argument("--account", help="pmi-ops account", default=None)  # noqa
    parser.add_argument("--service-account", help="gcp iam service account", default=None)  # noqa
    parser.add_argument("--id", help="Analyze TheBasics data for a single participant_id",
                        type=int, default=None)
    parser.add_argument("--from-file", help="Analyze TheBasics data for a list of participant ids in the file",
                        metavar='FILE', type=str, default=None)
    parser.add_argument("--verbose", help="Show details for participant response history if an issue exists",
                        default=False, action="store_true")


    args = parser.parse_args()
    with GCPProcessContext(tool_cmd, args.project, args.account, args.service_account) as gcp_env:
        ids = None
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
