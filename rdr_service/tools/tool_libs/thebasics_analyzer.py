#! /bin/env python
#
# Template for RDR tool python program.
#

import argparse

# pylint: disable=superfluous-parens
# pylint: disable=broad-except
import logging
import os
import sys

from dateutil.relativedelta import relativedelta
from datetime import datetime

from rdr_service.dao.bigquery_sync_dao import BigQuerySyncDao
from rdr_service.model.bigquery_sync import BigQuerySync
from rdr_service.model.questionnaire_response import QuestionnaireResponse
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
    'SecondaryContactInfo_PersonOneFirstName',
    'SecondaryContactInfo_PersonOneMiddleInitial',
    'SecondaryContactInfo_PersonOneLastName',
    'SecondaryContactInfo_PersonOneAddressOne',
    'SecondaryContactInfo_PersonOneAddressTwo',
    'PersonOneAddress_PersonOneAddressCity',
    'PersonOneAddress_PersonOneAddressState',
    'PersonOneAddress_PersonOneAddressZipCode',
    'SecondaryContactInfo_PersonOneEmail',
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
        self.missing_full_survey_list = []
        self.first_response_is_partial = []
        self.conflicting_payloads_with_same_authored = []

    def process_participant_responses(self, pid, response_list):
        if not len(response_list):
            raise (ValueError, f'{pid} TheBasics response list was empty')

        result_details = list()
        for response in response_list:
            full_survey = False
            for field, value in response.items():
                if field in NON_QUESTION_CODE_FIELDS:
                    continue
                if field not in PROFILE_UPDATE_QUESTION_CODES and value:
                    full_survey = True
                    break

            result_details.append(
                {'authored': response['authored'],
                 'questionnaire_response_id': response['questionnaire_response_id'],
                 'full_survey': full_survey,
                 'external_id': response['external_id']
                 }
             )

        # Find the first full survey response location in the list of processed responses, if one exists
        first_full_index = next((index for (index, rsp) in enumerate(result_details) if rsp['full_survey']), None)
        # first_qrid = result_details[0]['questionnaire_response_id']
        # first_authored = result_details[0]['authored']
        if first_full_index is None:
            self.missing_full_survey_list.append(pid)
            _logger.error(
                " ".join([f'{pid} did not have a full survey response,',
                          f'first partial response {result_details[0]["questionnaire_response_id"]}',
                          f'authored {result_details[0]["authored"]}'])
            )

        # Process cases where the first response was preceded by a partial at index 0
        elif first_full_index > 0:
            first_rsp = result_details[0]
            first_full_rsp = result_details[first_full_index]
            first_full_ts = first_full_rsp['authored']
            first_partial_ts = first_rsp['authored']
            # Look for cases where the first full payload came in with same authored time (only have H:M:S)
            # (results ordered by QuestionnaireResponse.created time, partial response was processed first by RDR)
            if first_full_ts == first_partial_ts:
                _logger.warning(
                    " ".join([
                        f'{pid} partial response {first_rsp["questionnaire_response_id"]}',
                        f'(external id {first_rsp["external_id"]})',
                        f'received/processed before full response {first_full_rsp["questionnaire_response_id"]}',
                        f'(external id {first_rsp["external_id"]}, both have same authored {first_full_ts}'])
                )
                self.conflicting_payloads_with_same_authored.append(pid)
            else:
                delta = relativedelta(datetime.strptime(first_full_ts, "%Y-%m-%dT%H:%M:%S"),
                                               datetime.strptime(first_partial_ts, "%Y-%m-%dT%H:%M:%S"))

                delta_str = " ".join([
                         f'offset {delta.days} days / {delta.hours} hours /',
                         f'{delta.minutes} minutes / {delta.seconds} seconds ' ])

                _logger.error(
                    " ".join([f'{pid} Partial authored {first_partial_ts} before full {first_full_ts},',
                              delta_str])
                )
                self.first_response_is_partial.append(pid)

    def run(self):
        """
        Main program process
        :return: Exit code value
        """
        if not len(self.id_list):
            _logger.error('No ids to process')
            return 1

        self.gcp_env.activate_sql_proxy(replica=True)

        dao = BigQuerySyncDao(backup=True)
        with dao.session() as session:
            records = session.query(
                BigQuerySync
            ).join(
                QuestionnaireResponse, QuestionnaireResponse.questionnaireResponseId == BigQuerySync.pk_id
            ).filter(
                BigQuerySync.tableId == 'pdr_mod_thebasics',
                BigQuerySync.projectId == 'aou-pdr-data-prod',
            ).filter(
                QuestionnaireResponse.participantId.in_(self.id_list)
            ).order_by(QuestionnaireResponse.participantId, QuestionnaireResponse.authored,
                       QuestionnaireResponse.created
            ).all()

            last_participant = None
            participant_responses = []
            for record in records:
                # Keep appending responses for the same participant to a list, until we get to a different pid in the
                # ordered by participant_id results
                current_participant = int(record.resource.get('participant_id', None))
                if not last_participant:  # Need to initialize on the first pass through
                    last_participant = current_participant

                if last_participant != current_participant:
                    if len(participant_responses):
                        self.process_participant_responses(last_participant, participant_responses)
                        participant_responses = []
                    last_participant = current_participant
                participant_responses.append(record.resource)

            # Process responses for the last participant in the resultset
            if len(participant_responses):
                self.process_participant_responses(current_participant, participant_responses)

            if len(self.id_list) > 1:
                print(f'{len(self.missing_full_survey_list)} participants are missing a full survey response')
                print(f'{len(self.first_response_is_partial)} participants had partial survey as first response')
                print(" ".join([f'{len(self.conflicting_payloads_with_same_authored)} participants',
                                'had partial and full payloads with same timestamp, but partial received by RDR first'
                                ])
                )

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
