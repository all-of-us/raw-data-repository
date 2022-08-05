from collections import defaultdict
from typing import List

from rdr_service.model.consent_file import ConsentFile, ConsentType
from rdr_service.model.consent_response import ConsentResponse
from rdr_service.model.questionnaire_response import QuestionnaireResponse
from rdr_service.tools.tool_libs.tool_base import cli_run, ToolBase

tool_cmd = 'consent-match'
tool_desc = 'find the response for consents that do not have a response set'


class ConsentMatchScript(ToolBase):
    logger_name = None

    def run(self):
        super(ConsentMatchScript, self).run()

        with self.get_session() as session:
            unmatched_wear_consents = session.query(ConsentFile).filter(
                ConsentFile.type == ConsentType.WEAR,
                ConsentFile.consent_response_id.is_(None)
            ).all()

            consent_response_list = session.query(ConsentResponse).join(
                QuestionnaireResponse
            ).filter(
                ConsentResponse.type == ConsentType.WEAR,
                QuestionnaireResponse.participantId.in_([file.participant_id for file in unmatched_wear_consents])
            ).all()

            self.find_matches(
                file_list=unmatched_wear_consents,
                consent_response_list=consent_response_list
            )

    def find_matches(self, file_list: List[ConsentFile], consent_response_list: List[ConsentResponse]):
        consent_response_list_by_pid = defaultdict(list)
        for consent_response in consent_response_list:
            consent_response_list_by_pid[consent_response.response.participantId].append(consent_response)

        file_list_by_participant = defaultdict(list)
        for file in file_list:
            file_list_by_participant[file.participant_id].append(file)

        for participant_id, file_list in file_list_by_participant.items():
            consent_response_list_for_participant = consent_response_list_by_pid[participant_id]

            self.match_files_to_responses(
                file_list=file_list,
                consent_response_list=consent_response_list_for_participant
            )

    @classmethod
    def match_files_to_responses(cls, file_list: List[ConsentFile], consent_response_list: List[ConsentResponse]):
        # Match up all the validation results to a corresponding consent_response

        if len(file_list) == 1 and len(consent_response_list) == 1:
            file_list[0].consent_response = consent_response_list[0]
            return

        previously_matched_consent_response_list = []

        for file in file_list:
            unmatched_consent_response_list = [
                consent_response for consent_response in consent_response_list
                if consent_response not in previously_matched_consent_response_list
            ]
            match_found = False
            for consent_response in unmatched_consent_response_list:
                if file.file_path[30:] in consent_response.response.resource:
                    file.consent_response = consent_response
                    previously_matched_consent_response_list.append(consent_response)
                    match_found = True
                    break

            if not match_found:
                for consent_response in unmatched_consent_response_list:
                    if file.expected_sign_date == consent_response.response.authored.date():
                        file.consent_response = consent_response
                        previously_matched_consent_response_list.append(consent_response)
                        match_found = True
                        break

            if not match_found:
                raise Exception(f'File id {file.id} was unable to match')


def run():
    return cli_run(tool_cmd, tool_desc, ConsentMatchScript)
