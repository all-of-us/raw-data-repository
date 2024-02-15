from datetime import datetime
from typing import List

from rdr_service import code_constants
from rdr_service.participant_enums import QuestionnaireStatus
from rdr_service.model.participant_summary import ParticipantSummary
from rdr_service.repository.questionnaire_response_repository import QuestionnaireResponseRepository
from rdr_service.tools.tool_libs.tool_base import cli_run, ToolBase
from rdr_service.services.consent.validation import EhrStatusUpdater
from rdr_service.model.consent_file import ConsentFile, ConsentType, ConsentSyncStatus

tool_cmd = 'roc-1816'
tool_desc = 'Deal with EHR consents that should have been recorded as yes'


class Roc1816(ToolBase):

    def run(self):
        super().run()

        with self.get_session() as session:
            possible_summaries = self.load_possible_summaries(session)
            summaries_marked_incorrectly = self.get_summaries_with_real_consent(
                session=session,
                possible_summaries=possible_summaries
            )

            updater = EhrStatusUpdater(self.args.project, session=session)
            for summary in summaries_marked_incorrectly:
                ehr_consent_files: List[ConsentFile] = session.query(ConsentFile).filter(
                    ConsentFile.participant_id == summary.participantId,
                    ConsentFile.type == ConsentType.EHR
                ).all()

                if any(
                    file.sync_status in [
                        ConsentSyncStatus.SYNC_COMPLETE,
                        ConsentSyncStatus.READY_FOR_SYNC
                    ]
                    for file in ehr_consent_files
                ):
                    print(f'updating P{summary.participantId} to Yes')
                    updater._update_status(participant_id=summary.participantId, has_valid_file=True,
                                           status_check=QuestionnaireStatus.SUBMITTED_NO_CONSENT)
                else:
                    print(f'P{summary.participantId} has no valid files (from {len(ehr_consent_files)} ehr files)')

            print(f"finished at {datetime.now()}")

    def load_possible_summaries(self, session) -> List[ParticipantSummary]:
        print(f"starting load of possibles at {datetime.now()}")
        possible_summaries = session.query(ParticipantSummary).filter(
            ParticipantSummary.consentForElectronicHealthRecords == QuestionnaireStatus.SUBMITTED_NO_CONSENT,
            ParticipantSummary.consentForElectronicHealthRecordsAuthored > '2024-01-01'
        ).all()
        print(f"found {len(possible_summaries)} summaries")

        return possible_summaries

    def get_summaries_with_real_consent(
        self, session, possible_summaries: List[ParticipantSummary]
    ) -> List[ParticipantSummary]:

        consent_answer_map = {
            code_constants.EHR_CONSENT_QUESTION_CODE:           code_constants.CONSENT_PERMISSION_YES_CODE,
            code_constants.EHR_SENSITIVE_CONSENT_QUESTION_CODE: code_constants.SENSITIVE_EHR_YES,
            code_constants.EHR_PEDIATRIC_CONSENT_QUESTION_CODE: code_constants.PEDIATRIC_SHARE_AGREE
        }
        result = []
        for summary in possible_summaries:
            query_result = QuestionnaireResponseRepository.get_responses_to_surveys(
                session=session,
                survey_codes=[code_constants.CONSENT_FOR_ELECTRONIC_HEALTH_RECORDS_MODULE,
                              code_constants.PEDIATRIC_EHR_CONSENT],
                participant_ids=[summary.participantId]
            )
            if summary.participantId not in query_result:
                print(f'error with {summary.participantId}: no response found')
                continue

            ehr_responses = query_result[summary.participantId]

            latest_response = ehr_responses.in_authored_order[-1]

            for question_code, yes_answer in consent_answer_map.items():
                answer_value = latest_response.get_single_answer_for(question_code)
                if answer_value and answer_value.value.lower() == yes_answer.lower():
                    result.append(summary)
                    break

        return result

def run():
    cli_run(tool_cmd, tool_desc, Roc1816)
