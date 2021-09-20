import argparse

from sqlalchemy import and_
from sqlalchemy.orm import Session

from rdr_service.model.code import Code
from rdr_service.model.utils import from_client_participant_id
from rdr_service.model.participant_summary import ParticipantSummary
from rdr_service.model.questionnaire import QuestionnaireQuestion
from rdr_service.model.questionnaire_response import QuestionnaireResponse, QuestionnaireResponseAnswer,\
    QuestionnaireResponseExtension
from rdr_service.tools.tool_libs.tool_base import cli_run, logger, ToolBase


tool_cmd = 'unconsent'
tool_desc = 'Remove participants that we have received ConsentPII payloads for, but have not actually consented'


class UnconsentTool(ToolBase):

    def run(self):
        super(UnconsentTool, self).run()

        with self.get_session() as session:
            for participant_id in self._load_participant_ids():
                if not self._participant_has_signed_consent(session, participant_id):
                    self._delete_participant_consent(session, participant_id)
                else:
                    logger.info(f'P{participant_id} has signed consent')

    def _load_participant_ids(self) -> set:
        participant_ids = set()
        with open(self.args.pid_file) as file:
            for participant_id_str in file:
                participant_id = from_client_participant_id(participant_id_str)
                participant_ids.add(participant_id)

        return participant_ids

    @classmethod
    def _participant_has_signed_consent(cls, session: Session, participant_id: int):
        """
        If the participant has a module where they've actually signed the consent, then don't unconsent them.
        This is to make sure we don't remove participants that have consented since verifying the list.
        """
        signed_response_query = (
            session.query(QuestionnaireResponseAnswer.questionnaireResponseAnswerId)
            .join(
                QuestionnaireResponse,
                and_(
                    QuestionnaireResponse.participantId == participant_id,
                    QuestionnaireResponse.questionnaireResponseId == QuestionnaireResponseAnswer.questionnaireResponseId
                )
            ).join(
                QuestionnaireQuestion,
                QuestionnaireQuestion.questionnaireQuestionId == QuestionnaireResponseAnswer.questionId
            ).join(
                Code,
                and_(
                    Code.value == 'ExtraConsent_Signature',
                    Code.codeId == QuestionnaireQuestion.codeId
                )
            )
        )

        return signed_response_query.count() > 0

    def _delete_participant_consent(self, session: Session, participant_id: int):
        """Remove the participant's consent status and responses from the RDR"""

        # Retrieve the participant summary
        summary_query = session.query(ParticipantSummary.participantId).filter(
            ParticipantSummary.participantId == participant_id
        )
        if not self.args.dry_run:
            # Obtain a lock on the participant summary to prevent possible race conditions with incoming responses
            summary_query = summary_query.with_for_update()
        participant_summary = summary_query.one_or_none()

        if participant_summary is None:
            logger.info(f'No participant summary found for P{participant_id}')
        elif self.args.dry_run:
            logger.info(f'would remove consent for P{participant_id}')
        else:
            # Delete the participant summary
            session.query(ParticipantSummary).filter(
                ParticipantSummary.participantId == participant_id
            ).delete()

            # Delete the QuestionnaireResponses and associated objects
            questionnaire_response_ids = session.query(QuestionnaireResponse.questionnaireResponseId).filter(
                QuestionnaireResponse.participantId == participant_id
            ).all()
            session.query(QuestionnaireResponseAnswer).filter(
                QuestionnaireResponseAnswer.questionnaireResponseId.in_(questionnaire_response_ids)
            ).delete()
            session.query(QuestionnaireResponseExtension).filter(
                QuestionnaireResponseExtension.questionnaireResponseId.in_(questionnaire_response_ids)
            ).delete()
            session.query(QuestionnaireResponse).filter(
                QuestionnaireResponse.questionnaireResponseId.in_(questionnaire_response_ids)
            ).delete()

        # Commit to finalize the changes for this participant and release the locks
        session.commit()


def add_additional_arguments(parser: argparse.ArgumentParser):
    parser.add_argument('--pid-file', required=True)
    parser.add_argument('--dry-run', default=False, action="store_true")


def run():
    cli_run(tool_cmd, tool_desc, UnconsentTool, add_additional_arguments)
