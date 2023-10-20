import logging
from datetime import datetime
from sqlalchemy.orm import aliased

from rdr_service.cloud_utils.gcp_google_pubsub import submit_pipeline_pubsub_msg
from rdr_service.dao.physical_measurements_dao import PhysicalMeasurementsDao
from rdr_service.code_constants import WEAR_CONSENT_QUESTION_CODE, WEAR_YES_ANSWER_CODE, WEAR_NO_ANSWER_CODE
from rdr_service.model.code import Code
from rdr_service.model.etm import EtmQuestionnaireResponse
from rdr_service.model.participant_summary import ParticipantSummary
from rdr_service.model.questionnaire import QuestionnaireConcept, QuestionnaireQuestion
from rdr_service.model.questionnaire_response import QuestionnaireResponse, QuestionnaireResponseAnswer
from rdr_service.participant_enums import QuestionnaireStatus
from rdr_service.services.system_utils import list_chunks, min_or_none
from rdr_service.tools.tool_libs.tool_base import cli_run, ToolBase

tool_cmd = 'backfill-participant-summary'
tool_desc = 'One-off tool to backfill fields recently added to participant_summary for DA-3777'

logger = logging.getLogger("rdr_logger")

class BackfillParticipantSummary(ToolBase):
    """
    This tool will make the following changes to participant_summary, provided the fields are not already populated:
    - add values consentForWearStudy/consentForWearStudyAuthored/consentForWearStudyTime if WEAR consent(s) exist
    - add values to latestEtMTaskAuthored/latestEtMTaskTime if EtM task responses exist
    - add values to hasHeightAndWeight/hasHeightAndWeightTime if qualifying PM records exist
    It will intentionally not update the lastModified field since the fields are new/not currently used by partners
    (do not want to include these modifications when they issue GET requests with a lastModified filter)
    """

    def get_height_weight_details(self, session, pid: int):
        """ Determine if/when participant PM measurements reflected both a valid height and valid weight measurement """

        has_height_weight = False
        has_height_weight_time = None
        pm_recs = PhysicalMeasurementsDao.get_core_measurements_for_participant(session, pid)
        # Borrowing logic from the update_enrollment_status() code
        earliest_height_time = min_or_none(
            meas.finalized for meas in pm_recs if meas.satisfiesHeightRequirements
        )
        earliest_weight_time = min_or_none(
            meas.finalized for meas in pm_recs if meas.satisfiesWeightRequirements
        )
        if earliest_height_time and earliest_weight_time:
            has_height_weight = True
            # Use later of the two timestamps (if they differ/are from different PM records) to reflect when
            # requirement for both height and weight was satisfied
            has_height_weight_time = max(earliest_weight_time, earliest_height_time)

        return has_height_weight, has_height_weight_time

    def get_last_etm_task_details(self, session, pid: int):
        """ Look for the most recently authored etm_questionnaire_response record for the specifid participantId """
        etm_task = session.query(
            EtmQuestionnaireResponse.authored,
            EtmQuestionnaireResponse.created,
        ).filter(
            EtmQuestionnaireResponse.participant_id == pid
        ).order_by(EtmQuestionnaireResponse.authored.desc()).first()
        return etm_task

    def get_last_wear_consent_details(self, session, pid: int) -> tuple:
        """
        Look for the most recently authored WEAR consent response and consent question answer (/consent status)
        for the specified participantId
        """
        # Default consent status for the participant_summary.consentForWearStudy status field
        wear_status = QuestionnaireStatus.UNSET
        answer_code = aliased(Code)
        question_code = aliased(Code)
        wear_response = session.query(
            QuestionnaireResponse.authored,
            QuestionnaireResponse.created,
            answer_code.value
        ).select_from(
            QuestionnaireResponse
        ).join(
            QuestionnaireConcept, QuestionnaireResponse.questionnaireId == QuestionnaireConcept.questionnaireId
        ).join(
            QuestionnaireResponseAnswer,
            QuestionnaireResponseAnswer.questionnaireResponseId == QuestionnaireResponse.questionnaireResponseId
        ).join(
            QuestionnaireQuestion,
            QuestionnaireResponseAnswer.questionId == QuestionnaireQuestion.questionnaireQuestionId
        ).join(
            question_code, QuestionnaireQuestion.codeId == question_code.codeId
        ).join(
            answer_code, QuestionnaireResponseAnswer.valueCodeId == answer_code.codeId
        ).filter(
            QuestionnaireResponse.participantId == pid,
            question_code.value == WEAR_CONSENT_QUESTION_CODE
        ).order_by(QuestionnaireResponse.authored.desc()).first()
        if wear_response:
            if wear_response.value.lower() == WEAR_YES_ANSWER_CODE.lower():
                wear_status = QuestionnaireStatus.SUBMITTED
            elif wear_response.value.lower() == WEAR_NO_ANSWER_CODE.lower():
                wear_status = QuestionnaireStatus.SUBMITTED_NO_CONSENT

        return wear_response, wear_status

    def run(self):
        super().run()
        count = 0
        last_id = None
        with self.get_session() as session:
            # --id option takes precedence over --from-file option
            if self.args.id:
                participant_id_list = [int(i) for i in self.args.id.split(',')]
            elif self.args.from_file:
                participant_id_list = self.get_int_ids_from_file(self.args.from_file)
            else:
                # Default to all participant_summary ids
                participant_id_list = session.query(
                    ParticipantSummary.participantId
                ).order_by(ParticipantSummary.participantId).all()

        chunk_size = 250
        skipped_pids = 0
        for id_list_subset in list_chunks(lst=participant_id_list, chunk_size=chunk_size):
            pub_sub_pk_id_list = []
            recs_to_commit = False
            logger.info(
                f'{datetime.now()}: Updated {count} of {len(participant_id_list)} (last id: {last_id}, ' +
                f'total skipped: {skipped_pids})'
            )
            count += chunk_size
            participants = session.query(
                ParticipantSummary
            ).order_by(
                ParticipantSummary.participantId
            ).filter(ParticipantSummary.participantId.in_(id_list_subset)).all()
            for rec in participants:
                rec_updated = False
                # Defensive check because of running into records in lower environments that are missing both email
                # and loginPhoneNumber.  Continuing/updating fields will hit a SA InvalidDataState exception in
                # validate_participant_summary() in the ParticipantSummary model definition and rollback the entire
                # transaction.  Skip the problem pids.
                if not (rec.email or rec.loginPhoneNumber):
                    logger.error(
                        f'P{rec.participantId} has no email or phone number, would cause InvalidDataState exception.' +
                        f' Skipping'
                    )
                    skipped_pids += 1
                    continue
                # Note: Skip the checks if the participant_summary record already has values (no backfill needed)
                if rec.consentForWearStudy is None:
                    wear_result, wear_status = self.get_last_wear_consent_details(session, rec.participantId)
                    if wear_result:
                        rec.consentForWearStudy = wear_status
                        rec.consentForWearStudyAuthored = wear_result.authored
                        rec.consentForWearStudyTime = wear_result.created
                        rec_updated = True

                if rec.latestEtMTaskAuthored is None:
                    etm_task_result = self.get_last_etm_task_details(session, rec.participantId)
                    if etm_task_result:
                        rec.latestEtMTaskAuthored = etm_task_result.authored
                        rec.latestEtMTaskTime = etm_task_result.created
                        rec_updated = True

                if rec.hasHeightAndWeight is None:
                    rec.hasHeightAndWeight, rec.hasHeightAndWeightTime = \
                        self.get_height_weight_details(session, rec.participantId)
                    rec_updated = True

                if rec_updated:
                    pub_sub_pk_id_list.append(rec.participantId)
                last_id = rec.participantId
                recs_to_commit = recs_to_commit or rec_updated

            if recs_to_commit:
                session.commit()
                # pubsub messages will only be generated in configured/supported environments
                submit_pipeline_pubsub_msg(database='rdr', table='participant_summary', action='upsert',
                                           pk_columns=['participant_id'], pk_values=pub_sub_pk_id_list,
                                           project=self.gcp_env.project)

def add_additional_arguments(parser):
    parser.add_argument('--id', required=False,
                        help="Single participant id or comma-separated list of id integer values to backfill")
    parser.add_argument('--from-file', required=False,
                        help="file of integer participant id values to backfill")

def run():
    return cli_run(tool_cmd, tool_desc, BackfillParticipantSummary, add_additional_arguments)
