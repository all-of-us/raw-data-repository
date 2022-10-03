import logging

from rdr_service import clock
from rdr_service.logic.enrollment_info import EnrollmentCalculation, EnrollmentDependencies
from rdr_service.model.participant_summary import ParticipantSummary
from rdr_service.participant_enums import EnrollmentStatus
from rdr_service.repository.questionnaire_response_repository import QuestionnaireResponseRepository
from rdr_service.services.system_utils import list_chunks, min_or_none
from rdr_service.tools.tool_libs.tool_base import cli_run, ToolBase

tool_cmd = 'backfill-coreminuspmtime'
tool_desc = 'Backfill CoreMinusPMTime values that had been incorrectly updated'

logger = logging.getLogger("rdr_logger")


class BackfillCoreMinusPMTime(ToolBase):
    def run(self):
        super().run()
        with self.get_session() as session:
            participant_ids = session.query(
                ParticipantSummary.participantId
            ).filter(
                ParticipantSummary.enrollmentStatusCoreMinusPMTime.isnot(None)
            ).order_by(
                ParticipantSummary.participantId
            ).all()

            logger.info(f'PIDs to validate Core Minus PM Time: {len(participant_ids)}')
            chunk_size = 50
            count = 0
            for participant_id_subset in list_chunks(lst=participant_ids, chunk_size=chunk_size):
                batch_count = 0
                logger.debug(f'pid subset: {participant_id_subset}')
                logger.info(f'Validating PIDs {count*chunk_size} - {((count+1) * chunk_size)-1}')
                summary_list = session.query(
                    ParticipantSummary
                ).filter(
                    ParticipantSummary.participantId.in_(participant_id_subset)
                ).with_for_update().all()
                for summary in summary_list:
                    calculated_date = self.calculate_core_minus_pm_time(summary, session)
                    if calculated_date and calculated_date < summary.enrollmentStatusCoreMinusPMTime:
                        batch_count += 1
                        summary.enrollmentStatusCoreMinusPMTime = calculated_date
                        summary.modifiedTime = clock.CLOCK.now()
                session.commit()
                logger.info(f'Updated {batch_count} of {chunk_size}')
                count += 1

    @staticmethod
    def calculate_core_minus_pm_time(summary: ParticipantSummary, session):
        earliest_physical_measurements_time = min_or_none([
            summary.clinicPhysicalMeasurementsFinalizedTime,
            summary.selfReportedPhysicalMeasurementsAuthored
        ])
        earliest_biobank_received_dna_time = min_or_none([
            summary.sampleStatus1ED10Time,
            summary.sampleStatus2ED10Time,
            summary.sampleStatus1ED04Time,
            summary.sampleStatus1SALTime,
            summary.sampleStatus1SAL2Time
        ])

        ehr_consent_ranges = QuestionnaireResponseRepository.get_interest_in_sharing_ehr_ranges(
            participant_id=summary.participantId,
            session=session
        )

        dna_update_time_list = [summary.questionnaireOnDnaProgramAuthored]
        if summary.consentForStudyEnrollmentAuthored != summary.consentForStudyEnrollmentFirstYesAuthored:
            dna_update_time_list.append(summary.consentForStudyEnrollmentAuthored)
        dna_update_time = min_or_none(dna_update_time_list)

        enrollment_info = EnrollmentCalculation.get_enrollment_info(
            EnrollmentDependencies(
                consent_cohort=summary.consentCohort,
                primary_consent_authored_time=summary.consentForStudyEnrollmentAuthored,
                gror_authored_time=summary.consentForGenomicsRORAuthored,
                basics_authored_time=summary.questionnaireOnTheBasicsAuthored,
                overall_health_authored_time=summary.questionnaireOnOverallHealthAuthored,
                lifestyle_authored_time=summary.questionnaireOnLifestyleAuthored,
                earliest_ehr_file_received_time=summary.firstEhrReceiptTime,
                earliest_physical_measurements_time=earliest_physical_measurements_time,
                earliest_biobank_received_dna_time=earliest_biobank_received_dna_time,
                ehr_consent_date_range_list=ehr_consent_ranges,
                dna_update_time=dna_update_time
            )
        )

        legacy_dates = enrollment_info.version_legacy_dates
        if EnrollmentStatus.CORE_MINUS_PM in legacy_dates:
            return legacy_dates[EnrollmentStatus.CORE_MINUS_PM]


def run():
    return cli_run(tool_cmd, tool_desc, BackfillCoreMinusPMTime)
