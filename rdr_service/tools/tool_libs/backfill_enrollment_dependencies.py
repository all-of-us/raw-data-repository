from typing import List

from dateutil.parser import parse
from sqlalchemy.orm import joinedload

from rdr_service import code_constants
from rdr_service.dao.biobank_stored_sample_dao import BiobankStoredSampleDao
from rdr_service.dao.genomics_dao import GenomicSetMemberDao
from rdr_service.dao.physical_measurements_dao import PhysicalMeasurementsDao
from rdr_service.model.enrollment_dependencies import EnrollmentDependencies
from rdr_service.model.participant_summary import ParticipantSummary
from rdr_service.model.pediatric_data_log import PediatricDataLog, PediatricDataType
from rdr_service.participant_enums import ParticipantCohortEnum, QuestionnaireStatus, SampleStatus
from rdr_service.repository.questionnaire_response_repository import QuestionnaireResponseRepository
from rdr_service.services.system_utils import min_or_none
from rdr_service.tools.tool_libs.tool_base import cli_run, ToolBase

tool_cmd = 'enrollment-dependencies-backfill'
tool_desc = 'Backfill enrollment dependencies'


class EnrollmentDependenciesBackfill(ToolBase):
    def run(self):
        super().run()

        with self.get_session() as session:
            summary_list = session.query(ParticipantSummary).options(
                joinedload(ParticipantSummary.guardianParticipants).load_only(),
                joinedload(ParticipantSummary.pediatricData)
            ).yield_per(500)
            for summary in summary_list:
                self.create_dependency_data(summary, session)

    @classmethod
    def create_dependency_data(cls, summary: ParticipantSummary, session):
        core_measurements = PhysicalMeasurementsDao.get_core_measurements_for_participant(
            session=session,
            participant_id=summary.participantId
        )

        dependency_data = EnrollmentDependencies(
            participant_id=summary.participantId,
            consent_cohort=ParticipantCohortEnum(int(summary.consentCohort)),
            primary_consent_authored_time=summary.consentForStudyEnrollmentFirstYesAuthored,
            intent_to_share_ehr_time=cls.get_ehr_intent_to_share_time(summary, session),
            full_ehr_consent_authored_time=summary.consentForElectronicHealthRecordsFirstYesAuthored,
            gror_consent_authored_time=summary.consentForGenomicsRORAuthored,
            dna_consent_update_time=cls.get_revised_consent_time(
                participant_id=summary.participantId, session=session
            ),
            basics_survey_authored_time=summary.questionnaireOnTheBasicsAuthored,
            overall_health_survey_authored_time=summary.questionnaireOnOverallHealthAuthored,
            lifestyle_survey_authored_time=summary.questionnaireOnLifestyleAuthored,
            exposures_survey_authored_time=cls.get_exposures_time(summary.pediatricData),
            biobank_received_dna_time=cls.get_biobank_dna_time(summary=summary, session=session),
            wgs_sequencing_time=GenomicSetMemberDao.get_wgs_pass_date(session=session, biobank_id=summary.biobankId),
            first_ehr_file_received_time=min_or_none([
                summary.ehrReceiptTime, summary.firstParticipantMediatedEhrReceiptTime
            ]),
            first_mediated_ehr_received_time=summary.firstParticipantMediatedEhrReceiptTime,
            physical_measurements_time=min_or_none([
                summary.clinicPhysicalMeasurementsFinalizedTime,
                summary.selfReportedPhysicalMeasurementsAuthored
            ]),
            weight_physical_measurements_time=min_or_none([
                meas.finalized for meas in core_measurements if meas.satisfiesWeightRequirements
            ]),
            height_physical_measurements_time=min_or_none([
                meas.finalized for meas in core_measurements if meas.satisfiesHeightRequirements
            ]),
            is_pediatric_participant=summary.isPediatric,
            has_linked_guardian_account=bool(summary.guardianParticipants and len(summary.guardianParticipants) > 0)
        )
        session.add(dependency_data)

    @classmethod
    def get_ehr_intent_to_share_time(cls, summary: ParticipantSummary, session):
        default_ehr_date = None
        if summary.consentForElectronicHealthRecords == QuestionnaireStatus.SUBMITTED:
            default_ehr_date = summary.consentForElectronicHealthRecordsFirstYesAuthored

        ehr_consent_ranges = QuestionnaireResponseRepository.get_interest_in_sharing_ehr_ranges(
            participant_id=summary.participantId,
            session=session,
            default_authored_datetime=default_ehr_date
        )
        return min_or_none([date_range.start for date_range in ehr_consent_ranges])

    @classmethod
    def get_biobank_dna_time(cls, summary: ParticipantSummary, session):
        if summary.samplesToIsolateDNA != SampleStatus.RECEIVED:
            return None

        return BiobankStoredSampleDao.get_earliest_confirmed_dna_sample_timestamp(
            session=session,
            biobank_id=summary.biobankId
        )

    @classmethod
    def get_revised_consent_time(cls, participant_id, session):
        revised_consent_time_list = []
        response_collection = QuestionnaireResponseRepository.get_responses_to_surveys(
            session=session,
            survey_codes=[code_constants.PRIMARY_CONSENT_UPDATE_MODULE],
            participant_ids=[participant_id]
        )
        if participant_id in response_collection:
            program_update_response_list = response_collection[participant_id].responses.values()
            for response in program_update_response_list:
                reconsent_answer = response.get_single_answer_for(
                    code_constants.PRIMARY_CONSENT_UPDATE_QUESTION_CODE
                ).value.lower()
                if reconsent_answer == code_constants.COHORT_1_REVIEW_CONSENT_YES_CODE.lower():
                    revised_consent_time_list.append(response.authored_datetime)
        return min_or_none(revised_consent_time_list)

    @classmethod
    def get_exposures_time(cls, data_log_list: List[PediatricDataLog]):
        return min_or_none([
            parse(item.value)
            for item in data_log_list
            if item.data_type == PediatricDataType.ENVIRONMENTAL_EXPOSURES
        ])


def run():
    return cli_run(tool_cmd, tool_desc, EnrollmentDependenciesBackfill)
