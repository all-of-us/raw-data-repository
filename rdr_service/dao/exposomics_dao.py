from sqlalchemy.sql.expression import literal
from sqlalchemy import func

from rdr_service import config
from rdr_service.dao.base_dao import BaseDao
from rdr_service.model.biobank_stored_sample import BiobankStoredSample
from rdr_service.model.config_utils import get_biobank_id_prefix
from rdr_service.model.exposomics import ExposomicsM0
from rdr_service.model.genomics import GenomicSetMember
from rdr_service.model.participant import Participant
from rdr_service.model.participant_summary import ParticipantSummary
from rdr_service.participant_enums import QuestionnaireStatus, SuspensionStatus, WithdrawalStatus


class ExposomicsM0Dao(BaseDao):

    def __init__(self):
        super().__init__(ExposomicsM0, order_by_ending=["id"])

    def from_client_json(self):
        pass

    def get_id(self, obj):
        pass

    def get_manifest_data(self, **kwargs):
        form_data = kwargs.get("form_data")
        sample_list = kwargs.get("sample_list")
        biobank_ids = [obj.get('biobank_id') for obj in sample_list]
        # need to know which stored sample for collection tube id - getting first for now
        with self.session() as session:
            return session.query(
                BiobankStoredSample.biobankStoredSampleId.label('collection_tube_id'),
                literal(form_data.get('sample_type')).label('sample_type'),
                func.concat(get_biobank_id_prefix(), BiobankStoredSample.biobankId).label('biobank_id'),
                literal(form_data.get('treatment_type')).label('treatment_type'),
                func.IF(GenomicSetMember.nyFlag == 1,
                        literal("Y"),
                        literal("N")).label('ny_flag'),
                literal('Y').label('validation_passed'),  # hardcoded for now
                literal(form_data.get('study_name')).label('study_name'),
                literal(form_data.get('study_pi_first_name')).label('study_pi_first_name'),
                literal(form_data.get('study_pi_last_name')).label('study_pi_last_name'),
                literal(form_data.get('quantity_ul')).label('quantity_ul'),
                literal(form_data.get('total_concentration_ng_ul')).label('total_concentration_ng_ul'),
                literal(form_data.get('freeze_thaw_count')).label('freeze_thaw_count')
            ).join(
                ParticipantSummary,
                ParticipantSummary.biobankId == BiobankStoredSample.biobankId
            ).join(
                Participant,
                Participant.biobankId == ParticipantSummary.biobankId
            ).join(
                GenomicSetMember,
                GenomicSetMember.biobankId == ParticipantSummary.biobankId
            ).filter(
                BiobankStoredSample.biobankId.in_(biobank_ids),
                GenomicSetMember.genomeType == config.GENOME_TYPE_ARRAY,
                GenomicSetMember.blockResults != 1,
                GenomicSetMember.blockResearch != 1,
                ParticipantSummary.withdrawalStatus == WithdrawalStatus.NOT_WITHDRAWN,
                ParticipantSummary.suspensionStatus == SuspensionStatus.NOT_SUSPENDED,
                ParticipantSummary.consentForStudyEnrollment == QuestionnaireStatus.SUBMITTED,
                Participant.isGhostId.is_(None),
                Participant.isTestParticipant != 1
            ).all()


class ExposomicsDefaultBaseDao(BaseDao):
    def __init__(self, model_type):
        super().__init__(
            model_type, order_by_ending=['id']
        )

    def from_client_json(self):
        pass

    def get_id(self, obj):
        pass
