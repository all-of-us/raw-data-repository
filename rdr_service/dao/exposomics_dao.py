from abc import abstractmethod, ABC
from typing import List, Dict

from sqlalchemy.sql.expression import literal
from sqlalchemy import func, orm

from rdr_service import config
from rdr_service.dao.base_dao import BaseDao, UpdatableDao
from rdr_service.model.config_utils import get_biobank_id_prefix
from rdr_service.model.exposomics import ExposomicsM0, ExposomicsSamples, ExposomicsM1
from rdr_service.model.genomics import GenomicSetMember
from rdr_service.model.participant import Participant
from rdr_service.model.participant_summary import ParticipantSummary
from rdr_service.participant_enums import QuestionnaireStatus, SuspensionStatus, WithdrawalStatus


class ExposomicsBase:

    def insert_bulk(self, batch: List[Dict]) -> None:
        with self.session() as session:
            session.bulk_insert_mappings(self.model_type, batch)

    def bulk_update(self, model_objects: List[Dict]) -> None:
        with self.session() as session:
            session.bulk_update_mappings(self.model_type, model_objects)


class ExposomicsSamplesDao(BaseDao, ExposomicsBase):

    def __init__(self):
        super().__init__(ExposomicsSamples, order_by_ending=["id"])

    def from_client_json(self):
        pass

    def get_id(self, obj):
        pass

    @classmethod
    def get_max_set_subquery(cls):
        return orm.Query(
            func.max(ExposomicsSamples.exposomics_set).label('exposomics_set')
        ).subquery()

    def get_max_set(self):
        with self.session() as session:
            return session.query(
                self.get_max_set_subquery()
            ).one()


class ExposomicsManifestDao(ABC, BaseDao):

    def from_client_json(self):
        pass

    def get_id(self, obj):
        pass

    @abstractmethod
    def get_manifest_data(self, **kwargs):
        ...


class ExposomicsM0Dao(BaseDao, ExposomicsBase):

    def __init__(self):
        super().__init__(ExposomicsM0, order_by_ending=["id"])

    def get_manifest_data(self, **kwargs):
        form_data = kwargs.get("form_data")
        sample_list = kwargs.get("sample_list")
        current_set_num = kwargs.get("set_num")
        biobank_ids = [obj.get('biobank_id') for obj in sample_list]
        with self.session() as session:
            return session.query(
                ExposomicsSamples.collection_tube_id.label('collection_tube_id'),
                literal(form_data.get('sample_type')).label('sample_type'),
                func.concat(get_biobank_id_prefix(), ExposomicsSamples.biobank_id).label('biobank_id'),
                literal(form_data.get('treatment_type')).label('treatment_type'),
                func.IF(GenomicSetMember.nyFlag == 1,
                        literal("Y"),
                        literal("N")).label('ny_flag'),
                literal('Y').label('validation_passed'),  # hardcoded for now
                ExposomicsSamples.sample_id.label('sample_id'),
                literal(form_data.get('study_name')).label('study_name'),
                literal(form_data.get('study_pi_first_name')).label('study_pi_first_name'),
                literal(form_data.get('study_pi_last_name')).label('study_pi_last_name'),
                literal(form_data.get('quantity_ul')).label('quantity_ul'),
                literal(form_data.get('total_concentration_ng_ul')).label('total_concentration_ng_ul'),
                literal(form_data.get('freeze_thaw_count')).label('freeze_thaw_count')
            ).join(
                Participant,
                Participant.biobankId == ExposomicsSamples.biobank_id
            ).join(
                GenomicSetMember,
                GenomicSetMember.biobankId == Participant.biobankId
            ).filter(
                ExposomicsSamples.biobank_id.in_(biobank_ids),
                ExposomicsSamples.exposomics_set == current_set_num,
                GenomicSetMember.genomeType == config.GENOME_TYPE_ARRAY,
                GenomicSetMember.blockResults != 1,
                GenomicSetMember.blockResearch != 1,
                ParticipantSummary.withdrawalStatus == WithdrawalStatus.NOT_WITHDRAWN,
                ParticipantSummary.suspensionStatus == SuspensionStatus.NOT_SUSPENDED,
                ParticipantSummary.consentForStudyEnrollment == QuestionnaireStatus.SUBMITTED,
                Participant.isGhostId.is_(None),
                Participant.isTestParticipant != 1
            ).distinct().all()


class ExposomicsM1Dao(UpdatableDao, ExposomicsBase):

    validate_version_match = False

    def __init__(self):
        super().__init__(ExposomicsM1, order_by_ending=["id"])

    def from_client_json(self):
        pass

    def get_id(self, obj):
        pass

    def get_manifest_data(self, **kwargs):
        file_path = kwargs.get('file_path')
        with self.session() as session:
            return session.query(
                func.json_extract(ExposomicsM1.row_data, "$.package_id").label('package_id'),
                func.json_extract(ExposomicsM1.row_data, "$.box_storageunit_id").label('box_storageunit_id'),
                func.json_extract(ExposomicsM1.row_data, "$.box_id_plate_id").label('box_id_plate_id'),
                func.json_extract(ExposomicsM1.row_data, "$.well_position").label('well_position'),
                func.json_extract(ExposomicsM1.row_data, "$.biobankid_sampleid").label('biobankid_sampleid'),
                func.concat(get_biobank_id_prefix(), ExposomicsM1.biobank_id).label('biobank_id'),
                func.json_extract(ExposomicsM1.row_data, "$.sample_id").label('sample_id'),
                func.json_extract(ExposomicsM1.row_data, "$.matrix_id").label('matrix_id'),
                func.json_extract(ExposomicsM1.row_data, "$.parent_sample_id").label('parent_sample_id'),
                func.json_extract(ExposomicsM1.row_data, "$.collection_tube_id").label('collection_tube_id'),
                func.json_extract(ExposomicsM1.row_data, "$.sample_type").label('sample_type'),
                func.json_extract(ExposomicsM1.row_data, "$.ny_flag").label('ny_flag'),
                func.json_extract(ExposomicsM1.row_data, "$.quantity_ul").label('quantity_ul'),
                func.json_extract(ExposomicsM1.row_data, "$.total_concentration_ng_ul").label(
                    'total_concentration_ng_ul'
                ),
                func.json_extract(ExposomicsM1.row_data, "$.total_yield_ng").label('total_yield_ng'),
                func.json_extract(ExposomicsM1.row_data, "$.rqs").label('rqs'),
                # func.json_extract(ExposomicsM1.row_data, "$.260_230").label('260_230'),
                # func.json_extract(ExposomicsM1.row_data, "$.260_280").label('260_280'),
                func.json_extract(ExposomicsM1.row_data, "$.study_name").label('study_name'),
                func.json_extract(ExposomicsM1.row_data, "$.contact").label('contact'),
                func.json_extract(ExposomicsM1.row_data, "$.email").label('email'),
                func.json_extract(ExposomicsM1.row_data, "$.tracking_number").label('tracking_number'),
            ).filter(
                ExposomicsM1.file_path == file_path
            ).all()

    def get_id_from_file_path(self, *, file_path: str):
        with self.session() as session:
            return session.query(
                ExposomicsM1.id
            ).filter(
                ExposomicsM1.file_path == file_path
            )

