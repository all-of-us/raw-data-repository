from datetime import datetime

from sqlalchemy import and_
from sqlalchemy.orm import Query, aliased
from sqlalchemy.sql import functions

from rdr_service.dao.base_dao import BaseDao
from rdr_service.model.genomics import GenomicInformingLoop, GenomicSetMember
from rdr_service.model.genomic_datagen import GenomicDataGenCaseTemplate, GenomicDataGenRun, \
    GenomicDatagenMemberRun, GenomicDataGenOutputTemplate, GenomicDataGenManifestSchema
from rdr_service.model.participant_summary import ParticipantSummary


class GenomicDataGenRunDao(BaseDao):
    def __init__(self):
        super(GenomicDataGenRunDao, self).__init__(
            GenomicDataGenRun, order_by_ending=['id'])

    def get_id(self, obj):
        pass

    def from_client_json(self):
        pass

    def get_max_run_id(self):
        with self.session() as session:
            return session.query(
                functions.max(GenomicDataGenRun.id)
            ).filter(
                GenomicDataGenRun.ignore_flag != 1
            ).one()

    def get_output_template_data(
        self,
        attr_records,
        datagen_run_id=None,
        sample_ids=None,
        loop_types=None
    ):
        eval_attrs = [eval(obj) for obj in attr_records]
        hdr_informing_loop_decision = aliased(GenomicInformingLoop)
        pgx_informing_loop_decision = aliased(GenomicInformingLoop)

        loop_attrs_map = {
            'hdr': hdr_informing_loop_decision.decision_value.label('hdr_decision_value'),
            'pgx': pgx_informing_loop_decision.decision_value.label('pgx_decision_value')
        }
        for loop in loop_types:
            eval_attrs.append(loop_attrs_map.get(loop))

        records = Query(eval_attrs)

        with self.session() as session:
            records = records.join(
                ParticipantSummary,
                ParticipantSummary.participantId == GenomicSetMember.participantId
            )
            records = records.outerjoin(
                hdr_informing_loop_decision,
                and_(
                    hdr_informing_loop_decision.participant_id == GenomicSetMember.participantId,
                    hdr_informing_loop_decision.module_type.ilike('hdr')
                )
            )
            records = records.outerjoin(
                pgx_informing_loop_decision,
                and_(
                    pgx_informing_loop_decision.participant_id == GenomicSetMember.participantId,
                    pgx_informing_loop_decision.module_type.ilike('pgx')
                )
            )
            if datagen_run_id:
                records = records.join(
                    GenomicDatagenMemberRun,
                    and_(
                        GenomicDatagenMemberRun.created_run_id == datagen_run_id,
                        GenomicSetMember.id == GenomicDatagenMemberRun.genomic_set_member_id
                    )
                )
                return records.with_session(session).distinct().all()
            if sample_ids:
                records = records.filter(
                    GenomicSetMember.sampleId.in_(sample_ids)
                )
                return records.with_session(session).distinct().all()


class GenomicDataGenMemberRunDao(BaseDao):
    def __init__(self):
        super(GenomicDataGenMemberRunDao, self).__init__(
            GenomicDatagenMemberRun, order_by_ending=['id'])

    def get_id(self, obj):
        pass

    def from_client_json(self):
        pass

    def get_set_members_from_run_id(self, datagen_run_id):
        with self.session() as session:
            return session.query(
                GenomicSetMember
            ).join(
                GenomicDatagenMemberRun,
                GenomicDatagenMemberRun.genomic_set_member_id == GenomicSetMember.id
            ).filter(
                GenomicDatagenMemberRun.created_run_id == datagen_run_id
            ).all()

    def batch_insert_member_records(self, datagen_run_id, template_name, member_ids):
        member_obj_mappings = [{
            'created': datetime.utcnow(),
            'modified': datetime.utcnow(),
            'created_run_id': datagen_run_id,
            'template_name': template_name,
            'genomic_set_member_id': member_id
        } for member_id in member_ids]

        with self.session() as session:
            session.bulk_insert_mappings(self.model_type, member_obj_mappings)


class GenomicDateGenCaseTemplateDao(BaseDao):
    def __init__(self):
        super(GenomicDateGenCaseTemplateDao, self).__init__(
            GenomicDataGenCaseTemplate, order_by_ending=['id'])

    def get_id(self, obj):
        pass

    def from_client_json(self):
        pass

    def get_default_template_records(self, *, project):
        with self.session() as session:
            return session.query(
                GenomicDataGenCaseTemplate
            ).filter(
                GenomicDataGenCaseTemplate.project_name == project,
                GenomicDataGenCaseTemplate.template_name == 'default'
            ).all()

    def get_template_records_template(self, *, project, template_type):
        with self.session() as session:
            return session.query(
                GenomicDataGenCaseTemplate
            ).filter(
                GenomicDataGenCaseTemplate.project_name == project,
                GenomicDataGenCaseTemplate.template_name == template_type
            ).all()


class GenomicDataGenOutputTemplateDao(BaseDao):
    def __init__(self):
        super(GenomicDataGenOutputTemplateDao, self).__init__(
            GenomicDataGenOutputTemplate, order_by_ending=['id'])

    def get_id(self, obj):
        pass

    def from_client_json(self):
        pass

    def get_output_template_records(self, *, project, template_type):
        with self.session() as session:
            return session.query(
                GenomicDataGenOutputTemplate
            ).filter(
                GenomicDataGenOutputTemplate.project_name == project,
                GenomicDataGenOutputTemplate.template_name == template_type
            ).order_by(
                GenomicDataGenOutputTemplate.field_index
            ).all()


class GenomicDataGenManifestSchemaDao(BaseDao):
    def __init__(self):
        super(GenomicDataGenManifestSchemaDao, self).__init__(
            GenomicDataGenManifestSchema, order_by_ending=['id'])

    def get_id(self, obj):
        pass

    def from_client_json(self):
        pass

    def get_template_by_name(self, project_name, template_name):
        with self.session() as session:
            return session.query(GenomicDataGenManifestSchema).filter(
                GenomicDataGenManifestSchema.project_name == project_name,
                GenomicDataGenManifestSchema.template_name == template_name,
                GenomicDataGenManifestSchema.ignore_flag == 0
            ).order_by(GenomicDataGenManifestSchema.field_index).all()

    def execute_manifest_query(self, columns, sample_ids):
        with self.session() as session:
            return session.query(
                *columns
            ).join(
                ParticipantSummary,
                ParticipantSummary.participantId == GenomicSetMember.participantId
            ).filter(GenomicSetMember.sampleId.in_(sample_ids)).all()
