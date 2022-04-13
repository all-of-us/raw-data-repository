from datetime import datetime

from sqlalchemy.orm import Query
from sqlalchemy.sql import functions

from rdr_service.dao.base_dao import BaseDao
from rdr_service.model.genomic_datagen import GenomicDataGenCaseTemplate, GenomicDataGenRun, GenomicDatagenMemberRun,\
    GenomicDataGenOutputTemplate
from rdr_service.model.genomics import *
from rdr_service.model.participant import Participant
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
            ).one()

    def get_output_template_data(
        self,
        attr_records,
        datagen_run_id=None,
        sample_ids=None
    ):
        # build base tables
        eval_attrs = [eval(obj) for obj in attr_records]
        with self.session() as session:
            records = Query(eval_attrs, session=session)
            # base joins
            records = records.join(
                ParticipantSummary,
                ParticipantSummary.participantId == GenomicSetMember.participantId
            ).join(
                Participant,
                ParticipantSummary.participantId == Participant.participantId
            )
            if datagen_run_id:
                records = records.join(
                    GenomicDatagenMemberRun,
                    GenomicDatagenMemberRun.created_run_id == datagen_run_id
                )
                return records.distinct().all()
            if sample_ids:
                records = records.filter(
                    GenomicSetMember.sampleId.in_(sample_ids)
                )
                return records.distinct().all()


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

