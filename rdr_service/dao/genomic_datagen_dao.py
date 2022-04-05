from datetime import datetime
from rdr_service.dao.base_dao import BaseDao
from rdr_service.model.genomic_datagen import GenomicDataGenCaseTemplate, GenomicDataGenRun, \
    GenomicDatagenMemberRun, GenomicDataGenOutputTemplate


class GenomicDataGenRunDao(BaseDao):
    def __init__(self):
        super(GenomicDataGenRunDao, self).__init__(
            GenomicDataGenRun, order_by_ending=['id'])

    def get_id(self, obj):
        pass

    def from_client_json(self):
        pass


class GenomicDataGenMemberRunDao(BaseDao):
    def __init__(self):
        super(GenomicDataGenMemberRunDao, self).__init__(
            GenomicDatagenMemberRun, order_by_ending=['id'])

    def get_id(self, obj):
        pass

    def from_client_json(self):
        pass

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
