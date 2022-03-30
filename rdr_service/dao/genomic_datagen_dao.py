from rdr_service.dao.base_dao import BaseDao
from rdr_service.model.genomic_datagen import GenomicDataGenCaseTemplate


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
