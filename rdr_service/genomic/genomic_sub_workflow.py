from rdr_service import clock
from rdr_service.genomic_enums import GenomicJob


class GenomicSubWorkflow:

    def __init__(self, dao, job_id):
        self.dao = dao()
        self.job_id = job_id
        self.row_data = []

    def __get_subworkflow_method(self):
        return {
            GenomicJob.PR_PR_WORKFLOW: self.run_request_ingestion
        }[self.job_id]

    @classmethod
    def create_genomic_sub_workflow(cls, *, dao, job_id):
        return cls(dao, job_id)

    def run_workflow(self, *, row_data):
        self.row_data = row_data
        self.__get_subworkflow_method()()

    def run_request_ingestion(self):
        current_set = self.dao.get_max_set()
        incremented_set_number = 1 if current_set[0] is None else current_set[0]

        new_pipeline_members = self.dao.get_new_pipeline_members(
            biobank_ids=[row.get('biobank_id')[1:] for row in self.row_data],
        )

        pipeline_objs = []
        p_site_id = self.row_data[0].get('p_site_id')

        for member in new_pipeline_members:
            pipeline_objs.append({
                'created': clock.CLOCK.now(),
                'modified': clock.CLOCK.now(),
                'genomic_set_member_id': member.genomic_set_member_id,
                'biobank_id': member.biobank_id,
                # later sub-pipelines will have logic for handling this defaulting for now
                'p_site_id': p_site_id,
                'proteomics_set': incremented_set_number
            })

        self.dao.insert_bulk(pipeline_objs)
