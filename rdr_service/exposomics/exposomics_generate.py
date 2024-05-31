from typing import List

from rdr_service import clock
from rdr_service.dao.exposomics_dao import ExposomicsSamplesDao
from rdr_service.exposomics.exposomics_manifests import ExposomicsM0Workflow


class ExposomicsGenerate:

    def __init__(self, sample_list: List[dict], form_data: dict):
        self.sample_list = sample_list
        self.form_data = form_data
        self.new_set = None
        self.samples_dao = ExposomicsSamplesDao()

    @classmethod
    def create_exposomics_generate_workflow(cls, *, sample_list: List[dict], form_data: dict):
        return cls(sample_list, form_data)

    def get_incremented_set_number(self) -> int:
        current_set: List[int] = self.samples_dao.get_max_set()
        return 1 if current_set[0] is None else current_set[0] + 1

    def store_current_samples(self):
        self.new_set = self.get_incremented_set_number()
        for el in self.sample_list:
            el['created'] = clock.CLOCK.now()
            el['modified'] = clock.CLOCK.now()
            el['exposomics_set'] = self.new_set
        self.samples_dao.insert_bulk(self.sample_list)

    def run_generation(self):
        self.store_current_samples()
        ExposomicsM0Workflow(
            sample_list=self.sample_list,
            form_data=self.form_data,
            set_num=self.new_set
        ).generate_manifest()
