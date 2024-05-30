from typing import List

from rdr_service.exposomics.exposomics_manifests import ExposomicsM0Workflow


class ExposomicsGenerate:

    def __init__(self, sample_list: List[dict], form_data: dict):
        self.sample_list = sample_list
        self.form_data = form_data

    @classmethod
    def create_exposomics_generate_workflow(cls, *, sample_list: List[dict], form_data: dict):
        return cls(sample_list, form_data)

    def run_generation(self):
        ExposomicsM0Workflow(
            sample_list=self.sample_list,
            form_data=self.form_data
        ).generate_manifest()
