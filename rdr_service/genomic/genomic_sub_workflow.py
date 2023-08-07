import logging
from typing import List, OrderedDict


class GenomicSubWorkflow:

    def __init__(self, dao):
        self.dao = dao()

    @classmethod
    def create_genomic_sub_workflow(cls, *, dao):
        return cls(dao)

    @classmethod
    def run_ingestion(cls, row_data: List[OrderedDict]):
        logging.info(row_data)
