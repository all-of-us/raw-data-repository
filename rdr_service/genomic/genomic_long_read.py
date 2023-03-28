from rdr_service.dao.genomics_dao import GenomicLongReadDao


class GenomicLongRead:

    dao = GenomicLongReadDao()

    @classmethod
    def create_genomic_long_read(cls) -> 'GenomicLongRead':
        return GenomicLongRead()

    def run_lr_workflow(self, file_data):
        pass
