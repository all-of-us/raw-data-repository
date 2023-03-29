from typing import List, OrderedDict

from rdr_service import clock
from rdr_service.dao.genomics_dao import GenomicLongReadDao


class GenomicLongReadWorkFlow:

    def __init__(self):
        self.long_read_dao = GenomicLongReadDao()

    def run_lr_workflow(self, row_data: List[OrderedDict]) -> None:

        long_read_members = self.long_read_dao.get_new_long_read_members(
            biobank_ids=[row.get('biobank_id')[1:] for row in row_data],
            parent_tube_ids=[row.get('parent_tube_id') for row in row_data]
        )

        long_read_objs = []
        for long_read_member in long_read_members:
            long_read_objs.append({
                'created': clock.CLOCK.now(),
                'modified': clock.CLOCK.now(),
                'genomic_set_member_id': long_read_member.genomic_set_member_id,
                'biobank_id': long_read_member.biobank_id,
                'lr_site_id': row_data[0]['lr_site_id'],
                'long_read_platform': row_data[0]['long_read_platform']
            })

        self.long_read_dao.insert_bulk(long_read_objs)

