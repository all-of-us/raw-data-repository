from typing import List, OrderedDict

from rdr_service import clock
from rdr_service.dao.genomics_dao import GenomicLongReadDao


class GenomicLongReadWorkFlow:

    def __init__(self):
        self.long_read_dao = GenomicLongReadDao()

    def run_lr_workflow(self, row_data: List[OrderedDict]) -> None:
        current_set = self.long_read_dao.get_max_set()
        incremented_set_number = 0 if current_set[0] is None else current_set[0]

        long_read_members = self.long_read_dao.get_new_long_read_members(
            biobank_ids=[row.get('biobank_id')[1:] for row in row_data],
            parent_tube_ids=[row.get('parent_tube_id') for row in row_data]
        )

        long_read_objs = []
        incremented_set_number += 1
        for long_read_member in long_read_members:
            long_read_objs.append({
                'created': clock.CLOCK.now(),
                'modified': clock.CLOCK.now(),
                'genomic_set_member_id': long_read_member.genomic_set_member_id,
                'biobank_id': long_read_member.biobank_id,
                'lr_site_id': row_data[0]['lr_site_id'],
                'long_read_platform': row_data[0]['long_read_platform'],
                'long_read_set': incremented_set_number
            })

        self.long_read_dao.insert_bulk(long_read_objs)

