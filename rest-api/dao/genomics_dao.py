from dao.base_dao import UpdatableDao
from model.genomics import GenomicSet, GenomicSetMember
from query import Query, Operator, FieldFilter, OrderBy

class GenomicSetDao(UpdatableDao):
  """ Stub for GenomicSet model """

  def __init__(self):
    super(GenomicSetDao, self).__init__(GenomicSet, order_by_ending=['id'])

  def get_one_by_file_name(self, filename):
    return super(GenomicSetDao, self) \
      .query(Query([FieldFilter('genomicSetFile', Operator.EQUALS, filename)], None, 1, None)).items

  def get_new_version_number(self, genomic_set_name):
    genomic_sets = super(GenomicSetDao, self)\
      .query(Query([FieldFilter('genomicSetName', Operator.EQUALS, genomic_set_name)],
                   OrderBy('genomicSetVersion', False), 1, None)).items
    if genomic_sets:
      return genomic_sets[0].genomicSetVersion + 1
    else:
      return 1

class GenomicSetMemberDao(UpdatableDao):
  """ Stub for GenomicSetMember model """

  def __init__(self):
    super(GenomicSetMemberDao, self).__init__(GenomicSetMember, order_by_ending=['id'])

  def upsert_all(self, genomic_set_members):
    """Inserts/updates members. """
    members = list(genomic_set_members)

    def upsert(session):
      written = 0
      for member in members:
        session.merge(member)
        written += 1
      return written
    return self._database.autoretry(upsert)

  def get_all_by_genomic_set_id(self, genomic_set_id):
    return super(GenomicSetMemberDao, self) \
      .query(Query([FieldFilter('genomicSetId', Operator.EQUALS, genomic_set_id)],
                   None, None, None)).items
