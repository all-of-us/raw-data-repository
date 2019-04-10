from dao.base_dao import UpdatableDao
from model.genomics import GenomicSet, GenomicSetMember
from query import Query, Operator, PropertyType, FieldFilter, Results

class GenomicSetDao(UpdatableDao):
  """ Stub for GenomicSet model """

  def __init__(self):
    super(GenomicSetDao, self).__init__(GenomicSet, order_by_ending=['id'])

  def get_one_by_file_name(self, filename):
    return super(GenomicSetDao, self) \
      .query(Query([FieldFilter('genomicSetFile', Operator.EQUALS, filename)], None, 1, None)).items


class GenomicSetMemberDao(UpdatableDao):
  """ Stub for GenomicSetMember model """

  def __init__(self):
    super(GenomicSetMemberDao, self).__init__(GenomicSetMember, order_by_ending=['id'])


