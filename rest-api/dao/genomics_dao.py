from dao.base_dao import UpdatableDao
from model.genomics import GenomicSet, GenomicSetMember

class GenomicSetDao(UpdatableDao):
  """ Stub for GenomicSet model """

  def __init__(self):
    super(GenomicSetDao, self).__init__(GenomicSet)


class GenomicSetMemberDao(UpdatableDao):
  """ Stub for GenomicSetMember model """

  def __init__(self):
    super(GenomicSetMemberDao, self).__init__(GenomicSetMember)

