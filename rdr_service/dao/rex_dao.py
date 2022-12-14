from rdr_service.dao.base_dao import BaseDao
from rdr_service.model.rex import Study


class RexStudyDao(BaseDao):

    def __init__(self):
        super(RexStudyDao, self).__init__(Study)

    def get_id(self, obj: Study):
        return obj.id
