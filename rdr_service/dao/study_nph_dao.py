from rdr_service.dao.base_dao import BaseDao
from rdr_service.model.study_nph import Participant


class NphParticipantDao(BaseDao):

    def __init__(self):
        super(NphParticipantDao, self).__init__(Participant)

    def get_id(self, obj: Participant):
        return obj.id
