from rdr_service.dao.base_dao import BaseDao
from rdr_service.model.rex import Study, ParticipantMapping


class RexStudyDao(BaseDao):

    def __init__(self):
        super(RexStudyDao, self).__init__(Study)

    def get_id(self, obj: Study):
        return obj.id


class RexParticipantMappingDao(BaseDao):

    def __init__(self):
        super(RexParticipantMappingDao, self).__init__(ParticipantMapping)

    def get_id(self, obj: ParticipantMapping):
        return obj.id
