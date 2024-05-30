from rdr_service.dao.base_dao import BaseDao
from rdr_service.model.ppsc import Participant


class ParticipantDao(BaseDao):

    def __init__(self):
        super().__init__(Participant)

    def to_client_json(self, participant: Participant) -> str:
        return f'Participant P{participant.id} was created successfully'

    def get_participant_by_participant_id(self, *, participant_id: int):
        with self.session() as session:
            return session.query(Participant).filter(Participant.id == participant_id).all()


class PPSCDefaultBaseDao(BaseDao):
    def __init__(self, model_type):
        super().__init__(model_type)

    def from_client_json(self):
        pass

    def get_id(self, obj):
        return obj.id
