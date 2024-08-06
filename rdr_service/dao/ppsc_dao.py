from typing import List, Dict

from rdr_service.dao.base_dao import BaseDao, UpdatableDao
from rdr_service.model.ppsc import Participant, Site


class ParticipantDao(BaseDao):

    def __init__(self):
        super().__init__(Participant)

    def to_client_json(self, participant: Participant) -> str:
        return f'Participant P{participant.id} was created successfully'

    def get_participant_by_participant_id(self, *, participant_id: int):
        with self.session() as session:
            return session.query(Participant).filter(Participant.id == participant_id).all()

    def get_participant_by_biobank_id(self, *, biobank_id: int):
        with self.session() as session:
            return session.query(Participant).filter(Participant.biobank_id == biobank_id).all()


class SiteDao(UpdatableDao):

    validate_version_match = False

    def __init__(self):
        super().__init__(Site)

    def to_client_json(self, site_data: dict) -> str:
        return f'Site {site_data["id"]} was created successfully'


class PPSCDefaultBaseDao(BaseDao):

    def __init__(self, model_type):
        super().__init__(model_type)

    def from_client_json(self):
        pass

    def to_client_json(self, payload):
        return f"Event Record Created for: {payload['participantId']}"

    def get_id(self, obj):
        return obj.id

    def insert_bulk(self, batch: List[Dict]) -> None:
        with self.session() as session:
            session.bulk_insert_mappings(
                self.model_type,
                batch
            )
