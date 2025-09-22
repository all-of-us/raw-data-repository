from typing import List

from rdr_service.model.participant import Participant
from rdr_service.dao.base_dao import BaseDao
from rdr_service.model.participant_research_ids import ParticipantResearchIds

_MIN_RESEARCH_ID = 1000000
_MAX_RESEARCH_ID = 9999999

class ParticipantResearchIdsDao(BaseDao):
    def __init__(self):
        super().__init__(ParticipantResearchIds)

    def insert_random_research_ids(self, obj: ParticipantResearchIds, fields: List[str],
                                   min_id: int = _MIN_RESEARCH_ID,
                                   max_id: int = _MAX_RESEARCH_ID) -> None:
        """Attempts to insert an entity with randomly assigned ID(s) repeatedly until success
    or a maximum number of attempts are performed."""
        self._insert_with_random_id(obj, fields, min_id=min_id, max_id=max_id)

    def get_new_participants(self, participant_count: int = 500) -> List[Participant]:
        with self.session() as session:
            to_insert = session.query(
                Participant
            ).join(
                ParticipantResearchIds, ParticipantResearchIds.participant_id == Participant.participantId, isouter=True
            ).filter(
                ParticipantResearchIds.participant_id.is_(None)
            ).order_by(
                Participant.signUpTime.asc()
            ).limit(participant_count).all()
            return to_insert

    def insert_new_participants(self, participant_objects: List[Participant]) -> None:
            insert_objects = []
            for p in participant_objects:
                p_to_insert = ParticipantResearchIds(participant_id=p.participantId,
                                               research_id=p.researchId,
                                               external_id=p.externalId)
                insert_objects.append(p_to_insert)
            with self.session() as session:
                session.bulk_save_objects(insert_objects)

            for p in participant_objects:
                research_ids = ParticipantResearchIds(participant_id=p.participantId)
                self.insert_random_research_ids(research_ids, ['controlled_tier_id', 'registered_tier_id', 'controlled_tier_plus_id'])



