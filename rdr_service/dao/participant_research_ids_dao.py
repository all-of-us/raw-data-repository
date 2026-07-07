from typing import List

from rdr_service.model.participant import Participant
from rdr_service.dao.base_dao import UpsertableDao
from rdr_service.model.participant_research_ids import ParticipantResearchIds

_MIN_REGISTERED_TIER_RESEARCH_ID = 200001000000
_MAX_REGISTERED_TIER_RESEARCH_ID = 200009999999
_MIN_CONTROLLED_TIER_PLUS_RESEARCH_ID = 300001000000
_MAX_CONTROLLED_TIER_PLUS_RESEARCH_ID = 300009999999

class ParticipantResearchIdsDao(UpsertableDao):
    def __init__(self):
        super().__init__(ParticipantResearchIds)

    def insert_random_research_ids(self, obj: ParticipantResearchIds, fields: List[str],
                                   min_id: int = _MIN_REGISTERED_TIER_RESEARCH_ID,
                                   max_id: int = _MAX_REGISTERED_TIER_RESEARCH_ID) -> None:
        """Attempts to insert an entity with randomly assigned ID(s) repeatedly until success
    or a maximum number of attempts are performed."""
        self._insert_with_random_id(obj, fields, min_id=min_id, max_id=max_id, insert_fun=self.upsert_with_session)

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

    def get_participants_missing_controlled_tier_plus_ids(
        self, participant_count: int = 500
    ) -> List[ParticipantResearchIds]:
        with self.session() as session:
            return session.query(
                ParticipantResearchIds
            ).filter(
                ParticipantResearchIds.controlled_tier_plus_id.is_(None)
            ).order_by(
                ParticipantResearchIds.participant_id.asc()
            ).limit(participant_count).all()

    def insert_missing_controlled_tier_plus_ids(
        self, participant_research_ids: List[ParticipantResearchIds]
    ) -> None:
        for research_ids in participant_research_ids:
            self.insert_random_research_ids(
                research_ids,
                ['controlled_tier_plus_id'],
                min_id=_MIN_CONTROLLED_TIER_PLUS_RESEARCH_ID,
                max_id=_MAX_CONTROLLED_TIER_PLUS_RESEARCH_ID
            )

    def insert_new_participants(self, participant_objects: List[Participant]) -> None:
        insert_objects = []
        for participant in participant_objects:
            p_to_insert = ParticipantResearchIds(participant_id=participant.participantId,
                                                 research_id=participant.researchId,
                                                 external_id=participant.externalId)
            insert_objects.append(p_to_insert)
        with self.session() as session:
            session.bulk_save_objects(insert_objects)

        for participant in participant_objects:
            research_ids = ParticipantResearchIds(participant_id=participant.participantId)
            self.insert_random_research_ids(research_ids,['registered_tier_id'])
            self.insert_random_research_ids(research_ids,['controlled_tier_plus_id'], min_id=_MIN_CONTROLLED_TIER_PLUS_RESEARCH_ID, max_id=_MAX_CONTROLLED_TIER_PLUS_RESEARCH_ID)
