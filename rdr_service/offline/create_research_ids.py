from rdr_service.dao.participant_research_ids_dao import ParticipantResearchIdsDao



def create_participant_research_ids():
    research_id_dao = ParticipantResearchIdsDao()
    participants = research_id_dao.get_new_participants()
    research_id_dao.insert_new_participants(participants)

