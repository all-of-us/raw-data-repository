from datetime import date
from logging import Logger

from sqlalchemy.orm import Session

from rdr_service.dao.ghost_check_dao import GhostCheckDao, GhostFlagModification
from rdr_service.dao.participant_dao import ParticipantDao
from rdr_service.model.participant import Participant
from rdr_service.model.utils import from_client_participant_id
from rdr_service.offline.bigquery_sync import dispatch_participant_rebuild_tasks
from rdr_service.services.ptsc_client import PtscClient

class GhostCheckService:
    def __init__(self, session: Session, logger: Logger, ptsc_config: dict):
        self._session = session
        self._logger = logger
        self._config = ptsc_config
        self._participant_dao = ParticipantDao()

    def run_ghost_check(self, start_date: date, end_date: date = None):
        """
        Finds all the participants that need to be checked to see if they're ghosts and calls out to Vibrent's API
        to check them, recording the result.
        """
        # PDR-855:  Need to rebuild the PDR participant summary data for pids whose ghost status has changed
        pdr_rebuild_list = list()
        db_participants = GhostCheckDao.get_participants_needing_checked(
            session=self._session,
            earliest_signup_time=start_date,
            latest_signup_time=end_date
        )
        participant_map = {participant.participantId: participant for participant in db_participants}
        ids_not_found = {participant.participantId for participant in db_participants}

        client = PtscClient(
            auth_url=self._config['token_endpoint'],
            request_url=self._config['request_url'],
            client_id=self._config['client_id'],
            client_secret=self._config['client_secret']
        )
        response = client.get_participant_lookup(start_date=start_date, end_date=end_date)
        while response:  # Keep checking until no more pages are left
            for participant_data in response['participants']:
                participant_id_str = participant_data['drcId']
                if not participant_id_str:
                    self._logger.error(f'Vibrent has missing drc id: {participant_data}')
                else:
                    participant_id = from_client_participant_id(participant_id_str)
                    if participant_id in ids_not_found:
                        ghost_change = self._record_ghost_result(
                            is_ghost_response=False,
                            participant=participant_map[participant_id]
                        )
                        ids_not_found.remove(participant_id)
                        if ghost_change:
                            pdr_rebuild_list.append(participant_id)
                    else:
                        self._logger.error(f'Vibrent had unknown id: {participant_id}')

            response = client.request_next_page(response)

        for participant_id in ids_not_found:
            # Individually check each participant not seen in the API data yet (their date might be a little off).
            response = client.get_participant_lookup(participant_id=participant_id)
            is_ghost_response = response is None
            ghost_change = self._record_ghost_result(
                is_ghost_response=is_ghost_response,
                participant=participant_map[participant_id]
            )
            if ghost_change:
                pdr_rebuild_list.append(participant_id)

        if len(pdr_rebuild_list):
            # PDR BQ module views select the test/ghost flag from participant data; don't need to rebuild module data
            dispatch_participant_rebuild_tasks(pdr_rebuild_list, build_modules=False)

    def _record_ghost_result(self, is_ghost_response: bool, participant: Participant) -> bool:
        """ Update the participant isGhostId status if needed.  Returns true if update was performed """
        ghost_flag_change_made = None
        is_ghost_database = bool(participant.isGhostId)
        if is_ghost_database and not is_ghost_response:
            ghost_flag_change_made = GhostFlagModification.GHOST_FLAG_REMOVED
        elif is_ghost_response and not is_ghost_database:
            ghost_flag_change_made = GhostFlagModification.GHOST_FLAG_SET

        if ghost_flag_change_made:
            self._logger.info(f'{str(ghost_flag_change_made)} for {participant.participantId}')
            self._participant_dao.update_ghost_participant(
                session=self._session,
                pid=participant.participantId,
                is_ghost=ghost_flag_change_made == GhostFlagModification.GHOST_FLAG_SET
            )

        GhostCheckDao.record_ghost_check(
            session=self._session,
            participant_id=participant.participantId,
            modification_performed=ghost_flag_change_made
        )
        self._session.commit()

        return True if ghost_flag_change_made else False

