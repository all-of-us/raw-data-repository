from datetime import date
from logging import Logger

from sqlalchemy.orm import Session

from rdr_service.dao.ghost_check_dao import GhostCheckDao, GhostFlagModification
from rdr_service.model.participant import Participant
from rdr_service.model.utils import from_client_participant_id
from rdr_service.services.ptsc_client import PtscClient


class GhostCheckService:
    def __init__(self, session: Session, logger: Logger, ptsc_config: dict):
        self._session = session
        self._logger = logger
        self._config = ptsc_config

    def run_ghost_check(self, start_date: date, end_date: date = None):
        """
        Finds all the participants that need to be checked to see if they're ghosts and calls out to Vibrent's API
        to check them, recording the result.
        """
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
        while response:
            for participant_data in response['participants']:
                participant_id_str = participant_data['drcId']
                if not participant_id_str:
                    self._logger.error(f'Vibrent has missing drc id: {participant_data}')
                else:
                    participant_id = from_client_participant_id(participant_id_str)
                    if participant_id in ids_not_found:
                        self._record_ghost_result(
                            is_ghost_response=False,
                            participant=participant_map[participant_id]
                        )
                        ids_not_found.remove(participant_id)
                    else:
                        self._logger.error(f'Vibrent had unknown id: {participant_id}')

            response = client.request_next_page(response)

        for participant_id in ids_not_found:
            response = client.get_participant_lookup(participant_id=participant_id)
            is_ghost_response = response is None
            self._record_ghost_result(
                is_ghost_response=is_ghost_response,
                participant=participant_map[participant_id]
            )

    def _record_ghost_result(self, is_ghost_response: bool, participant: Participant):
        ghost_flag_change_made = None
        is_ghost_database = bool(participant.isGhostId)
        if is_ghost_database and not is_ghost_response:
            ghost_flag_change_made = GhostFlagModification.GHOST_FLAG_REMOVED
        elif is_ghost_response and not is_ghost_database:
            ghost_flag_change_made = GhostFlagModification.GHOST_FLAG_SET

        if ghost_flag_change_made:
            self._logger.error(f'{str(ghost_flag_change_made)} for {participant.participantId}')
            # TODO: load participant and set/unset ghost flag

        GhostCheckDao.record_ghost_check(
            session=self._session,
            participant_id=participant.participantId,
            modification_performed=ghost_flag_change_made
        )
        self._session.commit()
