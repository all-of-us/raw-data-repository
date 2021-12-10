from datetime import datetime
from logging import Logger

from sqlalchemy.orm import Session

from rdr_service.clock import CLOCK
from rdr_service.dao.ghost_check_dao import GhostCheckDao
from rdr_service.model.ghost_api_check import GhostApiCheck, GhostFlagModification
from rdr_service.services.ptsc_client import PtscClient


class GhostCheckService:
    def __init__(self, session: Session, logger: Logger, max_number_of_participants_to_check: int, ):
        self._session = session
        self._logger = logger
        self._check_limit = max_number_of_participants_to_check

    def run_ghost_check(self):
        """
        Finds all the participants that need to be checked to see if they're ghosts and calls out to Vibrent's API
        to check them, recording the result.
        """
        # Get participants that will be checked, limiting to a fixed number and logging if a large number of
        # participants are not being checked.
        ignore_checks_since = datetime.now()  # TODO: get the time window from the config
        checks_ready_to_make = GhostCheckDao.get_participants_needing_checked(
            session=self._session,
            start_date=ignore_checks_since
        )
        checks_ready_to_make.sort(key=lambda x: x.timestamp)  # TODO: Make sure to check new participants first
        if len(checks_ready_to_make) > self._check_limit:
            num_checks_missed = len(checks_ready_to_make) - self._check_limit
            self._logger.warning(f'{num_checks_missed} participants need to be checked but will not be')
            checks_ready_to_make = checks_ready_to_make[:self._check_limit]

        # Check each of the participants, recording their result
        client = PtscClient()
        for check_needed in checks_ready_to_make:
            response = client.get_participant_lookup(participant_id=check_needed.participant_id)
            is_ghost_response = ...  # TODO: figure out if the API has them as a ghost
            self._record_ghost_result(
                is_ghost_database=check_needed.is_ghost,
                is_ghost_response=is_ghost_response,
                participant_id=check_needed.participant_id
            )

    def _record_ghost_result(self, is_ghost_database: bool, is_ghost_response: bool, participant_id: int):
        ghost_flag_change_made = None
        if is_ghost_database and not is_ghost_response:
            ghost_flag_change_made = GhostFlagModification.GHOST_FLAG_REMOVED
        elif is_ghost_response and not is_ghost_database:
            ghost_flag_change_made = GhostFlagModification.GHOST_FLAG_SET

        if ghost_flag_change_made:
            # TODO: load participant and set/unset ghost flag

        self._session.add(GhostApiCheck(
            participant_id=participant_id,
            timestamp=CLOCK.now(),
            modification_performed=ghost_flag_change_made
        ))
        self._session.commit()
