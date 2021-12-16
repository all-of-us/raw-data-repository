from datetime import date, datetime
from typing import Optional

from sqlalchemy.orm import Session

from rdr_service.clock import CLOCK
from rdr_service.dao.base_dao import BaseDao
from rdr_service.model.ghost_api_check import GhostApiCheck, GhostFlagModification
from rdr_service.model.participant import Participant


class GhostCheckDao(BaseDao):
    @classmethod
    def _date_to_datetime(cls, date_obj: date):
        return datetime(
            year=date_obj.year,
            month=date_obj.month,
            day=date_obj.day
        )

    @classmethod
    def get_participants_needing_checked(cls, session: Session, earliest_signup_time: date,
                                         latest_signup_time: date = None):
        """
        Loads the participants that should be checked for ghost status (i.e. to see if they exist in Vibrent's system).
        :param session: Session used to interact with the database.
        :param earliest_signup_time: Data for any Vibrent participants that have a signup
            time later than the provided date will be returned.
        :param latest_signup_time: Optional argument that sets another filter to get participants
            with signup times before the given date.
        :return: Participant objects with the participantId and isGhostId fields populated
        """
        query = session.query(Participant.participantId, Participant.isGhostId).filter(
            Participant.participantOrigin == 'vibrent',
            Participant.signUpTime > cls._date_to_datetime(earliest_signup_time)
        )
        if latest_signup_time:
            query = query.filter(Participant.signUpTime < cls._date_to_datetime(latest_signup_time))
        return query.all()

    @classmethod
    def record_ghost_check(cls, session: Session, participant_id: int,
                           modification_performed: Optional[GhostFlagModification]):
        session.add(GhostApiCheck(
            participant_id=participant_id,
            modification_performed=modification_performed,
            timestamp=CLOCK.now(),
        ))

    def get_id(self, obj):
        ...

    def from_client_json(self):
        ...
