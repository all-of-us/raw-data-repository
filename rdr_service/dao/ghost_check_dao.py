from datetime import date, datetime

from sqlalchemy.orm import Session

from rdr_service.dao.base_dao import BaseDao
from rdr_service.model.participant import Participant


class GhostCheckDao(BaseDao):
    @classmethod
    def get_participants_needing_checked(cls, session: Session, earliest_signup_time: date):
        """
        Loads the participants that should be checked for ghost status (i.e. to see if they exist in Vibrent's system).
        :param session: Session used to interact with the database.
        :param earliest_signup_time: Data for any Vibrent participants that have a signup
            time later than the provided date will be returned.
        :return: Participant objects with the participantId and isGhostId fields populated
        """
        return session.query(Participant.participantId, Participant.isGhostId).filter(
            Participant.participantOrigin == 'vibrent',
            Participant.signUpTime > datetime(
                year=earliest_signup_time.year,
                month=earliest_signup_time.month,
                day=earliest_signup_time.day
            )
        ).all()

    def get_id(self, obj):
        ...

    def from_client_json(self):
        ...
