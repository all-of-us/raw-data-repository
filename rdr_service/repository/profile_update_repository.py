
from sqlalchemy.orm import Session

from rdr_service.dao import database_factory
from rdr_service.model.profile_update import ProfileUpdate


class ProfileUpdateRepository:
    def __init__(self, session: Session = None):
        self._session = session

    @classmethod
    def _init_session(cls):
        with database_factory.get_database().session() as session:
            return session

    def store_update_json(self, json):
        update_object = ProfileUpdate(json=json)

        if self._session is None:
            with database_factory.get_database().session() as session:
                session.add(update_object)
        else:
            self._session.add(update_object)
