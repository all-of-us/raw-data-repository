
from sqlalchemy.orm import Session

from rdr_service.dao import database_factory


class BaseRepository:
    def __init__(self, session: Session = None):
        self._session = session

    def _add_to_session(self, schema_object):
        if self._session is None:
            with database_factory.get_database().session() as session:
                session.add(schema_object)
        else:
            self._session.add(schema_object)
            self._session.flush()
