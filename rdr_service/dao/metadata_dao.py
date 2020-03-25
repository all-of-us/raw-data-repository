from rdr_service.dao.base_dao import BaseDao
from rdr_service.model.metadata import Metadata

WORKBENCH_LAST_SYNC_KEY = 'WORKBENCH_LAST_SYNC'


class MetadataDao(BaseDao):
    def __init__(self):
        super(MetadataDao, self).__init__(Metadata)

    def upsert_with_session(self, session, key, str_value=None, int_value=None, date_value=None):
        metadata = Metadata(
            key=key,
            strValue=str_value,
            intValue=int_value,
            dateValue=date_value
        )
        exist = self.get_by_key_with_session(session, key)
        if exist:
            setattr(exist, 'strValue', str_value)
            setattr(exist, 'intValue', int_value)
            setattr(exist, 'dateValue', date_value)
        else:
            session.add(metadata)

    def get_by_key_with_session(self, session, key):
        return session.query(Metadata).filter(Metadata.key == key).first()

    def get_by_key(self, key):
        with self.session() as session:
            self.get_by_key_with_session(session, key)

    def upsert(self, key, str_value=None, int_value=None, date_value=None):
        with self.session() as session:
            self.upsert_with_session(session, key, str_value, int_value, date_value)
