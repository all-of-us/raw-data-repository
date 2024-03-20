from rdr_service.dao.base_dao import BaseDao
from rdr_service.model.retention_status_import_failures import RetentionStatusImportFailures


class RetentionStatusImportFailuresDao(BaseDao):

    def __init__(self):
        super(RetentionStatusImportFailuresDao, self).__init__(RetentionStatusImportFailures)

    def insert(self, obj: RetentionStatusImportFailures):
        with self.session() as session:
            self.insert_with_session(session, obj)

    def get(self, failure_id):
        with self.session() as session:
            query = session.query(RetentionStatusImportFailures).filter(
                RetentionStatusImportFailures.id == failure_id
            )
            return query.one_or_none()
