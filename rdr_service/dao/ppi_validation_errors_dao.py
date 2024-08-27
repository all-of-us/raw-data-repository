from rdr_service.model.utils import UTCDateTime
from rdr_service.dao.base_dao import BaseDao
from rdr_service.model.ppi_validation_errors import PpiValidationErrors
from rdr_service.cloud_utils.gcp_google_pubsub import submit_pipeline_pubsub_msg_from_model


class PpiValidationErrorsDao(BaseDao):

    def __init__(self):
        super(PpiValidationErrorsDao, self).__init__(PpiValidationErrors)

    def insert_with_session(self, session, obj: PpiValidationErrors):
        submit_pipeline_pubsub_msg_from_model(obj, self.get_connection_database_name())
        return obj

    def get(self, validation_error_id):
        with self.session() as session:
            query = session.query(PpiValidationErrors).filter(
                PpiValidationErrors.id == validation_error_id
            )
            return query.one_or_none()

    def get_errors_since(self, since_date: UTCDateTime):
        """Returns all validation errors since a specific date"""
        with self.session() as session:
            query = session.query(PpiValidationErrors).filter(
                PpiValidationErrors.created >= since_date
            )
            return query.all()

    def get_errors_within_range(self, start_date: UTCDateTime, end_date: UTCDateTime):
        """Returns all validation errors from START_DATE to END_DATE"""
        with self.session() as session:
            query = session.query(PpiValidationErrors).filter(
                PpiValidationErrors.created >= start_date,
                PpiValidationErrors.created <= end_date,
            )
            return query.all()
