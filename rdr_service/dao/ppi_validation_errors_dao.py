from rdr_service.model.utils import UTCDateTime
from rdr_service.dao.base_dao import BaseDao
from rdr_service.model.ppi_validation_errors import PpiValidationErrors


class PpiValidationErrorsDao(BaseDao):

    def __init__(self):
        super(PpiValidationErrorsDao, self).__init__(PpiValidationErrors)

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
                PpiValidationErrors.eval_date >= since_date
            )
            return query.all()

    def get_errors_within_range(self, start_date: UTCDateTime, end_date: UTCDateTime):
        """Returns all validation errors from START_DATE to END_DATE"""
        with self.session() as session:
            query = session.query(PpiValidationErrors).filter(
                PpiValidationErrors.eval_date >= start_date,
                PpiValidationErrors.eval_date <= end_date,
            )
            return query.all()
