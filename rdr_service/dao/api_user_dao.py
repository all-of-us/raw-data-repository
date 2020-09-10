from werkzeug.exceptions import BadRequest

from rdr_service.dao.base_dao import BaseDao
from rdr_service.model.api_user import ApiUser


class ApiUserDao(BaseDao):

    validate_version_match = False

    def __init__(self):
        super().__init__(ApiUser)

    def load_or_init(self, system, username):
        user = self.load_from_database(system, username)

        if user:
            return user
        else:
            if system is None:
                raise BadRequest('Missing system for user')
            if username is None:
                raise BadRequest('Missing username for user')
            return ApiUser(system=system, username=username)

    def load_from_database(self, system, username):
        with self.session() as session:
            return session.query(ApiUser).filter(
                ApiUser.system == system,
                ApiUser.username == username
            ).one_or_none()
