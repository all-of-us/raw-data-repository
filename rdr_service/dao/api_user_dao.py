from werkzeug.exceptions import BadRequest

from rdr_service.dao.base_dao import BaseDao
from rdr_service.model.api_user import ApiUser


class ApiUserDao(BaseDao):

    validate_version_match = False

    def __init__(self):
        super().__init__(ApiUser)

    def from_client_json(self, resource):
        user = ApiUser()
        if 'type' not in resource:
            raise BadRequest('User system type required')
        user.system = resource['type']
        if 'reference' not in resource:
            raise BadRequest('User name reference required')
        user.username = resource['reference']

        return user

    def load_or_init_from_client_json(self, resource):
        client_user = self.from_client_json(resource)
        user = self.load_from_database(client_user.system, client_user.username)

        if user:
            return user
        else:
            return client_user

    def get_id(self, obj: ApiUser):
        user = self.load_from_database(obj.system, obj.username)

        if user:
            return user.id
        else:
            return None

    def load_from_database(self, system, username):
        with self.session() as session:
            return session.query(ApiUser).filter(
                ApiUser.system == system,
                ApiUser.username == username
            ).one_or_none()

    def to_client_json(self, model: ApiUser):
        return {
            'type': model.system,
            'reference': model.username
        }
