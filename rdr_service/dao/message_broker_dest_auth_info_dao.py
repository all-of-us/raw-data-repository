from rdr_service.dao.base_dao import UpdatableDao
from rdr_service.model.message_broker import MessageBrokerDestAuthInfo


class MessageBrokerDestAuthInfoDao(UpdatableDao):
    def __init__(self):
        super(MessageBrokerDestAuthInfoDao, self).__init__(MessageBrokerDestAuthInfo)

    def get_auth_info(self, dest):
        with self.session() as session:
            query = session.query(MessageBrokerDestAuthInfo).filter(MessageBrokerDestAuthInfo.destination == dest)
            return query.first()

