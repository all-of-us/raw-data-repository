from rdr_service.dao.base_dao import BaseDao
from rdr_service.model.message_broker import MessageBrokerMetadata


class MessageBrokerMetadataDao(BaseDao):
    def __init__(self):
        super(MessageBrokerMetadataDao, self).__init__(MessageBrokerMetadata)

    def get_dest_url(self, event, dest):
        with self.session() as session:
            query = session.query(MessageBrokerMetadata).filter(MessageBrokerMetadata.eventType == event,
                                                                MessageBrokerMetadata.destination == dest)
            metadata = query.first()
            return metadata.url if metadata else None
