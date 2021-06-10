from rdr_service import app_util
from rdr_service.api.base_api import BaseApi
from rdr_service.api_util import RDR
from rdr_service.dao.message_broker_dao import MessageBrokerDao


class MessageBrokerApi(BaseApi):
    def __init__(self):
        super().__init__(MessageBrokerDao())

    @app_util.auth_required(RDR)
    def post(self):
        return super(MessageBrokerApi, self).post()
