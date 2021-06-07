from werkzeug.exceptions import BadRequest
import httplib2
from rdr_service import clock
from datetime import timedelta

from rdr_service.dao.message_broker_metadata_dao import MessageBrokerMetadataDao
from rdr_service.dao.message_broker_dest_auth_info_dao import MessageBrokerDestAuthInfoDao


# this is added based on the document, PTSC's test env is not ready, no test on real env yet
class BaseMessageBroker:
    def __init__(self, message):
        self.message = message
        self.message_metadata_dao = MessageBrokerMetadataDao()
        self.dest_auth_Dao = MessageBrokerDestAuthInfoDao()

    # Calls from DRC will use a token obtained using the client credentials grant,
    # used for machine to machine authentication/authorization.
    def get_access_token(self):
        """Returns the access token for the API endpoint."""
        auth_info = self.dest_auth_Dao.get_auth_info(self.message.messageDest)
        if not auth_info:
            raise BadRequest(f'can not find auth info for dest: {self.message.messageDest}')

        now = clock.CLOCK.now()
        five_mins_later = now + timedelta(minutes=5)
        if auth_info.accessToken and auth_info.expiredAt > five_mins_later:
            return auth_info.accessToken
        else:
            http = httplib2.Http()
            token_endpoint = auth_info.tokenEndpoint
            payload = f'grant_type=client_credentials&client_id={auth_info.key}&client_secret={auth_info.secret}'

            response, content = http.request(
                token_endpoint, method="POST",
                headers={"Content-type": "application/x-www-form-urlencoded"},
                body=payload
            )

            if response['status'] in ('200', '201'):
                auth_info.accessToken = content['access_token']
                now = clock.CLOCK.now()
                expired_at = now + timedelta(seconds=content['expires_in'])
                auth_info.expiredAt = expired_at
                self.dest_auth_Dao.update(auth_info)
                return content['access_token']
            else:
                raise BadRequest(f'can not get access token for dest: {self.message.messageDest}')

    def _get_message_dest_url(self):
        dest_url = self.message_metadata_dao.get_dest_url(self.message.eventType, self.message.messageDest)
        if not dest_url:
            raise BadRequest(f'no destination url found for dest: {self.message.messageDest} '
                             f'and event: {self.message.eventType}')
        return dest_url

    def make_request_body(self):
        """Returns the request body that need to be sent to the destination. Must be overridden by subclasses."""
        raise NotImplementedError()

    def send_request(self):
        dest_url = self._get_message_dest_url()
        token = self.get_access_token()
        request_body = self.make_request_body()
        http = httplib2.Http()

        # Token should be included in the HTTP Authorization header using the Bearer scheme.
        response, content = http.request(
            dest_url, method="POST", headers={"Content-type": "application/json", "Authorization": "Bearer " + token},
            body=request_body
        )
        if response['status'] == "200":
            return response['status'], content, ''
        else:
            return response['status'], content, content


class PtscMessageBroker(BaseMessageBroker):
    def __init__(self, message):
        super(PtscMessageBroker, self).__init__(message)

    def make_request_body(self):
        request_body = {
            'event': self.message.eventType,
            'eventAuthoredTime': self.message.eventAuthoredTime,
            'participantId': str(self.message.participantId),
            'messageBody': self.message.requestBody
        }
        return request_body

    def send_request(self):
        # PTSC's env is not ready, return mock result
        response_code = '200'
        response_body = {'result': 'mocked result'}
        response_error = ''
        return response_code, response_body, response_error


class MessageBrokerFactory:
    @staticmethod
    def create(message):
        if message.messageDest == 'vibrent':
            return PtscMessageBroker(message)
        else:
            raise BadRequest(f'no destination found: {message.messageDest}')
