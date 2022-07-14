import logging
from datetime import timedelta

import backoff
import requests
from werkzeug.exceptions import BadRequest, BadGateway, HTTPException

from rdr_service import clock
from rdr_service.model.message_broker import MessageBrokerDestAuthInfo
from rdr_service.model.utils import to_client_participant_id
from rdr_service.dao.database_utils import format_datetime
from rdr_service.dao.message_broker_metadata_dao import MessageBrokerMetadataDao
from rdr_service.dao.message_broker_dest_auth_info_dao import MessageBrokerDestAuthInfoDao


# this is added based on the document, PTSC's test env is not ready, no test on real env yet
class BaseMessageBroker:
    def __init__(self, message):
        self.message = message
        self.message_metadata_dao = MessageBrokerMetadataDao()
        self.dest_auth_dao = MessageBrokerDestAuthInfoDao()

    # Calls from DRC will use a token obtained using the client credentials grant,
    # used for machine to machine authentication/authorization.
    def get_access_token(self):
        """Returns the access token for the API endpoint."""
        auth_info = self.dest_auth_dao.get_auth_info(self.message.messageDest)
        if not auth_info:
            raise BadRequest(f'can not find auth info for dest: {self.message.messageDest}')

        now = clock.CLOCK.now()
        # the token will be expired in 300 secs, compare with the timestamp of 20 secs later
        # to make sure we use a valid token
        secs_later = now + timedelta(seconds=20)
        if auth_info.accessToken and auth_info.expiredAt > secs_later:
            return auth_info.accessToken
        else:
            return self._request_new_token(auth_info=auth_info)

    @backoff.on_exception(backoff.constant, HTTPException, max_tries=3)
    def _request_new_token(self, auth_info: MessageBrokerDestAuthInfo):
        token_endpoint = auth_info.tokenEndpoint
        payload = f'grant_type=client_credentials&client_id={auth_info.key}&client_secret={auth_info.secret}'
        response = requests.post(token_endpoint, data=payload,
                                 headers={"Content-type": "application/x-www-form-urlencoded"})

        if response.status_code in (200, 201):
            r_json = response.json()
            auth_info.accessToken = r_json['access_token']
            now = clock.CLOCK.now()
            expired_at = now + timedelta(seconds=r_json['expires_in'])
            auth_info.expiredAt = expired_at
            self.dest_auth_dao.update(auth_info)
            return r_json['access_token']
        else:
            logging.warning(
                f'received {response.status_code} from message broker auth endpoint for {self.message.messageDest}'
            )
            raise BadGateway(f'can not get access token for dest: {self.message.messageDest}, '
                             f'response error: {str(response.status_code)}')

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

        response = self.send_request_with_retry_on_conn_error(dest_url, request_body, token)
        if response.status_code == 200:
            return response.status_code, response.json(), ''
        else:
            return response.status_code, response.text, response.text

    @backoff.on_exception(backoff.constant, requests.exceptions.ConnectionError, max_tries=3)
    def send_request_with_retry_on_conn_error(self, url, request_body, token):
        # retry 3 time for the following error:
        # urllib3.exceptions.ProtocolError:
        # ('Connection aborted.', ConnectionResetError(104, 'Connection reset by peer'))

        # Traceback:
        # response = requests.post(url, json=request_body, headers={"Authorization": "Bearer " + token})
        # "/layers/google.python.pip/pip/lib/python3.7/site-packages/requests/api.py", line
        # in post
        # return request('post', url, data=data, json=json, **kwargs)
        # "/layers/google.python.pip/pip/lib/python3.7/site-packages/requests/api.py", line
        # in request
        # return session.request(method=method, url=url, **kwargs)
        # "/layers/google.python.pip/pip/lib/python3.7/site-packages/requests/sessions.py", line
        # in request
        # resp = self.send(prep, **send_kwargs)
        # "/layers/google.python.pip/pip/lib/python3.7/site-packages/requests/sessions.py", line
        # in send
        # r = adapter.send(request, **kwargs)
        # "/layers/google.python.pip/pip/lib/python3.7/site-packages/requests/adapters.py", line
        # in send
        # raise ConnectionError(err, request=request)
        # requests.exceptions.ConnectionError:
        # ('Connection aborted.', ConnectionResetError(104, 'Connection reset by peer'))

        # Token should be included in the HTTP Authorization header using the Bearer scheme.
        return requests.post(url, json=request_body, headers={"Authorization": "Bearer " + token})


class PtscMessageBroker(BaseMessageBroker):
    def __init__(self, message):
        super(PtscMessageBroker, self).__init__(message)

    def make_request_body(self):
        request_body = {
            'event': self.message.eventType,
            'eventAuthoredTime': format_datetime(self.message.eventAuthoredTime),
            'participantId': to_client_participant_id(self.message.participantId),
            'messageBody': self.message.requestBody
        }
        return request_body


class MessageBrokerFactory:
    @staticmethod
    def create(message):
        if message.messageDest == 'vibrent':
            return PtscMessageBroker(message)
        else:
            raise BadRequest(f'no destination found: {message.messageDest}')
