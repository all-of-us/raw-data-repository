import base64
import logging
import requests

from rdr_service import clock
from rdr_service.dao.ppsc_dao import PPSCDataTransferAuthDao
from rdr_service.ppsc.ppsc_enums import AuthType

# pylint: disable=broad-except
class PPSCTransferOauth:

    def __init__(self, auth_type: AuthType):
        self.auth_type = auth_type
        self.auth_record = self.get_auth_record()
        self.dao = PPSCDataTransferAuthDao()
        self.token = self.generate_token()

    def set_encoded_client(self):
        encoded = f'b {self.auth_record.client_id}:{self.auth_record.client_secret}'
        encoded = base64.b64encode(encoded)
        return

    def generate_token(self):
        response = requests.post(
            url=self.auth_record.auth_url,
            headers=self.get_headers()
        )
        try:
            if response:
                ...
            return response
        except Exception as e:
            logging.warning(f'Error generating token for Oauth: {self.auth_type}: {e}')

    def store_token(self):
        if self.token:
            self.auth_record.last_generated = clock.CLOCK.now()
            self.auth_record.expires = self.token.get('expires_in')
            self.auth_record.access_token = self.token
            self.dao.update(self.auth_record)

    def get_auth_record(self):
        auth_record = self.dao.get_auth_record_from_type(self.auth_type)
        if not auth_record:
            raise RuntimeError(f'Cannot locate auth record for {self.auth_type}')

        self.auth_record = auth_record

    def get_headers(self):
        return {
            "Content-Type": "application/x-www-form-urlencoded",
            "Authorization": f'Basic {self.set_encoded_client()}'
        }
