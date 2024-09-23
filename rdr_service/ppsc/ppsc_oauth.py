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
        self.dao = PPSCDataTransferAuthDao()
        self.oauth_record = self.get_oauth_record()
        self.encoded_client_str = self.encode_client_data()
        self.token = self.generate_token()

    def encode_client_data(self):
        encoded = f'{self.oauth_record.client_id}:{self.oauth_record.client_secret}'
        encoded_str = base64.b64encode(encoded.encode("utf-8")).decode("utf-8")
        return encoded_str

    def generate_token(self):
        response = requests.post(
            url=self.oauth_record.auth_url,
            headers=self.get_headers()
        )
        try:
            if response and response.status_code in (200, 201):
                token_dict = response.json()
                self.store_token(token_dict)
                return token_dict.get("access_token")
            else:
                logging.warning(f'Error generating token for Oauth: {self.auth_type}: Response {response.status_code}')
                raise RuntimeError(f'Error generating token for Oauth: '
                                   f'{self.auth_type}: Response {response.status_code}')
        except Exception as e:
            logging.warning(f'Error generating token for Oauth: {self.auth_type}: {e}')

    def store_token(self, token_dict: dict):
        self.oauth_record.last_generated = clock.CLOCK.now()
        self.oauth_record.expires = token_dict.get('expires_in')
        self.oauth_record.access_token = token_dict.get("access_token")
        self.dao.update(self.oauth_record)

    def get_oauth_record(self):
        oauth_record = self.dao.get_auth_record_from_type(self.auth_type)
        if not oauth_record:
            raise RuntimeError(f'Cannot locate auth record for {self.auth_type}')

        return oauth_record

    def get_headers(self):
        return {
            "Content-Type": "application/x-www-form-urlencoded",
            "Authorization": f'Basic {self.encoded_client_str}'
        }
