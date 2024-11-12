import base64
import logging
import requests
from abc import abstractmethod, ABC

from rdr_service import clock
from rdr_service.dao.ppsc_partner_transfer_dao import PPSCDataTransferAuthDao, RTIDataTransferAuthDao
from rdr_service.ppsc.ppsc_enums import AuthType


class BaseTransferOauth(ABC):

    def encode_client_data(self):
        encoded = f'{self.oauth_record.client_id}:{self.oauth_record.client_secret}'
        encoded_str = base64.b64encode(encoded.encode("utf-8")).decode("utf-8")
        return encoded_str

    def store_token(self, token_dict: dict):
        self.oauth_record.last_generated = clock.CLOCK.now()
        self.oauth_record.expires = token_dict.get('expires_in')
        self.oauth_record.access_token = token_dict.get("access_token")
        self.dao.update(self.oauth_record)

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
        except Exception as e:  # pylint: disable=broad-except
            logging.warning(f'Error generating token for Oauth: {self.auth_type}: {e}')

    def get_oauth_record(self):
        oauth_record = self.dao.get_auth_record_from_type(self.auth_type)
        if not oauth_record:
            raise RuntimeError(f'Cannot locate auth record for {self.auth_type}')

        return oauth_record

    @abstractmethod
    def get_headers(self):
        ...


class PPSCTransferOauth(BaseTransferOauth):

    def __init__(self):
        self.auth_type = AuthType.PPSC_DATA_TRANSFER
        self.dao = PPSCDataTransferAuthDao()
        self.oauth_record = self.get_oauth_record()
        self.encoded_client_str = self.encode_client_data()
        self.token = self.generate_token()

    def get_headers(self):
        return {
            "Content-Type": "application/x-www-form-urlencoded",
            "Authorization": f'Basic {self.encoded_client_str}'
        }


class RTITransferOauth(BaseTransferOauth):

    def __init__(self):
        self.auth_type = AuthType.RTI_DATA_TRANSFER
        self.dao = RTIDataTransferAuthDao()
        self.oauth_record = self.get_oauth_record()
        self.token = self.oauth_record.access_token
        self.x_public_key = self.oauth_record.x_public_key

    def get_headers(self):
        ...
