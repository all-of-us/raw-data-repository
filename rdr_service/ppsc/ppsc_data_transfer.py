import logging
import requests

from abc import ABC, abstractmethod

from rdr_service.dao.ppsc_dao import PPSCDataTransferEndpointDao, PPSCDataTransferBaseDao
from rdr_service.ppsc.ppsc_enums import DataSyncTransferType, AuthType
from rdr_service.ppsc.ppsc_oauth import PPSCTransferOauth


class BaseDataTransfer(ABC):

    def __init__(self):
        self.ppsc_oauth_data = PPSCTransferOauth(auth_type=AuthType.DATA_TRANSFER)
        self.transfer_type = None
        self.dao = None
        self.endpoint_dao = PPSCDataTransferEndpointDao()
        self.transfer_url = self.get_endpoint_for_transfer()
        self.transfer_items = self.get_transfer_items()
        self.headers = self.get_headers()

    def run_data_transfer(self):
        if not self.transfer_items:
            logging.info(f"No data transfer items found for the transfer type: {self.transfer_type}")
            return

        self.send_items()

    def get_endpoint_for_transfer(self):
        if self.transfer_type:
            endpoint = self.endpoint_dao.get_endpoint_by_type(self.transfer_type)
            if not endpoint:
                raise RuntimeError(f'Endpoint record cannot be retrieved for {self.transfer_type}')

            self.transfer_url = f'{endpoint.base_url}{endpoint.endpoint}'

    def get_transfer_items(self):
        items = self.dao.get_items_for_transfer(transfer_type=self.transfer_type)
        return items

    def set_tranfer_record(self):
        ...

    def get_headers(self):
        return {"Authorization": f'Bearer {self.ppsc_oauth_data.token}'}

    def send_item(self, transfer_item):
        response = requests.post(
            self.transfer_url,
            data=self.get_payload(transfer_item),
            headers=self.headers
        )
        return response

    @abstractmethod
    def get_payload(self, transfer_item):
        ...

    @abstractmethod
    def send_items(self):
        ...


class PPSCDataTransferCore(BaseDataTransfer):

    def __init__(self):
        super().__init__()
        self.dao = PPSCDataTransferBaseDao(PPSCDataTransferCore)
        self.transfer_type = DataSyncTransferType.CORE

    def send_items(self):
        for _ in self.transfer_items:
            # response = self.send_item(transfer_item)
            self.set_tranfer_record()

    def get_payload(self, transfer_item):
        ...


class PPSCDataTransferEHR(BaseDataTransfer):

    def __init__(self):
        super().__init__()
        self.dao = PPSCDataTransferBaseDao(PPSCDataTransferEHR)
        self.transfer_type = DataSyncTransferType.EHR

    def send_items(self):
        for _ in self.transfer_items:
            ...

    def get_payload(self, transfer_item):
        ...


class PPSCDataTransferBiobank(BaseDataTransfer):

    def __init__(self):
        super().__init__()
        self.dao = PPSCDataTransferBaseDao(PPSCDataTransferBiobank)
        self.transfer_type = DataSyncTransferType.BIOBANK_SAMPLE

    def send_items(self):
        for _ in self.transfer_items:
            ...

    def get_payload(self, transfer_item):
        ...


class PPSCDataTransferHealthData(BaseDataTransfer):

    def __init__(self):
        super().__init__()
        self.dao = PPSCDataTransferBaseDao(PPSCDataTransferHealthData)
        self.transfer_type = DataSyncTransferType.HEALTH_DATA

    def send_items(self):
        for _ in self.transfer_items:
            ...

    def get_payload(self, transfer_item):
        ...
