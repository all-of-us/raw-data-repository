import logging
import requests
from abc import ABC, abstractmethod
from typing import Union

from rdr_service import clock
from rdr_service.dao.ppsc_dao import PPSCDataTransferEndpointDao, PPSCDataTransferBaseDao, PPSCDataTransferRecordDao
from rdr_service.ppsc.ppsc_enums import DataSyncTransferType, AuthType
from rdr_service.ppsc.ppsc_oauth import PPSCTransferOauth
from rdr_service.model.ppsc_data_transfer import PPSCCore, PPSCBiobankSample, PPSCHealthData, PPSCEHR, PPSCDataBase


class BaseDataTransfer(ABC):

    def __init__(self):
        self.endpoint_dao = PPSCDataTransferEndpointDao()
        self.transfer_record_dao = PPSCDataTransferRecordDao()
        self.record_items = []

    def __enter__(self):
        self.ppsc_oauth_data = PPSCTransferOauth(auth_type=AuthType.DATA_TRANSFER)
        self.transfer_url = self.get_endpoint_for_transfer()
        self.headers = self.get_headers()
        self.transfer_items = self.get_transfer_items()
        logging.info(f"Starting PPSC data transfer {str(self.transfer_type)} for {len(self.transfer_items)}")
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.record_items:
            self.transfer_record_dao.insert_bulk(self.record_items)

        logging.info(f"Finished PPSC Data Transfer {str(self.transfer_type)} for {len(self.transfer_items)}")

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
            return f'{endpoint.base_url}{endpoint.endpoint}'
        raise RuntimeError(f'Transfer type not initiated on constructor')

    def get_transfer_items(self):
        return self.dao.get_items_for_transfer(transfer_type=self.transfer_type)

    def get_headers(self):
        # return {"Authorization": f'Bearer wrwrwrwrwwrwwr'}
        return {"Authorization": f'Bearer {self.ppsc_oauth_data.token}'}

    def send_item(self, post_obj: dict):
        response = requests.post(
            self.transfer_url,
            data=post_obj,
            headers=self.headers
        )
        return response

    def build_default_obj(self, transfer_item):
        filtered_keys = [obj for obj in PPSCDataBase.__dict__.keys() if obj[:1] != '_']
        updated_obj = {
            self.dao.snake_to_camel(k): v for k, v in
            transfer_item.asdict().items() if k not in filtered_keys
        }
        updated_obj['participantId'] = f'P{updated_obj["participantId"]}'
        return updated_obj

    def send_items(self):
        for item in self.transfer_items:
            prepared_obj = self.prepare_obj(item)
            response = self.send_item(prepared_obj)
            self.record_items.append({
                'created': clock.CLOCK.now(),
                'modified': clock.CLOCK.now(),
                'participant_id': item.participant_id,
                'data_sync_transfer_type': self.transfer_type,
                'request_payload': prepared_obj,
                'response_code': response.status_code
            })

    @abstractmethod
    def prepare_obj(self, transfer_item: Union[PPSCCore, PPSCEHR, PPSCBiobankSample, PPSCHealthData]):
        ...


class PPSCDataTransferCore(BaseDataTransfer):

    def __init__(self):
        super().__init__()
        self.dao = PPSCDataTransferBaseDao(PPSCCore)
        self.transfer_type = DataSyncTransferType.CORE

    def prepare_obj(self, transfer_item):
        updated_obj = self.build_default_obj(transfer_item)
        updated_obj['hasCoreData'] = True
        updated_obj['hasCoreDataDateTime'] = str(updated_obj['hasCoreDataDateTime'])
        return updated_obj


class PPSCDataTransferEHR(BaseDataTransfer):

    def __init__(self):
        super().__init__()
        self.dao = PPSCDataTransferBaseDao(PPSCEHR)
        self.transfer_type = DataSyncTransferType.EHR

    def prepare_obj(self, transfer_item):
        updated_obj = self.build_default_obj(transfer_item)
        updated_obj['firstTimeDateTime'] = str(updated_obj['firstTimeDateTime'])
        updated_obj['lastTimeDateTime'] = str(updated_obj['lastTimeDateTime'])
        return updated_obj


class PPSCDataTransferBiobank(BaseDataTransfer):

    def __init__(self):
        super().__init__()
        self.dao = PPSCDataTransferBaseDao(PPSCBiobankSample)
        self.transfer_type = DataSyncTransferType.BIOBANK_SAMPLE

    def prepare_obj(self, transfer_item):
        updated_obj = self.build_default_obj(transfer_item)
        updated_obj['firstTimeDateTime'] = str(updated_obj['firstTimeDateTime'])
        updated_obj['lastTimeDateTime'] = str(updated_obj['lastTimeDateTime'])
        return updated_obj


class PPSCDataTransferHealthData(BaseDataTransfer):

    def __init__(self):
        super().__init__()
        self.dao = PPSCDataTransferBaseDao(PPSCHealthData)
        self.transfer_type = DataSyncTransferType.HEALTH_DATA

    def prepare_obj(self, transfer_item):
        updated_obj = self.build_default_obj(transfer_item)
        updated_obj['healthDataStreamSharingStatusDateTime'] = str(updated_obj['healthDataStreamSharingStatusDateTime'])
        return updated_obj
