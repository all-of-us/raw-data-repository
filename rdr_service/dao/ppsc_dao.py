from abc import ABC, abstractmethod
from typing import List, Dict

from rdr_service.dao.base_dao import BaseDao, UpsertableDao
from rdr_service.model.ppsc import Participant, Site
from rdr_service.model.ppsc_data_transfer import PPSCDataTransferAuth, PPSCDataTransferEndpoint
from rdr_service.ppsc.ppsc_data_transfer import PPSCDataTransferCore, PPSCDataTransferEHR, PPSCDataTransferBiobank, \
    PPSCDataTransferHealthData


class ParticipantDao(BaseDao):

    def __init__(self):
        super().__init__(Participant)

    def to_client_json(self, participant: Participant) -> str:
        return f'Participant P{participant.id} was created successfully'

    def get_participant_by_participant_id(self, *, participant_id: int):
        with self.session() as session:
            return session.query(Participant).filter(Participant.id == participant_id).all()

    def get_participant_by_biobank_id(self, *, biobank_id: int):
        with self.session() as session:
            return session.query(Participant).filter(Participant.biobank_id == biobank_id).all()


class SiteDao(UpsertableDao):

    def __init__(self):
        super().__init__(Site)

    def to_client_json(self, obj: Site, action_type: str) -> str:
        return f'Site {obj.site_identifier} was {action_type} successfully'

    def get_site_by_identifier(self, *, site_identifier: str):
        with self.session() as session:
            return session.query(Site).filter(site_identifier == Site.site_identifier).first()


class PPSCDefaultBaseDao(BaseDao):

    def __init__(self, model_type):
        super().__init__(model_type)

    def from_client_json(self):
        pass

    def to_client_json(self, payload):
        return f"Event Record Created for: {payload['participantId']}"

    def get_id(self, obj):
        return obj.id

    def insert_bulk(self, batch: List[Dict]) -> None:
        with self.session() as session:
            session.bulk_insert_mappings(
                self.model_type,
                batch
            )


class PPSCDataTransferAuthDao(BaseDao):

    def __init__(self):
        super().__init__(PPSCDataTransferAuth, order_by_ending=["id"])

    def from_client_json(self):
        pass

    def get_id(self, obj):
        pass


class PPSCDataTransferEndpointDao(BaseDao):

    def __init__(self):
        super().__init__(PPSCDataTransferEndpoint, order_by_ending=["id"])

    def from_client_json(self):
        pass

    def get_id(self, obj):
        pass


class PPSCDataBaseDao(ABC, BaseDao):

    def from_client_json(self):
        pass

    def get_id(self, obj):
        pass

    @abstractmethod
    def get_items_for_transfer(self) -> List:
        ...

    def insert_bulk(self, batch: List[Dict]) -> None:
        with self.session() as session:
            session.bulk_insert_mappings(
                self.model_type,
                batch
            )


class PPSCDataTransferCoreDao(PPSCDataBaseDao):

    def __init__(self):
        super().__init__(PPSCDataTransferCore, order_by_ending=["id"])

    def get_items_for_transfer(self) -> List:
        ...


class PPSCDataTransferEHRDao(PPSCDataBaseDao):

    def __init__(self):
        super().__init__(PPSCDataTransferEHR, order_by_ending=["id"])

    def get_items_for_transfer(self) -> List:
        ...


class PPSCDataTransferBiobankDao(PPSCDataBaseDao):

    def __init__(self):
        super().__init__(PPSCDataTransferBiobank, order_by_ending=["id"])

    def get_items_for_transfer(self) -> List:
        ...


class PPSCDataTransferHealtDataDao(PPSCDataBaseDao):

    def __init__(self):
        super().__init__(PPSCDataTransferHealthData, order_by_ending=["id"])

    def get_items_for_transfer(self) -> List:
        ...
