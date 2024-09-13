from abc import ABC
from typing import List, Dict
from sqlalchemy import and_

from rdr_service.dao.base_dao import BaseDao, UpsertableDao, UpdatableDao
from rdr_service.model.ppsc import Participant, Site
from rdr_service.model.ppsc_data_transfer import PPSCDataTransferAuth, PPSCDataTransferEndpoint, PPSCDataTransferRecord
from rdr_service.ppsc.ppsc_enums import AuthType


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


class PPSCDataTransferAuthDao(UpdatableDao):

    def __init__(self):
        super().__init__(PPSCDataTransferAuth, order_by_ending=["id"])

    def from_client_json(self):
        pass

    def get_id(self, obj):
        pass

    def get_auth_record_from_type(self, auth_type: AuthType):
        with self.session() as session:
            return session.query(
                self.model_type
            ).filter(
                self.model_type.auth_type == auth_type,
                self.model_type.ignore_flag != 1
            ).one_or_none()


class PPSCDataTransferEndpointDao(BaseDao):

    def __init__(self):
        super().__init__(PPSCDataTransferEndpoint, order_by_ending=["id"])

    def from_client_json(self):
        pass

    def get_id(self, obj):
        pass

    def get_endpoint_by_type(self, transfer_type):
        with self.session() as session:
            session.query(
                PPSCDataTransferEndpoint
            ).filter(
                PPSCDataTransferEndpoint.data_sync_transfer_type == transfer_type,
                PPSCDataTransferEndpoint.ignore_flag != 1
            ).one_or_none()


class PPSCDataTransferBaseDao(ABC, BaseDao):

    def __init__(self, model_type):
        super().__init__(model_type)

    def from_client_json(self):
        pass

    def get_id(self, obj):
        pass

    def get_items_for_transfer(self, *, transfer_type) -> List:
        with self.session() as session:
            return session.query(
                self.model_type
            ).outerjoin(
                PPSCDataTransferRecord,
                and_(
                    PPSCDataTransferRecord.data_sync_transfer_type == transfer_type,
                    PPSCDataTransferRecord.participant_id == PPSCDataTransferRecord.participant_id
                )
            ).filter(
                PPSCDataTransferRecord.id.is_(None),
                self.model_type.ignore_flag != 1
            )

    def insert_bulk(self, batch: List[Dict]) -> None:
        with self.session() as session:
            session.bulk_insert_mappings(
                self.model_type,
                batch
            )

