from typing import List, Dict
from sqlalchemy import and_

from rdr_service.dao.base_dao import UpdatableDao, BaseDao
from rdr_service.model.ppsc_partner_data_transfer import PPSCDataTransferAuth, PPSCDataTransferEndpoint, \
    PPSCDataTransferRecord, RTIDataTransferEndpoint, RTIDataTransferAuth, RTIDataTransferRecord
from rdr_service.model.study_nph import EligibleParticipants
from rdr_service.ppsc.ppsc_enums import AuthType


class PPSCDataTransferAuthDao(UpdatableDao):

    validate_version_match = False

    def __init__(self):
        super().__init__(PPSCDataTransferAuth, order_by_ending=["id"])

    def from_client_json(self):
        pass

    def get_id(self, obj):
        return obj.id

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
            return session.query(
                self.model_type
            ).filter(
                self.model_type.data_sync_transfer_type == transfer_type,
                self.model_type.ignore_flag != 1
            ).one_or_none()


class PPSCDataTransferRecordDao(BaseDao):

    def __init__(self):
        super().__init__(PPSCDataTransferRecord, order_by_ending=["id"])

    def from_client_json(self):
        pass

    def get_id(self, obj):
        pass

    def insert_bulk(self, batch: List[Dict]) -> None:
        with self.session() as session:
            session.bulk_insert_mappings(
                self.model_type,
                batch
            )


class PPSCDataTransferBaseDao(BaseDao):

    def __init__(self, model_type):
        super().__init__(model_type)

    def get_items_for_transfer(self, *, transfer_type) -> List:
        with self.session() as session:
            return session.query(
                self.model_type
            ).outerjoin(
                PPSCDataTransferRecord,
                and_(
                    PPSCDataTransferRecord.data_sync_transfer_type == transfer_type,
                    PPSCDataTransferRecord.participant_id == self.model_type.participant_id,
                    PPSCDataTransferRecord.response_code == 200,
                    PPSCDataTransferRecord.data_type_record_id == self.model_type.id,
                    PPSCDataTransferRecord.ignore_flag != 1
                )
            ).filter(
                PPSCDataTransferRecord.id.is_(None),
                self.model_type.ignore_flag != 1
            ).distinct().all()


class RTIDataTransferAuthDao(UpdatableDao):

    validate_version_match = False

    def __init__(self):
        super().__init__(RTIDataTransferAuth, order_by_ending=["id"])

    def from_client_json(self):
        pass

    def get_id(self, obj):
        return obj.id

    def get_auth_record_from_type(self, auth_type: AuthType):
        with self.session() as session:
            return session.query(
                self.model_type
            ).filter(
                self.model_type.auth_type == auth_type,
                self.model_type.ignore_flag != 1
            ).one_or_none()


class RTIDataTransferEndpointDao(BaseDao):

    def __init__(self):
        super().__init__(RTIDataTransferEndpoint, order_by_ending=["id"])

    def from_client_json(self):
        pass

    def get_id(self, obj):
        pass

    def get_endpoint_by_type(self, transfer_type):
        with self.session() as session:
            return session.query(
                self.model_type
            ).filter(
                self.model_type.data_sync_transfer_type == transfer_type,
                self.model_type.ignore_flag != 1
            ).one_or_none()


class RTIDataTransferBaseDao(BaseDao):

    def __init__(self, model_type):
        super().__init__(model_type)

    def get_items_for_transfer(self, *, transfer_type) -> List:
        with self.session() as session:
            return session.query(
                self.model_type.nph_participant_id.label('npH_PID')
            ).join(
                EligibleParticipants,
                and_(
                    EligibleParticipants.nph_participant_id == self.model_type.nph_participant_id,
                    EligibleParticipants.ignore_flag != 1
                )
            ).outerjoin(
                RTIDataTransferRecord,
                and_(
                    RTIDataTransferRecord.nph_participant_id == self.model_type.nph_participant_id,
                    RTIDataTransferRecord.ignore_flag != 1
                )
            ).filter(
                RTIDataTransferRecord .id.is_(None),
                RTIDataTransferRecord.data_sync_transfer_type == transfer_type,
                self.model_type.ignore_flag != 1
            ).distinct().all()


class RTIDataTransferRecordDao(BaseDao):

    def __init__(self):
        super().__init__(RTIDataTransferRecord, order_by_ending=["id"])

    def from_client_json(self):
        pass

    def get_id(self, obj):
        pass

    def insert_bulk(self, batch: List[Dict]) -> None:
        with self.session() as session:
            session.bulk_insert_mappings(
                self.model_type,
                batch
            )
