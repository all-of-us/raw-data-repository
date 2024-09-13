from rdr_service.model.utils import Enum, UTCDateTime6

from sqlalchemy import Column, BigInteger, String, ForeignKey, event
from sqlalchemy.dialects.mysql import TINYINT, JSON

from rdr_service.model.base import model_insert_listener, model_update_listener, PPSCBase
from rdr_service.model.utils import UTCDateTime
from rdr_service.ppsc.ppsc_enums import DataSyncTransferType, AuthType


class PPSCDataTransferAuth(PPSCBase):
    __tablename__ = "ppsc_data_transfer_auth"

    id = Column("id", BigInteger, autoincrement=True, primary_key=True)
    created = Column(UTCDateTime)
    modified = Column(UTCDateTime)
    auth_type = Column(Enum(AuthType))
    auth_url = Column(String(512))
    client_id = Column(String(512))
    client_secret = Column(String(512))
    access_token = Column(String(512))
    expires = Column(String(256))
    last_generated = Column(UTCDateTime)
    ignore_flag = Column(TINYINT, default=0)


event.listen(PPSCDataTransferAuth, "before_insert", model_insert_listener)
event.listen(PPSCDataTransferAuth, "before_update", model_update_listener)


class PPSCDataTransferEndpoint(PPSCBase):
    __tablename__ = "ppsc_data_trasnfer_endpoint"

    id = Column("id", BigInteger, autoincrement=True, primary_key=True)
    created = Column(UTCDateTime)
    modified = Column(UTCDateTime)
    base_url = Column(String(512))
    end_point = Column(String(512))
    data_sync_transfer_type = Column(Enum(DataSyncTransferType))
    ignore_flag = Column(TINYINT, default=0)


event.listen(PPSCDataTransferEndpoint, "before_insert", model_insert_listener)
event.listen(PPSCDataTransferEndpoint, "before_update", model_update_listener)


class PPSCDataTransferRecord(PPSCBase):
    __tablename__ = "ppsc_data_transfer_record"

    id = Column("id", BigInteger, autoincrement=True, primary_key=True)
    created = Column(UTCDateTime)
    modified = Column(UTCDateTime)
    participant_id = Column(BigInteger, ForeignKey("participant.id"))
    data_sync_transfer_type = Column(Enum(DataSyncTransferType))
    request_payload = Column(JSON, nullable=True)
    response_code = Column(String(128))
    ignore_flag = Column(TINYINT, default=0)


event.listen(PPSCDataTransferRecord, "before_insert", model_insert_listener)
event.listen(PPSCDataTransferRecord, "before_update", model_update_listener)


class PPSCDataBase:

    id = Column("id", BigInteger, autoincrement=True, primary_key=True)
    created = Column(UTCDateTime)
    modified = Column(UTCDateTime)
    ignore_flag = Column(TINYINT, default=0)


class PPSCCore(PPSCDataBase, PPSCBase):
    __tablename__ = "ppsc_core"

    participant_id = Column(BigInteger, ForeignKey("participant.id"))
    has_core_data = Column(TINYINT, default=0)
    has_core_data_date = Column(UTCDateTime6)


event.listen(PPSCCore, "before_insert", model_insert_listener)
event.listen(PPSCCore, "before_update", model_update_listener)


class PPSCEHR(PPSCDataBase, PPSCBase):
    __tablename__ = "ppsc_ehr"

    participant_id = Column(BigInteger, ForeignKey("participant.id"))
    first_time_date = Column(UTCDateTime6)
    last_time_date = Column(UTCDateTime6)


event.listen(PPSCEHR, "before_insert", model_insert_listener)
event.listen(PPSCEHR, "before_update", model_update_listener)


class PPSCBiobankSample(PPSCDataBase, PPSCBase):
    __tablename__ = "ppsc_biobank_sample"

    participant_id = Column(BigInteger, ForeignKey("participant.id"))
    first_time_date = Column(UTCDateTime6)
    last_time_date = Column(UTCDateTime6)


event.listen(PPSCBiobankSample, "before_insert", model_insert_listener)
event.listen(PPSCBiobankSample, "before_update", model_update_listener)


class PPSCHealthData(PPSCDataBase, PPSCBase):
    __tablename__ = "ppsc_health_data"

    participant_id = Column(BigInteger, ForeignKey("participant.id"))
    health_data_stream_sharing_status = Column(TINYINT, default=0)
    health_data_stream_sharing_status_date = Column(UTCDateTime6)


event.listen(PPSCHealthData, "before_insert", model_insert_listener)
event.listen(PPSCHealthData, "before_update", model_update_listener)
