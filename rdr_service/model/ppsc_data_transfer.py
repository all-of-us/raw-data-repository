from sqlalchemy import Column, BigInteger, String, ForeignKey, event
from sqlalchemy.dialects.mysql import TINYINT, JSON

from rdr_service.model.base import model_insert_listener, model_update_listener, PPSCBase
from rdr_service.model.utils import UTCDateTime


class PPSCDataSyncAuth(PPSCBase):
    __tablename__ = "ppsc_data_sync_auth"

    id = Column("id", BigInteger, autoincrement=True, primary_key=True)
    created = Column(UTCDateTime)
    modified = Column(UTCDateTime)
    data_sync_type = Column(TINYINT)
    client_id = Column(String(512))
    client_secret = Column(String(512))
    access_token = Column(String(512))
    expires = Column(String(256))
    last_generated = Column(UTCDateTime)


event.listen(PPSCDataSyncAuth, "before_insert", model_insert_listener)
event.listen(PPSCDataSyncAuth, "before_update", model_update_listener)


class PPSCDataSyncEndpoint(PPSCBase):
    __tablename__ = "ppsc_data_sync_endpoint"

    id = Column("id", BigInteger, autoincrement=True, primary_key=True)
    created = Column(UTCDateTime)
    modified = Column(UTCDateTime)
    base_url = Column(String(512))
    data_sync_transfer_type = Column(TINYINT)


event.listen(PPSCDataSyncEndpoint, "before_insert", model_insert_listener)
event.listen(PPSCDataSyncEndpoint, "before_update", model_update_listener)


class PPSCDataSyncRecord(PPSCBase):
    __tablename__ = "ppsc_data_sync_record"

    id = Column("id", BigInteger, autoincrement=True, primary_key=True)
    created = Column(UTCDateTime)
    modified = Column(UTCDateTime)
    participant_id = Column(BigInteger, ForeignKey("participant.id"))
    data_sync_transfer_type = Column(TINYINT)
    request_payload = Column(JSON, nullable=True)
    response_code = Column(String(128))


event.listen(PPSCDataSyncRecord, "before_insert", model_insert_listener)
event.listen(PPSCDataSyncRecord, "before_update", model_update_listener)


class PPSCDataSyncBase(PPSCBase):

    id = Column("id", BigInteger, autoincrement=True, primary_key=True)
    created = Column(UTCDateTime)
    modified = Column(UTCDateTime)
    participant_id = Column(BigInteger, ForeignKey("participant.id"))


class PPSCDataSyncCore(PPSCDataSyncBase):
    __tablename__ = "ppsc_data_sync_core"

    has_core_data = Column(TINYINT, default=0)
    has_core_data_date = Column(UTCDateTime)


event.listen(PPSCDataSyncCore, "before_insert", model_insert_listener)
event.listen(PPSCDataSyncCore, "before_update", model_update_listener)


class PPSCDataSyncEHR(PPSCDataSyncBase):
    __tablename__ = "ppsc_data_sync_ehr"

    first_time_date = Column(UTCDateTime)
    last_time_date = Column(UTCDateTime)


event.listen(PPSCDataSyncEHR, "before_insert", model_insert_listener)
event.listen(PPSCDataSyncEHR, "before_update", model_update_listener)


class PPSCDataSyncBiobankSample(PPSCDataSyncBase):
    __tablename__ = "ppsc_data_sync_biobank_sample"

    first_time_date = Column(UTCDateTime)
    last_time_date = Column(UTCDateTime)


event.listen(PPSCDataSyncBiobankSample, "before_insert", model_insert_listener)
event.listen(PPSCDataSyncBiobankSample, "before_update", model_update_listener)


class PPSCDataSyncHealthData(PPSCDataSyncBase):
    __tablename__ = "ppsc_data_sync_health_data"

    health_data_stream_sharing_status = Column(TINYINT, default=0)
    health_data_stream_sharing_status_date = Column(UTCDateTime)


event.listen(PPSCDataSyncCore, "before_insert", model_insert_listener)
event.listen(PPSCDataSyncCore, "before_update", model_update_listener)
