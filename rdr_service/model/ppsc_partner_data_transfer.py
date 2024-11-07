from rdr_service.model.utils import Enum, UTCDateTime6

from sqlalchemy import Column, BigInteger, String, ForeignKey, event
from sqlalchemy.dialects.mysql import TINYINT, JSON

from rdr_service.model.base import model_insert_listener, model_update_listener, PPSCBase
from rdr_service.model.utils import UTCDateTime
from rdr_service.ppsc.ppsc_enums import DataSyncTransferType, AuthType, SpecimenType, SpecimenStatus


class BaseDataTransferAuth:

    id = Column("id", BigInteger, autoincrement=True, primary_key=True)
    created = Column(UTCDateTime)
    modified = Column(UTCDateTime)
    auth_type = Column(Enum(AuthType))
    auth_url = Column(String(512), nullable=False)
    client_id = Column(String(512), nullable=False)
    client_secret = Column(String(512), nullable=False)
    access_token = Column(String(1024))
    expires = Column(String(256))
    last_generated = Column(UTCDateTime6)
    ignore_flag = Column(TINYINT, default=0)


class BaseDataTransferEndpoint:

    id = Column("id", BigInteger, autoincrement=True, primary_key=True)
    created = Column(UTCDateTime)
    modified = Column(UTCDateTime)
    base_url = Column(String(512), nullable=False)
    endpoint = Column(String(512), nullable=False)
    data_sync_transfer_type = Column(Enum(DataSyncTransferType), nullable=False)
    ignore_flag = Column(TINYINT, default=0)


class BaseDataTransferRecord:

    id = Column("id", BigInteger, autoincrement=True, primary_key=True)
    created = Column(UTCDateTime)
    modified = Column(UTCDateTime)
    data_sync_transfer_type = Column(Enum(DataSyncTransferType))
    data_type_record_id = Column(BigInteger)
    request_payload = Column(JSON, nullable=True)
    response_code = Column(String(128))
    ignore_flag = Column(TINYINT, default=0)


class PPSCDataTransferAuth(BaseDataTransferAuth, PPSCBase):
    __tablename__ = "ppsc_data_transfer_auth"


event.listen(PPSCDataTransferAuth, "before_insert", model_insert_listener)
event.listen(PPSCDataTransferAuth, "before_update", model_update_listener)


class PPSCDataTransferEndpoint(BaseDataTransferEndpoint, PPSCBase):
    __tablename__ = "ppsc_data_transfer_endpoint"


event.listen(PPSCDataTransferEndpoint, "before_insert", model_insert_listener)
event.listen(PPSCDataTransferEndpoint, "before_update", model_update_listener)


class PPSCDataTransferRecord(BaseDataTransferRecord, PPSCBase):
    __tablename__ = "ppsc_data_transfer_record"

    participant_id = Column(BigInteger, ForeignKey("participant.id"))


event.listen(PPSCDataTransferRecord, "before_insert", model_insert_listener)
event.listen(PPSCDataTransferRecord, "before_update", model_update_listener)


class PPSCData:

    id = Column("id", BigInteger, autoincrement=True, primary_key=True)
    created = Column(UTCDateTime)
    modified = Column(UTCDateTime)
    ignore_flag = Column(TINYINT, default=0)


class PPSCDataBase(PPSCData):

    event_date_time = Column(UTCDateTime6)


class PPSCCore(PPSCDataBase, PPSCBase):
    __tablename__ = "ppsc_core"

    participant_id = Column(BigInteger, ForeignKey("participant.id"))
    has_core_data = Column(TINYINT, default=0)


event.listen(PPSCCore, "before_insert", model_insert_listener)
event.listen(PPSCCore, "before_update", model_update_listener)


class PPSCEHR(PPSCDataBase, PPSCBase):
    __tablename__ = "ppsc_ehr"

    participant_id = Column(BigInteger, ForeignKey("participant.id"))


event.listen(PPSCEHR, "before_insert", model_insert_listener)
event.listen(PPSCEHR, "before_update", model_update_listener)


class PPSCBiobankSample(PPSCDataBase, PPSCBase):
    __tablename__ = "ppsc_biobank_sample"

    participant_id = Column(BigInteger, ForeignKey("participant.id"))
    specimen_type = Column(Enum(SpecimenType))
    specimen_status = Column(Enum(SpecimenStatus))


event.listen(PPSCBiobankSample, "before_insert", model_insert_listener)
event.listen(PPSCBiobankSample, "before_update", model_update_listener)


class PPSCHealthData(PPSCDataBase, PPSCBase):
    __tablename__ = "ppsc_health_data"

    participant_id = Column(BigInteger, ForeignKey("participant.id"))
    health_data_stream_sharing_status = Column(TINYINT, default=0)


event.listen(PPSCHealthData, "before_insert", model_insert_listener)
event.listen(PPSCHealthData, "before_update", model_update_listener)


class PPSCNphEligibleParticipants(PPSCBase):
    __tablename__ = "ppsc_nph_eligible_participants"

    id = Column("id", BigInteger, autoincrement=True, primary_key=True)
    created = Column(UTCDateTime)
    modified = Column(UTCDateTime)
    nph_participant_id = Column(BigInteger, ForeignKey("nph.participant.id"))


event.listen(PPSCNphEligibleParticipants, "before_insert", model_insert_listener)
event.listen(PPSCNphEligibleParticipants, "before_update", model_update_listener)


class RTIDataTransferAuth(BaseDataTransferAuth, PPSCBase):
    __tablename__ = "rti_data_transfer_auth"


event.listen(RTIDataTransferAuth, "before_insert", model_insert_listener)
event.listen(RTIDataTransferAuth, "before_update", model_update_listener)


class RTIDataTransferEndpoint(BaseDataTransferEndpoint, PPSCBase):
    __tablename__ = "rti_data_transfer_endpoint"


event.listen(RTIDataTransferEndpoint, "before_insert", model_insert_listener)
event.listen(RTIDataTransferEndpoint, "before_update", model_update_listener)


class RTIDataTransferRecord(BaseDataTransferRecord, PPSCBase):
    __tablename__ = "rti_data_transfer_record"

    nph_participant_id = Column(BigInteger, ForeignKey("nph.participant.id"))


event.listen(RTIDataTransferRecord, "before_insert", model_insert_listener)
event.listen(RTIDataTransferRecord, "before_update", model_update_listener)


class RTINphOptIn(PPSCDataBase, PPSCBase):
    __tablename__ = "rti_nph_opt_in"

    nph_participant_id = Column(BigInteger, ForeignKey("nph.participant.id"))


event.listen(RTINphOptIn, "before_insert", model_insert_listener)
event.listen(RTINphOptIn, "before_update", model_update_listener)
