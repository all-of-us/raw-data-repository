from sqlalchemy import Column, BigInteger, ForeignKey, event
from sqlalchemy.dialects.mysql import TINYINT

from rdr_service.model.base import model_insert_listener, model_update_listener, PPSCBase
from rdr_service.model.utils import UTCDateTime, UTCDateTime6


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
