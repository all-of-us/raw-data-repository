from sqlalchemy import (
    Column, ForeignKey, BigInteger, String, Index, event
)
from sqlalchemy.dialects.mysql import TINYINT

from rdr_service.model.base import RexBase, model_insert_listener, model_update_listener
from rdr_service.model.utils import UTCDateTime


class Study(RexBase):
    __tablename__ = 'study'

    id = Column('id', BigInteger, autoincrement=True, primary_key=True)
    created = Column(UTCDateTime)
    modified = Column(UTCDateTime)
    ignore_flag = Column(TINYINT)
    name = Column(String(128))
    prefix = Column(BigInteger)


event.listen(Study, "before_insert", model_insert_listener)
event.listen(Study, "before_update", model_update_listener)


class ParticipantMapping(RexBase):
    __tablename__ = 'participant_mapping'

    id = Column("id", BigInteger, autoincrement=True, primary_key=True)
    created = Column(UTCDateTime)
    modified = Column(UTCDateTime)
    ignore_flag = Column(TINYINT)
    primary_study_id = Column(BigInteger, ForeignKey("study.id"))
    ancillary_study_id = Column(BigInteger, ForeignKey("study.id"))
    primary_participant_id = Column(BigInteger)
    ancillary_participant_id = Column(BigInteger)


Index("participant_mapping_primary_participant_id", ParticipantMapping.primary_participant_id)
Index("participant_mapping_ancillary_participant_id", ParticipantMapping.ancillary_participant_id)

event.listen(ParticipantMapping, "before_insert", model_insert_listener)
event.listen(ParticipantMapping, "before_update", model_update_listener)
