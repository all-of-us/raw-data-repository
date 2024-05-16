from sqlalchemy import Column, Integer, BigInteger, String, ForeignKey, event
from sqlalchemy.dialects.mysql import TINYINT, JSON

from rdr_service.model.base import model_insert_listener, model_update_listener, PPSCBase
from rdr_service.model.utils import UTCDateTime


class Participant(PPSCBase):
    __tablename__ = "participant"

    id = Column("id", BigInteger, primary_key=True)
    created = Column(UTCDateTime)
    modified = Column(UTCDateTime)
    ignore_flag = Column(TINYINT, default=0)
    disable_flag = Column(TINYINT, default=0)
    disable_reason = Column(String(1024))
    biobank_id = Column(BigInteger, nullable=False, unique=True, index=True)
    registered_date = Column(UTCDateTime, nullable=False)


event.listen(Participant, "before_insert", model_insert_listener)
event.listen(Participant, "before_update", model_update_listener)


class Activity(PPSCBase):
    __tablename__ = "activity"

    id = Column("id", Integer, autoincrement=True, primary_key=True)
    created = Column(UTCDateTime)
    modified = Column(UTCDateTime)
    ignore_flag = Column(TINYINT, default=0)
    name = Column(String(128))
    rdr_note = Column(String(1024), nullable=True)
    rule_codes = Column(JSON, nullable=True)


event.listen(Activity, "before_insert", model_insert_listener)
event.listen(Activity, "before_update", model_update_listener)


class ParticipantEventActivity(PPSCBase):
    __tablename__ = "participant_event_activity"

    id = Column("id", BigInteger, autoincrement=True, primary_key=True)
    created = Column(UTCDateTime)
    modified = Column(UTCDateTime)
    ignore_flag = Column(TINYINT, default=0)
    participant_id = Column(BigInteger, ForeignKey("participant.id"))
    activity_id = Column(Integer, ForeignKey("activity.id"))
    resource = Column(JSON, nullable=True)


event.listen(ParticipantEventActivity, "before_insert", model_insert_listener)
event.listen(ParticipantEventActivity, "before_update", model_update_listener)


class EnrollmentEventType(PPSCBase):
    __tablename__ = "enrollment_event_type"

    id = Column("id", BigInteger, autoincrement=True, primary_key=True)
    created = Column(UTCDateTime)
    modified = Column(UTCDateTime)
    ignore_flag = Column(TINYINT, default=0)
    name = Column(String(128))
    source_name = Column(String(128))
    rule_codes = Column(JSON, nullable=True)
    version = Column(String(128), nullable=True)


event.listen(EnrollmentEventType, "before_insert", model_insert_listener)
event.listen(EnrollmentEventType, "before_update", model_update_listener)


class EnrollmentEvent(PPSCBase):
    __tablename__ = "enrollment_event"

    id = Column("id", BigInteger, autoincrement=True, primary_key=True)
    created = Column(UTCDateTime)
    modified = Column(UTCDateTime)
    ignore_flag = Column(TINYINT, default=0)
    event_authored_time = Column(UTCDateTime)
    participant_id = Column(BigInteger, ForeignKey("participant.id"))
    event_id = Column(BigInteger, ForeignKey("participant_event_activity.id"))
    event_type_id = Column(BigInteger, ForeignKey("enrollment_event_type.id"))


event.listen(EnrollmentEvent, "before_insert", model_insert_listener)
event.listen(EnrollmentEvent, "before_update", model_update_listener)
