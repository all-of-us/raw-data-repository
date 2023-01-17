from sqlalchemy import (
    Column, Integer, BigInteger, String, ForeignKey, Index, event)
from sqlalchemy.dialects.mysql import TINYINT, JSON
from sqlalchemy.orm import relation

from rdr_service.model.base import NphBase, model_insert_listener, model_update_listener
from rdr_service.model.utils import UTCDateTime


class Participant(NphBase):
    __tablename__ = "participant"

    id = Column("id", BigInteger, primary_key=True)
    created = Column(UTCDateTime)
    modified = Column(UTCDateTime)
    ignore_flag = Column(TINYINT, default=0)
    disable_flag = Column(TINYINT, default=0)
    disable_reason = Column(String(1024))
    biobank_id = Column(BigInteger)
    research_id = Column(BigInteger)


event.listen(Participant, "before_insert", model_insert_listener)
event.listen(Participant, "before_update", model_update_listener)


class StudyCategory(NphBase):
    __tablename__ = "study_category"

    id = Column("id", BigInteger, autoincrement=True, primary_key=True)
    created = Column(UTCDateTime)
    name = Column(String(128))
    type_label = Column(String(128))
    parent_id = Column(BigInteger, ForeignKey("study_category.id"))
    parent = relation("StudyCategory", remote_side=[id])
    children = relation("StudyCategory", remote_side=[parent_id], uselist=True)


event.listen(StudyCategory, "before_insert", model_insert_listener)


class Site(NphBase):
    __tablename__ = "site"
    id = Column("id", Integer, autoincrement=True, primary_key=True)
    created = Column(UTCDateTime)
    modified = Column(UTCDateTime)
    external_id = Column(String(256), index=True)
    name = Column(String(512))
    awardee_external_id = Column(String(256), index=True)
    organization_external_id = Column(String(256), index=True)


event.listen(Site, "before_insert", model_insert_listener)
event.listen(Site, "before_update", model_update_listener)


class Order(NphBase):
    __tablename__ = "order"

    id = Column("id", BigInteger, autoincrement=True, primary_key=True)
    nph_order_id = Column(String(64))
    created = Column(UTCDateTime)
    modified = Column(UTCDateTime)
    order_created = Column(UTCDateTime)
    ignore_flag = Column(TINYINT, default=0)
    category_id = Column(BigInteger, ForeignKey("study_category.id"))
    participant_id = Column(BigInteger, ForeignKey("participant.id"))
    created_author = Column(String(128))
    created_site = Column(Integer, ForeignKey("site.id"))
    collected_author = Column(String(128))
    collected_site = Column(Integer, ForeignKey("site.id"))
    finalized_author = Column(String(128))
    finalized_site = Column(Integer, ForeignKey("site.id"))
    amended_author = Column(String(128))
    amended_site = Column(Integer, ForeignKey("site.id"))
    amended_reason = Column(String(1024))
    notes = Column(JSON, nullable=False)
    status = Column(String(128))


Index("order_participant_id", Order.participant_id)
Index("order_created_site", Order.created_site)
Index("order_collected_site", Order.collected_site)
Index("order_finalized_site", Order.finalized_site)
Index("order_amended_site", Order.amended_site)

event.listen(Order, "before_insert", model_insert_listener)
event.listen(Order, "before_update", model_update_listener)


class OrderedSample(NphBase):
    __tablename__ = "ordered_sample"

    id = Column("id", BigInteger, autoincrement=True, primary_key=True)
    nph_sample_id = Column(String(64))
    order_id = Column(BigInteger, ForeignKey("order.id"))
    parent_sample_id = Column(BigInteger, ForeignKey("ordered_sample.id"))
    test = Column(String(40))
    description = Column(String(256))
    collected = Column(UTCDateTime)
    finalized = Column(UTCDateTime)
    aliquot_id = Column(String(128))
    identifier = Column(String(128))
    container = Column(String(128))
    volume = Column(String(128))
    status = Column(String(128))
    supplemental_fields = Column(JSON, nullable=True)
    parent = relation("OrderedSample", remote_side=[id])
    children = relation("OrderedSample", remote_side=[parent_sample_id], uselist=True)


class Activity(NphBase):
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


class ParticipantEventActivity(NphBase):
    __tablename__ = "participant_event_activity"

    id = Column("id", BigInteger, autoincrement=True, primary_key=True)
    created = Column(UTCDateTime)
    modified = Column(UTCDateTime)
    ignore_flag = Column(TINYINT, default=0)
    participant_id = Column(BigInteger, ForeignKey("participant.id"))
    activity_id = Column(Integer, ForeignKey("activity.id"))


event.listen(ParticipantEventActivity, "before_insert", model_insert_listener)
event.listen(ParticipantEventActivity, "before_update", model_update_listener)


class EnrollmentEventType(NphBase):
    __tablename__ = "enrollment_event_type"

    id = Column("id", BigInteger, autoincrement=True, primary_key=True)
    created = Column(UTCDateTime)
    modified = Column(UTCDateTime)
    ignore_flag = Column(TINYINT, default=0)
    name = Column(String(128))
    rule_codes = Column(JSON, nullable=True)
    version = Column(String(128), nullable=True)


event.listen(EnrollmentEventType, "before_insert", model_insert_listener)
event.listen(EnrollmentEventType, "before_update", model_update_listener)


class EnrollmentEvent(NphBase):
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


class PairingEventType(NphBase):
    __tablename__ = "pairing_event_type"

    id = Column("id", BigInteger, autoincrement=True, primary_key=True)
    created = Column(UTCDateTime)
    modified = Column(UTCDateTime)
    ignore_flag = Column(TINYINT, default=0)
    name = Column(String(1024))
    rule_codes = Column(JSON, nullable=True)


event.listen(PairingEventType, "before_insert", model_insert_listener)
event.listen(PairingEventType, "before_update", model_update_listener)


class PairingEvent(NphBase):
    __tablename__ = "pairing_event"

    id = Column("id", BigInteger, autoincrement=True, primary_key=True)
    created = Column(UTCDateTime)
    modified = Column(UTCDateTime)
    ignore_flag = Column(TINYINT, default=0)
    event_authored_time = Column(UTCDateTime)
    participant_id = Column(BigInteger, ForeignKey("participant.id"))
    event_id = Column(BigInteger, ForeignKey("participant_event_activity.id"))
    event_type_id = Column(BigInteger, ForeignKey("pairing_event_type.id"))
    site_id = Column(Integer, ForeignKey("site.id"))


event.listen(PairingEvent, "before_insert", model_insert_listener)
event.listen(PairingEvent, "before_update", model_update_listener)


class ConsentEventType(NphBase):
    __tablename__ = "consent_event_type"

    id = Column("id", BigInteger, autoincrement=True, primary_key=True)
    created = Column(UTCDateTime)
    modified = Column(UTCDateTime)
    ignore_flag = Column(TINYINT, default=0)
    disable_flag = Column(TINYINT, default=0)
    disable_reason = Column(String(1024), nullable=True)
    name = Column(String(1024))
    rule_codes = Column(JSON, nullable=True)
    version = Column(String(128), nullable=True)


event.listen(ConsentEventType, "before_insert", model_insert_listener)
event.listen(ConsentEventType, "before_update", model_update_listener)


class ConsentEvent(NphBase):
    __tablename__ = "consent_event"

    id = Column("id", BigInteger, autoincrement=True, primary_key=True)
    created = Column(UTCDateTime)
    modified = Column(UTCDateTime)
    ignore_flag = Column(TINYINT, default=0)
    event_authored_time = Column(UTCDateTime)
    participant_id = Column(BigInteger, ForeignKey("participant.id"))
    event_id = Column(BigInteger, ForeignKey("participant_event_activity.id"))
    event_type_id = Column(BigInteger, ForeignKey("consent_event_type.id"))


event.listen(ConsentEvent, "before_insert", model_insert_listener)
event.listen(ConsentEvent, "before_update", model_update_listener)
