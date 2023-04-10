from typing import List

from sqlalchemy import Column, Integer, BigInteger, String, ForeignKey, Index, event
from sqlalchemy.dialects.mysql import TINYINT, JSON
from sqlalchemy.orm import relation, relationship

from rdr_service.ancillary_study_resources.nph.enums import ConsentOptInTypes, ParticipantOpsElementTypes
from rdr_service.model.base import NphBase, model_insert_listener, model_update_listener
from rdr_service.model.study_nph_enums import StoredSampleStatus, IncidentStatus, IncidentType
from rdr_service.model.utils import UTCDateTime, Enum


class Participant(NphBase):
    # A new participant in this table can only be be added
    # if they exist in rdr.participant table
    __tablename__ = "participant"

    id = Column("id", BigInteger, primary_key=True)
    created = Column(UTCDateTime)
    modified = Column(UTCDateTime)
    ignore_flag = Column(TINYINT, default=0)
    disable_flag = Column(TINYINT, default=0)
    disable_reason = Column(String(1024))
    biobank_id = Column(BigInteger, nullable=False, unique=True)
    research_id = Column(BigInteger, unique=True)


Index("participant_biobank_id", Participant.biobank_id)

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
    client_id = Column(String(64), nullable=True)
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

    samples: List['OrderedSample'] = relationship('OrderedSample', back_populates='order')


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
    volumeUnits = Column(String(128))
    status = Column(String(128))
    supplemental_fields = Column(JSON, nullable=True)
    parent = relation("OrderedSample", remote_side=[id])
    children: List['OrderedSample'] = relation("OrderedSample", remote_side=[parent_sample_id], uselist=True)

    order = relationship(Order, back_populates='samples')


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
    resource = Column(JSON, nullable=True)


event.listen(ParticipantEventActivity, "before_insert", model_insert_listener)
event.listen(ParticipantEventActivity, "before_update", model_update_listener)


class EnrollmentEventType(NphBase):
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
    source_name = Column(String(128))
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
    source_name = Column(String(128))
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
    opt_in = Column(Enum(ConsentOptInTypes), nullable=False)


event.listen(ConsentEvent, "before_insert", model_insert_listener)
event.listen(ConsentEvent, "before_update", model_update_listener)


class WithdrawalEvent(NphBase):
    __tablename__ = "withdrawal_event"

    id = Column("id", BigInteger, autoincrement=True, primary_key=True)
    created = Column(UTCDateTime)
    modified = Column(UTCDateTime)
    ignore_flag = Column(TINYINT, default=0)
    event_authored_time = Column(UTCDateTime)
    participant_id = Column(BigInteger, ForeignKey("participant.id"))
    event_id = Column(BigInteger, ForeignKey("participant_event_activity.id"))


event.listen(WithdrawalEvent, "before_insert", model_insert_listener)
event.listen(WithdrawalEvent, "before_update", model_update_listener)


class DeactivationEvent(NphBase):
    __tablename__ = "deactivation_event"

    id = Column("id", BigInteger, autoincrement=True, primary_key=True)
    created = Column(UTCDateTime)
    modified = Column(UTCDateTime)
    ignore_flag = Column(TINYINT, default=0)
    event_authored_time = Column(UTCDateTime)
    participant_id = Column(BigInteger, ForeignKey("participant.id"))
    event_id = Column(BigInteger, ForeignKey("participant_event_activity.id"))


event.listen(DeactivationEvent, "before_insert", model_insert_listener)
event.listen(DeactivationEvent, "before_update", model_update_listener)


class SampleUpdate(NphBase):
    __tablename__ = "sample_update"

    id = Column("id", BigInteger, autoincrement=True, primary_key=True)
    created = Column(UTCDateTime)
    ignore_flag = Column(TINYINT, default=0)
    rdr_ordered_sample_id = Column(BigInteger, ForeignKey("ordered_sample.id"))
    ordered_sample_json = Column(JSON)


event.listen(SampleUpdate, "before_insert", model_insert_listener)


class BiobankFileExport(NphBase):
    __tablename__ = "biobank_file_export"

    id = Column("id", BigInteger, autoincrement=True, primary_key=True)
    created = Column(UTCDateTime)
    file_name = Column(String(256))
    crc32c_checksum = Column(String(64), nullable=False)


event.listen(BiobankFileExport, "before_insert", model_insert_listener)


class SampleExport(NphBase):
    __tablename__ = "sample_export"

    id = Column("id", BigInteger, autoincrement=True, primary_key=True)
    ignore_flag = Column(TINYINT, default=0)
    export_id = Column(BigInteger, ForeignKey("biobank_file_export.id"))
    sample_update_id = Column(BigInteger, ForeignKey("sample_update.id"))


class StoredSample(NphBase):
    __tablename__ = "stored_sample"

    id = Column("id", BigInteger, autoincrement=True, primary_key=True)
    created = Column(UTCDateTime)
    modified = Column(UTCDateTime)
    biobank_modified = Column(UTCDateTime)
    biobank_id = Column(BigInteger, ForeignKey("participant.biobank_id"))
    ignore_flag = Column(TINYINT, default=0)
    sample_id = Column(BigInteger, index=True)
    lims_id = Column(String(64))
    status = Column(Enum(StoredSampleStatus), default=StoredSampleStatus.SHIPPED)
    disposition = Column(String(256))


event.listen(StoredSample, "before_insert", model_insert_listener)
event.listen(StoredSample, "before_update", model_update_listener)


class Incident(NphBase):
    __tablename__ = "incident"

    id = Column("id", BigInteger, autoincrement=True, primary_key=True)
    created = Column(UTCDateTime)
    modified = Column(UTCDateTime)
    ignore_flag = Column(TINYINT, default=0)
    dev_note = Column(String(1024))
    status_str = Column(String(512), default=str(IncidentStatus.OPEN))
    status_id = Column(Enum(IncidentStatus), default=IncidentStatus.OPEN)
    message = Column(String(1024))
    notification_sent_flag = Column(TINYINT, default=0)
    notification_date = Column(UTCDateTime)
    incident_type_str = Column(String(512), default=str(IncidentType.UNSET))
    incident_type_id = Column(Enum(IncidentType), default=IncidentType.UNSET)
    participant_id = Column(BigInteger, ForeignKey("participant.id"))
    event_id = Column(BigInteger, ForeignKey("participant_event_activity.id"))
    trace_id = Column(String(128))  # Job Run Id for Tracing


event.listen(Incident, "before_insert", model_insert_listener)
event.listen(Incident, "before_update", model_update_listener)


class ParticipantOpsDataElement(NphBase):
    __tablename__ = "participant_ops_data_element"

    id = Column("id", BigInteger, primary_key=True)
    created = Column(UTCDateTime)
    modified = Column(UTCDateTime)
    ignore_flag = Column(TINYINT, default=0)
    participant_id = Column(BigInteger, ForeignKey("participant.id"))
    source_data_element = Column(Enum(ParticipantOpsElementTypes), nullable=False)
    source_value = Column(String(512))


event.listen(ParticipantOpsDataElement, "before_insert", model_insert_listener)
event.listen(ParticipantOpsDataElement, "before_update", model_update_listener)
