from sqlalchemy import Column, ForeignKey, Integer, Boolean, String, JSON, event, UniqueConstraint

from rdr_service.model.base import Base, model_insert_listener, model_update_listener
from rdr_service.model.utils import UTCDateTime6


class MessageBrokerRecord(Base):
    __tablename__ = "message_broker_record"

    id = Column("id", Integer, primary_key=True, autoincrement=True, nullable=False)
    """Auto increment, primary key."""
    created = Column("created", UTCDateTime6, nullable=True)
    """The create time for this record."""
    modified = Column("modified", UTCDateTime6, nullable=True)
    """The last modified time for this record."""

    participantId = Column("participant_id", Integer, ForeignKey("participant.participant_id"), nullable=False)
    """message participant id"""
    eventType = Column("event_type", String(128))
    """message event type"""
    eventAuthoredTime = Column("event_authored_time", UTCDateTime6)
    """The actual time at which the participant trigger this event"""
    messageOrigin = Column("message_origin", String(80))
    """indicate where this message is sent from"""
    messageDest = Column("message_dest", String(80))
    """indicate where this message will be sent to"""
    requestBody = Column("request_body", JSON)
    """http request body send to destination"""
    responseCode = Column("response_code", String(20))
    """http response code returned from the destination"""
    responseBody = Column("response_body", JSON, nullable=True)
    """Original resource value; whole payload response that was returned from the destination"""
    responseError = Column("response_error", String(2048))
    """The error message returned from the destination"""
    requestTime = Column("request_time", UTCDateTime6)
    """The time at which RDR received the request from requester"""
    responseTime = Column("response_time", UTCDateTime6)
    """The time at which RDR received the response from the destination"""
    requestResource = Column("request_resource", JSON, nullable=True)
    """Original resource value; whole payload request that was sent from the requester"""


event.listen(MessageBrokerRecord, "before_insert", model_insert_listener)
event.listen(MessageBrokerRecord, "before_update", model_update_listener)


class MessageBrokerMetadata(Base):
    __tablename__ = "message_broker_metadata"

    id = Column("id", Integer, primary_key=True, autoincrement=True, nullable=False)
    """Auto increment, primary key."""
    created = Column("created", UTCDateTime6, nullable=True)
    """The create time for this record."""
    modified = Column("modified", UTCDateTime6, nullable=True)
    """The last modified time for this record."""

    eventType = Column("event_type", String(128))
    """message event type"""
    destination = Column("destination", String(80))
    """message destination, decided by participant origin"""
    url = Column("url", String(512))
    """message destination endpoint"""

    __table_args__ = (UniqueConstraint('event_type', 'destination', 'url', name='unique_message_target'),)


event.listen(MessageBrokerMetadata, "before_insert", model_insert_listener)
event.listen(MessageBrokerMetadata, "before_update", model_update_listener)


class MessageBrokerDestAuthInfo(Base):
    __tablename__ = "Message_broker_dest_auth_info"

    id = Column("id", Integer, primary_key=True, autoincrement=True, nullable=False)
    """Auto increment, primary key."""
    created = Column("created", UTCDateTime6, nullable=True)
    """The create time for this record."""
    modified = Column("modified", UTCDateTime6, nullable=True)
    """The last modified time for this record."""

    destination = Column("destination", String(80))
    """message destination, decided by participant origin"""
    key = Column("key", String(256))
    """access client key"""
    secret = Column("secret", String(256))
    """access client secret"""
    tokenEndpoint = Column("token_endpoint", String(512))
    """token endpoint"""
    accessToken = Column("access_token", String(4000))
    """access token for the destination API"""
    expiredAt = Column("expired_at", UTCDateTime6, nullable=True)
    """access token expired time"""

    __table_args__ = (UniqueConstraint('destination', name='unique_destination'),)


event.listen(MessageBrokerDestAuthInfo, "before_insert", model_insert_listener)
event.listen(MessageBrokerDestAuthInfo, "before_update", model_update_listener)


class MessageBrokerEventData(Base):
    __tablename__ = "message_broker_event_data"

    id = Column("id", Integer, primary_key=True, autoincrement=True, nullable=False)
    """Auto increment, primary key."""
    created = Column("created", UTCDateTime6, nullable=True)
    """The create time for this record."""
    modified = Column("modified", UTCDateTime6, nullable=True)
    """The last modified time for this record."""

    messageRecordId = Column("message_record_id", Integer, ForeignKey("message_broker_record.id"))
    """message record id, foreign key of message_broker_record.id"""
    participantId = Column("participant_id", Integer, ForeignKey("participant.participant_id"))
    """participant id, foreign key of participant.participant_id"""
    eventType = Column("event_type", String(128))
    """message event type"""
    eventAuthoredTime = Column("event_authored_time", UTCDateTime6)
    """The actual time at which the participant trigger this event"""

    fieldName = Column("field_name", String(512))
    """message body field name"""
    valueString = Column("value_string", String(512))
    """message field value for string value"""
    valueInteger = Column("value_integer", Integer)
    """message field value for integer value"""
    valueDatetime = Column("value_datetime", UTCDateTime6)
    """message field value for datetime value"""
    valueBool = Column("value_bool", Boolean)
    """message field value for boolean value"""
    valueJson = Column("value_json", JSON)
    """message field value for json value"""


event.listen(MessageBrokerEventData, "before_insert", model_insert_listener)
event.listen(MessageBrokerEventData, "before_update", model_update_listener)
