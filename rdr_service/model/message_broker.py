from sqlalchemy import Column, ForeignKey, Integer, String, JSON, event

from rdr_service.model.base import Base, model_insert_listener, model_update_listener
from rdr_service.model.utils import UTCDateTime6


class MessageBrokerRecord(Base):
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
    requestResource = Column("request_resource", JSON, nullable=True)
    """Original resource value; whole payload request that was sent from the requester"""
    responseCode = Column("response_code", String(20))
    """http response code returned from the destination"""
    responseBody = Column("response_body", JSON, nullable=True)
    """Original resource value; whole payload response that was returned from the destination"""
    responseError = Column("response_error", String(1024))
    """The error message returned from the destination"""
    requestTime = Column("request_time", UTCDateTime6)
    """The time at which RDR received the request from requester"""
    responseTime = Column("response_time", UTCDateTime6)
    """The time at which RDR received the response from the destination"""


class MessageBrokerMetadata(Base):
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
    endpoint = Column("endpoint", String(512))
    """message destination endpoint"""


event.listen(MessageBrokerRecord, "before_insert", model_insert_listener)
event.listen(MessageBrokerRecord, "before_update", model_update_listener)
