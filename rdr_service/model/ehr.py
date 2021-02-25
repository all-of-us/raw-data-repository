from sqlalchemy import Column, ForeignKey, Index, Integer

from rdr_service.model.base import Base
from rdr_service.model.participant import Participant
from rdr_service.model.utils import UTCDateTime


class EhrReceipt(Base):
    """
    A receipt log recording when HPOs submit EHR data.
    """

    __tablename__ = "ehr_receipt"
    ehrReceiptId = Column("ehr_receipt_id", Integer, primary_key=True)
    organizationId = Column(
        "organization_id", Integer, ForeignKey("organization.organization_id", ondelete="CASCADE"), nullable=False
    )
    """An organization a participant is paired with or "unset" if none"""
    receiptTime = Column("receipt_time", UTCDateTime, nullable=False, index=True)


class ParticipantEhrReceipt(Base):
    """
    A receipt log recording when EHR data submissions are made for participants
    """

    __tablename__ = "participant_ehr_receipt"
    id = Column("id", Integer, primary_key=True, autoincrement=True, nullable=False)
    participantId = Column(
        "participant_id", Integer, ForeignKey(Participant.participantId, ondelete="CASCADE"), nullable=False
    )
    fileTimestamp = Column("file_timestamp", UTCDateTime, nullable=False)
    firstSeen = Column("first_seen", UTCDateTime, nullable=False)
    lastSeen = Column("last_seen", UTCDateTime, nullable=False)


Index(
    'idx_participant_ehr_receipt_participant_file_time',
    ParticipantEhrReceipt.participantId,
    ParticipantEhrReceipt.fileTimestamp,
    unique=True
)
