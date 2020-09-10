from sqlalchemy import Column, Date, ForeignKey, event, Integer, String
from sqlalchemy.orm import relationship

from rdr_service.model.base import Base, model_insert_listener, model_update_listener
from rdr_service.model.utils import Enum, UTCDateTime
from rdr_service.participant_enums import DeceasedNotification, DeceasedReportDenialReason, DeceasedReportStatus


class DeceasedReport(Base):
    __tablename__ = "deceased_report"
    id = Column("id", Integer, primary_key=True, autoincrement=True, nullable=False)
    created = Column("created", UTCDateTime, nullable=False)
    modified = Column("modified", UTCDateTime, nullable=False)
    participantId = Column("participant_id", Integer, ForeignKey("participant.participant_id"), nullable=False)
    dateOfDeath = Column("date_of_death", Date)
    causeOfDeath = Column("cause_of_death", String(1024))
    notification = Column("notification", Enum(DeceasedNotification), nullable=False)
    notificationOther = Column("notification_other", String(1024))
    reporterName = Column('reporter_name', String(255))
    reporterRelationship = Column('reporter_relationship', String(8))
    reporterEmail = Column('reporter_email', String(255))
    reporterPhone = Column('reporter_phone', String(16))
    authorId = Column("author_id", Integer, ForeignKey("api_user.id"), nullable=False)
    authored = Column("authored", UTCDateTime, nullable=False)
    reviewerId = Column("reviewer_id", Integer, ForeignKey("api_user.id"))
    reviewed = Column("reviewed", UTCDateTime)
    status = Column("status", Enum(DeceasedReportStatus), nullable=False)
    denialReason = Column("denial_reason", Enum(DeceasedReportDenialReason))
    denialReasonOther = Column("denial_reason_other", String(1024))

    author = relationship("ApiUser", foreign_keys='DeceasedReport.authorId', lazy='joined')
    reviewer = relationship("ApiUser", foreign_keys='DeceasedReport.reviewerId', lazy='joined')
    participant = relationship("Participant", foreign_keys='DeceasedReport.participantId')


event.listen(DeceasedReport, "before_insert", model_insert_listener)
event.listen(DeceasedReport, "before_update", model_update_listener)
