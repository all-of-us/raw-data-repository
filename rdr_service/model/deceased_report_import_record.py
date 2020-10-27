from sqlalchemy import Column, ForeignKey, Integer
from sqlalchemy.orm import relationship

from rdr_service.model.base import Base
from rdr_service.model.deceased_report import DeceasedReport
from rdr_service.model.participant import Participant
from rdr_service.model.utils import UTCDateTime


class DeceasedReportImportRecord(Base):
    __tablename__ = "deceased_report_import_record"
    id = Column("id", Integer, primary_key=True, autoincrement=True, nullable=False)
    created = Column("created", UTCDateTime, nullable=False)
    lastSeen = Column("last_seen", UTCDateTime, nullable=False)
    participantId = Column("participant_id", Integer, ForeignKey(Participant.participantId), nullable=False)
    deceasedReportId = Column("deceased_report_id", Integer, ForeignKey(DeceasedReport.id))

    deceasedReport = relationship(DeceasedReport)
