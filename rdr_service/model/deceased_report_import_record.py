from sqlalchemy import Column, ForeignKey, Index, Integer
from sqlalchemy.orm import relationship

from rdr_service.clock import CLOCK
from rdr_service.model.base import Base
from rdr_service.model.deceased_report import DeceasedReport
from rdr_service.model.utils import UTCDateTime


class DeceasedReportImportRecord(Base):
    __tablename__ = "deceased_report_import_record"
    id = Column("id", Integer, primary_key=True, autoincrement=True, nullable=False)
    created = Column("created", UTCDateTime, default=CLOCK.now(), nullable=False)
    lastSeen = Column("last_seen", UTCDateTime, nullable=False)
    recordId = Column("record_id", Integer, nullable=False)
    deceasedReportId = Column("deceased_report_id", Integer, ForeignKey(DeceasedReport.id))

    deceasedReport = relationship(DeceasedReport)


Index('idx_deceased_report_import_history_record_id', DeceasedReportImportRecord.recordId)
