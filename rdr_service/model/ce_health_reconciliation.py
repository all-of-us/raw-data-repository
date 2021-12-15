from sqlalchemy import Column, Integer, Boolean, String, event

from rdr_service.model.base import Base, model_insert_listener, model_update_listener
from rdr_service.model.utils import UTCDateTime6


class CeHealthReconciliation(Base):
    __tablename__ = "ce_health_reconciliation"

    id = Column("id", Integer, primary_key=True, autoincrement=True, nullable=False)
    """Auto increment, primary key."""
    created = Column("created", UTCDateTime6, nullable=True)
    """The create time for this record."""
    modified = Column("modified", UTCDateTime6, nullable=True)
    """The last modified time for this record."""

    missingFilePath = Column("missing_file_path", String(512))
    """The missing fitbit file google bucket path"""
    fileTransferredTime = Column("file_transferred_time", UTCDateTime6)
    """The date time that the file is transferred"""
    reportFilePath = Column('report_file_path', String(512))
    """CE daily report file path"""
    reportDate = Column("report_date", UTCDateTime6)
    """The date time that the report is uploaded to google bucket"""
    status = Column('status', Boolean)
    """If a missing file is found or not, False means not exist, True means found"""


event.listen(CeHealthReconciliation, "before_insert", model_insert_listener)
event.listen(CeHealthReconciliation, "before_update", model_update_listener)
