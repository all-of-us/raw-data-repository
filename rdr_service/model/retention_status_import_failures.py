from sqlalchemy import (
    BigInteger,
    Column,
    Integer,
    String
)
from rdr_service.model.base import Base
from rdr_service.model.utils import UTCDateTime


class RetentionStatusImportFailures(Base):
    __tablename__ = "retention_status_import_failures"

    id = Column("id", BigInteger, primary_key=True, autoincrement=True, nullable=False)
    created = Column("created", UTCDateTime, nullable=True)
    modified = Column("modified", UTCDateTime, nullable=True)
    file_path = Column("file_path", String(512), nullable=True)
    failure_count = Column("failure_count", Integer, nullable=True)
