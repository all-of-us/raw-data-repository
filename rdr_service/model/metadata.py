from sqlalchemy import Column, Integer, String
from rdr_service.model.base import Base
from rdr_service.model.utils import UTCDateTime6


class Metadata(Base):
    __tablename__ = "metadata"

    # Primary Key
    id = Column("id", Integer, primary_key=True, autoincrement=True, nullable=False)
    created = Column("created", UTCDateTime6, nullable=True)
    modified = Column("modified", UTCDateTime6, nullable=True)

    key = Column("key", String(255), nullable=False)
    strValue = Column("str_value", String(500), nullable=True)
    intValue = Column("int_value", Integer, nullable=True)
    dateValue = Column("date_value", UTCDateTime6, nullable=True)
