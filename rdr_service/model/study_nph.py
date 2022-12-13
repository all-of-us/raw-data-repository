
from sqlalchemy import Column, DateTime, BigInteger, String
from sqlalchemy.dialects.mysql import TINYINT

from rdr_service.model.base import NphBase


class Participant(NphBase):
    __tablename__ = 'participant'

    id = Column('id', BigInteger, primary_key=True)
    created = Column(DateTime)
    modified = Column(DateTime)
    ignore_flag = Column(TINYINT)
    disable_flag = Column(TINYINT)
    disable_reason = Column(String(1024))
    biobank_id = Column(BigInteger)
    research_id = Column(BigInteger)
