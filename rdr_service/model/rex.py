from sqlalchemy import Column, BigInteger, String
from sqlalchemy.dialects.mysql import TINYINT

from rdr_service.model.base import RexBase
from rdr_service.model.utils import UTCDateTime


class Study(RexBase):
    __tablename__ = 'study'
    id = Column('id', BigInteger, autoincrement=True, primary_key=True)
    created = Column(UTCDateTime)
    modified = Column(UTCDateTime)
    ignore_flag = Column(TINYINT)
    name = Column(String(128))
    prefix = Column(BigInteger)


# class Participant(RexBase):
#     __tablename__ = 'participant'
#     id = Column("id", BigInteger, autoincrement=True, primary_key=True)
#     created = Column(DateTime)
#     modified = Column(DateTime)
#     ignore_flag = Column(TINYINT)
#     primary_study_id = Column(BigInteger, ForeignKey("study.id"))
#     ancillary_study_id = Column(BigInteger, ForeignKey("study.id"))
#     primary_participant_id = Column(BigInteger, ForeignKey("rdr.participant.participant_id"))
#     ancillary_participant_id = Column(BigInteger, ForeignKey("nph.participant.id"))
