from sqlalchemy import BigInteger, Column, String, event
from sqlalchemy.dialects.mysql import TINYINT

from rdr_service.model.base import model_insert_listener, model_update_listener, PPSCBase
from rdr_service.model.utils import UTCDateTime


class Participant(PPSCBase):
    __tablename__ = "participant"

    id = Column("id", BigInteger, primary_key=True)
    created = Column(UTCDateTime)
    modified = Column(UTCDateTime)
    ignore_flag = Column(TINYINT, default=0)
    disable_flag = Column(TINYINT, default=0)
    disable_reason = Column(String(1024))
    biobank_id = Column(BigInteger, nullable=False, unique=True, index=True)
    research_id = Column(BigInteger, unique=True)


event.listen(Participant, "before_insert", model_insert_listener)
event.listen(Participant, "before_update", model_update_listener)



