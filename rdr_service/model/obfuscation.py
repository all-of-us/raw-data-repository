
import sqlalchemy as sa

from rdr_service.clock import CLOCK
from rdr_service.model.base import Base
from rdr_service.model.utils import UTCDateTime


class Obfuscation(Base):
    __tablename__ = 'obfuscation'
    id = sa.Column(sa.String(32), primary_key=True, nullable=False)
    created = sa.Column(UTCDateTime, nullable=False, default=CLOCK.now)
    expires = sa.Column(UTCDateTime, nullable=False)
    data = sa.Column(sa.JSON)
