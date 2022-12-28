
import sqlalchemy as sa

from rdr_service.model.base import Base
from rdr_service.model.utils import UTCDateTime


class ProfileUpdate(Base):
    __tablename__ = 'profile_update'
    id = sa.Column(sa.Integer, primary_key=True, autoincrement=True, nullable=False)
    created = sa.Column(UTCDateTime, server_default=sa.text('CURRENT_TIMESTAMP'), nullable=False)
    json = sa.Column(sa.JSON, nullable=False)
