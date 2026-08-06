import sqlalchemy as sa

from rdr_service.model.base import Base
from rdr_service.model.utils import UTCDateTime


class BiospecimenFlags(Base):
    __tablename__ = "biospecimen_flags"
    participant_id = sa.Column(
        sa.Integer, sa.ForeignKey("participant.participant_id"), primary_key=True, nullable=False
    )
    timestamp = sa.Column(UTCDateTime, nullable=False, server_default=sa.text('CURRENT_TIMESTAMP'))
    bmt = sa.Column(sa.Boolean)
