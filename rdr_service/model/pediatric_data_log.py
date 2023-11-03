from enum import Enum

import sqlalchemy as sa
from sqlalchemy.orm import relationship

from rdr_service.model.base import Base
from rdr_service.model.utils import UTCDateTime


class PediatricDataType(Enum):
    AGE_RANGE = 1
    ENVIRONMENTAL_EXPOSURES = 2


class PediatricDataLog(Base):
    __tablename__ = 'pediatric_data_log'
    id = sa.Column(sa.BigInteger, primary_key=True, autoincrement=True, nullable=False)
    created = sa.Column(UTCDateTime, nullable=False, server_default=sa.text('CURRENT_TIMESTAMP'))
    participant_id = sa.Column(sa.Integer, sa.ForeignKey('participant.participant_id'), nullable=False)
    replaced_by_id = sa.Column(sa.BigInteger, sa.ForeignKey('pediatric_data_log.id'), )
    """If an id is present, then this record was made obsolete by the one indicated by the id"""
    data_type = sa.Column(sa.Enum(PediatricDataType), nullable=False)
    value = sa.Column(sa.String(256))

    replaced_by = relationship(
        'PediatricDataLog',
        foreign_keys=replaced_by_id,
        remote_side=id,
        primaryjoin=replaced_by_id == id
    )
