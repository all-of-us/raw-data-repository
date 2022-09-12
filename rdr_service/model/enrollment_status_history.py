
import sqlalchemy as sa

from rdr_service.model.base import Base
from rdr_service.model.utils import UTCDateTime


class EnrollmentStatusHistory(Base):
    __tablename__ = 'enrollment_status_history'
    id = sa.Column(sa.Integer, primary_key=True, autoincrement=True, nullable=False)
    participant_id = sa.Column(sa.Integer, sa.ForeignKey('participant.participant_id'), nullable=False)
    version = sa.Column(sa.String(16))
    status = sa.Column(sa.String(64))
    timestamp = sa.Column(UTCDateTime)
