
import sqlalchemy as sa

from rdr_service.model.base import Base, model_insert_listener, model_update_listener
from rdr_service.participant_enums import OrderStatus
from rdr_service.model.utils import Enum, UTCDateTime


class SampleOrderStatus(Base):
    __tablename__ = 'sample_order_status'
    id = sa.Column('id', sa.Integer, primary_key=True, autoincrement=True, nullable=False)
    created = sa.Column(UTCDateTime, nullable=False)
    modified = sa.Column(UTCDateTime, nullable=False)
    participant_id = sa.Column(sa.Integer, sa.ForeignKey('participant.participant_id'), nullable=False)
    test_code = sa.Column(sa.String(80), nullable=False)
    status = sa.Column(Enum(OrderStatus), nullable=False)
    status_time = sa.Column(UTCDateTime)


sa.event.listen(SampleOrderStatus, 'before_insert', model_insert_listener)
sa.event.listen(SampleOrderStatus, 'before_update', model_update_listener)
