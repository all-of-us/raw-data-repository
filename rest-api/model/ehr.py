from model.base import Base
from model.utils import UTCDateTime
from sqlalchemy import Column, Integer, ForeignKey


class EhrReceipt(Base):
  """A receipt log for Electronic Health Records.
  """
  __tablename__ = 'ehr_receipt'
  ehrReceiptId = Column('ehr_receipt_id', Integer, primary_key=True)
  recordedTime = Column('recorded_time', UTCDateTime, nullable=False, index=True)
  receivedTime = Column('received_time', UTCDateTime, nullable=False)
  participantId = Column('participant_id', Integer,
                         ForeignKey('participant.participant_id', ondelete='CASCADE'),
                         nullable=False)
  siteId = Column('site_id', Integer, ForeignKey('site.site_id', ondelete='CASCADE'),
                  nullable=False)
