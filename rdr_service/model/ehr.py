from model.base import Base
from model.utils import UTCDateTime
from sqlalchemy import Column, Integer, ForeignKey


class EhrReceipt(Base):
  """A receipt log recording when HPOs submit EHR data.
  """
  __tablename__ = 'ehr_receipt'
  ehrReceiptId = Column('ehr_receipt_id', Integer, primary_key=True)
  organizationId = Column('organization_id', Integer,
                          ForeignKey('organization.organization_id', ondelete='CASCADE'),
                          nullable=False)
  receiptTime = Column('receipt_time', UTCDateTime, nullable=False, index=True)
