from model.base import Base
from model.utils import UTCDateTime
from sqlalchemy import Column, Integer, String, ForeignKey


class BiobankStoredSample(Base):
  """Physical sampels which have been reported as received at Biobank.

  Each participant has an associated list of samples. Biobank uploads a list of all received
  samples periodically, and we update our list of stored samples to match. The output is a
  reconciliation report of ordered and stored samples; see BiobankOrder.

  Note that additional columns appear in the CSV uploaded from Biobank but are not persisted since
  they are unused in the reconciliation report; we also only exclude child samples.
  """
  __tablename__ = 'biobank_stored_sample'
  # A unique ID assigned by Biobank for the sample. (AKA "RLIMS Sample ID.)
  # We omit autoincrement=False to avoid warnings & instead validate clients provide an ID upstream.
  biobankStoredSampleId = Column('biobank_stored_sample_id', String(80), primary_key=True)

  # The participant the sample is associated to. We use Biobank's ID for streamlined importing.
  biobankId = Column('biobank_id', Integer, ForeignKey('participant.biobank_id'))

  # Which test was performed to produce this sample (ex: "1UR10" for blood draw). Rarely, the same
  # test may be performed multiple times for the same participant.
  test = Column('test', String(80), nullable=False)

  # Timestamp when Biobank finished receiving/preparing the sample (status changed from "In Prep"
  # to "In Circulation" in Mayo). This is the end time used for order-to-sample latency measurement.
  # We may receive samples before they are confirmed (and see a confirmed date added later).
  confirmed = Column('confirmed', UTCDateTime)
