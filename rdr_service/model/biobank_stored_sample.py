from sqlalchemy import Column, ForeignKey, Index, Integer, String, DateTime, event

from rdr_service import clock
from rdr_service.model.base import Base, model_update_listener
from rdr_service.model.utils import Enum, UTCDateTime
from rdr_service.participant_enums import SampleStatus


class BiobankStoredSample(Base):
    """Physical samples which have been reported as received at Biobank.

  Each participant has an associated list of samples. Biobank uploads a list of all received
  samples periodically, and we update our list of stored samples to match. The output is a
  reconciliation report of ordered and stored samples; see BiobankOrder.

  Note that additional columns appear in the CSV uploaded from Biobank but are not persisted since
  they are unused in the reconciliation report; we also only exclude child samples.

  Additional field information from biobank:

    A sample family (family_id) is created upon a parent tube being created. The family
    contains study, visit, parent specimen information and others. Knowing the study,
    visit and specimen information we are able to capture the test code value from the
    study build information.  Each sample is then linked to the family by their family id.

    Order Identifier (biobank_order_identifier.value) is the unique id generated for
    each order placed into MayoLink. This value has two different phases, before Aug
    2018 and after.

    Sample Order Id (biobank_ordered_sample.order_id) is the unique id generated by MayoLink as a
    internal primary key value.

    Sample Id (biobank_stored_sample_id) is the unique specimen id. Each sample created has a
    unique Id. Sample ids are contained within a family. Family is created at same time the
    parent tube is created.

    Specimen Id for this project now is the order id plus an extra 4 numeric values.
    First 2 represent study and the next two are identifying the test we are receiving.


  """

    __tablename__ = "biobank_stored_sample"
    biobankStoredSampleId = Column("biobank_stored_sample_id", String(80), primary_key=True)
    """A unique ID assigned by Biobank for the sample. (also referred to as 'RLIMS Sample ID')"""

    biobankId = Column("biobank_id", Integer, ForeignKey("participant.biobank_id"))
    """
    PMI-specific ID generated by the RDR and used exclusively for communicating with the biobank.
    10-character string beginning with B.
    """

    biobankOrderIdentifier = Column("biobank_order_identifier", String(80), nullable=False)
    """
    The globally unique ID created by HealthPro when a biobank order is created.
    This order ID is pushed to MayoLINK when the biobank order is created in their system.
    As requested/suggested by Mayo, it should be 12 alphanumeric characters long
    """
    test = Column("test", String(80), nullable=False, index=True)
    """
    The name of the test run to produce this sample
    @rdr_dictionary_show_unique_values
    """

    # Timestamp when Biobank finished receiving/preparing the sample (status changed from "In Prep"
    # to "In Circulation" in Mayo). This is the end time used for order-to-sample latency measurement.
    # We may receive samples before they are confirmed (and see a confirmed date added later).
    confirmed = Column("confirmed", UTCDateTime)
    """Whether or not biobank reports having received it (or if lost, etc.)"""

    created = Column("created", UTCDateTime)
    """The datetime at which the biobank received/created the sample"""

    status = Column("status", Enum(SampleStatus), default=SampleStatus.RECEIVED)
    """The biobank marks the status of sample itself numerator"""

    disposed = Column("disposed", UTCDateTime)
    """The datetime at which the sample was disposed of"""

    family_id = Column("family_id", String(80), nullable=True)
    """Sample family ID"""

    rdrCreated = Column("rdr_created", DateTime)
    """stored sample - AFTER the biobank pipeline runs and RDR adds the record to the stored sample table"""
    modified = Column("modified", DateTime)
    """When that stored sample record was modified"""

    __table_args__ = (Index("ix_boi_test", "biobank_order_identifier", "test"),)


# pylint: disable=unused-argument
def stored_sample_insert_listener(mapper, connection, target):
    """ On insert auto set `rdrCreated` and `modified` column values """
    now = clock.CLOCK.now()
    target.rdrCreated = now
    target.modified = now


event.listen(BiobankStoredSample, 'before_insert', stored_sample_insert_listener)
event.listen(BiobankStoredSample, "before_update", model_update_listener)
