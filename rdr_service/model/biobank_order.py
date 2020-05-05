from sqlalchemy import Boolean, Column, ForeignKey, Integer, String, UnicodeText, event
from sqlalchemy.dialects.mysql import JSON
from sqlalchemy.ext.declarative import declared_attr
from sqlalchemy.orm import relationship
from rdr_service.model.field_types import BlobUTF8
from rdr_service.model.base import Base, model_insert_listener, model_update_listener
from rdr_service.model.utils import Enum, UTCDateTime, UTCDateTime6
from rdr_service.participant_enums import BiobankOrderStatus


class BiobankOrderBase(object):
    """An order requesting samples.

  The order contains a list of samples stored in BiobankOrderedSample; the actual delivered and
  stored samples are tracked in BiobankStoredSample. Our reconciliation report compares the two.
  """

    _MAIN_ID_SYSTEM = "https://orders.mayomedicallaboratories.com"

    # A GUID for the order, provided by Biobank. This is the ID assigned in HealthPro, which is sent
    # to us as an identifier with the mayomedicallaboritories.com "system".
    biobankOrderId = Column("biobank_order_id", String(80), primary_key=True)

    # Incrementing version, starts at 1 and is incremented on each update.
    version = Column("version", Integer, nullable=False)

    # The username / email of the HealthPro user that created the order -- createdInfo['author']
    # in the resulting JSON.
    sourceUsername = Column("source_username", String(255))

    # The username / email of the HealthPro user that collected the order -- collectedInfo['author']
    # in the resulting JSON.
    collectedUsername = Column("collected_username", String(255))

    # The username / email of the HealthPro user that processed the order -- processedInfo['author']
    # in the resulting JSON.
    processedUsername = Column("processed_username", String(255))

    # The username / email of the HealthPro user that finalized the order -- finalizedInfo['author']
    # in the resulting JSON.
    finalizedUsername = Column("finalized_username", String(255))
    finalizedTime = Column('finalized_time', UTCDateTime)

    # cancelled finalized order may still be shipped to biobank for destruction
    # orderstatus can be cancelled/amended/restored
    # A null value or UNSET == finalized (i.e. the current accepted value)

    orderStatus = Column("order_status", Enum(BiobankOrderStatus))
    # a cancelled or edited order must have a reason. Set on the old row because cancelled orders
    # don't create a new row like amended orders do.
    amendedReason = Column("amended_reason", UnicodeText)
    lastModified = Column("last_modified", UTCDateTime)

    restoredUsername = Column("restored_username", String(255))
    restoredTime = Column("restored_time", UTCDateTime)

    amendedUsername = Column("amended_username", String(255))
    amendedTime = Column("amended_time", UTCDateTime)

    cancelledUsername = Column("cancelled_username", String(255))
    cancelledTime = Column("cancelled_time", UTCDateTime)

    # Additional fields stored for future use.
    created = Column("created", UTCDateTime, nullable=False)
    collectedNote = Column("collected_note", UnicodeText)
    processedNote = Column("processed_note", UnicodeText)
    finalizedNote = Column("finalized_note", UnicodeText)

    orderOrigin = Column("order_origin", String(80))

    @declared_attr
    def participantId(cls):
        return Column("participant_id", Integer, ForeignKey("participant.participant_id"), nullable=False)

    @declared_attr
    def amendedBiobankOrderId(cls):
        return Column("amended_biobank_order_id", String(80), ForeignKey("biobank_order.biobank_order_id"))

    # For syncing new orders.
    @declared_attr
    def logPositionId(cls):
        return Column("log_position_id", Integer, ForeignKey("log_position.log_position_id"), nullable=False)

    # The site that created the order -- createdInfo['site'] in the resulting JSON
    @declared_attr
    def sourceSiteId(cls):
        return Column("source_site_id", Integer, ForeignKey("site.site_id"))

    # The site that collected the order -- collectedInfo['site'] in the resulting JSON
    @declared_attr
    def collectedSiteId(cls):
        return Column("collected_site_id", Integer, ForeignKey("site.site_id"))

    # The site that processed the order -- processedInfo['site'] in the resulting JSON
    @declared_attr
    def processedSiteId(cls):
        return Column("processed_site_id", Integer, ForeignKey("site.site_id"))

    # The site that finalized the order -- finalizedInfo['site'] in the resulting JSON
    @declared_attr
    def finalizedSiteId(cls):
        return Column("finalized_site_id", Integer, ForeignKey("site.site_id"))

    @declared_attr
    def restoredSiteId(cls):
        return Column("restored_site_id", Integer, ForeignKey("site.site_id"))

    @declared_attr
    def amendedSiteId(cls):
        return Column("amended_site_id", Integer, ForeignKey("site.site_id"))

    @declared_attr
    def cancelledSiteId(cls):
        return Column("cancelled_site_id", Integer, ForeignKey("site.site_id"))


class BiobankOrder(BiobankOrderBase, Base):
    __tablename__ = "biobank_order"
    logPosition = relationship("LogPosition")
    identifiers = relationship("BiobankOrderIdentifier", cascade="all, delete-orphan")
    samples = relationship("BiobankOrderedSample", cascade="all, delete-orphan")
    dvOrders = relationship("BiobankDVOrder", cascade="all, delete-orphan")
    genomicSetMember = relationship("GenomicSetMember", cascade="all, delete-orphan")


class BiobankOrderIdentifierBase(object):
    system = Column("system", String(80), primary_key=True)
    value = Column("value", String(80), primary_key=True)

    @declared_attr
    def biobankOrderId(cls):
        return Column("biobank_order_id", String(80), ForeignKey("biobank_order.biobank_order_id"), nullable=False)


class BiobankOrderIdentifier(BiobankOrderIdentifierBase, Base):
    """Arbitrary IDs for a BiobankOrder in other systems.

  Other clients may create these, but they must be unique within each system.
  """

    __tablename__ = "biobank_order_identifier"


class BiobankOrderedSampleBase(object):
    @declared_attr
    def biobankOrderId(cls):
        return Column("order_id", String(80), ForeignKey("biobank_order.biobank_order_id"), primary_key=True)

    # Unique within an order, though the same test may be redone in another order for the participant.
    test = Column("test", String(80), primary_key=True)

    # Free text description of the sample.
    description = Column("description", UnicodeText, nullable=False)
    processingRequired = Column("processing_required", Boolean, nullable=False)
    collected = Column("collected", UTCDateTime)
    processed = Column("processed", UTCDateTime)
    finalized = Column("finalized", UTCDateTime)


class BiobankOrderedSample(BiobankOrderedSampleBase, Base):
    """Samples listed by a Biobank order.

  These are distinct from BiobankStoredSamples, which tracks received samples. The two should
  eventually match up, but we see BiobankOrderedSamples first and track them separately.
  """

    __tablename__ = "biobank_ordered_sample"


class BiobankOrderHistory(BiobankOrderBase, Base):
    __tablename__ = "biobank_history"

    version = Column("version", Integer, primary_key=True)


class BiobankOrderedSampleHistory(BiobankOrderedSampleBase, Base):
    __tablename__ = "biobank_ordered_sample_history"

    version = Column("version", Integer, primary_key=True)


class BiobankOrderIdentifierHistory(BiobankOrderIdentifierBase, Base):
    __tablename__ = "biobank_order_identifier_history"

    version = Column("version", Integer, primary_key=True)


class MayolinkCreateOrderHistory(Base):
    __tablename__ = "mayolink_create_order_history"

    # Primary Key
    id = Column("id", Integer, primary_key=True, autoincrement=True, nullable=False)
    # have mysql set the creation data for each new order
    created = Column("created", UTCDateTime6, nullable=True)
    # have mysql always update the modified data when the record is changed
    modified = Column("modified", UTCDateTime6, nullable=True)
    requestParticipantId = Column("request_participant_id", Integer)
    requestTestCode = Column("request_test_code", String(500))
    requestOrderId = Column("response_order_id", String(80))
    requestOrderStatus = Column("response_order_status", String(80))
    requestPayload = Column("request_payload", BlobUTF8)
    responsePayload = Column("response_payload", BlobUTF8)


class BiobankSpecimenBase(object):
    id = Column("id", Integer, primary_key=True, autoincrement=True, nullable=False)
    created = Column("created", UTCDateTime6, nullable=True)
    modified = Column("modified", UTCDateTime6, nullable=True)


class SpecimenAliquotBase(object):
    sampleType = Column("sample_type", String(80))
    status = Column("status", String(100))
    disposalReason = Column("disposal_reason", String(80))
    disposalDate = Column("disposal_date", UTCDateTime)
    freezeThawCount = Column("freeze_thaw_count", Integer)
    location = Column("location", String(200))
    quantity = Column("quantity", String(80))
    quantityUnits = Column("quantity_units", String(80))
    processingCompleteDate = Column("processing_complete_date", UTCDateTime)
    deviations = Column('deviations', JSON)


class BiobankSpecimen(Base, BiobankSpecimenBase, SpecimenAliquotBase):
    __tablename__ = "biobank_specimen"

    rlimsId = Column("rlims_id", String(80), unique=True)
    biobankId = Column("biobank_id", Integer, ForeignKey("participant.biobank_id"), nullable=False)
    orderId = Column("order_id", String(80), ForeignKey("biobank_order.biobank_order_id"), primary_key=True)
    testCode = Column("test_code", String(80))
    repositoryId = Column("repository_id", String(80))
    studyId = Column("study_id", String(80))
    cohortId = Column("cohort_id", String(80))
    collectionDate = Column("collection_date", UTCDateTime)
    confirmedDate = Column("confirmed_date", UTCDateTime)


class BiobbankSpecimenAliquotBase(object):
    @declared_attr
    def specimen_id(cls):
        return Column("specimen_id", Integer, ForeignKey("biobank_specimen.id"))
    @declared_attr
    def specimen_rlims_id(cls):
        return Column("specimen_rlims_id", String(80), ForeignKey("biobank_specimen.rlims_id"))


class BiobankSpecimenAttribute(Base, BiobankSpecimenBase, BiobbankSpecimenAliquotBase):
    __tablename__ = "biobank_specimen_attribute"
    name = Column("name", String(80))
    value = Column("value", String(80))


class BiobankAliquot(Base, BiobankSpecimenBase, BiobbankSpecimenAliquotBase, SpecimenAliquotBase):
    __tablename__ = "biobank_aliquot"
    @declared_attr
    def specimen_id(cls):
        return Column("specimen_id", Integer, ForeignKey("biobank_specimen.id"))

    @declared_attr
    def specimen_rlims_id(cls):
        return Column("specimen_rlims_id", String(80), ForeignKey("biobank_specimen.order_id"))
    @declared_attr
    def parent_aliquot_id(cls):
        return Column("parent_aliquot_id", String(80))
    @declared_attr
    def parent_aliquot_rlims_id(cls):
        return Column("parent_aliquot_rlims_id", String(80))
    rlimsId = Column("rlims_id", String(80), unique=True)
    childPlanService = Column("child_plan_service", String(100))
    initialTreatment = Column("initial_treatment", String(100))
    containerTypeId = Column("container_type_id", String(100))


class BiobankAliquotDataset(Base, BiobankSpecimenBase):
    __tablename__ = "biobank_aliquot_dataset"
    @declared_attr
    def aliquot_id(cls):
        return Column("aliquot_id", Integer, ForeignKey("biobank_aliquot.id"))
    @declared_attr
    def aliquot_rlims_id(cls):
        return Column("aliquot_rlims_id", String(80), ForeignKey("biobank_aliquot.rlims_id"))
    rlimsId = Column("rlims_id", String(80), unique=True)
    name = Column("name", String(80))
    status = Column("status", String(80))


class BiobankAliquotDatasetItem(Base, BiobankSpecimenBase):
    __tablename__ = "biobank_aliquot_dataset_item"
    @declared_attr
    def dataset_id(cls):
        return Column("dataset_id", Integer, ForeignKey("biobank_aliquot_dataset.id"))
    @declared_attr
    def dataset_rlims_id(cls):
        return Column("dataset_rlims_id", String(80), ForeignKey("biobank_aliquot_dataset.rlims_id"))
    paramId = Column("param_id", String(80))
    displayValue = Column("display_value", String(80))
    displayUnits = Column("display_units", String(80))


event.listen(MayolinkCreateOrderHistory, "before_insert", model_insert_listener)
event.listen(MayolinkCreateOrderHistory, "before_update", model_update_listener)
