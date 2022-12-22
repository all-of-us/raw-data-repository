from sqlalchemy import Column, String, Integer, BIGINT, JSON, DateTime, ForeignKey, sql
from sqlalchemy.orm import relationship


from rdr_service.model.base import Base


class StudyCategory(Base):
    """
    Will store the hierarchy of categories that can contain other categories or orders.
    """
    __tablename__ = "study_category"
    # Auto-incremented primary key for the table
    id = Column(Integer, primary_key=True, autoincrement=True)
    # Timestamp of when a category was created, useful for diagnostic purposes
    created = Column(DateTime, nullable=False)
    # Name of the category
    type_label = Column(String(128), nullable=False)
    # Foreign Key to this table(study_category) that defines what category
    # this one should be nested into another (such as with modules).
    parent_id = Column(Integer, ForeignKey("study_category.id"))
    # Relationship
    children = relationship("StudyCategory")


class Order(Base):
    """
    Stores metadata for an instance of an object
    """
    __tablename__ = "order"
    # Auto-incremented primary key for the table
    id = Column(BIGINT, primary_key=True, autoincrement=True)
    # NPH ID for the order
    nph_order_id = Column(String(64), nullable=False)
    # Timestamp of when the RDR stored this sample in db
    created = Column(DateTime, server_default=sql.func.utcnow())
    # Timestamp of when the most recent changes for this order, or any contained samples, occurred
    modified = Column(DateTime, onupdate=sql.func.utcnow())
    # Timestamp received on the API from the client partner of when the order was created
    order_created = Column(DateTime, nullable=False)
    # Foreign Key to the study_category table that specific which category this sample belongs to
    category_id = Column(Integer, ForeignKey("study_category.id"))
    # Foreign Key to the participant table.
    participant_id = Column(Integer, ForeignKey("participant.participant_id"))
    # Email address send to the API of the user that created the order
    created_author = Column(String(128), nullable=False)
    # Foreign Key to the NPH site table; Gives the ID for the site specific as the creation site for an order
    created_site = Column(Integer, ForeignKey("site.site_id"))
    # Email address sent to the API of the user that collected thr order
    collected_author = Column(String(128), nullable=False)
    # Foreign Key to the NPH site table; Gives the ID for the site specific as the collection site for an order
    collected_site = Column(Integer, ForeignKey("site.site_id"))
    # Email address send to the API of the user that finalized the order
    finalized_author = Column(String(128), nullable=False)
    # Foreign key to the NPH site table; Gives the id for the site specified as the finalization site for an order
    finalized_site = Column(Integer, ForeignKey("site.site_id"))
    # Email address sent to the API of the user that amended the order. Will be null if the order isn’t amended
    amended_author = Column(String(128), nullable=False)
    # Foreign key to the NPH site table. Gives the id for the site specified as the amendment site when an order
    # is amended. Will be null if the order isn’t amended
    amended_site = Column(Integer, ForeignKey("site.site_id"))
    # Amendment reason provided to the API when an order is amended. Will be null if the order isn’t amended
    amended_reason = Column(String(1024))
    # Notes provided to the API for the order
    notes = Column(JSON)
    # Status provided to the API for an order. Is used to indicate that the order as a whole has been
    # canceled or restored
    status = Column(String(128), nullable=False)
    # Relationship
    category = relationship("StudyCategory")
    participant = relationship("Participant")
    site = relationship("Site")


class OrderedSample(Base):
    """
    Will store the data for ordered samples and their aliquots.
    """
    __tablename__ = "ordered_sample"
    # Auto-incremented primary key for the table
    id = Column(BIGINT, primary_key=True, autoincrement=True)
    # NPH ID for this sample. Given by the identifier provided in the payload
    # that has the “http://nph.org/sample-id” system url.
    nph_sample_id = Column(String(64), nullable=False)
    # Foreign key to the order table. Specifies which order this sample is a part of
    order_id = Column(BIGINT, ForeignKey("order.id"))
    # Foreign key to this table (ordered_sample). Should be NULL for parent samples
    # (and in that case, the order_id would be populated). Should be populated for aliquots only.
    # Specifies which sample this aliquot is derived from
    parent_sample_id = Column(BIGINT, ForeignKey("ordered_sample.id"))
    # String representation of the test code for the sample. Should be NULL for aliquots,
    # since they will be the same test as their parent
    test = Column(String(40))
    # Description received on the API for the sample
    description = Column(String(256))
    # Collected datetime received on the API for the sample
    collected = Column(DateTime, nullable=False)
    # Finalized datetime received on the API for the sample
    finalized = Column(DateTime, nullable=False)
    # ID value received on the API for a sample
    aliquot_id = Column(String(128), nullable=False)
    # Container string value received for aliquots
    container = Column(String(128), nullable=False)
    # Volume string value received for aliquots
    volume = Column(String(128), nullable=False)
    # Status provided to the API for a sample or aliquot. Is used to indicate that specific aliquots
    # have been canceled or restored.
    status = Column(String(128), nullable=False)
    # Stores additional, required fields for some samples (bowel movement and urine). Set to NULL otherwise.
    supplemental_fields = Column(JSON)
    # Relationship
    order = relationship("Order")
    sample = relationship("OrderedSample")
