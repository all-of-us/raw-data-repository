
from sqlalchemy import event, Column, ForeignKey, UniqueConstraint, BigInteger, String, Index
from sqlalchemy.dialects.mysql import JSON

from rdr_service.model.base import Base, model_insert_listener, model_update_listener
from rdr_service.model.utils import UTCDateTime6


class ResourceSchema(Base):
    """
    Resource Schema
    """
    __tablename__ = "resource_schema"

    # Primary Key
    id = Column("id", BigInteger, primary_key=True, autoincrement=True, nullable=False)
    # have mysql set the creation data for each new order
    created = Column("created", UTCDateTime6, nullable=True)
    # have mysql always update the modified data when the record is changed
    modified = Column("modified", UTCDateTime6, nullable=True)
    resourceTypeID = Column("resource_type_id", ForeignKey("resource_type.id"), nullable=False)
    schema = Column("schema", JSON, nullable=False)
    schemaHash = Column("schema_hash", String(64), default='', nullable=False)

    __table_args__ = (
        UniqueConstraint("resource_type_id", "modified"),
    )

Index("ix_res_type_schema_hash", ResourceSchema.resourceTypeID, ResourceSchema.schemaHash)

event.listen(ResourceSchema, "before_insert", model_insert_listener)
event.listen(ResourceSchema, "before_update", model_update_listener)
