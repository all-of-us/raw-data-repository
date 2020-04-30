
from sqlalchemy import event, Index, Column, String, Integer, ForeignKey, BigInteger
from sqlalchemy.dialects.mysql import JSON

from rdr_service.model.base import Base, model_insert_listener, model_update_listener
from rdr_service.model.utils import UTCDateTime6


class ResourceData(Base):
    """
    Resource Data Model
    """
    __tablename__ = "resource_data"

    # Primary Key
    id = Column("id", BigInteger, primary_key=True, autoincrement=True, nullable=False)
    # have mysql set the creation data for each new order
    created = Column("created", UTCDateTime6, nullable=True)
    # have mysql always update the modified data when the record is changed
    modified = Column("modified", UTCDateTime6, nullable=True)
    resourceTypeID = Column("resource_type_id", ForeignKey("resource_type.id"), nullable=False)
    resourceSchemaID = Column("resource_schema_id", ForeignKey("resource_schema.id"), nullable=False)
    hpoId = Column("hpo_id", Integer, nullable=True)
    resourcePK = Column("resource_pk", String(65), nullable=True)
    resourcePKID = Column("resource_pk_id", Integer, nullable=True)
    # Alternate Primary Key when the primary key is a string instead of an Integer.
    resource_PKAltID = Column("resource_pk_alt_id", String(80), nullable=True)
    # Points to parent record in ResourceData. To keep the number of indexes down, these are not setup as ForeignKeys.
    parentID = Column("parent_id", BigInteger, nullable=True)
    parentTypeID = Column("parent_type_id", BigInteger, nullable=True)
    resource = Column("resource", JSON, nullable=False)

    # TODO: Decide if this constraint is really enforceable.
    # __table_args__ = (
    #     UniqueConstraint("resource_type_id", "resource_pk_id", "resource_pk_alt_id"),
    # )

Index("ix_res_data_type_modified_hpo_id", ResourceData.resourceTypeID, ResourceData.modified, ResourceData.hpoId)

event.listen(ResourceData, "before_insert", model_insert_listener)
event.listen(ResourceData, "before_update", model_update_listener)
