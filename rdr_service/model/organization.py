from sqlalchemy import Column, ForeignKey, Integer, String
from sqlalchemy.orm import relationship

from rdr_service.model.base import Base
from rdr_service.model.site_enums import ObsoleteStatus
from rdr_service.model.utils import Enum


class Organization(Base):
    """An organization, under an awardee/HPO, and containing sites."""

    __tablename__ = "organization"
    # Database ID for the organization
    organizationId = Column("organization_id", Integer, primary_key=True)
    # External ID for the organization, e.g. WISC_MADISON
    externalId = Column("external_id", String(80), nullable=False)
    """
    Vibrent's internal ID for organizations
    @rdr_dictionary_show_unique_values
    """
    displayName = Column("display_name", String(255), nullable=False)
    """Human readable display name for the organization, e.g. University of Wisconsin, Madison"""
    hpoId = Column("hpo_id", Integer, ForeignKey("hpo.hpo_id"), nullable=False)
    """Foreign key to awardee/hpo this organization belongs to"""
    # Sites belonging to this organization.
    sites = relationship("Site", cascade="all, delete-orphan", order_by="Site.googleGroup")
    isObsolete = Column("is_obsolete", Enum(ObsoleteStatus))
    """Whether or not the organization has been inactivated (1 if obsolete)"""
    resourceId = Column('resource_id', String(255))
