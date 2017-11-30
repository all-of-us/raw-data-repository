from model.base import Base
from sqlalchemy import Column, Integer, String, ForeignKey
from sqlalchemy.orm import relationship

class Organization(Base):
  """An organization, under an awardee/HPO, and containing sites."""
  __tablename__ = 'organization'
  # Database ID for the organization
  organizationId = Column('organization_id', Integer, primary_key=True)
  # External ID for the organization, e.g. WISC_MADISON
  externalId = Column('external_id', String(80), nullable=False)
  # Human readable display name for the organization, e.g. University of Wisconsin, Madison
  displayName = Column('display_name', String(255), nullable=False)
  # Foreign key to awardee/hpo this organization belongs to.
  hpoId = Column('hpo_id', Integer, ForeignKey('hpo.hpo_id'), nullable=False)
  # Sites belonging to this organization.
  sites = relationship('Site', cascade='all, delete-orphan')
