from model.base import Base
from sqlalchemy import Column, Integer, String, ForeignKey
from sqlalchemy.orm import relationship

class Organization(Base):
  """An organization, under an awardee/HPO, and containing sites."""
  __tablename__ = 'organization'
  organizationId = Column('organization_id', Integer, primary_key=True)
  name = Column('name', String(80), nullable=False)
  displayName = Column('display_name', String(255), nullable=False)
  hpoId = Column('hpo_id', Integer, ForeignKey('hpo.hpo_id'), nullable=False)
  sites = relationship('Site', cascade='all, delete-orphan')
