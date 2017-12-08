from participant_enums import OrganizationType
from model.base import Base
from model.utils import Enum
from sqlalchemy import Column, Integer, String, UniqueConstraint
from sqlalchemy.orm import relationship

class HPO(Base):
  """An awardee, containing organizations (which in turn contain sites.)"""
  __tablename__ = 'hpo'
  hpoId = Column('hpo_id', Integer, primary_key=True, autoincrement=False)
  name = Column('name', String(20))
  displayName = Column('display_name', String(255))
  organizationType = Column('organization_type', Enum(OrganizationType),
                            default=OrganizationType.UNSET)
  organizations = relationship('Organization', cascade='all, delete-orphan',
                               order_by='Organization.externalId')
  __table_args__ = (
    UniqueConstraint('name'),
  )
