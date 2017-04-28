from participant_enums import OrganizationType
from model.base import Base
from model.utils import Enum
from sqlalchemy import Column, Integer, String, UniqueConstraint

class HPO(Base):
  __tablename__ = 'hpo'
  hpoId = Column('hpo_id', Integer, primary_key=True, autoincrement=False)
  name = Column('name', String(20))
  displayName = Column('display_name', String(255))
  organizationType = Column('organization_type', Enum(OrganizationType),
                            default=OrganizationType.UNSET)
  __table_args__ = (
    UniqueConstraint('name'),
  )
