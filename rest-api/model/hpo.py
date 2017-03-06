from model.base import Base
from sqlalchemy import Column, Integer, String, UniqueConstraint

class HPO(Base):
  __tablename__ = 'hpo'
  hpoId = Column('hpo_id', Integer, primary_key=True, autoincrement=False)
  name = Column(String(20))
  __table_args__ = (
    UniqueConstraint('name'),
  )
