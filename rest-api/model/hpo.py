from model.base import Base
from sqlalchemy import Column, Integer, String, UniqueConstraint

"""An HPO entity"""
class HPO(Base):
  __tablename__ = 'hpo'
  id = Column(Integer, primary_key=True)  
  name = Column(String(20))
  __table_args__ = (
    UniqueConstraint('name'),
  )
