from model.base import Base
from sqlalchemy import Column, Integer, String, UniqueConstraint

"""An ID for an HPO"""
class HPOId(Base):
  __tablename__ = 'hpo_id'
  id = Column(Integer, primary_key=True)
  name = Column(String(20))
  __table_args__ = (
    UniqueConstraint('name'),
  )
