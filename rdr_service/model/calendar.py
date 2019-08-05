from model.base import Base
from sqlalchemy import Date, Column, Index

class Calendar(Base):

  __tablename__ = 'calendar'
  day = Column('day', Date, primary_key=True)


Index('calendar_idx', Calendar.day)
