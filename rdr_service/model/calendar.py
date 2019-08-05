from sqlalchemy import Column, Date, Index

from rdr_service.model.base import Base


class Calendar(Base):

  __tablename__ = 'calendar'
  day = Column('day', Date, primary_key=True)


Index('calendar_idx', Calendar.day)
