import clock

from model.base import Base
from model.utils import UTCDateTime
from sqlalchemy import Column, Integer, Date, String

class MetricsEnrollmentStatusCache(Base):
  """Contains enrollment status metrics data grouped by HPO ID and date.
  """
  __tablename__ = 'metrics_enrollment_status_cache'
  dateInserted = Column('date_inserted', UTCDateTime, default=clock.CLOCK.now,
                        nullable=False, primary_key=True)
  hpoId = Column('hpo_id', String(20), primary_key=True)
  hpoName = Column('hpo_name', String(255), primary_key=True)
  date = Column('date', Date, nullable=False, primary_key=True)
  registeredCount = Column('registered_count', Integer, nullable=False)
  consentedCount = Column('consented_count', Integer, nullable=False)
  coreCount = Column('core_count', Integer, nullable=False)

class MetricsRaceCache(Base):
  """Contains race metrics data grouped by HPO ID and date.
  """
  __tablename__ = 'metrics_race_cache'
  dateInserted = Column('date_inserted', UTCDateTime, default=clock.CLOCK.now,
                        nullable=False, primary_key=True)
  hpoId = Column('hpo_id', String(20), primary_key=True)
  hpoName = Column('hpo_name', String(255), primary_key=True)
  date = Column('date', Date, nullable=False, primary_key=True)
  raceName = Column('race_name', String(255), primary_key=True)
  raceCount = Column('race_count', Integer, nullable=False)

class MetricsGenderCache(Base):
  """Contains gender metrics data grouped by HPO ID and date.
  """
  __tablename__ = 'metrics_gender_cache'
  dateInserted = Column('date_inserted', UTCDateTime, default=clock.CLOCK.now,
                        nullable=False, primary_key=True)
  hpoId = Column('hpo_id', String(20), primary_key=True)
  hpoName = Column('hpo_name', String(255), primary_key=True)
  date = Column('date', Date, nullable=False, primary_key=True)
  genderName = Column('gender_name', String(255), primary_key=True)
  genderCount = Column('gender_count', Integer, nullable=False)

class MetricsAgeCache(Base):
  """Contains age range metrics data grouped by HPO ID and date.
  """
  __tablename__ = 'metrics_age_cache'
  dateInserted = Column('date_inserted', UTCDateTime, default=clock.CLOCK.now,
                        nullable=False, primary_key=True)
  hpoId = Column('hpo_id', String(20), primary_key=True)
  hpoName = Column('hpo_name', String(255), primary_key=True)
  date = Column('date', Date, nullable=False, primary_key=True)
  ageRange = Column('age_range', String(255), primary_key=True)
  ageCount = Column('age_count', Integer, nullable=False)
