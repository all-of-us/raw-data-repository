import clock

from model.base import Base
from sqlalchemy import Column, Integer, BLOB, Boolean, DateTime, Date, String, ForeignKey

class MetricsVersion(Base):
  """A resource representing a batch of metrics generated from our pipeline."""
  __tablename__ = 'metrics_version'
  metricsVersionId = Column('metrics_version_id', Integer, primary_key=True)
  inProgress = Column('in_progress', Boolean, default=False, nullable=False)
  complete = Column('complete', Boolean, default=False, nullable=False)
  date = Column('date', DateTime, default=clock.CLOCK.now(), nullable=False)
  dataVersion = Column('data_version', Integer, nullable=False)

class MetricsBucket(Base):
  __tablename__ = 'metrics_bucket'
  metricsVersionId = Column('metrics_version_id', Integer, 
                            ForeignKey('metrics_version.metrics_version_id'), primary_key=True)
  date = Column('date', Date, primary_key=True)
  hpoId = Column('hpo_id', String, primary_key=True) # Set to '' for cross-HPO metrics
  metrics = Column('metrics', BLOB, nullable=False)
