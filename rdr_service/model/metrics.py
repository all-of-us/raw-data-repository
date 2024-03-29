from sqlalchemy import (
    Boolean,
    Column,
    Date,
    ForeignKey,
    Integer,
    String
)
from sqlalchemy import BLOB  # pylint: disable=unused-import
from sqlalchemy.orm import relationship

from rdr_service import clock
from rdr_service.model.base import Base
from rdr_service.model.utils import UTCDateTime
from rdr_service.model.field_types import BlobUTF8

BUCKETS = {"buckets": {}}


class MetricsVersion(Base):
    """A version containing a set of metrics in the database, generated by a pipeline.

  Contains buckets with metrics grouped by HPO ID and date.
  """

    __tablename__ = "metrics_version"
    metricsVersionId = Column("metrics_version_id", Integer, primary_key=True)
    inProgress = Column("in_progress", Boolean, default=False, nullable=False)
    complete = Column("complete", Boolean, default=False, nullable=False)
    date = Column("date", UTCDateTime, default=clock.CLOCK.now, nullable=False)
    dataVersion = Column("data_version", Integer, nullable=False)
    buckets = relationship("MetricsBucket", cascade="all, delete-orphan", passive_deletes=True)


class MetricsBucket(Base):
    """A bucket belonging to a MetricsVersion, containing metrics for a particular HPO ID and date.
  """

    __tablename__ = "metrics_bucket"
    __rdr_internal_table__ = True
    metricsVersionId = Column(
        "metrics_version_id",
        Integer,
        ForeignKey("metrics_version.metrics_version_id", ondelete="CASCADE"),
        primary_key=True,
    )
    date = Column("date", Date, primary_key=True)
    hpoId = Column("hpo_id", String(20), primary_key=True)  # Set to '' for cross-HPO metrics
    metrics = Column("metrics", BlobUTF8, nullable=False)
