from sqlalchemy import Column, ForeignKey, Integer, String
from sqlalchemy.orm import relationship

from rdr_service.model.base import MetricsBase
from rdr_service.model.utils import Enum, UTCDateTime
from rdr_service.participant_enums import MetricSetType, MetricsKey


class MetricSet(MetricsBase):
    """A version containing a set of metrics in the database, generated by a pipeline.

  Contains buckets with metrics grouped by HPO ID and date.
  """

    __tablename__ = "metric_set"
    metricSetId = Column("metric_set_id", String(50), primary_key=True)
    metricSetType = Column("metric_set_type", Enum(MetricSetType), nullable=False)
    lastModified = Column("last_modified", UTCDateTime, nullable=False)
    metrics = relationship("AggregateMetrics", cascade="all, delete-orphan", passive_deletes=True)


class AggregateMetrics(MetricsBase):
    """Aggregate metric value within a metric set."""

    __tablename__ = "aggregate_metrics"
    metricSetId = Column(
        "metric_set_id", String(50), ForeignKey("metric_set.metric_set_id", ondelete="CASCADE"), primary_key=True
    )
    metricsKey = Column("metrics_key", Enum(MetricsKey), primary_key=True)
    value = Column("value", String(50), primary_key=True)
    count = Column("count", Integer, nullable=False)
