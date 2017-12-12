from dao.base_dao import BaseDao
from model.metric_set import AggregateMetrics, MetricSet
from dao import database_factory


class MetricSetDao(BaseDao):

  def __init__(self):
    super(MetricSetDao, self).__init__(MetricSet,
                                       db=database_factory.get_metrics_database())


class AggregateMetricsDao(BaseDao):

  def __init__(self):
    super(AggregateMetricsDao, self).__init__(
        AggregateMetrics, db=database_factory.get_metrics_database())
