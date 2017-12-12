from dao.base_dao import BaseDao
from model.metric_set import AggregateMetrics, MetricSet


class MetricSetDao(BaseDao):

  def __init__(self):
    super(MetricSetDao, self).__init__(MetricSet)


class AggregateMetricsDao(BaseDao):

  def __init__(self):
    super(AggregateMetricsDao, self).__init__(AggregateMetrics)
