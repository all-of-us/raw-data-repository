from unit_test_util import SqlTestBase
from dao.metric_set_dao import MetricSetDao, AggregateMetricsDao
from model.metric_set import MetricSet, AggregateMetrics
from participant_enums import MetricSetType, MetricsKey


class MetricSetDaoTest(SqlTestBase):

  def setUp(self):
    super(MetricSetDaoTest, self).setUp()
    self.metric_set_dao = MetricSetDao()
    self.aggregate_metrics_dao = AggregateMetricsDao()

  def test_get(self):
    ms = MetricSet(
        metricSetId='123',
        metricSetType=MetricSetType.PUBLIC_PARTICIPANT_AGGREGATIONS)
    self.assertIsNone(self.metric_set_dao.get(ms.metricSetId))
    self.metric_set_dao.insert(ms)
    self.assertEquals(ms.asdict(), self.metric_set_dao.get(ms.metricSetId).asdict())

    agg = AggregateMetrics(
        metricSetId=ms.metricSetId,
        metricsKey=MetricsKey.GENDER,
        value="female",
        count=123)
    agg_key = (agg.metricSetId, agg.metricsKey, agg.value)
    self.assertIsNone(self.aggregate_metrics_dao.get(agg_key))
    self.aggregate_metrics_dao.insert(agg)
    self.assertEquals(agg.asdict(), self.aggregate_metrics_dao.get(agg_key).asdict())
