import datetime

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
        metricSetType=MetricSetType.PUBLIC_PARTICIPANT_AGGREGATIONS,
        lastModified=datetime.datetime(2017, 1, 1)
    )
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

  def test_delete_all_for_metric_set(self):
    ms1 = MetricSet(
        metricSetId='123',
        metricSetType=MetricSetType.PUBLIC_PARTICIPANT_AGGREGATIONS,
        lastModified=datetime.datetime(2017, 1, 1)
    )
    ms2 = MetricSet(
        metricSetId='456',
        metricSetType=MetricSetType.PUBLIC_PARTICIPANT_AGGREGATIONS,
        lastModified=datetime.datetime(2017, 1, 1)
    )
    self.metric_set_dao.insert(ms1)
    self.metric_set_dao.insert(ms2)

    self.aggregate_metrics_dao.insert(AggregateMetrics(
        metricSetId=ms1.metricSetId,
        metricsKey=MetricsKey.GENDER,
        value="female",
        count=123)
    )
    self.aggregate_metrics_dao.insert(AggregateMetrics(
        metricSetId=ms2.metricSetId,
        metricsKey=MetricsKey.GENDER,
        value="male",
        count=987)
    )

    self.assertEquals(2, len(self.aggregate_metrics_dao.get_all()))
    self.aggregate_metrics_dao.delete_all_for_metric_set(ms1.metricSetId)
    aggs = self.aggregate_metrics_dao.get_all()
    self.assertEquals(1, len(aggs))
    self.assertEquals(ms2.metricSetId, aggs[0].metricSetId)

    self.aggregate_metrics_dao.delete_all_for_metric_set(ms1.metricSetId)
    aggs = self.aggregate_metrics_dao.get_all()
    self.assertEquals(1, len(aggs))
