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

  def create_metric_set(self, ms_id):
    ms = MetricSet(
        metricSetId=ms_id,
        metricSetType=MetricSetType.PUBLIC_PARTICIPANT_AGGREGATIONS,
        lastModified=datetime.datetime(2017, 1, 1)
    )
    self.metric_set_dao.insert(ms)
    return ms

  def test_get(self):
    self.assertIsNone(self.metric_set_dao.get('123'))
    ms = self.create_metric_set('123')
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

  def test_get_all_for_metric_set(self):
    ms1 = self.create_metric_set('123')
    ms2 = self.create_metric_set('456')

    agg1 = AggregateMetrics(
        metricSetId=ms1.metricSetId,
        metricsKey=MetricsKey.GENDER,
        value="female",
        count=123)
    agg2 = AggregateMetrics(
        metricSetId=ms2.metricSetId,
        metricsKey=MetricsKey.GENDER,
        value="male",
        count=987)
    self.aggregate_metrics_dao.insert(agg1)
    self.aggregate_metrics_dao.insert(agg2)

    aggs = self.aggregate_metrics_dao.get_all_for_metric_set(ms1.metricSetId)
    self.assertEquals([agg1.asdict()], [a.asdict() for a in aggs])

    aggs = self.aggregate_metrics_dao.get_all_for_metric_set(ms2.metricSetId)
    self.assertEquals([agg2.asdict()], [a.asdict() for a in aggs])

  def test_delete_all_for_metric_set(self):
    ms1 = self.create_metric_set('123')
    ms2 = self.create_metric_set('456')

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
