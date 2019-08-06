import datetime

from rdr_service.dao.metric_set_dao import AggregateMetricsDao, MetricSetDao
from rdr_service.model.metric_set import AggregateMetrics, MetricSet
from rdr_service.participant_enums import MetricSetType, MetricsKey
from rdr_service.test.unit_test.unit_test_util import SqlTestBase


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
    self.assertEqual(ms.asdict(), self.metric_set_dao.get(ms.metricSetId).asdict())

    agg = AggregateMetrics(
        metricSetId=ms.metricSetId,
        metricsKey=MetricsKey.GENDER,
        value="female",
        count=123)
    agg_key = (agg.metricSetId, agg.metricsKey, agg.value)
    self.assertIsNone(self.aggregate_metrics_dao.get(agg_key))
    self.aggregate_metrics_dao.insert(agg)
    self.assertEqual(agg.asdict(), self.aggregate_metrics_dao.get(agg_key).asdict())

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
    self.assertEqual([agg1.asdict()], [a.asdict() for a in aggs])

    aggs = self.aggregate_metrics_dao.get_all_for_metric_set(ms2.metricSetId)
    self.assertEqual([agg2.asdict()], [a.asdict() for a in aggs])

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

    self.assertEqual(2, len(self.aggregate_metrics_dao.get_all()))
    self.aggregate_metrics_dao.delete_all_for_metric_set(ms1.metricSetId)
    aggs = self.aggregate_metrics_dao.get_all()
    self.assertEqual(1, len(aggs))
    self.assertEqual(ms2.metricSetId, aggs[0].metricSetId)

    self.aggregate_metrics_dao.delete_all_for_metric_set(ms1.metricSetId)
    aggs = self.aggregate_metrics_dao.get_all()
    self.assertEqual(1, len(aggs))
