import datetime

from dao.metric_set_dao import MetricSetDao, AggregateMetricsDao
from model.metric_set import MetricSet, AggregateMetrics
from participant_enums import MetricSetType, MetricsKey
from test.unit_test.unit_test_util import FlaskTestBase
from parameterized import param, parameterized


class MetricSetsApiTest(FlaskTestBase):

  def setUp(self):
    super(MetricSetsApiTest, self).setUp()
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

  def test_get_metric_sets_no_data(self):
    response = self.send_get('MetricSets')
    self.assertEquals({'metricSets': []}, response)

  def test_get_metric_sets(self):
    self.create_metric_set('live1')
    self.create_metric_set('live2')
    response = self.send_get('MetricSets')
    self.assertItemsEqual(['live1', 'live2'], [ms['id'] for ms in response['metricSets']])

  @parameterized.expand([
      param('empty', ms_id='empty', want={}),
      param('all', want={
        'GENDER': [
            {'value': 'female', 'count': 123},
            {'value': 'male', 'count': 789}
        ],
        'STATE': [
            {'value': 'NJ', 'count': 789},
            {'value': 'CA', 'count': 123}
        ]
      }),
      param('proper key subset', keys=['STATE'], want={
        'STATE': [
            {'value': 'NJ', 'count': 789},
            {'value': 'CA', 'count': 123}
        ]
      }),
      param('overlapping key subset', keys=['STATE', 'AGE_RANGE'], want={
        'STATE': [
            {'value': 'NJ', 'count': 789},
            {'value': 'CA', 'count': 123}
        ]
      }),
      param('non-matching subset', keys=['AGE_RANGE'], want={}),
  ])
  def test_get_metrics(self, _, ms_id='live', keys=None, want=None):
    self.create_metric_set('empty')
    self.create_metric_set('live')
    self.aggregate_metrics_dao.insert(AggregateMetrics(
        metricSetId='live',
        metricsKey=MetricsKey.GENDER,
        value="female",
        count=123)
    )
    self.aggregate_metrics_dao.insert(AggregateMetrics(
        metricSetId='live',
        metricsKey=MetricsKey.GENDER,
        value="male",
        count=789)
    )
    self.aggregate_metrics_dao.insert(AggregateMetrics(
        metricSetId='live',
        metricsKey=MetricsKey.STATE,
        value="NJ",
        count=789)
    )
    self.aggregate_metrics_dao.insert(AggregateMetrics(
        metricSetId='live',
        metricsKey=MetricsKey.STATE,
        value="CA",
        count=123)
    )
    q = {'keys': keys} if keys else None
    got = self.send_get('MetricSets/{}/Metrics'.format(ms_id), query_string=q)['metrics']
    self.assertEquals(len(want), len(got), 'got unexpected number of metrics:'
                      '\nwant:  {}\ngot: {}'.format(want, got))
    for m in got:
      self.assertIn(m['key'], want)
      self.assertItemsEqual(m['values'], want[m['key']])

  def test_get_metrics_bad_keys(self):
    self.create_metric_set('live')
    self.send_get('MetricSets/live/Metrics', query_string={
      'keys': 'mugman'
    }, expected_status=400)

  def test_get_metrics_nonexistent(self):
    self.send_get('MetricSets/unknown/Metrics', expected_status=404)
