import datetime

from clock import FakeClock
from model.metrics import MetricsVersion, MetricsBucket
from dao.metrics_dao import MetricsVersionDao, MetricsBucketDao, SERVING_METRICS_DATA_VERSION
from unit_test_util import SqlTestBase
from sqlalchemy.exc import IntegrityError
from werkzeug.exceptions import PreconditionFailed

TIME = datetime.datetime(2016, 1, 1, 10, 0)
TIME_2 = datetime.datetime(2016, 1, 2, 9, 59)
TIME_3 = datetime.datetime(2016, 1, 2, 10, 0)
TIME_4 = datetime.datetime(2016, 1, 4, 10, 0)
TIME_5 = datetime.datetime(2016, 1, 4, 10, 1)
PITT = 'PITT'

class MetricsDaoTest(SqlTestBase):

  def setUp(self):
    super(MetricsDaoTest, self).setUp()
    self.metrics_version_dao = MetricsVersionDao()
    self.metrics_bucket_dao = MetricsBucketDao()

  def test_get_before_insert(self):
    self.assertIsNone(self.metrics_version_dao.get(1))
    self.assertIsNone(self.metrics_version_dao.get_with_children(1))
    self.assertIsNone(self.metrics_bucket_dao.get([1, TIME, None]))
    self.assertIsNone(self.metrics_bucket_dao.get([1, TIME, PITT]))
    self.assertIsNone(self.metrics_version_dao.get_version_in_progress())
    self.assertIsNone(self.metrics_version_dao.get_serving_version())
    self.assertIsNone(self.metrics_bucket_dao.get_active_buckets())

  def test_set_pipeline_in_progress(self):
    with FakeClock(TIME):
      self.metrics_version_dao.set_pipeline_in_progress()
    expected_mv = MetricsVersion(metricsVersionId=1, inProgress=True, complete=False,
                                 date=TIME, dataVersion=SERVING_METRICS_DATA_VERSION)
    self.assertEquals(expected_mv.asdict(), self.metrics_version_dao.get(1).asdict())
    self.assertEquals(expected_mv.asdict(),
                      self.metrics_version_dao.get_version_in_progress().asdict())
    self.assertIsNone(self.metrics_version_dao.get_serving_version())
    self.assertIsNone(self.metrics_bucket_dao.get_active_buckets())

  def test_set_pipeline_in_progress_while_in_progress(self):
    with FakeClock(TIME):
      self.metrics_version_dao.set_pipeline_in_progress()

    with FakeClock(TIME_2):
      with self.assertRaises(PreconditionFailed):
        # Until a day passes, setting the pipeline in progress will raise an error.
        self.metrics_version_dao.set_pipeline_in_progress()

    # After a day passes, break the lock.
    with FakeClock(TIME_3):
      self.metrics_version_dao.set_pipeline_in_progress()
    expected_mv = MetricsVersion(metricsVersionId=1, inProgress=False, complete=False,
                                 date=TIME, dataVersion=SERVING_METRICS_DATA_VERSION)
    self.assertEquals(expected_mv.asdict(), self.metrics_version_dao.get(1).asdict())
    expected_mv2 = MetricsVersion(metricsVersionId=2, inProgress=True, complete=False,
                                  date=TIME_3, dataVersion=SERVING_METRICS_DATA_VERSION)
    self.assertEquals(expected_mv2.asdict(), self.metrics_version_dao.get(2).asdict())

  def test_set_pipeline_finished_not_in_progress(self):
    self.metrics_version_dao.set_pipeline_finished(True)

  def test_set_pipeline_finished_in_progress_no_buckets(self):
    with FakeClock(TIME):
      self.metrics_version_dao.set_pipeline_in_progress()
    with FakeClock(TIME_2):
      self.metrics_version_dao.set_pipeline_finished(True)
    expected_mv = MetricsVersion(metricsVersionId=1, inProgress=False, complete=True,
                                 date=TIME, dataVersion=SERVING_METRICS_DATA_VERSION)
    self.assertEquals(expected_mv.asdict(), self.metrics_version_dao.get(1).asdict())
    self.assertEquals([], self.metrics_bucket_dao.get_active_buckets())

  def test_set_pipeline_finished_in_progress_with_buckets(self):
    with FakeClock(TIME):
      self.metrics_version_dao.set_pipeline_in_progress()
    metrics_bucket_1 = MetricsBucket(metricsVersionId=1, date=datetime.date.today(), hpoId='',
                                     metrics='foo')
    metrics_bucket_2 = MetricsBucket(metricsVersionId=1, date=datetime.date.today(), hpoId=PITT,
                                     metrics='bar')
    tomorrow = datetime.date.today() + datetime.timedelta(days=1)
    metrics_bucket_3 = MetricsBucket(metricsVersionId=1, date=tomorrow, hpoId='',
                                     metrics='baz')
    self.metrics_bucket_dao.insert(metrics_bucket_1)
    self.metrics_bucket_dao.insert(metrics_bucket_2)
    self.metrics_bucket_dao.insert(metrics_bucket_3)
    with FakeClock(TIME_2):
      self.metrics_version_dao.set_pipeline_finished(True)
    expected_mv = MetricsVersion(metricsVersionId=1, inProgress=False, complete=True,
                                 date=TIME, dataVersion=SERVING_METRICS_DATA_VERSION)
    self.assertEquals(expected_mv.asdict(),
                      self.metrics_version_dao.get_serving_version().asdict())
    active_buckets = self.metrics_bucket_dao.get_active_buckets()
    self.assertEquals(3, len(active_buckets))
    self.assertEquals(metrics_bucket_1.asdict(), active_buckets[0].asdict())
    self.assertEquals(metrics_bucket_2.asdict(), active_buckets[1].asdict())
    self.assertEquals(metrics_bucket_3.asdict(), active_buckets[2].asdict())

    # Filter on start date.
    active_buckets = self.metrics_bucket_dao.get_active_buckets(start_date=datetime.date.today())
    self.assertEquals(3, len(active_buckets))
    self.assertEquals(metrics_bucket_1.asdict(), active_buckets[0].asdict())
    self.assertEquals(metrics_bucket_2.asdict(), active_buckets[1].asdict())
    self.assertEquals(metrics_bucket_3.asdict(), active_buckets[2].asdict())

    active_buckets = self.metrics_bucket_dao.get_active_buckets(start_date=tomorrow)
    self.assertEquals(1, len(active_buckets))
    self.assertEquals(metrics_bucket_3.asdict(), active_buckets[0].asdict())

    # Filter on end date.
    active_buckets = self.metrics_bucket_dao.get_active_buckets(end_date=tomorrow)
    self.assertEquals(3, len(active_buckets))
    self.assertEquals(metrics_bucket_1.asdict(), active_buckets[0].asdict())
    self.assertEquals(metrics_bucket_2.asdict(), active_buckets[1].asdict())
    self.assertEquals(metrics_bucket_3.asdict(), active_buckets[2].asdict())

    active_buckets = self.metrics_bucket_dao.get_active_buckets(end_date=datetime.date.today())
    self.assertEquals(2, len(active_buckets))
    self.assertEquals(metrics_bucket_1.asdict(), active_buckets[0].asdict())
    self.assertEquals(metrics_bucket_2.asdict(), active_buckets[1].asdict())

  def test_insert_duplicate_bucket(self):
    with FakeClock(TIME):
      self.metrics_version_dao.set_pipeline_in_progress()
    metrics_bucket_1 = MetricsBucket(metricsVersionId=1, date=datetime.date.today(), hpoId=PITT,
                                     metrics='foo')
    metrics_bucket_2 = MetricsBucket(metricsVersionId=1, date=datetime.date.today(), hpoId=PITT,
                                     metrics='bar')
    self.metrics_bucket_dao.insert(metrics_bucket_1)
    with self.assertRaises(IntegrityError):
      self.metrics_bucket_dao.insert(metrics_bucket_2)

    # Upsert should work, and replace the bucket.
    self.metrics_bucket_dao.upsert(metrics_bucket_2)
    self.assertEquals(metrics_bucket_2.asdict(),
                      self.metrics_bucket_dao.get([1, datetime.date.today(), PITT]).asdict())

  def test_delete_old_metrics(self):
    with FakeClock(TIME):
      self.metrics_version_dao.set_pipeline_in_progress()
    metrics_bucket_1 = MetricsBucket(metricsVersionId=1, date=datetime.date.today(), hpoId='',
                                     metrics='foo')
    metrics_bucket_2 = MetricsBucket(metricsVersionId=1, date=datetime.date.today(), hpoId=PITT,
                                     metrics='bar')
    self.metrics_bucket_dao.insert(metrics_bucket_1)
    self.metrics_bucket_dao.insert(metrics_bucket_2)

    # For up to 3 days, the metrics stay around.
    with FakeClock(TIME_4):
      self.metrics_version_dao.delete_old_versions()
      expected_mv = MetricsVersion(metricsVersionId=1, inProgress=True, complete=False,
                                   date=TIME, dataVersion=SERVING_METRICS_DATA_VERSION)
      expected_mv.buckets.append(metrics_bucket_1)
      expected_mv.buckets.append(metrics_bucket_2)
      self.assertEquals(expected_mv.asdict(follow=['buckets']),
                        self.metrics_version_dao.get_with_children(1).asdict(follow=['buckets']))

     # After 3 days, the metrics are gone.
    with FakeClock(TIME_5):
      self.metrics_version_dao.delete_old_versions()
      self.assertIsNone(self.metrics_version_dao.get_with_children(1))
