import datetime

from clock import FakeClock
from model.metrics import MetricsVersion, MetricsBucket
from dao.metrics_dao import MetricsVersionDao, MetricsBucketDao, SERVING_METRICS_DATA_VERSION
from unit_test_util import SqlTestBase
from sqlalchemy.exc import IntegrityError
from werkzeug.exceptions import BadRequest, PreconditionFailed

TIME = datetime.datetime(2016, 1, 1, 10, 0)
TIME_2 = datetime.datetime(2016, 1, 2, 9, 59)
TIME_3 = datetime.datetime(2016, 1, 2, 10, 0)
PITT = 'PITT'
BUCKETS = {'buckets': {}}

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
  
  def test_set_pipeline_in_progress(self):
    with FakeClock(TIME):
      self.metrics_version_dao.set_pipeline_in_progress()
    expected_mv = MetricsVersion(metricsVersionId=1, inProgress=True, complete=False,
                                 date=TIME, dataVersion=SERVING_METRICS_DATA_VERSION)
    self.assertEquals(expected_mv.asdict(), self.metrics_version_dao.get(1).asdict())
    self.assertEquals(expected_mv.asdict(follow=BUCKETS), 
                      self.metrics_version_dao.get_with_children(1).asdict(follow=BUCKETS))
    self.assertEquals(expected_mv.asdict(), 
                      self.metrics_version_dao.get_version_in_progress().asdict())
    self.assertIsNone(self.metrics_version_dao.get_serving_version())
  
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
    with self.assertRaises(PreconditionFailed):
      self.metrics_version_dao.set_pipeline_finished(True)
  
  def test_set_pipeline_finished_in_progress_no_buckets(self):
    with FakeClock(TIME):
      self.metrics_version_dao.set_pipeline_in_progress()    
    with FakeClock(TIME_2):    
      self.metrics_version_dao.set_pipeline_finished(True)
    expected_mv = MetricsVersion(metricsVersionId=1, inProgress=False, complete=True,
                                 date=TIME, dataVersion=SERVING_METRICS_DATA_VERSION)
    self.assertEquals(expected_mv.asdict(), self.metrics_version_dao.get(1).asdict())
    self.assertEquals(expected_mv.asdict(follow=BUCKETS), 
                      self.metrics_version_dao.get_serving_version().asdict(follow=BUCKETS))
  
  def test_set_pipeline_finished_in_progress_with_buckets(self):
    with FakeClock(TIME):
      self.metrics_version_dao.set_pipeline_in_progress()    
    metrics_bucket_1 = MetricsBucket(metricsVersionId=1, date=datetime.date.today(), hpoId='', 
                                     metrics='foo')
    metrics_bucket_2 = MetricsBucket(metricsVersionId=1, date=datetime.date.today(), hpoId=PITT, 
                                     metrics='bar')
    self.metrics_bucket_dao.insert(metrics_bucket_1)
    self.metrics_bucket_dao.insert(metrics_bucket_2)
    with FakeClock(TIME_2):    
      self.metrics_version_dao.set_pipeline_finished(True)
    expected_mv = MetricsVersion(metricsVersionId=1, inProgress=False, complete=True,
                                 date=TIME, dataVersion=SERVING_METRICS_DATA_VERSION)
    expected_mv.buckets.append(metrics_bucket_1)
    expected_mv.buckets.append(metrics_bucket_2)        
    self.assertEquals(expected_mv.asdict(follow=BUCKETS), 
                      self.metrics_version_dao.get_serving_version().asdict(follow=BUCKETS))
  
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
