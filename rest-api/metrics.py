"""Implementation of the metrics API"""

import collections
import datetime
import participant
import pickle

from dateutil.relativedelta import relativedelta
from google.appengine.ext import ndb
from protorpc import message_types
from protorpc import messages

class InvalidMetricException(BaseException):
  """Exception thrown when a invalid metric is specified."""

class PipelineAlreadyRunningException(BaseException):
  """Exception thrown when starting pipeline if it is already running."""

START_DATE = datetime.date(2016, 9, 1)

class MetricsBucket(ndb.Model):
  date = ndb.DateProperty()
  metrics = ndb.PickleProperty()

class MetricsVersion(ndb.Model):
  in_progress = ndb.BooleanProperty()
  complete = ndb.BooleanProperty()
  date = ndb.DateTimeProperty(auto_now=True)

class Metrics(messages.Enum):
  """Predefined metric types"""
  NONE = 0
  PARTICIPANT_TOTAL = 1
  MEMBERSHIP_TIER = 2

class BucketBy(messages.Enum):
  """How to bucket the results"""
  NONE = 0
  DAY = 1
  WEEK = 2
  MONTH = 3

class MetricsResponseBucket(messages.Message):
  date = message_types.DateTimeField(1)
  name = messages.StringField(2)
  value = messages.FloatField(3)

class MetricsResponse(messages.Message):
  bucket = messages.MessageField(MetricsResponseBucket, 1, repeated=True)

class MetricsRequest(messages.Message):
  metric = messages.EnumField(Metrics, 1, default='NONE')
  bucket_by = messages.EnumField(BucketBy, 2, default='NONE')

_metric_map = {
    Metrics.PARTICIPANT_TOTAL: {
        'model': participant.Participant,
    },
    Metrics.MEMBERSHIP_TIER: {
        'model': participant.Participant,
        'column': participant.Participant.membership_tier,
        'column_name': 'membership_tier',
        'enum': participant.MembershipTier,
    },
}

class MetricService(object):

  def get_metrics(self, request):
    if request.metric not in _metric_map:
      raise InvalidMetricException(
          '{} is not a valid metric.'.format(request.metric))

    metric_config = _metric_map[request.metric]
    model = metric_config['model']
    column = metric_config['column'] if 'column' in metric_config else None
    enum = metric_config['enum'] if 'enum' in metric_config else None

    buckets = _make_buckets(request.bucket_by)

    for db_bucket in MetricsBucket.query(parent=get_active_version()).fetch():
      for bucket in buckets:
        if db_bucket.date >= bucket.start and db_bucket.date < bucket.end:
          counts = pickle.loads(db_bucket.metrics)
          for metric, count in counts.iteritems():
            if metric == metric_config['column_name']:
              bucket.cnt += count

    response = MetricsResponse()
    for bucket in buckets:
      resp_bucket = MetricsResponseBucket()
      start_midnight = datetime.datetime.combine(bucket.start,
                                                 datetime.datetime.min.time())
      resp_bucket.date = start_midnight
      resp_bucket.name = str(request.metric)
      resp_bucket.value = float(bucket.cnt[request.metric])
      response.bucket.append(resp_bucket)

    return response

def set_pipeline_in_progress():
  # Ensure that no pipeline is currently running.
  if get_in_progress_version():
    raise PipelineAlreadyRunningException()

  new_version = MetricsVersion(in_progress=True)
  new_version.put()
  return new_version

def get_in_progress_version():
  running = MetricsVersion.query(MetricsVersion.in_progress==True).fetch()
  if running:
    return running[0]
  return None

def get_serving_version():
  query = MetricsVersion.query(completed=True, limit=1)
  running = query.order(MetricsVersion.date).fetch()
  if running:
    return running[0].key
  return None

def start_metrics_pipeline():
  pipeline.start()

def _convert_name(result, enum):
  if result is None:
    return 'NULL'
  else:
    return str(enum(result))


def _make_buckets(bucket_by):
  increments = {
      BucketBy.DAY: relativedelta(days=1),
      BucketBy.WEEK: relativedelta(days=7),
      BucketBy.MONTH: relativedelta(months=1),
  }

  buckets = []
  end_date = datetime.datetime.now().date()
  if bucket_by == BucketBy.NONE:
    buckets.append(Bucket(START_DATE, end_date))
  else:
    last_date = START_DATE
    increment = increments[bucket_by]
    date = last_date
    while date < end_date:
      date = date + increment
      buckets.append(Bucket(last_date, date))
      last_date = date

  return buckets

class Bucket(object):
  def __init__(self, start, end):
    self.start = start
    self.end = end
    self.cnt = collections.Counter()

SERVICE = MetricService()
