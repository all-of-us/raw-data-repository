"""Implementation of the metrics API"""

import collections
import copy
import datetime
import json
import participant

import api_util

from dateutil.relativedelta import relativedelta
from google.appengine.ext import ndb
from protorpc import message_types
from protorpc import messages
from protorpc import protojson

class InvalidMetricException(BaseException):
  """Exception thrown when a invalid metric is specified."""

class PipelineAlreadyRunningException(BaseException):
  """Exception thrown when starting pipeline if it is already running."""

START_DATE = datetime.date(2016, 9, 1)

DATE_FORMAT = '%Y-%m-%d'

class MetricsBucket(ndb.Model):
  date = ndb.DateProperty() # Used on metrics where we are tracking history.
  facets = ndb.StringProperty()
  metrics = ndb.JsonProperty()

class MetricsVersion(ndb.Model):
  in_progress = ndb.BooleanProperty()
  complete = ndb.BooleanProperty()
  date = ndb.DateTimeProperty(auto_now=True)

class FacetType(messages.Enum):
  """The Facets (dimensions) to bucket by"""
  NONE = 0
  DATE = 1
  HPO_ID = 2

class Facet(messages.Message):
  """Used in the response to describe what the aggregated value represents."""
  facet_type = messages.EnumField(FacetType, 1, repeated=False)
  value = messages.StringField(2)

class MetricsResponseBucketEntry(messages.Message):
  name = messages.StringField(1)
  value = messages.FloatField(2)

class MetricsResponseBucket(messages.Message):
  date = message_types.DateTimeField(1)
  facets = messages.MessageField(Facet, 2, repeated=True)
  entries = messages.MessageField(MetricsResponseBucketEntry, 3, repeated=True)

class MetricsResponse(messages.Message):
  bucket = messages.MessageField(MetricsResponseBucket, 1, repeated=True)

class MetricsRequest(messages.Message):
  facets = messages.EnumField(FacetType, 1, repeated=True)

class ResultsBuckets(object):
  """Collects all the results buckets for a request"""
  def __init__(self):
    self.buckets_by_facet_key = {}

  def find_or_create(self, facet_types, db_bucket):
    facets = []
    for facet_type in facet_types:
      for db_facet in json.loads(db_bucket.facets):
        if str(facet_type) == db_facet['type']:
          facets.append(Facet(facet_type=facet_type, value=db_facet['value']))

    json_key_parts = [protojson.encode_message(part) for part in facets]
    facet_key = json.dumps(json_key_parts)

    if facet_key not in self.buckets_by_facet_key:
      self.buckets_by_facet_key[facet_key] = ResultsBucket(facets)

    return self.buckets_by_facet_key[facet_key]

  def list(self):
    return self.buckets_by_facet_key.values()


class ResultsBucket(object):
  """Used to aggregate data for a given set of facets."""
  no_date = 'no_date'  # Used in place of a date for non date based metrics.

  def __init__(self, facets):
    self.facets = facets
    self.counts_by_date = {}

  def add_counts(self, date, counts):
    date = date or self.no_date
    if date:
      if date not in self.counts_by_date:
        self.counts_by_date[date] = collections.Counter()
      self.counts_by_date[date] += counts

  def aggregated_dates(self):
    """Goes through the deltas in date order and adds up the deltas."""
    sorted_by_date = sorted(self.counts_by_date.iteritems())
    aggregated = collections.Counter()
    aggregated_by_date = []
    for date, counter in sorted_by_date:
      aggregated += counter
      date = None if date == self.no_date else date
      aggregated_by_date.append((date, copy.deepcopy(aggregated)))
    return aggregated_by_date


class MetricService(object):

  def get_metrics(self, request):
    serving_version = get_serving_version()

    results_buckets = ResultsBuckets()
    for db_bucket in MetricsBucket.query(ancestor=serving_version).fetch():
      results_bucket = results_buckets.find_or_create(request.facets, db_bucket)
      if results_bucket is not None:
        counts = collections.Counter(json.loads(db_bucket.metrics))
        results_bucket.add_counts(db_bucket.date, counts)

    response = MetricsResponse()
    for bucket in results_buckets.list():
      for date, counts in bucket.aggregated_dates():
        resp_bucket = MetricsResponseBucket(facets=bucket.facets)
        response.bucket.append(resp_bucket)

        if date:
          resp_bucket.date = datetime.datetime.combine(date, datetime.datetime.min.time())

        for k, val in counts.iteritems():
          entry = MetricsResponseBucketEntry()
          entry.name = str(k)
          entry.value = float(val)
          resp_bucket.entries.append(entry)

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
  query = MetricsVersion.query(MetricsVersion.complete==True)
  running = query.order(-MetricsVersion.date).fetch(1)
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

SERVICE = MetricService()
