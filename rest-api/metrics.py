"""Implementation of the metrics API"""

import collections
import copy
import datetime
import json
import logging

from offline.metrics_config import get_config

from google.appengine.ext import ndb
from protorpc import message_types
from protorpc import messages
from protorpc import protojson
from werkzeug.exceptions import NotFound

class InvalidMetricException(BaseException):
  """Exception thrown when a invalid metric is specified."""

class PipelineAlreadyRunningException(BaseException):
  """Exception thrown when starting pipeline if it is already running."""

START_DATE = datetime.date(2016, 9, 1)

DATE_FORMAT = '%Y-%m-%d'

# Only metrics that match this version will be served.  See the corresponding
# comment in metrics_pipeline.py.
SERVING_METRICS_DATA_VERSION = 1

class MetricsBucket(ndb.Model):
  date = ndb.DateProperty() # Used on metrics where we are tracking history.
  hpoId = ndb.StringProperty()
  metrics = ndb.JsonProperty()

class MetricsVersion(ndb.Model):
  in_progress = ndb.BooleanProperty()
  complete = ndb.BooleanProperty()
  date = ndb.DateTimeProperty(auto_now=True)
  data_version = ndb.IntegerProperty()

class FieldDefinition(messages.Message):
  """Defines a field and its values"""
  name = messages.StringField(1)
  values = messages.StringField(2, repeated=True)

# TODO: consider getting rid of these, breaking up field definitions into 
# separate request, and streaming JSON directly from results
class MetricsResponseBucketEntry(messages.Message):
  name = messages.StringField(1)
  value = messages.FloatField(2)

class MetricsResponseBucket(messages.Message):
  date = message_types.DateTimeField(1)
  hpoId = messages.StringField(2)
  entries = messages.MessageField(MetricsResponseBucketEntry, 3, repeated=True)

class MetricsResponse(messages.Message):
  field_definition = messages.MessageField(FieldDefinition, 1, repeated=True)
  bucket = messages.MessageField(MetricsResponseBucket, 2, repeated=True)

class MetricsRequest(messages.Message):
  start_date = message_types.DateTimeField(1)
  end_date = message_types.DateTimeField(2)

class MetricService(object):

  def get_metrics(self, request):
    serving_version = get_serving_version()
    if not serving_version:
      raise NotFound(
          'No Metrics with data version {} calculated yet.'.format(SERVING_METRICS_DATA_VERSION))
    results_buckets = ResultsBuckets()
    response = MetricsResponse()
    bucket_map = {}
    query = MetricsBucket.query(ancestor=serving_version).order(MetricsBucket.date)
    if request.start_date:
      start_date_val = datetime.datetime.strptime(request.start_date, DATE_FORMAT)
      query = query.filter(MetricsBucket.date >= start_date_val)
    if request.end_date:
      end_date_val = datetime.datetime.strptime(request.end_date, DATE_FORMAT) 
      query = query.filter(MetricsBucket.date <= end_date_val) 
    response = MetricsResponse()
    for db_bucket in query.fetch():
      resp_bucket = MetricsResponseBucket(hpoId=db_bucket.hpoId, date=db_bucket.date)
      response.bucket.append(resp_bucket)
      for k, val in json.loads(db_bucket.metrics):
          entry = MetricsResponseBucketEntry()
          entry.name = str(k)
          entry.value = float(val)
          resp_bucket.entries.append(entry)
    response.field_definition = [
        FieldDefinition(name=type_ + '.' + field.name, values=[str(r) for r in field.func_range])
        for type_, conf in get_config().iteritems()
        for field_list in conf['fields'].values()
        for field in field_list]
    return response

def set_pipeline_in_progress():
  # Ensure that no pipeline is currently running.
  if get_in_progress_version():
    raise PipelineAlreadyRunningException()

  new_version = MetricsVersion(in_progress=True)
  new_version.put()
  return new_version

def get_in_progress_version():
  running = MetricsVersion.query(MetricsVersion.in_progress == True).fetch()
  if running:
    return running[0]
  return None

def get_serving_version():
  query = MetricsVersion.query(MetricsVersion.complete == True,
                               MetricsVersion.data_version == SERVING_METRICS_DATA_VERSION)
  running = query.order(-MetricsVersion.date).fetch(1)
  if running:
    return running[0].key
  return None

def _convert_name(result, enum):
  if result is None:
    return 'NULL'
  else:
    return str(enum(result))

SERVICE = MetricService()
