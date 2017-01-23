"""Implementation of the metrics API"""

import datetime
import json

from offline.metrics_config import get_fields

from google.appengine.ext import ndb
from protorpc import messages

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
  hpoId = ndb.StringProperty() # Set to '' for cross-HPO metrics
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

class MetricsRequest(messages.Message):
  start_date = messages.StringField(1)
  end_date = messages.StringField(2)

class MetricService(object):

  def get_metrics_fields(self):
    """Returns a list of fields that can be returned in buckets in the result of get_metrics()."""
    return get_fields()

  def get_metrics(self, request, serving_version):
    """Returns a list of JSON buckets that look like:
        {"facets": {"date": <date> [, "hpoId": <hpo ID> ] }, 
         "entries": [{ "Participant": <# of participants>, .... }] }
    """
    query = MetricsBucket.query(ancestor=serving_version).order(MetricsBucket.date, 
                                                                MetricsBucket.hpoId)
    if request.start_date:
      start_date_val = datetime.datetime.strptime(request.start_date, DATE_FORMAT)
      query = query.filter(MetricsBucket.date >= start_date_val)
    if request.end_date:
      end_date_val = datetime.datetime.strptime(request.end_date, DATE_FORMAT) 
      query = query.filter(MetricsBucket.date <= end_date_val)     
    for db_bucket in query.fetch():
      facets_dict = {"date": db_bucket.date.isoformat()}
      if db_bucket.hpoId != '':
        facets_dict["hpoId"] = db_bucket.hpoId      
      yield '{"facets": ' + json.dumps(facets_dict) + ', "entries": ' + db_bucket.metrics + '}'

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
