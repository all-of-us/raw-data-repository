"""Offline process for calculating metrics.

This is a two stage mapreduce that goes over the history tables and dumps
summaries into MetricsBucket entities.

In order to segregate metrics from one run from another, a MetricsVersion record
is created at the beginning of each run.  At the end of the run its 'completed'
property is set to true.  For every MetricsBucket that is created by this
pipeline, it's parent is set to the current MetricsVersion.

The metrics to be collected are specified in the CONFIGS dict.  In addition to
the fields specified there, for evey entity, a synthetic '_total' metric is
generated.  This is to record the total number of entities over time.


The stages are as follows:

SingleMetricsPipeline
---------------------

The mapper for SingleMetricsPipeline, 'map_to_id' takes all of the entries from
a history table and maps them to: (id, history_object) pairs.  These are fed to
the reducer.

The reducer, 'reduce_by_id' goes over each of the history entries for a given
entity in order.  When a value appears on an entity for the first time, a "+1"
is generated for that value.  When that value changes to something else, a "+1"
is created for the new value, and a "-1" is created for the old one (on the date
that it changes).

The output from the reducer is objects of the form:
 {date: date, summary: { "new_value": 1, "old_value": -1}}

Where the timestamps on the history objects are rounded off to the day.

SingleMetricsPhaseTwo
---------------------

The mapper for phase two, 'key_by_date', takes the output from phase one, and
re-keys the entities on date.

The reducer for phase two, 'reduce_date', takes all of the entries for a given
date, adds them up, and writes out the aggregated counts for each day.

"""

import datetime
import json
import pickle
import pipeline

import api_util
import metrics
import participant


from collections import Counter
from google.appengine.api import app_identity
from google.appengine.ext import ndb
from mapreduce import mapreduce_pipeline
from mapreduce import util
from mapreduce import operation as op
from protorpc import messages

from metrics import MetricsBucket


class PipelineNotRunningException(BaseException):
  """Exception thrown when a pipeline is expected to be running but is not."""

DATE_FORMAT = '%Y-%m-%d'

CONFIGS = {
    'ParticipantHistory': {
        'name': 'Participant',
        'id_field': 'drc_internal_id',
        'model_name': 'participant.DAO.history_model',
        'model': participant.DAO.history_model,
        'fields': ['membership_tier', 'zip_code'],
        'dao': participant.DAO,
    },
}

class MetricsPipeline(pipeline.Pipeline):
  def run(self, *args, **kwargs):
    print "========== Starting ============="
    metrics.set_pipeline_in_progress()
    futures = []
    for config_name in CONFIGS.keys():
      future = yield SingleMetricPipeline(config_name)
      futures.append(future)
    yield FinalizeMetrics(*futures)

class FinalizeMetrics(pipeline.Pipeline):
  def run(self, *args):
    current_version = metrics.get_in_progress_version()
    if not current_version:
      raise PipelineNotRunningException()
    current_version.in_progress = False
    current_version.complete = True
    current_version.put()

class SingleMetricPipeline(pipeline.Pipeline):
  def run(self, config_name):
    config = CONFIGS[config_name]
    print '======= Starting {} Pipeline'.format(config['name'])
    mapper_params = {
        'entity_kind': config['model_name'],
    }
    bucket_name = app_identity.get_default_gcs_bucket_name()
    reducer_params = {
      "output_writer": {
          "bucket_name": bucket_name,
      },
    }
    by_id = yield mapreduce_pipeline.MapreducePipeline(
        "Extract Metrics",
        mapper_spec='offline.metrics_pipeline.map_to_id',
        input_reader_spec="mapreduce.input_readers.DatastoreInputReader",
        mapper_params=mapper_params,
        reducer_spec="offline.metrics_pipeline.reduce_by_id",
        reducer_params=reducer_params,
        output_writer_spec="mapreduce.output_writers.GoogleCloudStorageRecordOutputWriter",
        shards=2)
    yield SingleMetricsPhaseTwo(by_id)

class SingleMetricsPhaseTwo(pipeline.Pipeline):
  def run(self, files):
    bucket_name = app_identity.get_default_gcs_bucket_name()
    filenames_only = (util.strip_prefix_from_items("/%s/" % bucket_name, files))

    mapper_params = {
        "input_reader": {
            "objects": filenames_only,
            "bucket_name": bucket_name,
        },
    }

    yield mapreduce_pipeline.MapreducePipeline(
        "Aggregate Days",
        mapper_spec='offline.metrics_pipeline.key_by_date',
        mapper_params=mapper_params,
        input_reader_spec="mapreduce.input_readers.GoogleCloudStorageRecordInputReader",
        reducer_spec="offline.metrics_pipeline.reduce_date",
        shards=2)


def key_by_date(json_string):
  j = json.loads(json_string)
  date = j["date"]
  yield date, json.dumps(j)

def map_to_id(hist_obj):
  kind = hist_obj._get_kind()
  config = CONFIGS[kind]
  yield ('{}:{}'.format(kind, getattr(hist_obj.obj, config['id_field'])),
         json.dumps(config['dao'].history_to_json(hist_obj)))

def reduce_by_id(obj_id, history_objects):
  kind = obj_id.split(':')[0]
  config = CONFIGS[kind]
  history = []
  for obj in history_objects:
    history.append(config['dao'].history_from_json(json.loads(obj)))

  history = sorted(history, key=lambda o: o.date)
  last = None
  for hist_obj in history:
    if hist_obj == last:
      print "Hit duplicate history entry, skipping..."
      continue
    obj = hist_obj.obj
    summary = {}
    # Put out a single '_total' field for the first time this object shows up.
    if last is None:
      summary_key = '{}.{}'.format(config['name'], metrics.TOTAL_SENTINEL)
      summary[summary_key] = 1

    for field in config['fields']:
      val = getattr(obj, field)
      val_str = str(val)
      if type(getattr(config['model'].obj, field)) == messages.EnumField:
        if val == None:
          val_str = 'NONE'
      summary_key = '{}.{}.{}'.format(config['name'], field, val_str)
      summary[summary_key] = 1
      if last and getattr(obj, field) != getattr(last, field):
        old_summary_key = '{}.{}.{}'.format(
            config['name'], field, getattr(last, field))
        summary[old_summary_key] = -1

    emitted = {"date": hist_obj.date.date().isoformat(), 'summary': summary}
    yield (json.dumps(emitted))
    last = obj

def reduce_date(date, deltas):
  cnt = Counter()
  for delta in deltas:
    delta = json.loads(delta)
    for k, val in delta['summary'].iteritems():
      cnt[k] += val

  # Remove zeros
  cnt = Counter({k:v for k, v in cnt.iteritems() if v})

  current_version = metrics.get_in_progress_version()
  bucket = MetricsBucket(parent=current_version.key)
  bucket.date = datetime.datetime.strptime(date, DATE_FORMAT)
  bucket.metrics = pickle.dumps(cnt)
  yield op.db.Put(bucket)
