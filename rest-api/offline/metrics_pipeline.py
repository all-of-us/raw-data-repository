"""Mapper."""

import datetime
import json
import pickle
import pipeline

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
        'model': 'participant.DAO.history_model',
        'fields': ['membership_tier', 'zip_code'],
        'dao': participant.DAO,
    },
}

class MetricsPipeline(pipeline.Pipeline):
  def run(self, *args, **kwargs):
    print "========== Starting ============="
    metrics.set_pipeline_in_progress()
    for config_name in CONFIGS.keys():
      yield SingleMetricPipeline(config_name)
    yield FinalizeMetrics()

class FinalizeMetrics(pipeline.Pipeline):
  def run(self):
    current_version = metrics.get_in_progress_version()
    if not current_version:
      raise PipelineNotRunningException()
    current_version.in_process = False
    current_version.complete = True
    current_version.put()

class SingleMetricPipeline(pipeline.Pipeline):
  def run(self, config_name):
    config = CONFIGS[config_name]
    print '======= Starting {} Pipeline'.format(config['name'])
    mapper_params = {
        'entity_kind': config['model'],
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
    yield ComputeMetrics(by_id)

class ComputeMetrics(pipeline.Pipeline):
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
  del j["date"]
  yield date, json.dumps(j['obj'])

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
    date = hist_obj.date.isoformat()
    obj = hist_obj.obj
    summary = {}
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
    yield (json.dumps({"date": date, 'summary': summary}))
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
