"""Offline process for calculating metrics.

This is mapreduce that goes over the history tables and dumps summaries into
MetricsBucket entities.

In order to segregate metrics from one run from another, a MetricsVersion record
is created at the beginning of each run.  At the end of the run its 'completed'
property is set to true.  For every MetricsBucket that is created by this
pipeline, its parent is set to the current MetricsVersion.

The metrics to be collected are specified in the METRICS_CONFIGS dict.  In
addition to the fields specified there, for evey entity, a synthetic 'total'
metric is generated.  This is to record the total number of entities over time.

map_key_to_summary
------------------

The mapper, 'map_key_to_summary', takes as input a key for a single entity.
That key is passed to the load_history_func as defined in the METRICS_CONFIGS.

The load_history_func will return an array (not necessarily sorted) of history
object related to this entity.  Note that not all of the history objects need to
be of the type of the entity.  It may make sense to also include history objects
for objects that are children of this entity.

The 'fields' section of the METRICS_CONFIGS dictates which fields are to be
derived from each of the possible history objects, as well as functions for
extracting those values.

Next the algorithm goes over each of the history entries, in chronological
order, extracting fields.  When a value appears on an entity for the
first time, a "+1" is generated for that value.  When that value changes to
something else, a "+1" is created for the new value, and a "-1" is created for
the old one (on the date that it changes).

Note about facets: The summarized metrics are aggregate by dimensions referred
to as facets.  If a value that defines a facet changes between two history
objects, a summary containing all fields is emitted twice, once for the old
facet (with all -1s), and once for the new facet, (with all 1s).

The output from the mapper is objects of the form: (for Participant)
Key: {'date': '2016-10-02', 'facets': [{'type': 'hpo_id', 'value': 'jackson'}]}
Value:
{
  summary: {
    "Participant.Participant.race.asian": 1,
    "Participant.age_range.36-45": 1,
}

reduce_facets
-------------------
The reducer, 'reduce_facets', takes all the output from the mapper for a given
key, adds them up, and writes out the aggregated counts.
"""

import copy
import json
import pipeline

import config
import csv
import metrics
import participant
import offline.metrics_config

from datetime import datetime, timedelta
from google.appengine.ext import ndb
from mapreduce import base_handler
from mapreduce import mapreduce_pipeline
from mapreduce import operation as op
from mapreduce import context

from metrics import MetricsBucket
from mapreduce.lib.input_reader._gcs import GCSInputReader
from offline.base_pipeline import BasePipeline
from offline.metrics_fields import run_extractors

class PipelineNotRunningException(BaseException):
  """Exception thrown when a pipeline is expected to be running but is not."""

DATE_FORMAT = '%Y-%m-%d'

TOTAL_SENTINEL = '__total_sentinel__'
_NUM_SHARDS = '_NUM_SHARDS'

def default_params():
  """These can be used in a snapshot to ensure they stay the same across
  all instances of a MapReduce pipeline, even if datastore changes"""
  return {
        _NUM_SHARDS: int(config.getSetting(config.METRICS_SHARDS, 1))
    }

def get_config():
  return offline.metrics_config.get_config()

# This is a indicator of the format of the produced metrics.  If the metrics
# pipeline changes such that the produced metrics are not compatible with the
# serving side of the metrics API, increment this version and increment the
# version in metrics.py.  This will cause no metrics to be served while new
# metrics are calculated, which is better than crashing or serving incorrect
# data.
PIPELINE_METRICS_DATA_VERSION = 1

class BlobKeys(base_handler.PipelineBase):
  """A generator for the mapper params for the second MapReduce pipeline, containing the blob 
     keys produced by the first pipeline."""
  def run(self, bucket_name, keys, now):
    start_index = len(bucket_name) + 2
    return {'input_reader': {GCSInputReader.BUCKET_NAME_PARAM: bucket_name,
                             GCSInputReader.OBJECT_NAMES_PARAM: [k[start_index:] for k in keys]},
            'now': now}

class MetricsPipeline(BasePipeline):
  def run(self, *args, **kwargs):  # pylint: disable=unused-argument
    bucket_name = args[0]
    now = args[1]
    mapper_params = default_params()
    configs = get_config()
    validate_metrics(configs)
    metrics.set_pipeline_in_progress()
    futures = []

    for config_name in configs:
      future = yield SummaryPipeline(bucket_name, config_name, now, mapper_params)
      futures.append(future)

    yield FinalizeMetrics(*futures)

class FinalizeMetrics(pipeline.Pipeline):
  def run(self, *args):  # pylint: disable=unused-argument
    set_serving_version()

class SummaryPipeline(pipeline.Pipeline):
  def run(self, bucket_name, config_name, now, parent_params=None):
    print '======= Starting {} Pipeline'.format(config_name)
    
    mapper_params = {
        'entity_kind': config_name,
        'now': now
    }

    if parent_params:
      mapper_params.update(parent_params)

    num_shards = mapper_params[_NUM_SHARDS]
    # Chain together two map reduces
    # The first emits lines of the form hpoId|metric|date|count
    # The second reads those lines and emits metrics buckets in Datastore
    blob_key = (yield mapreduce_pipeline.MapreducePipeline(
        'Extract Metrics',
        mapper_spec='offline.metrics_pipeline.map1',
        input_reader_spec='mapreduce.input_readers.DatastoreKeyInputReader',
        output_writer_spec='mapreduce.output_writers.GoogleCloudStorageOutputWriter',
        mapper_params=mapper_params,
        combiner_spec='offline.metrics_pipeline.combine1',
        reducer_spec='offline.metrics_pipeline.reduce1',
        reducer_params={
            'now': now,
            'output_writer': {
                'bucket_name': bucket_name,
                'content_type': 'text/plain',
            }
        },
        shards=num_shards))
    # TODO(danrodney): 
    # We need to find a way to delete blob written above (DA-167)    
    yield mapreduce_pipeline.MapreducePipeline(
        'Write Metrics',
        mapper_spec='offline.metrics_pipeline.map2',
        input_reader_spec='mapreduce.input_readers.GoogleCloudStorageInputReader',
        mapper_params=(yield BlobKeys(bucket_name, blob_key, now)),                     
        reducer_spec='offline.metrics_pipeline.reduce2',
        shards=num_shards)

def map1(entity_key, now=None):
  """Takes a key for the entity. Emits (hpoId|metric, date|delta) tuples.

  Args:
    entity_key: The key of the entity to process.  The key read by the
      DatastoreKeyInputReader is the old format, not a ndb.Key, so it needs to
      be converted.
    now: Used to set the clock for testing.
  """

  entity_key = ndb.Key.from_old_key(entity_key)
  now = now or context.get().mapreduce_spec.mapper.params.get('now')
  kind = entity_key.kind()
  metrics_conf = get_config()[kind]
  # Note that history can contain multiple types of history objects.

  history = participant.load_history_entities(entity_key, now)
  if not history:
    return
  history = sorted(history, key=lambda o: o.date)

  last_state = {}
  last_hpo_id = None
  for hist_obj in history:
    date = hist_obj.date.date()
    if not last_state:
      new_state = copy.deepcopy(metrics_conf['initial_state'])
      new_state[TOTAL_SENTINEL] = 1
    else:
      new_state = copy.deepcopy(last_state)

    run_extractors(hist_obj, metrics_conf, new_state)
    if new_state == last_state:
      continue  # No changes so there's nothing to do.
    hpo_id = new_state.get('hpoId')
    hpo_change = (last_hpo_id is None or last_hpo_id != hpo_id)
    for k, v in new_state.iteritems():
      # Output a delta for this field if it is either the first value we have,
      # or if it has changed. In the case that one of the facets has changed,
      # we need deltas for all fields.
      old_val = last_state and last_state.get(k, None)
      if hpo_change or v != old_val:
        yield (map_result_key(hpo_id, kind, k, v), 
               make_pair_str(date.isoformat(), '1'))
        if last_state:
          # If the value changed, output -1 delta for the old value.
          yield (map_result_key(last_hpo_id, kind, k, old_val), 
                 make_pair_str(date.isoformat(), '-1'))

    last_state = new_state
    last_hpo_id = hpo_id


def make_pair_str(v1, v2):
  return v1 + '|' + v2

def parse_tuple(row):
  return tuple(row.split('|'))

def map_result_key(hpo_id, kind, k, v):
  return make_pair_str(hpo_id, make_metric_key(kind, k, v))

def sum_deltas(values, delta_map):
  for value in values:
    (date, delta) = parse_tuple(value)
    old_delta = delta_map.get(date)
    if old_delta:
      delta_map[date] = old_delta + int(delta)
    else:
      delta_map[date] = int(delta)

def combine1(key, new_values, old_values):  # pylint: disable=unused-argument
  """ Combines deltas generated for users into a single delta per date
  Args:
     key: hpoId|metric (unused)
     new_values: list of date|delta strings (one per participant + metric + date + hpoId)
     old_values: list of date|delta strings (one per metric + date + hpoId)
  """
  delta_map = {}
  for old_value in old_values:
    (date, delta) = parse_tuple(old_value)
    delta_map[date] = int(delta)
  sum_deltas(new_values, delta_map)
  for date, delta in delta_map.iteritems():
    yield make_pair_str(date, str(delta))

def reduce1(reducer_key, reducer_values, now=None):
  """Emits hpoId|metric|date|count for each date until today.
  Args:
    reducer_key: hpoId|metric
    reducer_values: list of date|delta strings
    now: use to set the clock for testing
  """ 
  delta_map = {}
  sum_deltas(reducer_values, delta_map)
  # Walk over the deltas by date  
  last_date = None
  count = 0
  one_day = timedelta(days=1)
  for date_str, delta in sorted(delta_map.items()):    
    date = datetime.strptime(date_str, DATE_FORMAT).date()      
    # Yield results for all the dates in between
    if last_date:
      middle_date = last_date + one_day
      while middle_date < date:
        yield reduce_result_value(reducer_key, middle_date.isoformat(), count)
        middle_date = middle_date + one_day    
    count += delta
    if count > 0:
      yield reduce_result_value(reducer_key, date_str, count)
    last_date = date
  now = now or context.get().mapreduce_spec.mapper.params.get('now')    
  # Yield results up until today.
  if count > 0 and last_date:
    last_date = last_date + one_day
    while last_date <= now.date():      
      yield reduce_result_value(reducer_key, last_date.isoformat(), count)
      last_date = last_date + one_day

def reduce_result_value(reducer_key, date_str, count):
  return reducer_key + '|' + date_str + '|' + str(count) + '\n'

def map2(row_buffer):
  """Emits (hpoId|date, metric|count) pairs for reducing ('*' for cross-HPO counts)
  Args:
     row_buffer: buffer containing hpoId|metric|date|count lines
  """
  reader = csv.reader(row_buffer, delimiter='|')
  for line in reader:
    hpo_id = line[0]
    metric_key = line[1]
    date_str = line[2]
    count = line[3]
    # Yield HPO ID + date -> metric + count
    yield (make_pair_str(hpo_id, date_str), make_pair_str(metric_key, count))
    # Yield '*' + date -> metric + count (for all HPO counts)
    yield (make_pair_str('*', date_str), make_pair_str(metric_key, count))

def reduce2(reducer_key, reducer_values):
  """Emits a metrics bucket with counts for metrics for a given hpoId + date
  Args:
     reducer_key: hpoId|date ('*' for hpoId for cross-HPO counts)
     reducer_values: list of metric|count strings
  """
  metrics_dict = {}
  (hpo_id, date_str) = parse_tuple(reducer_key)
  if hpo_id == '*':
    hpo_id = ''
  date = datetime.strptime(date_str, DATE_FORMAT)  
  for reducer_value in reducer_values:
    (metric_key, count) = parse_tuple(reducer_value)    
    # There may be multiple values for a metric due to failures and retries; we'll arbitrarily
    # pick the last one
    metrics_dict[metric_key] = int(count)
  parent = metrics.get_in_progress_version().key  
  
  bucket = MetricsBucket(date=date,
                         parent=parent,
                         hpoId=hpo_id,
                         metrics=json.dumps(metrics_dict))
  yield op.db.Put(bucket)                         
  
def set_serving_version():
  current_version = metrics.get_in_progress_version()
  if not current_version:
    raise PipelineNotRunningException()
  current_version.in_progress = False
  current_version.complete = True
  current_version.data_version = PIPELINE_METRICS_DATA_VERSION
  current_version.put()

def validate_metrics(configs):
  for cfg in configs.values():
    fields = [definition.name for def_list in cfg['fields'].values() for definition in def_list]
    assert len(fields) == len(set(fields))

def make_metric_key(kind, key, value):
  """Formats a metrics key for the summary.

  Normally the key is of the form:
    Participant.some_field.some_value

  However there is a special metric that just counts the total number of
  entities, for this kind of metric, just emit the kind.
  """
  if key is TOTAL_SENTINEL:
    return '{}'.format(kind)
  else:
    return '{}.{}.{}'.format(kind, key, value)
