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

import api_util
import config
import metrics
import participant
import offline.metrics_config

from datetime import datetime
from collections import Counter
from google.appengine.ext import ndb
from mapreduce import mapreduce_pipeline
from mapreduce import operation as op
from mapreduce import context

from metrics import MetricsBucket
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

class MetricsPipeline(pipeline.Pipeline):
  def run(self, *args, **kwargs):
    mapper_params = default_params()
    configs = get_config()
    validate_metrics(configs)
    metrics.set_pipeline_in_progress()
    futures = []

    for config_name in configs:
      future = yield SummaryPipeline(config_name, mapper_params)
      futures.append(future)

    yield FinalizeMetrics(*futures)

class FinalizeMetrics(pipeline.Pipeline):
  def run(self, *args):
    set_serving_version()

class SummaryPipeline(pipeline.Pipeline):
  def run(self, config_name, parent_params=None):
    print '======= Starting {} Pipeline'.format(config_name)

    today = datetime.now()
    mapper_params = {
        'entity_kind': config_name,
        'today': today
    }

    if parent_params:
      mapper_params.update(parent_params)

    num_shards = mapper_params[_NUM_SHARDS]
    # Chain together two map reduces
    # The first emits lines of the form hpoId|metric|date|count
    # The second reads those lines and emits metrics buckets in Datastore
    blob_key = (yield mapreduce_pipeline.MapreducePipeline(
        'Extract Metrics',
        mapper_spec='offline.metrics_pipeline.map',
        input_reader_spec='mapreduce.input_readers.DatastoreKeyInputReader',
        output_writer_spec='mapreduce.output_writers.BlobstoreOutputWriter',
        mapper_params=mapper_params,
        combiner_spec='offline.metrics_pipeline.combine',
        reducer_spec='offline.metrics_pipeline.reduce',
        reducer_params={
            "mime_type": "text/plain",
        },
        shards=num_shards))
    # We need to find a way to delete blob written above...
    yield mapreduce.mapreduce_pipeline.MapreducePipeline(
        'Write Metrics',
        mapper_spec='offline.metrics_pipeline.map2',
        input_reader_spec='mapreduce.input_readers.BlobstoreLineInputReader',
        mapper_params=(yield BlobKeys(blob_key)),
        reducer_spec='offline.metrics_pipeline.reduce2',
        shards=num_shards)

def map(entity_key, now=None):
  """Takes a key for the entity. Emits hpoId|metric, date|delta pairs.

  Args:
    entity_key: The key of the entity to process.  The key read by the
      DatastoreKeyInputReader is the old format, not a ndb.Key, so it needs to
      be converted.
    now: Used to set the clock for testing.
  """

  entity_key = ndb.Key.from_old_key(entity_key)
  now = now or ctx.mapreduce_spec.mapper.params.get('today') or datetime.now()
  kind = entity_key.kind()
  metrics_conf = get_config()[kind]
  # Note that history can contain multiple types of history objects.

  history = participant.load_history_entities(entity_key, now)
  history = sorted(history, key=lambda o: o.date)

  last_state = {}
  last_facets_key = None
  for hist_obj in history:
    summary = {}
    old_summary = {}
    date = hist_obj.date.date()
    if not last_state:
      new_state = copy.deepcopy(metrics_conf['initial_state'])
      new_state[TOTAL_SENTINEL] = 1
    else:
      new_state = last_state

    new_state = run_extractors(hist_obj, metrics_conf, new_state)
    if new_state == last_state:
      continue  # No changes so there's nothing to do.
    hpo_id = new_state.get('hpoId')
    if not hpo_id:
      continue

    hpo_change = (last_hpo_id is None or last_hpo_id != hpo_id)
    for k, v in new_state.iteritems():
      # Output a delta for this field if it is either the first value we have,
      # or if it has changed. In the case that one of the facets has changed,
      # we need deltas for all fields.
      old_val = last_state and last_state.get(k, None)
      if hpo_change or v != old_val:
        yield ('|'.join(hpo_id, _make_metric_key(kind, k, v)), 
               '|'.join(date.isoformat(), '1'))
        if last_state:
          # If the value changed, output -1 delta for the old value.
          yield ('|'.join(last_hpo_id, _make_metric_key(kind, k, old_val)), 
                 '|'.join(date.isoformat(), '-1'))

    last_state = new_state
    last_facets_key = facets_key

def combine(key, new_values, old_values):
    delta_map = {}
    for old_value in old_values:
      arr = old_value.split('|')
      delta_map[arr[0]] = int(arr[1])
    for new_value in new_values:
      arr = new_value.split('|')
      old_delta = delta_map.get(arr[0])
      if old_delta:
        delta_map[arr[0]] = old_delta + int(arr[1])
      else:
        delta_map[arr[0]] = int(arr[1])
    for date, delta in delta_map.iteritems():
        yield '|'.join(date.isoformat(), str(delta))

def reduce(reducer_key, reducer_values, now=None):
  """Emits hpoId|metric|date|count for each date until today.
  """ 
  delta_map = {}
  for reducer_value in reducer_values:
    arr = reducer_value.split('|')
    old_delta = delta_map.get(arr[0])
    if old_delta:
      delta_map[arr[0]] = old_delta + int(arr[1])
    else:
      delta_map[arr[0]] = int(arr[1])
  
  # Walk over the deltas by date  
  last_date = None
  count = 0
  one_day = datetime.timedelta(days=1)
  for date_str, delta in sorted(delta_map.items()):    
    date = datetime.datetime.strptime(date_str, DATE_FORMAT)      
    # Yield results for all the dates in between
    if last_date:
      middle_date = last_date + one_day
      while middle_date < date:
        yield _get_reducer_result(reducer_key, middle_date.isoformat(), count)
        middle_date = middle_date + one_day    
    count += delta
    if count > 0:
      yield '|'.join(reducer_key, date_str, str(count))
    last_date = date
  now = now or ctx.mapreduce_spec.mapper.params.get('today') or datetime.now()    
  # Yield results up until today.
  if count > 0 and last_date:
    last_date = last_date + one_day
    while last_date <= now.date():      
      yield '|'.join(reducer_key, last_date.isoformat(), str(count))

def map2(reducer_result):
  arr = reducer_result.split('|')
  # Yield HPO ID + date -> metric + count
  yield ('|'.join(arr[0], arr[2]), '|'.join(arr[1], arr[3]))

def reduce2(reducer_key, reducer_values):
  metrics_dict = {}
  key_arr = reducer_key.split('|')
  date = datetime.datetime.strptime(key_arr[1], DATE_FORMAT)
  for reducer_value in reducer_values:
    arr = reducer_value.split('|')
    metrics_dict[arr[0]] = int(arr[1])
  parent = metrics.get_in_progress_version().key  
  bucket = MetricsBucket(date=date,
                         parent=parent,
                         facets=arr[0],
                         metrics=json.dumps(cnt))
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
  for config in configs.values():
    fields = [definition.name for def_list in config['fields'].values() for definition in def_list]
    assert len(fields) == len(set(fields))

def _make_metric_key(kind, key, value):
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
