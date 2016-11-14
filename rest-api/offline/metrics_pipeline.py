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
import logging
import pipeline
import traceback

import api_util
import config
import metrics
import offline.metrics_config

from datetime import datetime
from collections import Counter
from google.appengine.ext import ndb
from mapreduce import mapreduce_pipeline
from mapreduce import operation as op
from mapreduce import context

from metrics import MetricsBucket


class PipelineNotRunningException(BaseException):
  """Exception thrown when a pipeline is expected to be running but is not."""

DATE_FORMAT = '%Y-%m-%d'

TOTAL_SENTINEL = '__total_sentinel__'
_EXTRA_METRICS = '_EXTRA_METRICS'
_NUM_SHARDS = '_NUM_SHARDS'

def default_params():
  """These can be used in a snapshot to ensure they stay the same across
  all instances of a MapReduce pipeline, even if datastore changes"""
  return {
        _NUM_SHARDS: int(config.getSetting(config.METRICS_SHARDS, 1)),
        _EXTRA_METRICS: offline.metrics_config.get_extra_metrics()
    }

def get_config(extras=None):
  """Resolve a complete config object, using the first available of:
     1. Supplied extras
     2. Extras from a MapreducePipeline context
     3. Config datastore entity (default behavior)"""

  if (not extras) and context.get():
    extras = context.get().mapreduce_spec.mapper.params[_EXTRA_METRICS]

  return offline.metrics_config.get_config(extras)

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
    configs = get_config(mapper_params[_EXTRA_METRICS])
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

    mapper_params = {
        'entity_kind': config_name,
    }

    if parent_params:
      mapper_params.update(parent_params)

    num_shards = mapper_params[_NUM_SHARDS]
    # The result of yield is a future that will contain the files that were
    # produced by MapreducePipeline.
    yield mapreduce_pipeline.MapreducePipeline(
        'Extract Metrics',
        mapper_spec='offline.metrics_pipeline.map_key_to_summary',
        input_reader_spec='mapreduce.input_readers.DatastoreKeyInputReader',
        mapper_params=mapper_params,
        reducer_spec='offline.metrics_pipeline.reduce_facets',
        shards=num_shards)


def map_key_to_summary(entity_key, now=None):
  """Takes a key for the entity. Emits deltas for all state changes.

  Args:
    entity_key: The key of the entity to process.  The key read by the
      DatastoreKeyInputReader is the old format, not a ndb.Key, so it needs to
      be converted.
    now: Used to set the clock for testing.
  """

  entity_key = ndb.Key.from_old_key(entity_key)
  now = now or datetime.now()
  kind = entity_key.kind()
  metrics_conf = get_config()[kind]
  # Note that history can contain multiple types of history objects.
  history = metrics_conf['load_history_func'](entity_key, datetime.now())
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
      new_state = copy.deepcopy(last_state)

    hist_kind = hist_obj.key.kind()
    for field in metrics_conf['fields'][hist_kind]:
      try:
        result = field.func(hist_obj)
        if result.extracted:
          new_state[field.name] = str(result.value)
      except Exception:
        logging.error('Exception extracting history field {0}: {1}'.format(
                field.name, traceback.format_exc()))

    for field in metrics_conf['summary_fields']:
      try:
        result = field.func(new_state)
        if result.extracted:
          new_state[field.name] = str(result.value)
      except Exception:
        logging.error('Exception extracting history summary field {0}: {1}'.format(
                field.name, traceback.format_exc()))

    if new_state == last_state:
      continue  # No changes so there's nothing to do.

    facets_key = _get_facets_key(date, metrics_conf, new_state)
    facets_change = (last_facets_key is None or
                     last_facets_key['facets'] != facets_key['facets'])

    for k, v in new_state.iteritems():
      # Output a delta for this field if it is either the first value we have,
      # or if it has changed. In the case that one of the facets has changed,
      # we need deltas for all fields.
      old_val = last_state and last_state.get(k, None)
      if facets_change or v != old_val:
        summary[_make_metric_key(kind, k, v)] = 1

        if last_state:
          # If the value changed, output -1 delta for the old value.
          old_summary[_make_metric_key(kind, k, old_val)] = -1

    if old_summary:
      # Can't just use last_facets_key, as it has the old date.
      yield (json.dumps(_get_facets_key(date, metrics_conf, last_state)),
             json.dumps(old_summary, sort_keys=True))

    yield (json.dumps(facets_key), json.dumps(summary, sort_keys=True))
    last_state = new_state
    last_facets_key = facets_key

def reduce_facets(facets_key_json, deltas):
  facets_key = json.loads(facets_key_json)
  cnt = Counter()
  for delta in deltas:
    for k, val in json.loads(delta).iteritems():
      cnt[k] += val

  # Remove zeros
  cnt = Counter({k:v for k, v in cnt.iteritems() if v is not 0})

  date = None
  if 'date' in facets_key:
    date = api_util.parse_date(facets_key['date'], DATE_FORMAT, True)

  parent = metrics.get_in_progress_version().key
  bucket = MetricsBucket(date=date,
                         parent=parent,
                         facets=json.dumps(facets_key['facets']),
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
  for config in configs:
    field_names = []
    for field_name in configs[config]['fields']:
      for definition in configs[config]['fields'][field_name]:
        assert not definition.name in field_names
        field_names.append(definition.name)

def _get_facets_key(date, metrics_conf, state):
  """Creates a string that can be used as a key specifying the facets.

  The key is a json encoded object of the form:
      {'date': '2016-10-02', 'facets': [{'type': 'hpo_id', 'value': 'jackson'}]}
  """
  key_parts = []
  facets = metrics_conf['facets']
  for axis in facets:
    key_parts.append({'type': str(axis.type), 'value': axis.func(state)})
  key = {'facets': key_parts}
  if date:
    key['date'] = date.isoformat()
  return key

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
