"""Offline process for calculating metrics.

This is a two stage mapreduce that goes over the history tables and dumps
summaries into MetricsBucket entities.

In order to segregate metrics from one run from another, a MetricsVersion record
is created at the beginning of each run.  At the end of the run its 'completed'
property is set to true.  For every MetricsBucket that is created by this
pipeline, its parent is set to the current MetricsVersion.

The metrics to be collected are specified in the CONFIGS dict.  In addition to
the fields specified there, for evey entity, a synthetic 'total' metric is
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

The output from the reducer is objects of the form: (for Participant)
{
  date: "2016-10-03",
  facets: [{"type": "HPO_ID", "value": "HPO-123"}],
  summary: { "new_value": 1, "old_value": -1},
}

Where the timestamps on the history objects are rounded off to the day.

SingleMetricsPhaseTwo
---------------------

The mapper for phase two, 'key_by_facets', takes the output from phase one, and
re-keys the entities on the facets that will be aggregated on the fly. For
participant this is date and hpo_id.

The reducer for phase two, 'reduce_facets', takes all of the entries for a given
combination of facets, adds them up, and writes out the aggregated counts.
"""

import datetime
import json
import pipeline

import api_util
import config
import metrics
import participant


from dateutil import relativedelta
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

HISTORY_DATE_FUNC = lambda ph: ph.date.date().isoformat()

# Extract the recruitment_source from a ParticipantHistory model.
HPO_ID_FUNC = lambda ph: (
    (ph.obj.recruitment_source and (str(ph.obj.recruitment_source) + ':') or '')
    + str(ph.obj.hpo_id))

def extract_bucketed_age(hist_obj):
  today = hist_obj.date
  if not hist_obj.obj.date_of_birth:
    return None
  age = relativedelta.relativedelta(today, hist_obj.obj.date_of_birth).years
  return _bucket_age(age)

# Configuration for each type of object that we are collecting metrics on.
#  name: The name of the ndb model object.
#  date_func: A function that presented with the history object, will return the
#    date that should be associated  with the changes.  This needs to return a
#    string (datetime.date.isoformat()).  In most cases this should be
#    HISTORY_DATE_FUNC.  If some other date is used, make sure that use_history
#    is False.
#  facets: A list of functions for extracting the different facets to aggregate
#    on.  The function will be passed a model object, its return value must be
#    convertible to a string.
#  id_field: The field that contains the unique id for this object.
#  model_name: The type of the history object as a string.
#  model: The type of the history object.
#  fields: The fields of the model to collect metrics on.
#  dao: The data access object associated with this model.
#  use_history: Compute deltas across the entire history of this object.  If
#    False, will only use the latest version of the object.  If use_history is
#    True, the date_func must return the date from the history object, otherwise
#    out of order dates could yield corrupt metrics.
METRICS_CONFIGS = {
    'ParticipantHistory': {
        'name': 'Participant',
        'date_func': HISTORY_DATE_FUNC,
        'facets': [
            {'type': metrics.FacetType.HPO_ID, 'func': HPO_ID_FUNC}
        ],
        'id_field': 'participant_id',
        'model_name': 'participant.DAO.history_model',
        'model': participant.DAO.history_model,
        'fields': [
            {
                'name': 'membership_tier',
                'func': lambda p: p.obj.membership_tier,
            },
            {
                'name': 'gender_identity',
                'func': lambda p: p.obj.gender_identity,
            },
            {
                'name': 'age_range',
                'func': extract_bucketed_age,
            },
            {
                'name': 'hpo_id',
                'func': HPO_ID_FUNC,
            },
        ],
        'dao': participant.DAO,
        'use_history': True,
    },
}

class MetricsPipeline(pipeline.Pipeline):
  def run(self, *args, **kwargs):
    print '========== Starting ============='
    metrics.set_pipeline_in_progress()
    futures = []
    for config_name in METRICS_CONFIGS.keys():
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
    metrics_config = METRICS_CONFIGS[config_name]
    print '======= Starting {} Pipeline'.format(metrics_config['name'])
    mapper_params = {
        'entity_kind': metrics_config['model_name'],
    }
    bucket_name = app_identity.get_default_gcs_bucket_name()
    reducer_params = {
      'output_writer': {
          'bucket_name': bucket_name,
      },
    }
    num_shards = int(config.getSetting(config.METRICS_SHARDS))
    # The result of yield is a future that will contian the files that were
    # produced by MapreducePipeline.
    by_id = yield mapreduce_pipeline.MapreducePipeline(
        'Extract Metrics',
        mapper_spec='offline.metrics_pipeline.map_to_id',
        input_reader_spec='mapreduce.input_readers.DatastoreInputReader',
        mapper_params=mapper_params,
        reducer_spec='offline.metrics_pipeline.reduce_by_id',
        reducer_params=reducer_params,
        output_writer_spec='mapreduce.output_writers.GoogleCloudStorageRecordOutputWriter',
        shards=num_shards)
    yield SingleMetricsPhaseTwo(by_id)

class SingleMetricsPhaseTwo(pipeline.Pipeline):
  def run(self, files):
    bucket_name = app_identity.get_default_gcs_bucket_name()
    filenames_only = (util.strip_prefix_from_items('/%s/' % bucket_name, files))

    mapper_params = {
        'input_reader': {
            'objects': filenames_only,
            'bucket_name': bucket_name,
        },
    }

    num_shards = int(config.getSetting(config.METRICS_SHARDS))
    yield mapreduce_pipeline.MapreducePipeline(
        'Aggregate Days',
        mapper_spec='offline.metrics_pipeline.key_by_facets',
        mapper_params=mapper_params,
        input_reader_spec='mapreduce.input_readers.GoogleCloudStorageRecordInputReader',
        reducer_spec='offline.metrics_pipeline.reduce_facets',
        shards=num_shards)


def key_by_facets(json_string):
  j = json.loads(json_string)
  facets_key = j['facets_key']
  yield facets_key, json.dumps(j)

def map_to_id(hist_obj):
  kind = hist_obj._get_kind()
  metrics_config = METRICS_CONFIGS[kind]
  yield ('{}:{}'.format(kind, getattr(hist_obj.obj, metrics_config['id_field'])),
         json.dumps(metrics_config['dao'].history_to_json(hist_obj)))

def reduce_by_id(obj_id, history_objects):
  kind = obj_id.split(':')[0]
  metrics_config = METRICS_CONFIGS[kind]
  history = []
  for obj in history_objects:
    history.append(metrics_config['dao'].history_from_json(json.loads(obj)))

  history = sorted(history, key=lambda o: o.date)

  # Look at just the latest?
  if not metrics_config['use_history']:
    history = history[-1:]
  last = None
  for hist_obj in history:
    summary = {}
    old_summary = {}
    # Put out a single 'total' entry for the first time this object shows up.
    if last is None:
      summary[metrics_config['name']] = 1

    for field in metrics_config['fields']:
      func = field['func']
      val = func(hist_obj)
      val_str = str(val)

      # Only output a delta for this field if it is either the first value we
      # have, or if it has changed.
      old_val = None
      if last:
        old_val = func(last)
      if not last or (last and val != old_val):
        summary_key = '{}.{}.{}'.format(metrics_config['name'], field['name'], val_str)
        summary[summary_key] = 1

        if last:
          # If the value changed, output a -1 delta for the old value.
          old_summary_key = '{}.{}.{}'.format(metrics_config['name'], field['name'], old_val)
          old_summary[old_summary_key] = -1

    date = None
    if metrics_config['use_history']:
      date = metrics_config['date_func'](hist_obj)

    if old_summary:
      old_emitted = {'summary': old_summary}
      if date:
        old_emitted['date'] = date
      old_emitted['facets_key'] = _get_facets_key(date, metrics_config, last)
      yield json.dumps(old_emitted)

    emitted = {'summary': summary}
    if date:
      emitted['date'] = date
    emitted['facets_key'] = _get_facets_key(date, metrics_config, hist_obj)
    yield json.dumps(emitted)
    last = hist_obj

def reduce_facets(facets_key_json, deltas):
  facets_key = json.loads(facets_key_json)
  cnt = Counter()
  for delta in deltas:
    delta = json.loads(delta)
    for k, val in delta['summary'].iteritems():
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

def _get_facets_key(date, metrics_config, hist_obj):
  key_parts = []
  facets = metrics_config['facets']
  for axis in facets:
    key_parts.append({'type': str(axis['type']), 'value': axis['func'](hist_obj)})
  key = {'facets': key_parts}
  if date:
    key['date'] = date
  return json.dumps(key)

def _bucket_age(age):
  ages = [0, 18, 26, 36, 46, 56, 66, 76, 86]
  for begin, end in zip(ages, [a - 1 for a in ages[1:]] + ['']):
    if age >= begin and not end or age <= end:
      return str(begin) + '-' + str(end)
