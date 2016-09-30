"""Mapper."""

import participant

import pipeline
from mapreduce import mapreduce_pipeline


class MetricsPipeline(pipeline.Pipeline):
  def run(self, *args, **kwargs):
    print "========== Starting ============="
    mapper_params = {}
    yield mapreduce_pipeline.MapperPipeline(
        "Extract Metrics",
        handler_spec='offline.metrics_pipeline.map_to_participant',
        input_reader_spec="mapreduce.input_readers.DatastoreInputReader",
        params=mapper_params,
        reducer_spec="offline.metrics_pipeline.reduce_participant",
        shards=2)


def map_to_date(history):
  # Truncate back to midnight by converting the timestamp to a date.
  yield history.date.date(), history.obj

def map_to_participant(p_hist):
  yield p_hist.obj.drc_internal_id, p_hist

def reduce_participant(drc_internal_id, history_objects):
  for obj in history_objects:
    print 'history {}'.format(obj.date)
  yield len(history_objects)
