"""Offline process for updating participant summaries.

This is mapreduce that goes over existing ParticipantSummary entities and updates their age
range if it has changed based on the current date.

Having an age range field is necessary because Datastore does
not support inequality filters on queries with a sort order starting with a different field.
"""

import config
import participant_summary

from mapreduce import mapper_pipeline
from mapreduce import context
from offline.base_pipeline import BasePipeline

_NUM_SHARDS = '_NUM_SHARDS'

class AgeRangePipeline(BasePipeline):
  def run(self, *args, **kwargs):  # pylint: disable=unused-argument
    now = args[0]
    num_shards = int(config.getSetting(config.AGE_RANGE_SHARDS, 1))
    mapper_params = {
        'entity_kind': 'participant_summary.ParticipantSummary',
        'now': now,
    }
    yield mapper_pipeline.MapperPipeline(
        'Update Age Ranges',
        handler_spec='offline.age_range_pipeline.update_age_range',
        input_reader_spec='mapreduce.input_readers.DatastoreInputReader',
        params=mapper_params,
        shards=num_shards)


def update_age_range(summary, now=None):
  """Takes a participant summary. Writes a new participant summary if the age range has changed.

  Args:
    participant_summary: The summary.
    now: Used to set the clock for testing.
  """
  if summary.dateOfBirth:
    now = now or context.get().mapreduce_spec.mapper.params.get('now')
    new_age_range = participant_summary.get_bucketed_age(summary.dateOfBirth, now.date())
    if summary.ageRange != new_age_range:
      participant_summary.DAO().update_computed_properties(summary.key)
