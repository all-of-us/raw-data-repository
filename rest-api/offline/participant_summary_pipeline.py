"""Offline process for updating participant summaries.

This is a mapreduce that goes over the participant and questionnaire response tables
and dumps summaries into ParticipantSummary entities.

The extractors defined in participant_summary_config are used for questionnaire responses.
"""

import pipeline

import config
import participant

from google.appengine.ext import ndb
from mapreduce import mapper_pipeline

_NUM_SHARDS = '_NUM_SHARDS'

class ParticipantSummaryPipeline(pipeline.Pipeline):
  def run(self, *args, **kwargs):  # pylint: disable=unused-argument
    num_shards = int(config.getSetting(config.PARTICIPANT_SUMMARY_SHARDS, 1))
    mapper_params = {
        'entity_kind': 'Participant',
    }
    yield mapper_pipeline.MapperPipeline(
        'Update Participant Summaries',
        handler_spec='offline.participant_summary_pipeline.regenerate_summary',
        input_reader_spec='mapreduce.input_readers.DatastoreKeyInputReader',
        params=mapper_params,
        shards=num_shards)

def regenerate_summary(entity_key):
  """Takes a key for the entity. 
  
  Writes a new participant summary if something has changed or the summary is missing.

  Args:
    entity_key: The key of the entity to process.  The key read by the
      DatastoreKeyInputReader is the old format, not a ndb.Key, so it needs to
      be converted.
  """
  entity_key = ndb.Key.from_old_key(entity_key)
  participant.DAO.regenerate_summary(entity_key)
