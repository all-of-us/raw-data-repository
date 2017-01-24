"""Configuration for the metrics pipeline.

This is the configuration for each type of object that we are collecting metrics
 on.  It is keyed on the name of the model to collect metrics on.

Keys for an individual configuration entry:
  load_history_func: A function that will take a ndb.Key for  the entity, and
    load all the related history objects for the given entity id.  It may also
    synthesize records or load related objects.
  initial_state: An object setting what the default state should be for an
    entity that is missing extracted values from subobjects.  For example, on
    Participant, any metrics that are not directly on the participant object
    should have sane defaults here which get used until those values are
    encountered.
  fields: The fields of the model to collect metrics on.

"""

import logging
import traceback

from collections import namedtuple
from google.appengine.ext import ndb

FieldDef = namedtuple('FieldDef', ['name', 'func', 'func_range'])

@ndb.non_transactional
def run_extractors(hist_obj, config, new_state):
  hist_kind = hist_obj.key.kind()
  for field in config['fields'][hist_kind]:
    try:
      result = field.func(hist_obj)
      if result.extracted:
        new_state[field.name] = str(result.value)
    except Exception: # pylint: disable=broad-except
      logging.error('Exception extracting history field {0}: {1}'.format(
              field.name, traceback.format_exc()))
  for field in config.get('summary_fields', []):
    try:
      result = field.func(new_state)
      if result.extracted:
        new_state[field.name] = str(result.value)
    except Exception: # pylint: disable=broad-except
      logging.error('Exception extracting history summary field {0}: {1}'.format(
              field.name, traceback.format_exc()))

