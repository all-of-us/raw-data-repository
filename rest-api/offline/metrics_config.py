"""Configuration for the metrics pipeline.

This is the configuration for each type of object that we are collecting metrics
 on.  It is keyed on the name of the model to collect metrics on.

Keys for an individual configuration entry:
  load_history_func: A function that will take a ndb.Key for  the entity, and
    load all the related history objects for the given entity id.  It may also
    synthesize records or load related objects.
  facets: A list of functions for extracting the different facets to aggregate
    on. For each hisory object, this function will be passed a dictionary with
    all the current extracted fields, with their current values. Its return
    value must be convertable to a string.
  initial_state: An object setting what the default state should be for an
    entity that is missing extracted values from subobjects.  For example, on
    Participant, any metrics that are not directly on the participant object
    should have sane defaults here which get used until those values are
    encountered.
  fields: The fields of the model to collect metrics on.

"""

import concepts
import config
import copy
import extraction
import participant
import questionnaire_response

from collections import namedtuple
from extraction import ExtractionResult
from protorpc import messages

class FacetType(messages.Enum):
  """The Facets (dimensions) to bucket by"""
  NONE = 0
  HPO_ID = 1

FieldDef = namedtuple('FieldDef', ['name', 'func', 'func_range'])
FacetDef = namedtuple('FacetDef', ['type', 'func'])

def biospecimen_summary(summary):
  """Summarizes the two biospecimen statuses into one."""
  samples = summary.get('biospecimen_samples', 'UNSET')
  order = summary.get('biospecimen', 'UNSET')
  ret = order
  if samples != 'UNSET':
    ret = samples
  return ExtractionResult(ret)

def get_extra_metrics():
  return config.getSettingJson(config.EXTRA_METRICS, default={})


def get_config(extra_metrics=None):
  if not extra_metrics:
    extra_metrics = get_extra_metrics()

  CONFIG = copy.deepcopy(DEFAULT_CONFIG)
  for k, v in extra_metrics.get('Participant', {}).iteritems():
    if v.get('type', None) == 'QuestionnaireResponse.SUBMITTED':
      CONFIG['Participant']['fields']['QuestionnaireResponseHistory'].append(
        FieldDef(k, questionnaire_response.extract_concept_presence(concepts.Concept(
                 v.get('concept', {}).get('system', ''),
                 v.get('concept', {}).get('code', ''))),
                set(['UNSET']) | questionnaire_response.submission_statuses()))
      CONFIG['Participant']['initial_state'][k] = 'UNSET'
  return CONFIG

DEFAULT_CONFIG = {
    'Participant': {
        'load_history_func': participant.load_history_entities,
        'facets': [
            FacetDef(FacetType.HPO_ID, lambda s: s.get('hpo_id', 'UNSET')),
        ],
        'initial_state': {
            'physical_evaluation': 'UNSET',
            'race': 'UNSET',
            'ethnicity': 'UNSET',
            'survey': 'UNSET',
            'biospecimen': 'UNSET',
            'biospecimen_samples': 'UNSET',
            'biospecimen_summary': 'UNSET'
        },
        'fields': {
            'ParticipantHistory': [
                FieldDef('membership_tier',
                         extraction.simple_field_extractor('membership_tier'),
                         list(participant.MembershipTier)),
                FieldDef('gender_identity',
                         extraction.simple_field_extractor('gender_identity'),
                         list(participant.GenderIdentity)),
                FieldDef('age_range', participant.extract_bucketed_age,
                         participant.AGE_BUCKETS),
                FieldDef('hpo_id', participant.extract_HPO_id,
                         participant.HPO_VALUES)
            ],
            'QuestionnaireResponseHistory': [
                FieldDef('race',
                         questionnaire_response.extract_race,
                         set(['UNSET']) | questionnaire_response.races()),
                FieldDef('ethnicity',
                         questionnaire_response.extract_ethnicity,
                         set(['UNSET']) | questionnaire_response.ethnicities()),
                # The presence of a response means that some have been submitted.
                FieldDef('survey',
                         lambda h: ExtractionResult('SUBMITTED_SOME'),
                         ('UNSET', 'SUBMITTED_SOME')),
                FieldDef('state',
                         questionnaire_response.extract_state_of_residence,
                         questionnaire_response.states()),
                FieldDef('census_region',
                         questionnaire_response.extract_census_region,
                         questionnaire_response.regions())
            ],
            'EvaluationHistory': [
                # The presence of a physical evaluation implies that it is complete.
                FieldDef('physical_evaluation',
                         lambda h: ExtractionResult('COMPLETE'),
                         ('UNSET', 'COMPLETE')),
            ],
            'BiobankOrderHistory': [
                # The presence of a biobank order implies that an order has been placed.
                FieldDef('biospecimen',
                         lambda h: ExtractionResult('ORDER_PLACED'),
                         ('UNSET', 'ORDER_PLACED'))
            ],
            'BiobankSamples': [
                # The presence of a biobank sample implies that samples have arrived
                # This overwrites the ORDER_PLACED value for biospecimen above
                FieldDef('biospecimen_samples', lambda h: ExtractionResult('SAMPLES_ARRIVED'),
                         ('UNSET', 'SAMPLES_ARRIVED'))
            ]
        },
        'summary_fields': [
            FieldDef('biospecimen_summary', biospecimen_summary,
                     ('UNSET', 'ORDER_PLACED', 'SAMPLES_ARRIVED')),
        ],
    },
}
