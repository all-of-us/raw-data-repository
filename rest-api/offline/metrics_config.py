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
import concepts
import extraction
import participant
import participant_summary
import questionnaire_response

import field_config.shared_config
from questionnaire_response import states, regions

from offline.metrics_fields import FieldDef
from extraction import ExtractionResult, BASE_VALUES, UNSET

def biospecimen_summary(summary):
  """Summarizes the two biospecimen statuses into one."""
  samples = summary.get('biospecimen_samples', UNSET)
  order = summary.get('biospecimen', UNSET)
  ret = order
  if samples != UNSET:
    ret = samples
  return ExtractionResult(ret)

def get_config():
  return ALL_CONFIG

ALL_CONFIG = {
    'Participant': {
        'initial_state': dict(list({
            'physicalEvaluation': UNSET,
            'survey': UNSET,
            'state': UNSET,
            'censusRegion': UNSET,
            'physicalEvaluation': UNSET,
            'biospecimen': UNSET,
            'biospecimenSamples': UNSET,
            'biospecimenSummary': UNSET,
            'ageRange': UNSET
        }.items()) + list(field_config.shared_config.questionnaire_defaults.items())),
        'fields': {
            'ParticipantHistory': [
              FieldDef('hpoId', participant.extract_HPO_id, set(participant_summary.HPOId)),
            ],
            'AgeHistory': [
              FieldDef('ageRange', participant_summary.extract_bucketed_age,
                       BASE_VALUES | set(participant_summary.AGE_BUCKETS)),
            ],
            'QuestionnaireResponseHistory':
                field_config.shared_config.questionnaire_fields + [
                    FieldDef('survey',
                        lambda h: extraction.ExtractionResult('SUBMITTED_SOME'),
                        (UNSET, 'SUBMITTED_SOME')),
                    FieldDef('state',
                        questionnaire_response.extractor_for(
                            concepts.STATE_OF_RESIDENCE, extraction.VALUE_CODING),
                        BASE_VALUES | states()),
                    FieldDef('censusRegion',
                        questionnaire_response.extract_census_region,
                        BASE_VALUES | regions()),
            ],
            'EvaluationHistory': [
                # The presence of a physical evaluation implies that it is complete.
                FieldDef('physicalEvaluation',
                         lambda h: ExtractionResult('COMPLETE'),
                         (UNSET, 'COMPLETE')),
            ],
            'BiobankOrderHistory': [
                # The presence of a biobank order implies that an order has been placed.
                FieldDef('biospecimen',
                         lambda h: ExtractionResult('ORDER_PLACED'),
                         (UNSET, 'ORDER_PLACED'))
            ],
            'BiobankSamples': [
                # The presence of a biobank sample implies that samples have arrived
                # This overwrites the ORDER_PLACED value for biospecimen above
                FieldDef('biospecimenSamples', lambda h: ExtractionResult('SAMPLES_ARRIVED'),
                         (UNSET, 'SAMPLES_ARRIVED'))
            ]
        },
        'summary_fields': [
            FieldDef('biospecimenSummary', biospecimen_summary,
                     (UNSET, 'ORDER_PLACED', 'SAMPLES_ARRIVED')),
        ],
    },
}

