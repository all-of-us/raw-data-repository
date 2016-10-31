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

import extraction
import metrics
import participant
import questionnaire_response

from collections import namedtuple
from extraction import ExtractionResult

FieldDef = namedtuple('FieldDef', ['name', 'func'])
FacetDef = namedtuple('FacetDef', ['type', 'func'])

METRICS_CONFIGS = {
    'Participant': {
        'load_history_func': participant.load_history_entities,
        'facets': [
            FacetDef(metrics.FacetType.HPO_ID, lambda s: s['hpo_id']),
        ],
        'initial_state': {
            'physical_evaluation': 'UNSET',
            'race': 'UNSET',
            'ethnicity': 'UNSET',
            'survey': 'UNSET',
            'biospecimen': 'UNSET',
        },
        'fields': {
            'ParticipantHistory': [
                FieldDef('membership_tier',
                         extraction.simple_field_extractor('membership_tier')),
                FieldDef('gender_identity',
                         extraction.simple_field_extractor('gender_identity')),
                FieldDef('age_range', participant.extract_bucketed_age),
                FieldDef('hpo_id', participant.extract_HPO_id)            
            ],
            'QuestionnaireResponseHistory': [
                FieldDef('race', questionnaire_response.extract_race),
                FieldDef('ethnicity', questionnaire_response.extract_ethnicity),
                # The presence of a response means that some have been submitted.
                FieldDef('survey', lambda h: ExtractionResult('SUBMITTED_SOME')),
                FieldDef('state',  questionnaire_response.extract_state_of_residence),
                FieldDef('census_region', questionnaire_response.extract_census_region)
            ],
            'EvaluationHistory': [
                # The presence of a physical evaluation implies that it is complete.
                FieldDef('physical_evaluation', lambda h: ExtractionResult('COMPLETE')),
            ],
            'BiobankOrder': [
                # The presence of a biobank order implies that an order has been placed.
                FieldDef('biospecimen', lambda h: ExtractionResult('ORDER_PLACED'))
            ],
        },
    },
}
