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

METRICS_CONFIGS = {
    'Participant': {
        'load_history_func': participant.load_history_entities,
        'facets': [
            FacetDef(FacetType.HPO_ID, lambda s: s['hpo_id']),
        ],
        'initial_state': {
            'physical_evaluation': 'UNSET',
            'race': 'UNSET',
            'ethnicity': 'UNSET',
            'survey': 'UNSET',
            'biospecimen': 'UNSET',
            'biospecimen_samples': 'UNSET'
        },
        'fields': {
            'ParticipantHistory': [
                FieldDef('membership_tier',
                         extraction.simple_field_extractor('membership_tier'),
                         iter(participant.MembershipTier)),
                FieldDef('gender_identity',
                         extraction.simple_field_extractor('gender_identity'),
                         iter(participant.GenderIdentity)),
                FieldDef('age_range', participant.extract_bucketed_age,
                         participant.AGE_BUCKETS),
                FieldDef('hpo_id', participant.extract_HPO_id,
                         participant.HPO_VALUES)
            ],
            'QuestionnaireResponseHistory': [
                FieldDef('race',
                         questionnaire_response.extract_race,
                         questionnaire_response.races()),
                FieldDef('ethnicity',
                         questionnaire_response.extract_ethnicity,
                         questionnaire_response.ethnicities()),
                # The presence of a response means that some have been submitted.
                FieldDef('survey',
                         lambda h: ExtractionResult('SUBMITTED_SOME'),
                         ('None', 'SUBMITTED_SOME')),
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
                         ('None', 'COMPLETE')),
            ],
            'BiobankOrderHistory': [
                # The presence of a biobank order implies that an order has been placed.
                FieldDef('biospecimen',
                         lambda h: ExtractionResult('ORDER_PLACED'),
                         ('None', 'ORDER_PLACED'))
            ],
            'BiobankSamples': [
                # The presence of a biobank sample implies that samples have arrived
                # This overwrites the ORDER_PLACED value for biospecimen above
                FieldDef('biospecimen_samples', lambda h: ExtractionResult('SAMPLES_ARRIVED'),
                         ('None', 'SAMPLES_ARRIVED'))
            ]
        },
    },
}
