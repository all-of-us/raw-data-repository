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
import participant_dao
import participant_enums
import questionnaire_response

import field_config.shared_config
from questionnaire_response import states, regions, submission_statuses

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
  
def consent_for_study_enrollment_and_ehr(summary):
  """True when both the study and EHR have been consented to."""
  consent_for_study = summary.get('consentForStudyEnrollment', UNSET)
  consent_for_ehr = summary.get('consentForElectronicHealthRecords', UNSET)
  ret = UNSET
  if consent_for_study == 'SUBMITTED' and consent_for_ehr == 'SUBMITTED':
    ret = 'SUBMITTED'
  return ExtractionResult(ret)

def get_config():
  return ALL_CONFIG

def get_fields():
  fields = []
  for type_, conf in get_config().iteritems():
    for field_list in conf['fields'].values():
      for field in field_list:
        field_dict = {'name': type_ + '.' + field.name,
                      'values': [str(r) for r in field.func_range]}
        fields.append(field_dict)
  return sorted(fields, key=lambda field: field['name'])    

ALL_CONFIG = {
    'Participant': {
        'initial_state': dict(list({
            'physicalMeasurements': UNSET,
            'survey': UNSET,
            'state': UNSET,
            'censusRegion': UNSET,
            'biospecimen': UNSET,
            'biospecimenSamples': UNSET,
            'biospecimenSummary': UNSET,
            'ageRange': UNSET
        }.items()) + list(field_config.shared_config.questionnaire_defaults.items())),
        'fields': {
            'ParticipantHistory': [
              FieldDef('hpoId', participant_dao.extract_HPO_id, set(participant_enums.HPOId)),
            ],
            'AgeHistory': [
              FieldDef('ageRange', participant_enums.extract_bucketed_age,
                       BASE_VALUES | set(participant_enums.AGE_BUCKETS)),
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
            'PhysicalMeasurementsHistory': [
                # The presence of physical measurements implies that it is complete.
                FieldDef('physicalMeasurements',
                         lambda h: ExtractionResult('COMPLETE'),
                         (UNSET, 'COMPLETE')),
            ],
            'BiobankOrderHistory': [
                # The presence of a biobank order implies that an order has been placed.
                FieldDef('biospecimen',
                         lambda h: ExtractionResult('SPECIMEN_COLLECTED'),
                         (UNSET, 'SPECIMEN_COLLECTED'))
            ],
            'BiobankSamples': [
                # The presence of a biobank sample implies that samples have arrived
                # This overwrites the SPECIMEN_COLLECTED value for biospecimen above
                FieldDef('biospecimenSamples', lambda h: ExtractionResult('SAMPLES_ARRIVED'),
                         (UNSET, 'SAMPLES_ARRIVED'))
            ]
        },
        'summary_fields': [
            FieldDef('biospecimenSummary', biospecimen_summary,
                     (UNSET, 'SPECIMEN_COLLECTED', 'SAMPLES_ARRIVED')),
            FieldDef('consentForStudyEnrollmentAndEHR', consent_for_study_enrollment_and_ehr,
                     set([UNSET]) | submission_statuses())
        ],
    },
}

