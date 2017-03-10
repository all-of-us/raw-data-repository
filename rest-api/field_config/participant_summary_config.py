import concepts
import config
import extraction
import shared_config

from offline.metrics_fields import FieldDef
from questionnaire_response import extractor_for, extract_concept_date
from participant_enums import QuestionnaireStatus

def make_sample_test_code_set(samples):
  return extraction.ExtractionResult(set(s.testCode for s in samples.samples))


def num_completed_baseline_ppi_modules(summary):
  baseline_ppi_module_fields = config.getSettingList(config.BASELINE_PPI_QUESTIONNAIRE_FIELDS, [])
  count = sum(1 for field in baseline_ppi_module_fields if summary.get(field) == 'SUBMITTED')
  return extraction.ExtractionResult(count)

def num_baseline_samples_arrived(summary):
  baseline_sample_test_codes = config.getSettingList(config.BASELINE_SAMPLE_TEST_CODES, [])
  samples_arrived = summary.get('samplesArrived')
  if not samples_arrived:
    return extraction.ExtractionResult(0)
  count = sum(1 for test_code in baseline_sample_test_codes if test_code in samples_arrived)
  return extraction.ExtractionResult(count)

questionnaire_fields = [
    FieldDef('firstName',
             extractor_for(concepts.FIRST_NAME, extraction.VALUE_STRING), None),
    FieldDef('middleName',
             extractor_for(concepts.MIDDLE_NAME, extraction.VALUE_STRING),
             None),
    FieldDef('lastName',
             extractor_for(concepts.LAST_NAME, extraction.VALUE_STRING), None),
    FieldDef('dateOfBirth',
             extractor_for(concepts.DATE_OF_BIRTH, extraction.VALUE_STRING),
             None),
] + [FieldDef(k + 'Time', extract_concept_date(concept), None)
     for k, concept in shared_config.KNOWN_QUESTIONNAIRES.iteritems()]

CONFIG = {
    'fields': {
        'QuestionnaireResponseHistory':
            questionnaire_fields + shared_config.questionnaire_fields,
        'BiobankSamples': [
            FieldDef('samplesArrived', make_sample_test_code_set, None)
        ]
    },
    'summary_fields': [
        FieldDef('numCompletedBaselinePPIModules',
                 num_completed_baseline_ppi_modules, None),
        FieldDef('numBaselineSamplesArrived', num_baseline_samples_arrived,
                 None)
    ]
}
