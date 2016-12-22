import concepts
import extraction
import shared_config

from offline.metrics_fields import FieldDef
from questionnaire_response import extractor_for


questionnaire_fields = [
  FieldDef('firstName',
              extractor_for(concepts.FIRST_NAME, extraction.VALUE_STRING),
              None),
  FieldDef('middleName',
              extractor_for(concepts.MIDDLE_NAME, extraction.VALUE_STRING),
              None),
  FieldDef('lastName',
              extractor_for(concepts.LAST_NAME, extraction.VALUE_STRING),
              None),
  FieldDef('dateOfBirth',
              extractor_for(concepts.DATE_OF_BIRTH, extraction.VALUE_STRING),
              None),
  ]

CONFIG = {
    'fields': {
        'QuestionnaireResponseHistory': questionnaire_fields + shared_config.questionnaire_fields
        }
}
