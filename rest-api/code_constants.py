'''Constants for code values for questions and modules and their mappings to fields on
participant summaries and metrics.'''

from protorpc import messages

UNSET = 'UNSET'
SKIPPED = 'SKIPPED'
UNMAPPED = 'UNMAPPED'
BASE_VALUES = [UNSET, SKIPPED, UNMAPPED]

PPI_SYSTEM = "http://terminology.pmi-ops.org/CodeSystem/ppi"

FIRST_NAME_QUESTION_CODE = "PIIName_First"
LAST_NAME_QUESTION_CODE = "PIIName_Last"
MIDDLE_NAME_QUESTION_CODE = "PIIName_Middle"
ZIPCODE_QUESTION_CODE = "PIIAddress_ZIP"
STATE_QUESTION_CODE = "PIIAddress_State"
DATE_OF_BIRTH_QUESTION_CODE = "PIIBirthInformation_BirthDate"

GENDER_IDENTITY_QUESTION_CODE = "Gender_GenderIdentity"
RACE_QUESTION_CODE = "Race_WhatRaceEthnicity"

# General PMI answer codes
PMI_SKIP_CODE = 'PMI_Skip'
PMI_PREFER_NOT_TO_ANSWER_CODE = 'PMI_PreferNotToAnswer'
PMI_OTHER_CODE = 'PMI_Other'
PMI_FREE_TEXT_CODE = 'PMI_FreeText'
PMI_UNANSWERED_CODE = 'PMI_Unanswered'

# Race answer codes
RACE_AIAN_CODE = 'WhatRaceEthnicity_AIAN'
RACE_ASIAN_CODE = 'WhatRaceEthnicity_Asian'
RACE_BLACK_CODE = 'WhatRaceEthnicity_Black'
RACE_MENA_CODE = 'WhatRaceEthnicity_MENA'
RACE_NHDPI_CODE = 'WhatRaceEthnicity_NHPI'
RACE_WHITE_CODE = 'WhatRaceEthnicity_White'
RACE_HISPANIC_CODE = 'WhatRaceEthnicity_Hispanic'
RACE_FREETEXT_CODE = 'WhatRaceEthnicity_FreeText'

# Module names for questionnaires / consent forms
# TODO: UPDATE THIS TO REAL CODEBOOK VALUES WHEN PRESENT
CONSENT_FOR_STUDY_ENROLLMENT_MODULE = "ConsentPII"
CONSENT_FOR_ELECTRONIC_HEALTH_RECORDS_MODULE = "ConsentForEHR"
OVERALL_HEALTH_PPI_MODULE = "OverallHealth"
LIFESTYLE_PPI_MODULE = "Lifestyle"
THE_BASICS_PPI_MODULE = "TheBasics"

# Field names for questionnaires / consent forms
CONSENT_FOR_STUDY_ENROLLMENT_FIELD = "consentForStudyEnrollment"
CONSENT_FOR_ELECTRONIC_HEALTH_RECORDS_FIELD = "consentForElectronicHealthRecords"
QUESTIONNAIRE_ON_OVERALL_HEALTH_FIELD = "questionnaireOnOverallHealth"
QUESTIONNAIRE_ON_LIFESTYLE_FIELD = "questionnaireOnLifestyle"
QUESTIONNAIRE_ON_THE_BASICS_FIELD = "questionnaireOnTheBasics"

class FieldType(messages.Enum):
  """A type of field that shows up in a questionnaire response."""
  CODE = 1
  STRING = 2
  DATE = 3

FIELD_TO_QUESTION_CODE = {
  "genderIdentityId": (GENDER_IDENTITY_QUESTION_CODE, FieldType.CODE),
  "firstName": (FIRST_NAME_QUESTION_CODE, FieldType.STRING),
  "lastName": (LAST_NAME_QUESTION_CODE, FieldType.STRING),
  "middleName": (MIDDLE_NAME_QUESTION_CODE, FieldType.STRING),
  "zipCode": (ZIPCODE_QUESTION_CODE, FieldType.STRING),
  "state": (STATE_QUESTION_CODE, FieldType.STRING),
  "dateOfBirth": (DATE_OF_BIRTH_QUESTION_CODE, FieldType.DATE)
}
QUESTION_CODE_TO_FIELD = {v[0]: (k, v[1]) for k, v in FIELD_TO_QUESTION_CODE.iteritems()}

FIELD_TO_QUESTIONNAIRE_MODULE_CODE = {
  # TODO: fill this in when correct codes are defined
  CONSENT_FOR_STUDY_ENROLLMENT_FIELD: CONSENT_FOR_STUDY_ENROLLMENT_MODULE,
  CONSENT_FOR_ELECTRONIC_HEALTH_RECORDS_FIELD: CONSENT_FOR_ELECTRONIC_HEALTH_RECORDS_MODULE,
  QUESTIONNAIRE_ON_OVERALL_HEALTH_FIELD: OVERALL_HEALTH_PPI_MODULE,
  QUESTIONNAIRE_ON_LIFESTYLE_FIELD: LIFESTYLE_PPI_MODULE,
  QUESTIONNAIRE_ON_THE_BASICS_FIELD: THE_BASICS_PPI_MODULE
  #'questionnaireOnHealthcareAccess': concepts.HEALTHCARE_ACCESS_PPI_MODULE,
  #'questionnaireOnMedicalHistory': concepts.MEDICAL_HISTORY_PPI_MODULE,
  # 'questionnaireOnMedications': concepts.MEDICATIONS_PPI_MODULE,
  #'questionnaireOnFamilyHealth': concepts.FAMILY_HEALTH_PPI_MODULE
}
QUESTIONNAIRE_MODULE_CODE_TO_FIELD = {v: k for k, v in
                                      FIELD_TO_QUESTIONNAIRE_MODULE_CODE.iteritems()}
QUESTIONNAIRE_MODULE_FIELD_NAMES = sorted(FIELD_TO_QUESTIONNAIRE_MODULE_CODE.keys())
