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

# Module names for questionnaires / consent forms
OVERALL_HEALTH_PPI_MODULE = "OverallHealth"
PERSONAL_HABITS_PPI_MODULE = "PersonalHabits"
SOCIODEMOGRAPHIC_PPI_MODULE = "Sociodemographic"

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
  #'consentForStudyEnrollment': concepts.ENROLLMENT_CONSENT_FORM,
  #'consentForElectronicHealthRecords': concepts.ELECTRONIC_HEALTH_RECORDS_CONSENT_FORM,
  'questionnaireOnOverallHealth': OVERALL_HEALTH_PPI_MODULE,
  'questionnaireOnPersonalHabits': PERSONAL_HABITS_PPI_MODULE,
  'questionnaireOnSociodemographics': SOCIODEMOGRAPHIC_PPI_MODULE
  #'questionnaireOnHealthcareAccess': concepts.HEALTHCARE_ACCESS_PPI_MODULE,
  #'questionnaireOnMedicalHistory': concepts.MEDICAL_HISTORY_PPI_MODULE,
  # 'questionnaireOnMedications': concepts.MEDICATIONS_PPI_MODULE,
  #'questionnaireOnFamilyHealth': concepts.FAMILY_HEALTH_PPI_MODULE
}
QUESTIONNAIRE_MODULE_CODE_TO_FIELD = {v: k for k, v in
                                      FIELD_TO_QUESTIONNAIRE_MODULE_CODE.iteritems()}
QUESTIONNAIRE_MODULE_FIELD_NAMES = sorted(FIELD_TO_QUESTIONNAIRE_MODULE_CODE.keys())
