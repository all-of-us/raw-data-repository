"""Mappings from fields to question or module codes."""

from protorpc import messages
from code_constants import GENDER_IDENTITY_QUESTION_CODE, FIRST_NAME_QUESTION_CODE
from code_constants import LAST_NAME_QUESTION_CODE, MIDDLE_NAME_QUESTION_CODE
from code_constants import STREET_ADDRESS_QUESTION_CODE, CITY_QUESTION_CODE
from code_constants import CITY_QUESTION_CODE, ZIPCODE_QUESTION_CODE, STATE_QUESTION_CODE
from code_constants import PHONE_NUMBER_QUESTION_CODE, EMAIL_QUESTION_CODE
from code_constants import RECONTACT_METHOD_QUESTION_CODE, LANGUAGE_QUESTION_CODE
from code_constants import SEX_QUESTION_CODE, SEXUAL_ORIENTATION_QUESTION_CODE
from code_constants import EDUCATION_QUESTION_CODE, INCOME_QUESTION_CODE
from code_constants import DATE_OF_BIRTH_QUESTION_CODE, CONSENT_FOR_STUDY_ENROLLMENT_MODULE
from code_constants import CONSENT_FOR_ELECTRONIC_HEALTH_RECORDS_MODULE
from code_constants import OVERALL_HEALTH_PPI_MODULE, LIFESTYLE_PPI_MODULE, THE_BASICS_PPI_MODULE

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
  "streetAddress": (STREET_ADDRESS_QUESTION_CODE, FieldType.STRING),
  "city": (CITY_QUESTION_CODE, FieldType.STRING),
  "zipCode": (ZIPCODE_QUESTION_CODE, FieldType.STRING),
  "stateId": (STATE_QUESTION_CODE, FieldType.CODE),
  "phoneNumber": (PHONE_NUMBER_QUESTION_CODE, FieldType.STRING),
  "email": (EMAIL_QUESTION_CODE, FieldType.STRING),
  "recontactMethodId": (RECONTACT_METHOD_QUESTION_CODE, FieldType.CODE),
  "languageId": (LANGUAGE_QUESTION_CODE, FieldType.CODE),
  "sexId": (SEX_QUESTION_CODE, FieldType.CODE),
  "sexualOrientationId": (SEXUAL_ORIENTATION_QUESTION_CODE, FieldType.CODE),
  "educationId": (EDUCATION_QUESTION_CODE, FieldType.CODE),
  "incomeId": (INCOME_QUESTION_CODE, FieldType.CODE),
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