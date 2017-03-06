'''Constants for code values for questions and modules and their mappings to fields on
participant summaries and metrics.'''

PPI_SYSTEM = "http://terminology.pmi-ops.org/CodeSystem/ppi"

GENDER_IDENTITY_QUESTION_CODE = "Gender_GenderIdentity"
# This is a topic in the codebook right now.
RACE_QUESTION_CODE = "Sociodemographic_Race"
# This doesn't exist in the codebook right now.
ETHNICITY_QUESTION_CODE = "Sociodemographic_Ethnicity"

# Module names for questionnaires / consent forms
OVERALL_HEALTH_PPI_MODULE = "OverallHealth"
PERSONAL_HABITS_PPI_MODULE = "PersonalHabits"
SOCIODEMOGRAPHIC_PPI_MODULE = "Sociodemographic"

FIELD_TO_QUESTION_CODE = {
  "genderIdentityId": GENDER_IDENTITY_QUESTION_CODE,
  "raceId": RACE_QUESTION_CODE,
  "ethnicityId": ETHNICITY_QUESTION_CODE
}
QUESTION_CODE_TO_FIELD = {v: k for k, v in FIELD_TO_QUESTION_CODE.iteritems()}

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
