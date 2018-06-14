'''Constants for code values for questions and modules and their mappings to fields on
participant summaries and metrics.'''

PPI_SYSTEM = "http://terminology.pmi-ops.org/CodeSystem/ppi"
# System for codes that are used in questionnaires but we don't need for analysis purposes;
# these codes are ignored by RDR.
PPI_EXTRA_SYSTEM = "http://terminology.pmi-ops.org/CodeSystem/ppi-extra"

SITE_ID_SYSTEM = "https://www.pmi-ops.org/site-id"
HEALTHPRO_USERNAME_SYSTEM = "https://www.pmi-ops.org/healthpro-username"
FIRST_NAME_QUESTION_CODE = "PIIName_First"
LAST_NAME_QUESTION_CODE = "PIIName_Last"
MIDDLE_NAME_QUESTION_CODE = "PIIName_Middle"
ZIPCODE_QUESTION_CODE = "StreetAddress_PIIZIP"
STATE_QUESTION_CODE = "StreetAddress_PIIState"
STREET_ADDRESS_QUESTION_CODE = "PIIAddress_StreetAddress"
CITY_QUESTION_CODE = "StreetAddress_PIICity"
PHONE_NUMBER_QUESTION_CODE = "PIIContactInformation_Phone"
EMAIL_QUESTION_CODE = "ConsentPII_EmailAddress"
RECONTACT_METHOD_QUESTION_CODE = "PIIContactInformation_RecontactMethod"
LANGUAGE_QUESTION_CODE = "Language_SpokenWrittenLanguage"
SEX_QUESTION_CODE = "BiologicalSexAtBirth_SexAtBirth"
SEXUAL_ORIENTATION_QUESTION_CODE = "TheBasics_SexualOrientation"
EDUCATION_QUESTION_CODE = "EducationLevel_HighestGrade"
INCOME_QUESTION_CODE = "Income_AnnualIncome"
EHR_CONSENT_QUESTION_CODE = "EHRConsentPII_ConsentPermission"
CABOR_SIGNATURE_QUESTION_CODE = "ExtraConsent_CABoRSignature"

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
RACE_NONE_OF_THESE_CODE = 'WhatRaceEthnicity_RaceEthnicityNoneOfThese'

# Consent answer codes
CONSENT_PERMISSION_YES_CODE = 'ConsentPermission_Yes'
CONSENT_PERMISSION_NO_CODE = 'ConsentPermission_No'

# Module names for questionnaires / consent forms
CONSENT_FOR_STUDY_ENROLLMENT_MODULE = "ConsentPII"
CONSENT_FOR_ELECTRONIC_HEALTH_RECORDS_MODULE = "EHRConsentPII"
OVERALL_HEALTH_PPI_MODULE = "OverallHealth"
LIFESTYLE_PPI_MODULE = "Lifestyle"
THE_BASICS_PPI_MODULE = "TheBasics"
FAMILY_HISTORY_MODULE = "FamilyHistory"
PERSONAL_MEDICAL_HISTORY_MODULE = "PersonalMedicalHistory"
MEDICATIONS_MODULE = "MedicationsPPI"
# TODO: UPDATE THIS TO REAL CODEBOOK VALUES WHEN PRESENT
HEALTHCARE_ACCESS_MODULE = "HealthcareAccess"


BIOBANK_TESTS = ['1ED10', '2ED10', '1ED04', '1SST8', '1SS08', '1PST8', '1PS08',\
                 '2SST8', '2PST8', '1HEP4', '1UR10', '1UR90', '1SAL2', '1SAL', '1ED02', '1CFD9',
                 '1PXR2']
BIOBANK_TESTS_SET = frozenset(BIOBANK_TESTS)

UNSET = 'UNSET'
UNMAPPED = 'UNMAPPED'
BASE_VALUES = [UNSET, UNMAPPED, PMI_SKIP_CODE]

# These headers are used to make the csv output for participant summary
ps_full_data_headers = [
 'PMI ID',
 'Biobank ID',
 'Last Name',
 'First Name',
 'Date of Birth',
 'Withdrawal Status',
 'Withdrawal Date',
 'General Consent Status',
 'General Consent Date',
 'EHR Consent Status',
 'EHR Consent Date',
 'Language',
 'Participant Status',
 'CABoR Consent Status',
 'CABoR Consent Date',
 'Street Address',
 'City',
 'State',
 'ZIP',
 'Email',
 'Phone',
 'Sex',
 'Gender Identity',
 'Race/Ethnicity',
 'Education',
 'Required PPI Surveys Complete',
 'Completed Surveys',
 'Basics PPI Survey Complete',
 'Basics PPI Survey Completion Date',
 'Health PPI Survey Complete',
 'Health PPI Survey Completion Date',
 'Lifestyle PPI Survey Complete',
 'Lifestyle PPI Survey Completion Date',
 'Hist PPI Survey Complete',
 'Hist PPI Survey Completion Date',
 'Meds PPI Survey Complete',
 'Meds PPI Survey Completion Date',
 'Family PPI Survey Complete',
 'Family PPI Survey Completion Date',
 'Access PPI Survey Complete',
 'Access PPI Survey Completion Date',
 'Physical Measurements Status',
 'Physical Measurements Completion Date',
 'Paired Site',
 'Paired Organization',
 'Physical Measurements Site',
 'Samples for DNA Received',
 'Biospecimens',
 '8 mL SST Collected',
 '8 mL SST Collection Date',
 '8 mL PST Collected',
 '8 mL PST Collection Date',
 '4 mL Na-Hep Collected',
 '4 mL Na-Hep Collection Date',
 '2 mL EDTA Collected',
 '2 mL EDTA Collection Date',
 '4 mL EDTA Collected',
 '4 mL EDTA Collection Date',
 '1st 10 mL EDTA Collected',
 '1st 10 mL EDTA Collection Date',
 '2nd 10 mL EDTA Collected',
 '2nd 10 mL EDTA Collection Date',
 'Cell-Free DNA Collected',
 'Cell-Free DNA Collection Date',
 'Paxgene RNA Collected',
 'Paxgene RNA Collection Date',
 'Urine 10 mL Collected',
 'Urine 10 mL Collection Date',
 'Urine 90 mL Collected',
 'Urine 90 mL Collection Date',
 'Saliva Collected',
 'Saliva Collection Date',
 'Biospecimens Site'
]

def ps_sample_status():
  # Used to iterate and append to csv return of csv output for participant summary
  sample_list = []
  for i in BIOBANK_TESTS:
    sample_list.append('sampleStatus' + i)
  return sample_list

ps_sample_status_collection = [
  'sampleStatus1SST8',
  'sampleStatus1PST8',
  'sampleStatus1HEP4',
  'sampleStatus1ED02',
  'sampleStatus1ED04',
  'sampleStatus1ED10',
  'sampleStatus2ED10',
  'sampleStatus1CFD9',
  'sampleStatus1PXR2',
  'sampleStatus1UR10',
  'sampleStatus1UR90',
  'sampleStatus1SAL'
]
