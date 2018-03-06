'''Constants for code values for questions and modules and their mappings to fields on
participant summaries and metrics.'''

UNSET = 'UNSET'
UNMAPPED = 'UNMAPPED'
BASE_VALUES = [UNSET, UNMAPPED]

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
                 '2SST8', '2PST8', '1HEP4', '1UR10', '1SAL']
BIOBANK_TESTS_SET = frozenset(BIOBANK_TESTS)


