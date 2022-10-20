"""Constants for code values for questions and modules and their mappings to fields on
participant summaries and metrics."""

PPI_SYSTEM = "http://terminology.pmi-ops.org/CodeSystem/ppi"
# System for codes that are used in questionnaires but we don't need for analysis purposes;
# these codes are ignored by RDR.
PPI_EXTRA_SYSTEM = "http://terminology.pmi-ops.org/CodeSystem/ppi-extra"
MEASUREMENT_SYS = 'http://terminology.pmi-ops.org/CodeSystem/physical-measurements'

SITE_ID_SYSTEM = "https://www.pmi-ops.org/site-id"
QUEST_SITE_ID_SYSTEM = "https://www.pmi-ops.org/quest-site"
HEALTHPRO_USERNAME_SYSTEM = "https://www.pmi-ops.org/healthpro-username"
QUEST_USERNAME_SYSTEM = "https://www.pmi-ops.org/quest-user"
KIT_ID_SYSTEM = "https://orders.mayomedicallaboratories.com/kit-id"
QUEST_BIOBANK_ORDER_ORIGIN = 'careevolution'
FIRST_NAME_QUESTION_CODE = "PIIName_First"
LAST_NAME_QUESTION_CODE = "PIIName_Last"
MIDDLE_NAME_QUESTION_CODE = "PIIName_Middle"
ZIPCODE_QUESTION_CODE = "StreetAddress_PIIZIP"
STATE_QUESTION_CODE = "StreetAddress_PIIState"
STREET_ADDRESS_QUESTION_CODE = "PIIAddress_StreetAddress"
STREET_ADDRESS2_QUESTION_CODE = "PIIAddress_StreetAddress2"
CITY_QUESTION_CODE = "StreetAddress_PIICity"
EMPLOYMENT_ZIPCODE_QUESTION_CODE = "EmploymentWorkAddress_ZipCode"
PHONE_NUMBER_QUESTION_CODE = "PIIContactInformation_Phone"
LOGIN_PHONE_NUMBER_QUESTION_CODE = "ConsentPII_VerifiedPrimaryPhoneNumber"
EMAIL_QUESTION_CODE = "ConsentPII_EmailAddress"
RECONTACT_METHOD_QUESTION_CODE = "PIIContactInformation_RecontactMethod"
LANGUAGE_QUESTION_CODE = "Language_SpokenWrittenLanguage"
SEX_QUESTION_CODE = "BiologicalSexAtBirth_SexAtBirth"
SEXUAL_ORIENTATION_QUESTION_CODE = "TheBasics_SexualOrientation"
EDUCATION_QUESTION_CODE = "EducationLevel_HighestGrade"
INCOME_QUESTION_CODE = "Income_AnnualIncome"
EHR_CONSENT_QUESTION_CODE = "EHRConsentPII_ConsentPermission"
EHR_SENSITIVE_CONSENT_QUESTION_CODE = "ehrconsentpii_sensitivetype2"
EHR_CONSENT_EXPIRED_QUESTION_CODE = "EHRConsentPII_ConsentExpired"
DVEHR_SHARING_QUESTION_CODE = "DVEHRSharing_AreYouInterested"
CABOR_SIGNATURE_QUESTION_CODE = "ExtraConsent_CABoRSignature"
GROR_CONSENT_QUESTION_CODE = "ResultsConsent_CheckDNA"
COPE_CONSENT_QUESTION_CODE = "section_participation"
WEAR_CONSENT_QUESTION_CODE = "resultsconsent_wear"
PRIMARY_CONSENT_UPDATE_QUESTION_CODE = "Reconsent_ReviewConsentAgree"
VA_EHR_RECONSENT_QUESTION_CODE = "vaehrreconsent_agree"

DATE_OF_BIRTH_QUESTION_CODE = "PIIBirthInformation_BirthDate"

GENDER_IDENTITY_QUESTION_CODE = "Gender_GenderIdentity"
RACE_QUESTION_CODE = "Race_WhatRaceEthnicity"

# DA-2419: TheBasics possible partial survey content for profile/secondary contact updates.
BASICS_PROFILE_UPDATE_QUESTION_CODES = [
    'PersonOneAddress_PersonOneAddressCity',
    'PersonOneAddress_PersonOneAddressState',
    'PersonOneAddress_PersonOneAddressZipCode',
    'SecondaryContactInfo_PersonOneEmail',
    'SecondaryContactInfo_PersonOneFirstName',
    'SecondaryContactInfo_PersonOneMiddleInitial',
    'SecondaryContactInfo_PersonOneLastName',
    'SecondaryContactInfo_PersonOneAddressOne',
    'SecondaryContactInfo_PersonOneAddressTwo',
    'SecondaryContactInfo_PersonOneTelephone',
    'SecondaryContactInfo_PersonOneRelationship',
    'SecondaryContactInfo_SecondContactsFirstName',
    'SecondaryContactInfo_SecondContactsMiddleInitial',
    'SecondaryContactInfo_SecondContactsLastName',
    'SecondaryContactInfo_SecondContactsAddressOne',
    'SecondaryContactInfo_SecondContactsAddressTwo',
    'SecondContactsAddress_SecondContactCity',
    'SecondContactsAddress_SecondContactZipCode',
    'SecondaryContactInfo_SecondContactsEmail',
    'SecondaryContactInfo_SecondContactsNumber',
    'SecondContactsAddress_SecondContactState',
    'SecondaryContactInfo_SecondContactsRelationship',
    'SocialSecurity_SocialSecurityNumber'
]


# General PMI answer codes
PMI_SKIP_CODE = "PMI_Skip"
PMI_PREFER_NOT_TO_ANSWER_CODE = "PMI_PreferNotToAnswer"
PMI_OTHER_CODE = "PMI_Other"
PMI_FREE_TEXT_CODE = "PMI_FreeText"
PMI_UNANSWERED_CODE = "PMI_Unanswered"
PMI_YES = "pmi_yes"

# Gender answer codes. 'GenderIdentity_MoreThanOne' is also an option, set in participant enums.
GENDER_MAN_CODE = "GenderIdentity_Man"
GENDER_WOMAN_CODE = "GenderIdentity_Woman"
GENDER_NONBINARY_CODE = "GenderIdentity_NonBinary"
GENDER_TRANSGENDER_CODE = "GenderIdentity_Transgender"
GENDER_OTHER_CODE = "GenderIdentity_AdditionalOptions"
GENDER_PREFER_NOT_TO_ANSWER_CODE = "PMI_PreferNotToAnswer"
GENDER_NO_GENDER_IDENTITY_CODE = "PMI_Skip"

# Race answer codes
RACE_AIAN_CODE = "WhatRaceEthnicity_AIAN"
RACE_ASIAN_CODE = "WhatRaceEthnicity_Asian"
RACE_BLACK_CODE = "WhatRaceEthnicity_Black"
RACE_MENA_CODE = "WhatRaceEthnicity_MENA"
RACE_NHDPI_CODE = "WhatRaceEthnicity_NHPI"
RACE_WHITE_CODE = "WhatRaceEthnicity_White"
RACE_HISPANIC_CODE = "WhatRaceEthnicity_Hispanic"
RACE_FREETEXT_CODE = "WhatRaceEthnicity_FreeText"
RACE_NONE_OF_THESE_CODE = "WhatRaceEthnicity_RaceEthnicityNoneOfThese"

WITHDRAWAL_CEREMONY_QUESTION_CODE = "withdrawalaianceremony"
WITHDRAWAL_CEREMONY_YES = "withdrawalaianceremony_yes"
WITHDRAWAL_CEREMONY_NO = "withdrawalaianceremony_no"

# Consent answer codes
CONSENT_PERMISSION_YES_CODE = "ConsentPermission_Yes"
CONSENT_PERMISSION_NO_CODE = "ConsentPermission_No"
CONSENT_PERMISSION_NOT_SURE = "ConsentPermission_NotSure"
EHR_CONSENT_EXPIRED_YES = "EHRConsentPII_ConsentExpired_Yes"
SENSITIVE_EHR_YES = "sensitivetype2__agree"

# Consent GROR Answer Codes
CONSENT_GROR_YES_CODE = "CheckDNA_Yes"
CONSENT_GROR_NO_CODE = "CheckDNA_No"
CONSENT_GROR_NOT_SURE = "CheckDNA_NotSure"

# Reconsent Answer Codes
COHORT_1_REVIEW_CONSENT_YES_CODE = "ReviewConsentAgree_Yes"
COHORT_1_REVIEW_CONSENT_NO_CODE = "ReviewConsentAgree_No"

WEAR_YES_ANSWER_CODE = "wear_yes"

# Cohort Group Code
CONSENT_COHORT_GROUP_CODE = "ConsentPII_CohortGroup"

# Consent COPE Answer Codes.  (Deferred = expressed interest in taking the survey later)
CONSENT_COPE_YES_CODE = "COPE_A_44"
CONSENT_COPE_NO_CODE = "COPE_A_13"
CONSENT_COPE_DEFERRED_CODE = "COPE_A_231"

COPE_DOSE_RECEIVED_QUESTION = 'cdc_covid_xx'
COPE_NUMBER_DOSES_QUESTION = 'cdc_covid_xx_a'
COPE_ONE_DOSE_ANSWER = 'cope_a_332'
COPE_TWO_DOSE_ANSWER = 'cope_a_333'
COPE_DOSE_TYPE_QUESTION = 'cdc_covid_xx_b'
COPE_FIRST_DOSE_QUESTION = 'cdc_covid_xx_firstdose'
COPE_FIRST_DOSE_DATE_QUESTION = 'cdc_covid_xx_a_date1'
COPE_FIRST_DOSE_TYPE_QUESTION = 'cdc_covid_xx_b_firstdose'
COPE_FIRST_DOSE_TYPE_OTHER_QUESTION = 'cdc_covid_xx_b_firstdose_other'
COPE_FIRST_DOSE_SYMPTOM_QUESTION = 'cdc_covid_xx_symptom'
COPE_FIRST_DOSE_SYMPTOM_OTHER_QUESTION = 'cdc_covid_xx_symptom_cope_350'
COPE_SECOND_DOSE_QUESTION = 'cdc_covid_xx_seconddose'
COPE_SECOND_DOSE_DATE_QUESTION = 'cdc_covid_xx_a_date2'
COPE_SECOND_DOSE_TYPE_QUESTION = 'cdc_covid_xx_b_seconddose'
COPE_SECOND_DOSE_TYPE_OTHER_QUESTION = 'cdc_covid_xx_b_seconddose_other'
COPE_SECOND_DOSE_SYMPTOM_QUESTION = 'cdc_covid_xx_symptom_seconddose'
COPE_SECOND_DOSE_SYMPTOM_OTHER_QUESTION = 'cdc_covid_xx_symptom_seconddose_cope_350'


# COPE Minute Survey Codes
COPE_VACCINE_MINUTE_1_MODULE_CODE = "cope_vaccine1"
COPE_VACCINE_MINUTE_2_MODULE_CODE = "cope_vaccine2"
COPE_VACCINE_MINUTE_3_MODULE_CODE = "cope_vaccine3"
COPE_VACCINE_MINUTE_4_MODULE_CODE = "cope_vaccine4"

# Module names for questionnaires / consent forms
CONSENT_FOR_GENOMICS_ROR_MODULE = "GROR"
CONSENT_FOR_STUDY_ENROLLMENT_MODULE = "ConsentPII"
CONSENT_FOR_ELECTRONIC_HEALTH_RECORDS_MODULE = "EHRConsentPII"
CONSENT_FOR_DVEHR_MODULE = "DVEHRSharing"
OVERALL_HEALTH_PPI_MODULE = "OverallHealth"
LIFESTYLE_PPI_MODULE = "Lifestyle"
THE_BASICS_PPI_MODULE = "TheBasics"
FAMILY_HISTORY_MODULE = "FamilyHistory"
SOCIAL_DETERMINANTS_OF_HEALTH_MODULE = "sdoh"
PERSONAL_AND_FAMILY_HEALTH_HISTORY_MODULE = 'personalfamilyhistory'
PERSONAL_MEDICAL_HISTORY_MODULE = "PersonalMedicalHistory"
MEDICATIONS_MODULE = "MedicationsPPI"
REMOTE_PM_MODULE = 'pm_height_weight'
REMOTE_PM_UNIT = 'measurement_unit'
HEALTHCARE_ACCESS_MODULE = "HealthcareAccess"
# COVID Experience surveys:
# The COPE module covers the May/June/July (2020) COPE Survey questionnaires
# A new survey was developed for November 2020
COPE_MODULE = 'COPE'
COPE_NOV_MODULE = 'cope_nov'
COPE_DEC_MODULE = "cope_dec"
COPE_FEB_MODULE = "cope_feb"
GENETIC_ANCESTRY_MODULE = 'GeneticAncestry'
LIFE_FUNCTIONING_SURVEY = 'lfs'

VA_PRIMARY_RECONSENT_C1_C2 = 'vaprimaryreconsent_c1_2'
VA_PRIMARY_RECONSENT_C3 = 'vaprimaryreconsent_c3'
VA_EHR_RECONSENT = 'vaehrreconsent'
NON_VA_PRIMARY_RECONSENT = 'nonvaprimaryreconsent'

# ConsentPII Questions
RECEIVE_CARE_STATE = "ReceiveCare_PIIState"

# ConsentPII Answers
OR_CARE_STATE = "PIIStateCare_OR"
TX_CARE_STATE = "PIIStateCare_TX"
AK_CARE_STATE = "PIIStateCare_AK"
MA_CARE_STATE = "PIIStateCare_MA"
MI_CARE_STATE = "PIIStateCare_MI"
OR_STATE = "PIIState_OR"
TX_STATE = "PIIState_TX"
AK_STATE = "PIIState_AK"
MA_STATE = "PIIState_MA"
MI_STATE = "PIIState_MI"

# DVEHR ANSWERS
DVEHRSHARING_CONSENT_CODE_YES = "DVEHRSharing_Yes"
DVEHRSHARING_CONSENT_CODE_NO = "DVEHRSharing_No"
DVEHRSHARING_CONSENT_CODE_NOT_SURE = "DVEHRSharing_NotSure"

# Genetic Ancestry Consent Answers
GENETIC_ANCESTRY_CONSENT_CODE_YES = "ConsentAncestryTraits_Yes"
GENETIC_ANCESTRY_CONSENT_CODE_NO = "ConsentAncestryTraits_No"
GENETIC_ANCESTRY_CONSENT_CODE_NOT_SURE = "ConsentAncestryTraits_NotSure"

# Digital Health Share Consents
APPLE_EHR_SHARING_MODULE = "participantintendstoshareappleehr"
APPLE_EHR_STOP_SHARING_MODULE = "participantintendstostopsharingappleehr"
APPLE_HEALTH_KIT_SHARING_MODULE = "participantintendstoshareapplehealthkit"
APPLE_HEALTH_KIT_STOP_SHARING_MODULE = "participantintendstostopsharingapplehealthkit"
FITBIT_SHARING_MODULE = "participantintendstosharefitbit"
FITBIT_STOP_SHARING_MODULE = "participantintendstostopsharingfitbit"

# General response answer codes
AGREE_YES = "agree_yes"
AGREE_NO = "agree_no"

BIOBANK_TESTS = [
    "1ED10",
    "2ED10",
    "1ED04",
    "1SST8",
    "1SS08",
    "1PST8",
    "1PS08",
    "2SST8",
    "2PST8",
    "1HEP4",
    "1UR10",
    "1UR90",
    "1SAL2",
    "1SAL",
    "1ED02",
    "1CFD9",
    "1PXR2",
]
BIOBANK_TESTS_SET = frozenset(BIOBANK_TESTS)

UNSET = "UNSET"
UNMAPPED = "UNMAPPED"
BASE_VALUES = [UNSET, UNMAPPED, PMI_SKIP_CODE]

# English and Spanish are the only accepted languages for now
LANGUAGE_OF_CONSENT = ["en", "es"]

# genome type values
GENOME_TYPE = ["aou_array", "aou_wgs"]
GC_SITE_IDs = ['rdr', 'bcm', 'jh', 'bi', 'uw']
AW1_BUCKETS = [
    'prod-genomics-baylor',
    'prod-genomics-broad',
    'prod-genomics-northwest'
]
AW2_BUCKETS = [
    'prod-genomics-data-baylor',
    'prod-genomics-data-broad',
    'prod-genomics-data-northwest'
]

# Source of a created participant
ORIGINATING_SOURCES = ['vibrent', 'careevolution', 'example']

SENSITIVE_EHR_STATES = [
    OR_CARE_STATE,
    TX_CARE_STATE,
    AK_CARE_STATE,
    MA_CARE_STATE,
    MI_CARE_STATE,
    OR_STATE,
    TX_STATE,
    AK_STATE,
    MA_STATE,
    MI_STATE
]
