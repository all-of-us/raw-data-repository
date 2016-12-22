"""A single place for FHIR concepts."""

from collections import namedtuple
from census_regions import census_regions

Concept = namedtuple('Concept', ['system', 'code'])


SYSTEM_CONSENT_FORM = "http://terminology.pmi-ops.org/CodeSystem/consent-form"
ENROLLMENT_CONSENT_FORM = Concept(SYSTEM_CONSENT_FORM, "enrollment")
ELECTRONIC_HEALTH_RECORDS_CONSENT_FORM = Concept(SYSTEM_CONSENT_FORM, "electronic-health-records")

SYSTEM_PPI_MODULE = "http://terminology.pmi-ops.org/CodeSystem/ppi-module"
OVERALL_HEALTH_PPI_MODULE = Concept(SYSTEM_PPI_MODULE, "overall-health")
PERSONAL_HABITS_PPI_MODULE = Concept(SYSTEM_PPI_MODULE, "personal-habits")
SOCIODEMOGRAPHICS_PPI_MODULE = Concept(SYSTEM_PPI_MODULE, "sociodemographics")
HEALTHCARE_ACCESS_PPI_MODULE = Concept(SYSTEM_PPI_MODULE, "healthcare-access")
MEDICAL_HISTORY_PPI_MODULE = Concept(SYSTEM_PPI_MODULE, "medical-history")
MEDICATIONS_PPI_MODULE = Concept(SYSTEM_PPI_MODULE, "medications")
FAMILY_HEALTH_PPI_MODULE = Concept(SYSTEM_PPI_MODULE, "family-health")

SYSTEM_EVALUATION = "http://terminology.pmi-ops.org/CodeSystem/document-type"
EVALUATION_CONCEPT_CODE_PREFIX = "intake-exam-v"

SYSTEM_LOINC = 'http://loinc.org'
SYSTEM_FHIR_NULL = 'http://hl7.org/fhir/v3/NullFlavor'
SYSTEM_PMI_BASE = 'http://terminology.pmi-ops.org/CodeSystem/'

SYSTEM_UNIT_OF_MEASURE = 'http://unitsofmeasure.org'

ASKED_BUT_NO_ANSWER = Concept(SYSTEM_FHIR_NULL, 'ASKU')
PREFER_NOT_TO_SAY = Concept(SYSTEM_FHIR_NULL, 'ASKU')

# Used in the questionnaire response.
ETHNICITY = Concept(SYSTEM_LOINC, '69490-1')
HISPANIC = Concept('http://hl7.org/fhir/v3/Ethnicity', '2135-2')
NON_HISPANIC = Concept('http://hl7.org/fhir/v3/Ethnicity', '2186-5')

RACE = Concept(SYSTEM_LOINC, '72826-1')
SYSTEM_RACE = 'http://hl7.org/fhir/v3/Race'
AMERICAN_INDIAN_OR_ALASKA_NATIVE = Concept(SYSTEM_RACE, '1002-5')
BLACK_OR_AFRICAN_AMERICAN = Concept(SYSTEM_RACE, '2054-5')
ASIAN = Concept(SYSTEM_RACE, '2028-9')
NATIVE_HAWAIIAN_OR_OTHER_PACIFIC_ISLANDER = Concept(SYSTEM_RACE, '2076-8')
WHITE = Concept(SYSTEM_RACE, '2106-3')
OTHER_RACE = Concept(SYSTEM_RACE, '2131-1')


GENDER_IDENTITY = Concept(SYSTEM_LOINC, '76691-5')

SYSTEM_GENDER_IDENTITY = SYSTEM_PMI_BASE + 'gender-identity'
FEMALE = Concept(SYSTEM_GENDER_IDENTITY, 'female')
FEMALE_TO_MALE_TRANSGENDER = Concept(SYSTEM_GENDER_IDENTITY, 'female-to-male-transgender')
MALE = Concept(SYSTEM_GENDER_IDENTITY, 'male')
MALE_TO_FEMALE_TRANSGENDER = Concept(SYSTEM_GENDER_IDENTITY, 'male-to-female-transgender')
INTERSEX = Concept(SYSTEM_GENDER_IDENTITY, 'intersex')
OTHER = Concept(SYSTEM_GENDER_IDENTITY, 'other')

SYSTEM_PPI_QUESTION = SYSTEM_PMI_BASE + 'ppi-question'

MEMBERSHIP_TIER = Concept(SYSTEM_PMI_BASE, 'membership-tier')

SYSTEM_MEMBERSHIP_TIER = SYSTEM_PMI_BASE + 'membership-tier'
REGISTERED = Concept(SYSTEM_MEMBERSHIP_TIER, 'registered')
VOLUNTEER = Concept(SYSTEM_MEMBERSHIP_TIER, 'volunteer')
FULL_PARTICIPANT = Concept(SYSTEM_MEMBERSHIP_TIER, 'full-participant')
ENROLLEE = Concept(SYSTEM_MEMBERSHIP_TIER, 'enrollee')


DATE_OF_BIRTH = Concept(SYSTEM_PPI_QUESTION, 'date-of-birth')
FIRST_NAME = Concept(SYSTEM_PPI_QUESTION, 'first-name')
MIDDLE_NAME = Concept(SYSTEM_PPI_QUESTION, 'middle-name')
LAST_NAME = Concept(SYSTEM_PPI_QUESTION, 'last-name')

STATE_OF_RESIDENCE = Concept(SYSTEM_LOINC, '46499-0')

SYSTEM_STATE = SYSTEM_PMI_BASE + 'us-state'
STATE_LIST = [Concept(SYSTEM_STATE, s) for s in census_regions.keys()]
STATES_BY_ABBREV = {c.code:c for c in STATE_LIST}


# Used in the evaluation.
SYSTOLIC_BP = Concept(SYSTEM_LOINC, '8480-6')
DIASTOLIC_BP = Concept(SYSTEM_LOINC, '8462-4')
HEART_RATE = Concept(SYSTEM_LOINC, '8867-4')
WEIGHT = Concept(SYSTEM_LOINC, '29463-7')
BMI = Concept(SYSTEM_LOINC, '39156-5')
HIP_CIRCUMFERENCE = Concept(SYSTEM_LOINC, '62409-8')
WAIST_CIRCUMFERENCE = Concept(SYSTEM_LOINC, '56086-2')



#UNITS
UNIT_MM_HG = Concept(SYSTEM_UNIT_OF_MEASURE, 'mm[Hg]')
UNIT_KG = Concept(SYSTEM_UNIT_OF_MEASURE, 'kg')
UNIT_CM = Concept(SYSTEM_UNIT_OF_MEASURE, 'cm')
UNIT_PER_MIN = Concept(SYSTEM_UNIT_OF_MEASURE, '/min')
UNIT_KG_M2 = Concept(SYSTEM_UNIT_OF_MEASURE, 'kg/m2')
