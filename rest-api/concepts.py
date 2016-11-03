"""A single place for FHIR concepts."""

from collections import namedtuple
from census_regions import census_regions

Concept = namedtuple('Concept', ['system', 'code'])

SYSTEM_LOINC = 'http://loinc.org'
_SYSTEM_FHIR_NULL = 'http://hl7.org/fhir/v3/NullFlavor'
_SYSTEM_PMI_BASE = 'http://terminology.pmi-ops.org/CodeSystem/'

ASKED_BUT_NO_ANSWER = Concept(_SYSTEM_FHIR_NULL, 'ASKU')
PREFER_NOT_TO_SAY = Concept(_SYSTEM_FHIR_NULL, 'ASKU')

# Used in the questionnaire response.
ETHNICITY = Concept(SYSTEM_LOINC, '69490-1')
HISPANIC = Concept('http://hl7.org/fhir/v3/Ethnicity', '2135-2')
NON_HISPANIC = Concept('http://hl7.org/fhir/v3/Ethnicity', '2186-5')

RACE = Concept(SYSTEM_LOINC, '72826-1')
_SYSTEM_RACE = 'http://hl7.org/fhir/v3/Race'
AMERICAN_INDIAN_OR_ALASKA_NATIVE = Concept(_SYSTEM_RACE, '1002-5')
BLACK_OR_AFRICAN_AMERICAN = Concept(_SYSTEM_RACE, '2054-5')
ASIAN = Concept(_SYSTEM_RACE, '2028-9')
NATIVE_HAWAIIAN_OR_OTHER_PACIFIC_ISLANDER = Concept(_SYSTEM_RACE, '2076-8')
WHITE = Concept(_SYSTEM_RACE, '2106-3')
OTHER_RACE = Concept(_SYSTEM_RACE, '2131-1')


GENDER_IDENTITY = Concept(SYSTEM_LOINC, '76691-5')

_SYSTEM_GENDER_IDENTITY = _SYSTEM_PMI_BASE + 'gender-identity'
FEMALE = Concept(_SYSTEM_GENDER_IDENTITY, 'female')
FEMALE_TO_MALE_TRANSGENDER = Concept(_SYSTEM_GENDER_IDENTITY, 'female-to-male-transgender')
MALE = Concept(_SYSTEM_GENDER_IDENTITY, 'male')
MALE_TO_FEMALE_TRANSGENDER = Concept(_SYSTEM_GENDER_IDENTITY, 'male-to-female-transgender')
INTERSEX = Concept(_SYSTEM_GENDER_IDENTITY, 'intersex')
OTHER = Concept(_SYSTEM_GENDER_IDENTITY, 'other')


STATE_OF_RESIDENCE = Concept(SYSTEM_LOINC, '46499-0')

_SYSTEM_STATE = _SYSTEM_PMI_BASE + 'us-state'
STATE_LIST = [Concept(_SYSTEM_STATE, s) for s in census_regions.keys()]
STATES_BY_ABBREV = {c.code:c for c in STATE_LIST}


# Used in the evaluation.
SYSTOLIC_BP = Concept(SYSTEM_LOINC, '8480-6')
DIASTOLIC_BP = Concept(SYSTEM_LOINC, '8462-4')
HEART_RATE = Concept(SYSTEM_LOINC, '8867-4')
WEIGHT = Concept(SYSTEM_LOINC, '29463-7')
BMI = Concept(SYSTEM_LOINC, '39156-5')
HIP_CIRCUMFERENCE = Concept(SYSTEM_LOINC, '62409-8')
WAIST_CIRCUMFERENCE = Concept(SYSTEM_LOINC, '56086-2')
