"""A single place for FHIR concepts."""

from collections import namedtuple
from census_regions import census_regions

Concept = namedtuple('Concept', ['system', 'code'])

SYSTEM_LOINC = 'http://loinc.org'

ASKED_BUT_NO_ANSWER = Concept('http://hl7.org/fhir/v3/NullFlavor', 'ASKU')


# Used in the questionnaire response.
ETHNICITY = Concept(SYSTEM_LOINC, '69490-1')
HISPANIC = Concept('http://hl7.org/fhir/v3/Ethnicity', '2135-2')
NON_HISPANIC = Concept('http://hl7.org/fhir/v3/Ethnicity', '2186-5')

RACE = Concept(SYSTEM_LOINC, '72826-1')
AMERICAN_INDIAN_OR_ALASKA_NATIVE = Concept('http://hl7.org/fhir/v3/Race', '1002-5')
BLACK_OR_AFRICAN_AMERICAN = Concept('http://hl7.org/fhir/v3/Race', '2054-5')
ASIAN = Concept('http://hl7.org/fhir/v3/Race', '2028-9')
NATIVE_HAWAIIAN_OR_OTHER_PACIFIC_ISLANDER = Concept('http://hl7.org/fhir/v3/Race', '2076-8')
WHITE = Concept('http://hl7.org/fhir/v3/Race', '2106-3')
OTHER_RACE = Concept('http://hl7.org/fhir/v3/Race', '2131-1')
ASKED_BUT_NO_ANSWER = Concept('http://hl7.org/fhir/v3/NullFlavor', 'ASKU')


GENDER_IDENTITY = Concept(SYSTEM_LOINC, '76691-5')
FEMALE = Concept('http://terminology.pmi-ops.org/ppi/gender-identity', 'female')
FEMALE_TO_MALE_TRANSGENDER = Concept('http://terminology.pmi-ops.org/ppi/gender-identity',
                                     'female-to-male-transgender')
MALE = Concept('http://terminology.pmi-ops.org/ppi/gender-identity', 'male')
MALE_TO_FEMALE_TRANSGENDER = Concept('http://terminology.pmi-ops.org/ppi/gender-identity',
                                     'male-to-female-transgender')
INTERSEX = Concept('http://terminology.pmi-ops.org/ppi/gender-identity', 'intersex')
OTHER = Concept('http://terminology.pmi-ops.org/ppi/gender-identity', 'other')
PREFER_NOT_TO_SAY = Concept('http://hl7.org/fhir/v3/NullFlavor', 'ASKU')

STATE_OF_RESIDENCE = Concept(SYSTEM_LOINC, '46499-0')

STATE_LIST = [Concept('http://terminology.pmi-ops.org/ppi/state', s) for s in census_regions.keys()]
STATES_BY_ABBREV = {c.code:c for c in STATE_LIST}


# Used in the evaluation.
SYSTOLIC_BP = Concept(SYSTEM_LOINC, '8480-6')
DIASTOLIC_BP = Concept(SYSTEM_LOINC, '8462-4')
HEART_RATE = Concept(SYSTEM_LOINC, '8867-4')
WEIGHT = Concept(SYSTEM_LOINC, '29463-7')
BMI = Concept(SYSTEM_LOINC, '39156-5')
HIP_CIRCUMFERENCE = Concept(SYSTEM_LOINC, '62409-8')
WAIST_CIRCUMFERENCE = Concept(SYSTEM_LOINC, '56086-2')
