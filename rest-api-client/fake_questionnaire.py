import copy
import json
import random

response_text = open("questionnaire_response_example.json").read()

def random_race():
    return random.choice([
        ['http://hl7.org/fhir/v3/Race', '1002-5', 'American Indian or Alaska Native'],
        ['http://hl7.org/fhir/v3/Race', '2054-5', 'Black or African American'],
        ['http://hl7.org/fhir/v3/Race', '2028-9', 'Asian'],
        ['http://hl7.org/fhir/v3/Race', '2076-8', 'Native Hawaiian or Other Pacific Islander'],
        ['http://hl7.org/fhir/v3/Race', '2106-3', 'White'],
        ['http://hl7.org/fhir/v3/Race', '2131-1', 'Other Race'],
        ['http://hl7.org/fhir/v3/NullFlavor', 'ASKU', 'Asked but no answer'],
    ])

def random_ethnicity():
    return random.choice([
        ['http://hl7.org/fhir/v3/Ethnicity', '2135-2', 'Hispanic or Latino'],
        ['http://hl7.org/fhir/v3/Ethnicity', '2186-5', 'Not Hispanic or Latino'],
        ['http://hl7.org/fhir/v3/NullFlavor', 'ASKU', 'Asked but no answer'],
    ])

def random_questionnaire(participant, response_time, questionnaire_id):
    q = copy.copy(response_text)
    race = random_race()
    ethnicity = random_ethnicity()
    for k,v in {
        '$questionnaire_id': questionnaire_id,
        '$participant_id': participant['participant_id'],
        '$race_code': race[0],
        '$race_system': race[1],
        '$race_display': race[2],
        '$ethnicity_code': ethnicity[0],
        '$ethnicity_system': ethnicity[1],
        '$ethnicity_display': ethnicity[2],
        '$authored': response_time
        }.iteritems():
        q = q.replace(k,v)
    print q
    return json.loads(q)
