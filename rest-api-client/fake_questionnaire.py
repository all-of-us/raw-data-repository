import copy
import json
import random

response_text = open("questionnaire_response_example.json").read()

def random_race():
    return random.choice([
        'american-indian-or-alaskan-native',
        'asian',
        'black-or-african-american',
        'native-hawaiian-or-pacific-islander',
        'white',
        'other',
        'prefer-not-to-answer'
    ])

def random_ethnicity():
    return random.choice([
        'hispanic-latino-or-spanish-origin',
        'not-hispanic-latino-or-spanish-origin',
        'prefer-not-to-answer'
    ])

def random_questionnaire(participant, response_time):
    q = copy.copy(response_text)
    for k,v in {
        '$participant_id': participant['drc_internal_id'],
        '$race_code': random_race(),
        '$ethnicity_code': random_ethnicity(),
        '$authored': response_time
        }.iteritems():
        q = q.replace(k,v)
    print q
    return json.loads(q)
