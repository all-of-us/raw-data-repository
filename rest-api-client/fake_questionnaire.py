import copy
import random

example = {
    "resourceType": "QuestionnaireResponse",
    "status": "completed",
    "subject": {
        "reference": "Patient/{participant_id}"
        },
    "questionnaire": {
        "reference": "Questionnaire/{questionnaire_id}"
        },
    "authored": "2013-02-19T14:15:00+10:00",

    "group": {
        "question": [
            {
                "linkId": "race",
                "text": "What is your race?",
                "answer": [
                    {
                        "valueCoding": {
                            "code": "asian"
                            }
                        }
                    ]
                }, {
                    "linkId": "ethnicity",
                    "text": "What is your ethnicity?",
                    "answer": [
                        {
                            "valueCoding": {
                                "code": "hispanic"
                                }
                            }
                        ]
                    }
                ]
        }
    }

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
    ret = copy.deepcopy(example)
    ret['subject']['reference'] = 'Patient/{}'.format(participant['drc_internal_id'])
    ret['group']['question'][0]['answer'][0]['valueCoding']['code'] = random_race()
    ret['group']['question'][1]['answer'][0]['valueCoding']['code'] = random_ethnicity()
    ret['authored'] = response_time
    return ret
