import argparse
import copy
import datetime
import json
import sys
from client.client import Client
from fake_questionnaire import random_questionnaire

from faker import Faker, Factory
import random
random.seed(1)
fake = Factory.create()
fake.random.seed(1)


two_months = datetime.timedelta(60)
one_year = datetime.timedelta(365)

hpo_ids = (
    'pitt',        # Pitt/UPMC
    'columbia',    # Columbia University Medical Center
    'illinois',    # Illinois Precision Medicine Consortium
    'az_tucson',   # University of Arizona, Tucson
    'comm_health', # Community Health Center
    'san_ysidro',  # San Ysidro health Center, Inc.
    'cherokee',    # Cherokee Health Systems
    'eau_claire',  # Eau Claire Cooperative Health Centers, Inc
    'hrhcare',     # HRHCare (Hudson River Healthcare)
    'jackson',     # Jackson-Hinds Comprehensive Health Center
    'geisinger',   # Geisinger Health System
    'cal_pmc',     # California Precision Medicine Consortium
    'ne_pmc',      # New England Precision Medicine Consortium
    'trans_am',    # Trans-American Consortium for the Health Care Systems Research Network
    'va',          # Veterans Affairs
)

def participant():
    birth_sex = random.choice(["MALE", "FEMALE"])
    first_name_fn = fake.first_name_male if birth_sex == "MALE" else fake.first_name_female
    (first_name, middle_name, last_name) = (first_name_fn(), first_name_fn(), fake.last_name())

    hpo_id = random.choice(hpo_ids)
    zip_code = fake.zipcode()
    gender_identity = birth_sex
    date_of_birth = fake.date(pattern="%Y-%m-%d")
    if random.random() < 0.05:
        gender_identity = random.choice(["MALE", "FEMALE", "OTHER", "PREFER_NOT_TO_SAY"])

    membership_tier = "REGISTERED"
    sign_up_time = fake.date_time_between(start_date="2016-11-15", end_date="+1y", tzinfo=None)

    initial_participant = {
        'date_of_birth': date_of_birth,
        'sign_up_time': sign_up_time.isoformat(),
        'gender_identity': gender_identity,
        'membership_tier': membership_tier,
        'recruitment_source': 'HPO',
        'hpo_id': hpo_id,
        'zip_code': zip_code,
    }

    if random.random() < 0.3:
      del initial_participant['hpo_id']
      initial_participant['recruitment_source'] = 'DIRECT_VOLUNTEER'

    consented_time = fake.date_time_between(start_date=sign_up_time, end_date=sign_up_time + two_months, tzinfo=None)
    consented_participant = copy.deepcopy(initial_participant)
    consented_participant['consent_time'] =  consented_time.isoformat()
    consented_participant['membership_tier'] =  'VOLUNTEER'

    engaged_time = fake.date_time_between(start_date=consented_time, end_date=consented_time + one_year, tzinfo=None)
    engaged_participant = copy.deepcopy(consented_participant)
    engaged_participant['membership_tier'] =  'FULL_PARTICIPANT'

    questionnaire_time = fake.date_time_between(start_date=sign_up_time, end_date=sign_up_time + one_year, tzinfo=None)
    return {
        'participant': [(initial_participant, sign_up_time.isoformat()),
                        (consented_participant, consented_time.isoformat()),
                        (engaged_participant, engaged_time.isoformat())],
            'questionnaire_time': questionnaire_time.isoformat()
    }


def parse_args(default_instance=None):
  parser = argparse.ArgumentParser()
  parser.add_argument(
      '--count',
      type=int,
      help='The number of example participants to create',
      default=10)

  parser.add_argument(
      '--instance',
      type=str,
      help='The instance to hit, either https://xxx.appspot.com, '
      'or http://localhost:8080',
      default=default_instance)
  return parser.parse_args()


def main():
  args = parse_args()
  client = Client('rdr/v1', default_instance=args.instance, parse_cli=False)

  questionnaire = json.load(open('test-data/questionnaire_example.json'))
  q_id = client.request_json('Questionnaire', 'POST', questionnaire)['id']

  for i in range(args.count):
    details = participant()
    participant_calls = details['participant']

    p, when = participant_calls[0]
    response = client.request_json('Participant', 'POST', p, headers={'X-Pretend-Date': when})
    participant_id = response['participant_id']
    for p, when in participant_calls[1:]:
      client.request_json('Participant/{}'.format(participant_id), 'PATCH', p, 
                          headers={'X-Pretend-Date': when, 'If-Match': client.last_etag})

    q = random_questionnaire(response, details['questionnaire_time'], q_id)
    q_response = client.request_json(
        'Participant/{}/QuestionnaireResponse'.format(participant_id),
        'POST',
        q,
        headers={'X-Pretend-Date': details['questionnaire_time']})

    print(q_response)

if __name__ == '__main__':
  main()
