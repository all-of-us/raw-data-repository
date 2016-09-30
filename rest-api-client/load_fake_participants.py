import argparse
import datetime
import copy
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
    "nyc",
    "chicago",
    "tucson",
    "pittsburgh",
    "knoxbille",
    "middletown",
    "peekskill",
    "jackson",
    "san-ysidro"
)

def participant():
    birth_sex = random.choice(["MALE", "FEMALE"])
    first_name_fn = fake.first_name_male if birth_sex == "MALE" else fake.first_name_female
    (first_name, middle_name, last_name) = (first_name_fn(), first_name_fn(), fake.last_name())

    hpo_id = random.choice(hpo_ids)
    gender_identity = birth_sex
    date_of_birth = fake.date(pattern="%Y-%m-%d")
    if random.random() < 0.05:
        gender_identity = random.choice(["MALE", "FEMALE", "NEITHER", "OTHER", "PREFER_NOT_TO_SAY"])

    membership_tier = "INTERESTED"
    sign_up_time = fake.date_time_between(start_date="2016-11-15", end_date="+1y", tzinfo=None)

    initial_participant = {
        'date_of_birth': date_of_birth,
        'sign_up_time': sign_up_time.isoformat(),
        'gender_identity': gender_identity,
        'membership_tier': membership_tier,
        'recruitment_source': 'HPO',
        'hpo_id': hpo_id
    }

    if random.random() < 0.3:
      del initial_participant['hpo_id']
      initial_participant['recruitment_source'] = 'DIRECT_VOLUNTEER'

    consent_time = fake.date_time_between(start_date=sign_up_time, end_date=sign_up_time + two_months, tzinfo=None)
    consented_participant = copy.deepcopy(initial_participant)
    consented_participant['consent_time'] =  consent_time.isoformat()
    consented_participant['membership_tier'] =  'CONSENTED'

    engaged_time = fake.date_time_between(start_date=consent_time, end_date=consent_time + one_year, tzinfo=None)
    engaged_participant = copy.deepcopy(consented_participant)
    engaged_participant['membership_tier'] =  'ENGAGED'

    questionnaire_time = fake.date_time_between(start_date=sign_up_time, end_date=sign_up_time + one_year, tzinfo=None)
    return {
            'participant': [initial_participant, consented_participant, engaged_participant],
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
  client = Client('', default_instance=args.instance, parse_cli=False)
  for i in range(args.count):
    details = participant()
    participant_calls = details['participant']

    response = client.request_json('participant/v1/participants', 'POST', participant_calls[0])
    drc_internal_id = response['drc_internal_id']
    for p in participant_calls[1:3]:
      client.request_json('participant/v1/participants/{}'.format(drc_internal_id), 'PATCH', p)

    q = random_questionnaire(response, details['questionnaire_time'])
    q_response = client.request_json('ppi/fhir/QuestionnaireResponse', 'POST', q)
    print("Q response")
    print(q_response)

if __name__ == '__main__':
  main()
