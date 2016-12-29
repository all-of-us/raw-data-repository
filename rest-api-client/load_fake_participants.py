import argparse
import copy
import datetime
import json
import sys
from client.client import Client

from faker import Faker, Factory
import random
random.seed(1)
fake = Factory.create()
fake.random.seed(1)

def random_hpo():
  return random.choice([
  "UNSET",
  "UNMAPPED",
  "PITT",
  "COLUMBIA",
  "ILLINOIS",
  "AZ_TUCSON",
  "COMM_HEALTH",
  "SAN_YSIDRO",
  "CHEROKEE",
  "EAU_CLAIRE",
  "HRHCARE",
  "JACKSON",
  "GEISINGER",
  "CAL_PMC",
  "NE_PMC",
  "TRANS_AM",
  "VA",
  ])

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

extra_ppi_modules = [
"overall-health",
"personal-habits",
"healthcare-access",
"medical-history",
"medications",
"family-health",
]

one_week = datetime.timedelta(7)
one_month = datetime.timedelta(30)
one_year = datetime.timedelta(365)

def participant():
  birth_sex = random.choice(["male", "female"])
  first_name_fn = fake.first_name_male if birth_sex == "male" else fake.first_name_female
  (first_name, middle_name, last_name) = (first_name_fn(), first_name_fn(), fake.last_name())

  hpo_id = random_hpo()
  zip_code = fake.zipcode()
  gender_identity = birth_sex
  date_of_birth = fake.date(pattern="%Y-%m-%d")
  if random.random() < 0.05:
    gender_identity = random.choice(["male", "female", "other", "female-to-male-transgender", "male-to-female-transgender"])

  membership_tier = "REGISTERED"
  sign_up_time = fake.date_time_between(start_date="2016-12-20", end_date="+1y", tzinfo=None)

  initial_participant = {
    'providerLink': [{
      'primary': True,
      'organization': {
          'reference': 'Organization/' + hpo_id
      }
    }]
  }

  if random.random() < 0.3:
    del initial_participant['providerLink']

  consent_questionnaire_time = fake.date_time_between(
          start_date=sign_up_time,
          end_date=sign_up_time + 2*one_month,
          tzinfo=None)

  sociodemographics_questionnaire_time = fake.date_time_between(
          start_date=consent_questionnaire_time,
          end_date=consent_questionnaire_time + one_month,
          tzinfo=None)

  ret = []
  race = random_race()
  ethnicity = random_ethnicity()
  ret.append({
    'when': sign_up_time.isoformat(),
    'endpoint': 'Participant',
    'payload': json.dumps(initial_participant),
    'vars':  {
        'first_name': first_name,
        'middle_name': middle_name,
        'last_name': last_name,
        'date_of_birth': date_of_birth,
        'gender_identity': gender_identity,
        'race_system': race[0],
        'race_code': race[1],
        'race_display': race[2],
        'ethnicity_system': ethnicity[0],
        'ethnicity_code': ethnicity[1],
        'ethnicity_display': ethnicity[2],
        'state': fake.state_abbr(),
        'consent_questionnaire_authored': consent_questionnaire_time.isoformat(),
        'sociodemographics_questionnaire_authored': sociodemographics_questionnaire_time.isoformat(),
    },
    'gather': {'participant_id': lambda r: r['participantId']}
  })

  ret.append({
    'when': consent_questionnaire_time.isoformat(),
    'endpoint': 'Participant/$participant_id/QuestionnaireResponse',
    'payload': open("test-data/consent_questionnaire_response.json").read()
  })

  ret.append({
    'when': sociodemographics_questionnaire_time.isoformat(),
    'endpoint': 'Participant/$participant_id/QuestionnaireResponse',
    'payload': open("test-data/sociodemographics_questionnaire_response.json").read()
  })

  for m in extra_ppi_modules:
    if random.random() < 0.5:
      when = fake.date_time_between(
              start_date=sociodemographics_questionnaire_time,
              end_date=sociodemographics_questionnaire_time + 2*one_month,
              tzinfo=None)

      ret.append({
        'when': when.isoformat(),
        'endpoint': 'Participant/$participant_id/QuestionnaireResponse',
        'vars': {
          'authored_time': when.isoformat()
        },
        'payload': """{
          "resourceType": "QuestionnaireResponse",
          "status": "completed",
          "subject": {
              "reference": "Patient/$participant_id"
          },
          "questionnaire": {
              "reference": "Questionnaire/$%s_questionnaire_id"
          },
          "authored": "$authored_time",
          "group": {}
        }"""%m,
      })

  if random.random() < 0.25:
    when = fake.date_time_between(
            start_date=sociodemographics_questionnaire_time + 2*one_month,
            end_date=sociodemographics_questionnaire_time + 12*one_month,
            tzinfo=None)

    ret.append({
      'when': when.isoformat(),
      'endpoint': 'Participant/$participant_id/QuestionnaireResponse',
      'vars': {
        'authored_time': when.isoformat()
      },
      'payload': open("test-data/consent_questionnaire_response.json").read()
    })


  return ret

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

  consent_questionnaire = json.load(open('test-data/consent_questionnaire.json'))
  consent_questionnaire_id = client.request_json('Questionnaire', 'POST', consent_questionnaire)['id']

  sociodemographics_questionnaire = json.load(open('test-data/sociodemographics_questionnaire.json'))
  sociodemographics_questionnaire_id = client.request_json('Questionnaire', 'POST', sociodemographics_questionnaire)['id']

  vars = {
    'consent_questionnaire_id': consent_questionnaire_id,
    'sociodemographics_questionnaire_id': sociodemographics_questionnaire_id,
  }

  for m in extra_ppi_modules:
    vars['%s_questionnaire_id'%m] = client.request_json('Questionnaire', 'POST', json.loads("""{
        "resourceType": "Questionnaire",
        "status": "published",
        "publisher":"fake",
        "group": {
            "concept": [{
            "system": "http://terminology.pmi-ops.org/CodeSystem/ppi-module",
            "code": "%s"
            }]
        }
    }"""%m))['id']

  for i in range(args.count):
    for details in participant():
      when = details['when']
      endpoint = details['endpoint']
      payload = details['payload']
      vars.update(details.get('vars', {}))
      for k,v in vars.iteritems():
        payload = payload.replace('$%s'%k, str(v))
        endpoint = endpoint.replace('$%s'%k, str(v))
      payload = json.loads(payload)
      response = client.request_json(endpoint, 'POST', payload, headers={'X-Pretend-Date': when})
      for k,f in details.get('gather', {}).iteritems():
        vars[k] = f(response)

if __name__ == '__main__':
  main()
