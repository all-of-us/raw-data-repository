"""Create fake participant data for testing."""

import datetime
import json
import pprint
import random
random.seed(1)

from faker import Faker, Factory
fake = Factory.create()
fake.random.seed(1)

_ONE_MONTH = datetime.timedelta(30)

_EXTRA_PPI_MODULES = [
    "overall-health",
    "personal-habits",
    "healthcare-access",
    "medical-history",
    "medications",
    "family-health",
]


def _random_hpo():
  return random.choice((
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
  ))


def _random_race():
  return random.choice((
      ['http://hl7.org/fhir/v3/Race', '1002-5', 'American Indian or Alaska Native'],
      ['http://hl7.org/fhir/v3/Race', '2054-5', 'Black or African American'],
      ['http://hl7.org/fhir/v3/Race', '2028-9', 'Asian'],
      ['http://hl7.org/fhir/v3/Race', '2076-8', 'Native Hawaiian or Other Pacific Islander'],
      ['http://hl7.org/fhir/v3/Race', '2106-3', 'White'],
      ['http://hl7.org/fhir/v3/Race', '2131-1', 'Other Race'],
      ['http://hl7.org/fhir/v3/NullFlavor', 'ASKU', 'Asked but no answer'],
  ))


def _random_ethnicity():
  return random.choice((
      ['http://hl7.org/fhir/v3/Ethnicity', '2135-2', 'Hispanic or Latino'],
      ['http://hl7.org/fhir/v3/Ethnicity', '2186-5', 'Not Hispanic or Latino'],
      ['http://hl7.org/fhir/v3/NullFlavor', 'ASKU', 'Asked but no answer'],
  ))


def create_fake_participant():
  """Creates data to use to create fake participants via the API.

  The returned dictionary contains:
    endpoint: The API endpoint to POST data to to create some part of participant data, either
        creating a participant or registering a questionnaire response.
    payload: Deserialized JSON to send as the request payload.
    vars: Arbitrary key/value pairs, to substitute into payload and endpoint.
    when: A fake "now" timestamp to send as X-Pretend-Date.
  """
  birth_sex = random.choice(["male", "female"])
  first_name_fn = fake.first_name_male if birth_sex == "male" else fake.first_name_female
  (first_name, middle_name, last_name) = (first_name_fn(), first_name_fn(), fake.last_name())

  hpo_id = _random_hpo()
  zip_code = fake.zipcode()
  gender_identity = birth_sex
  date_of_birth = fake.date(pattern="%Y-%m-%d")
  if random.random() < 0.05:
    gender_identity = random.choice([
        "male", "female", "other", "female-to-male-transgender", "male-to-female-transgender"])

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
      end_date=sign_up_time + 2 * _ONE_MONTH,
      tzinfo=None)

  sociodemographics_questionnaire_time = fake.date_time_between(
      start_date=consent_questionnaire_time,
      end_date=consent_questionnaire_time + _ONE_MONTH,
      tzinfo=None)

  ret = []
  race = _random_race()
  ethnicity = _random_ethnicity()
  ret.append({
    'when': sign_up_time.isoformat(),
    'endpoint': 'Participant',
    'payload': initial_participant,
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
        'sociodemographics_questionnaire_authored':
            sociodemographics_questionnaire_time.isoformat(),
    },
    'gather': {'participant_id': lambda r: r['participantId']}
  })

  ret.append({
    'when': consent_questionnaire_time.isoformat(),
    'endpoint': 'Participant/$participant_id/QuestionnaireResponse',
    'payload': json.load(open("test-data/consent_questionnaire_response.json"))
  })

  ret.append({
    'when': sociodemographics_questionnaire_time.isoformat(),
    'endpoint': 'Participant/$participant_id/QuestionnaireResponse',
    'payload': json.load(open("test-data/sociodemographics_questionnaire_response.json"))
  })

  for m in _EXTRA_PPI_MODULES:
    if random.random() < 0.5:
      when = fake.date_time_between(
              start_date=sociodemographics_questionnaire_time,
              end_date=sociodemographics_questionnaire_time + 2 * _ONE_MONTH,
              tzinfo=None)

      ret.append({
        'when': when.isoformat(),
        'endpoint': 'Participant/$participant_id/QuestionnaireResponse',
        'vars': {
          'authored_time': when.isoformat(),
        },
        'payload': {
          'resourceType': 'QuestionnaireResponse',
          'status': 'completed',
          'subject': {
              'reference': 'Patient/$participant_id',
          },
          'questionnaire': {
              'reference': 'Questionnaire/$%s_questionnaire_id' % m,
          },
          'authored': '$authored_time',
          'group': {},
        },
      })

  if random.random() < 0.4:
    when = fake.date_time_between(
        start_date=sociodemographics_questionnaire_time + 2 * _ONE_MONTH,
        end_date=sociodemographics_questionnaire_time + 12 * _ONE_MONTH,
        tzinfo=None)

    ret.append({
      'when': when.isoformat(),
      'endpoint': 'Participant/$participant_id/PhysicalEvaluation',
      'vars': {
        'authored_time': when.isoformat(),
      },
      'payload': json.load(open("test-data/evaluation-as-fhir.json")),
    })

  return ret


if __name__ == '__main__':
  pprint.PrettyPrinter().pprint(create_fake_participant())
