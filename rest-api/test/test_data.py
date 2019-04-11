"""Utilities for loading test data."""

import datetime
import json
import os
import random

from code_constants import PPI_SYSTEM, CONSENT_FOR_STUDY_ENROLLMENT_MODULE, BIOBANK_TESTS
from code_constants import FIRST_NAME_QUESTION_CODE, LAST_NAME_QUESTION_CODE, EMAIL_QUESTION_CODE, \
  LOGIN_PHONE_NUMBER_QUESTION_CODE
from model.code import Code, CodeType
from model.config_utils import to_client_biobank_id
from model.utils import to_client_participant_id

def consent_code():
  return Code(system=PPI_SYSTEM, value=CONSENT_FOR_STUDY_ENROLLMENT_MODULE,
              mapped=True, codeType=CodeType.MODULE)

def first_name_code():
  return Code(system=PPI_SYSTEM, value=FIRST_NAME_QUESTION_CODE,
              mapped=True, codeType=CodeType.QUESTION)

def last_name_code():
  return Code(system=PPI_SYSTEM, value=LAST_NAME_QUESTION_CODE,
              mapped=True, codeType=CodeType.QUESTION)

def email_code():
  return Code(system=PPI_SYSTEM, value=EMAIL_QUESTION_CODE,
              mapped=True, codeType=CodeType.QUESTION)

def login_phone_number_code():
  return Code(system=PPI_SYSTEM, value=LOGIN_PHONE_NUMBER_QUESTION_CODE,
              mapped=True, codeType=CodeType.QUESTION)

def data_path(filename):
  return os.path.join(os.path.dirname(__file__), 'test-data', filename)


def load_measurement_json(participant_id, now=None, alternate=False):
  """Loads a PhysicalMeasurement FHIR resource returns it as parsed JSON.
     If alternate is True, loads a different measurement order. Useful for making multiple
     orders to test against when cancelling/restoring. The alternate has less measurements and
     different processed sites and finalized sites."""
  if alternate:
    payload = 'alternate-measurements-as-fhir.json'
  else:
    payload = 'measurements-as-fhir.json'
  with open(data_path(payload)) as measurements_file:
    json_text = measurements_file.read() % {
      'participant_id': participant_id,
      'authored_time': now or datetime.datetime.now().isoformat(),
    }
    return json.loads(json_text)  # deserialize to validate


def load_measurement_json_amendment(participant_id, amended_id, now=None):
  """Loads a PhysicalMeasurement FHIR resource and adds an amendment extension."""
  with open(data_path('measurements-as-fhir-amendment.json')) as amendment_file:
    extension = json.loads(amendment_file.read() % {
          'physical_measurement_id': amended_id,
        })
  with open(data_path('measurements-as-fhir.json')) as measurements_file:
    measurement = json.loads(measurements_file.read() % {
          'participant_id': participant_id,
          'authored_time': now or datetime.datetime.now().isoformat(),
        })
  measurement['entry'][0]['resource'].update(extension)
  return measurement


def load_biobank_order_json(participant_id, filename='biobank_order_1.json'):
  with open(data_path(filename)) as f:
    return json.loads(f.read() % {
      'participant_id': participant_id,
      'client_participant_id': to_client_participant_id(participant_id),
    })


def open_biobank_samples(biobank_ids, tests):
  """
  Returns a string representing the biobank samples CSV file. The number of records returned
  is equal to the number of biobank_ids passed.
  :param biobank_ids: list of biobank ids.
  :param tests: list of tests
  :return: StringIO object
  """
  nids = len(biobank_ids)
  # get the same number of sample lines as biobank_ids, plus header line.
  lines = open(data_path('biobank_samples_1.csv')).readlines()[:nids+1]
  csv_str = lines[0]  # include header line every time.

  for x in range(0, nids):
    # if we don't have a test code for this index, use a random one.
    try:
      test_code = tests[x]
    except IndexError:
      test_code = random.choice(BIOBANK_TESTS)

    csv_str += lines[x+1].format(biobank_id=to_client_biobank_id(biobank_ids[x]), test=test_code)

  return csv_str

def open_genomic_set_file():
  """
  Returns a string representing the genomic set CSV file.
  :return: StringIO object
  """

  lines = open(data_path('Genomic-Test-Set-test-1.csv')).readlines()
  csv_str = ''
  for line in lines:
    csv_str += line

  return csv_str

def load_questionnaire_response_with_consents(
      questionnaire_id,
      participant_id,
      first_name_link_id,
      last_name_link_id,
      email_address_link_id,
      consent_pdf_paths):
  """Loads a consent QuestionnaireResponse and adds >= 1 consent PDF extensions."""
  # PDF paths are expected to be something like "/Participant/P550613540/ConsentPII__8270.pdf".
  assert len(consent_pdf_paths) >= 1
  with open(data_path('questionnaire_response_consent.json')) as f:
    resource = json.loads(f.read() % {
      'questionnaire_id': questionnaire_id,
      'participant_client_id': to_client_participant_id(participant_id),
      'first_name_link_id': first_name_link_id,
      'last_name_link_id': last_name_link_id,
      'email_address_link_id': email_address_link_id,
    })
  for path in consent_pdf_paths:
    resource['extension'].append({
      'url': 'http://terminology.pmi-ops.org/StructureDefinition/consent-form-signed-pdf',
      'valueString': path,
    })
  return resource


def load_test_data_json(filename):
  with open(data_path(filename)) as handle:
    return json.load(handle)
