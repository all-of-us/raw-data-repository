"""Utilities for loading test data."""

import StringIO
import datetime
import json
import os
import random

from code_constants import PPI_SYSTEM, CONSENT_FOR_STUDY_ENROLLMENT_MODULE, BIOBANK_TESTS
from code_constants import FIRST_NAME_QUESTION_CODE, LAST_NAME_QUESTION_CODE, EMAIL_QUESTION_CODE
from model.code import Code, CodeType
from model.utils import to_client_participant_id, to_client_biobank_id

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

def data_path(filename):
  return os.path.join(os.path.dirname(__file__), 'test-data', filename)


def load_measurement_json(participant_id, now=None):
  """Loads a PhysicalMeasurement FHIR resource returns it as parsed JSON."""
  with open(data_path('measurements-as-fhir.json')) as measurements_file:
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


def open_biobank_samples(
      biobank_id1, biobank_id2, biobank_id3,
      test1=None, test2=None, test3=None):
  """Returns an readable stream for the biobank samples CSV."""
  with open(data_path('biobank_samples_1.csv')) as f:
    csv_str = f.read() % {
      'biobank_id1': to_client_biobank_id(biobank_id1),
      'biobank_id2': to_client_biobank_id(biobank_id2),
      'biobank_id3': to_client_biobank_id(biobank_id3),
      'test1': test1 or random.choice(BIOBANK_TESTS),
      'test2': test2 or random.choice(BIOBANK_TESTS),
      'test3': test3 or random.choice(BIOBANK_TESTS),
    }
  return StringIO.StringIO(csv_str)


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
