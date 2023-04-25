"""Utilities for loading test data."""
import csv
import datetime
from itertools import cycle
import json
import os

from rdr_service.code_constants import (
    CONSENT_FOR_STUDY_ENROLLMENT_MODULE,
    EMAIL_QUESTION_CODE,
    FIRST_NAME_QUESTION_CODE,
    LAST_NAME_QUESTION_CODE,
    LOGIN_PHONE_NUMBER_QUESTION_CODE,
    PPI_SYSTEM,
    COPE_CONSENT_QUESTION_CODE
)
from rdr_service.model.code import Code, CodeType
from rdr_service.model.config_utils import to_client_biobank_id
from rdr_service.model.utils import to_client_participant_id


def consent_code():
    return Code(system=PPI_SYSTEM, value=CONSENT_FOR_STUDY_ENROLLMENT_MODULE, mapped=True, codeType=CodeType.MODULE)


def first_name_code():
    return Code(system=PPI_SYSTEM, value=FIRST_NAME_QUESTION_CODE, mapped=True, codeType=CodeType.QUESTION)


def last_name_code():
    return Code(system=PPI_SYSTEM, value=LAST_NAME_QUESTION_CODE, mapped=True, codeType=CodeType.QUESTION)


def email_code():
    return Code(system=PPI_SYSTEM, value=EMAIL_QUESTION_CODE, mapped=True, codeType=CodeType.QUESTION)


def cope_consent_code():
    return Code(system=PPI_SYSTEM, value=COPE_CONSENT_QUESTION_CODE, mapped=True, codeType=CodeType.QUESTION)


def login_phone_number_code():
    return Code(system=PPI_SYSTEM, value=LOGIN_PHONE_NUMBER_QUESTION_CODE, mapped=True, codeType=CodeType.QUESTION)


def data_path(filename, test_dir=None, data_dir=None):
    if not test_dir:
        test_dir = os.path.dirname(__file__)
    if not data_dir:
        data_dir = 'test-data'
    return os.path.join(test_dir, data_dir, filename)


def load_measurement_json(participant_id, now=None, alternate=False):
    """Loads a PhysicalMeasurement FHIR resource returns it as parsed JSON.
     If alternate is True, loads a different measurement order. Useful for making multiple
     orders to test against when cancelling/restoring. The alternate has less measurements and
     different processed sites and finalized sites."""
    if alternate:
        payload = "alternate-measurements-as-fhir.json"
    else:
        payload = "measurements-as-fhir.json"
    with open(data_path(payload)) as measurements_file:
        json_text = measurements_file.read() % {
            "participant_id": participant_id,
            "authored_time": now or datetime.datetime.now().isoformat(),
        }
        return json.loads(json_text)  # deserialize to validate


def load_measurement_json_amendment(participant_id, amended_id, now=None):
    """Loads a PhysicalMeasurement FHIR resource and adds an amendment extension."""
    with open(data_path("measurements-as-fhir-amendment.json")) as amendment_file:
        extension = json.loads(amendment_file.read() % {"physical_measurement_id": amended_id})
    with open(data_path("measurements-as-fhir.json")) as measurements_file:
        measurement = json.loads(
            measurements_file.read()
            % {"participant_id": participant_id, "authored_time": now or datetime.datetime.now().isoformat()}
        )
    measurement["entry"][0]["resource"].update(extension)
    return measurement


def load_qr_response_json(template_file_name, questionnaire_id, participant_id_str):
    with open(data_path(template_file_name)) as fd:
        resource = json.load(fd)

    resource["subject"]["reference"] = f'Patient/{participant_id_str}'
    resource["questionnaire"]["reference"] = f'Questionnaire/{questionnaire_id}'

    return resource


def load_biobank_order_json(participant_id, filename="biobank_order_1.json"):
    with open(data_path(filename)) as f:
        return json.loads(
            f.read()
            % {"participant_id": participant_id, "client_participant_id": to_client_participant_id(participant_id)}
        )


def open_biobank_samples(biobank_ids, tests):
    """
    Returns a dictionary representing the biobank samples based on the test CSV file: biobank_samples_1.csv.
    The number of records returned is equal to the number of biobank_ids passed.
    :param biobank_ids: list of biobank ids.
    :param tests: list of tests
    :return: Dict object to be sent to the Biobank/specimens endpoint.
    """
    result = []
    with open(data_path("biobank_samples_1.csv")) as f:
        test_data_collection = csv.DictReader(f, delimiter='\t')

        for biobank_id, test_code, template_record in zip(biobank_ids, cycle(tests), test_data_collection):
            if template_record['Parent Sample Id']:
                continue  # Skip child records from test data

            resulting_record = {
                'rlimsID': template_record['Sample Id'],
                'orderID': template_record['Sent Order Id'],
                'participantID': to_client_biobank_id(biobank_id),
                'testcode': test_code,
                'status': {
                    'status': template_record['Sample Storage Status']
                },
                'confirmationDate': template_record['Sample Confirmed Date']
            }
            if template_record['Sample Disposal Status']:
                resulting_record['disposalStatus'] = {
                    'reason': template_record['Sample Disposal Status'],
                    'disposalDate': template_record['Sample Disposed Date']
                }
            result.append(resulting_record)

    return result


def load_questionnaire_response_with_consents(
    questionnaire_id, participant_id, first_name_link_id, last_name_link_id, email_address_link_id, consent_pdf_paths
):
    """Loads a consent QuestionnaireResponse and adds >= 1 consent PDF extensions."""
    # PDF paths are expected to be something like "/Participant/P550613540/ConsentPII__8270.pdf".
    assert len(consent_pdf_paths) >= 1
    with open(data_path("questionnaire_response_consent.json")) as f:
        resource = json.loads(
            f.read()
            % {
                "questionnaire_id": questionnaire_id,
                "participant_client_id": to_client_participant_id(participant_id),
                "first_name_link_id": first_name_link_id,
                "last_name_link_id": last_name_link_id,
                "email_address_link_id": email_address_link_id,
            }
        )
    for path in consent_pdf_paths:
        resource["extension"].append(
            {"url": "http://terminology.pmi-ops.org/StructureDefinition/consent-form-signed-pdf", "valueString": path}
        )
    return resource


def load_test_data_json(filename):
    with open(data_path(filename)) as handle:
        return json.load(handle)
