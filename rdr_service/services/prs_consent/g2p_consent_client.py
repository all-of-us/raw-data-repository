import csv
import logging
from datetime import datetime
from io import BytesIO

from dateutil.parser import parse
from sqlalchemy.orm import Session

from rdr_service.model.prs_consent import PrsConsentResponse, PrsConsentValidationResult, PrsConsentValidationError
from rdr_service.services.prs_consent.validation import G2pConsentValidator, G2pConsentExpectedData, PdfParsingError
from rdr_service.services.redcap_client import RedcapClient

result_export_fields = [
    'record_id',
    'aou_pids',
    'study_id',
    'first_name',
    'last_name',
    'date_of_birth',
    'phone_number',
    'primary_language',
    'first_name_2',
    'last_name_2',
    'email',
    'consent_url',
    'consent_landing_page_url',
    'econsent_intro_form_complete',
    'consent_landing_page_complete',
    'if_you_agree___1',
    'date',
    'please_check_the_box_below___1',
    'name_of_the_person_who_hel',
    'the_url',
    'data_sharing_consent_complete',
    'consent_validation',
    'specify_why_the_pdf_did_no',
    'consent_pdf_complete'
]


class G2pConsentClient:
    def __init__(self, redcap_api_key: str):
        self._redcap_client = RedcapClient()
        self._api_key = redcap_api_key

    def sync_new_consents(self, session: Session):
        since_datetime = datetime(2026, 1, 1)

        recent_records = self._redcap_client.get_records(
            self._api_key, since_datetime
        )

        field_name_map = {
            'participant_id': 'record_id',
            'date': 'date',
            'received_help': 'please_check_the_box_below___1',
            'helper_name': 'name_of_the_person_who_hel',
            'completion_status': 'data_sharing_consent_complete',
            'consent_status': 'if_you_agree___1',
            'validation_status': 'consent_validation'
        }

        record_data = {}
        for record in recent_records:
            mapped_data = {}
            for mapped_name, redcap_name in field_name_map.items():
                if redcap_name in record:
                    mapped_data[mapped_name] = record[redcap_name]
            if mapped_data['date']:
                mapped_data['date'] = parse(mapped_data['date']).date()

            record_data[record['record_id']] = mapped_data

        passed_validation = []
        all_passed = True

        total_count = 0
        already_validated_count = 0
        for record_id, metadata in record_data.items():
            if (
                metadata['completion_status'] != '2'  # skip any records not set as complete
                or metadata['consent_status'] != '1'  # skip any records that don't provide consent
            ):
                continue
            total_count += 1
            if metadata['validation_status'] == '1':  # skip any that have already been validated
                already_validated_count += 1
                continue

            logging.info(f'validating consent for {record_id}...')
            expected_values = G2pConsentExpectedData(
                signed_date=metadata['date'],
                received_help=metadata['received_help'] == '1',
                helper_name=metadata['helper_name'] or None
            )
            error_list = self._validate_consent_pdf(record_id, expected_values)
            self._record_result(metadata, error_list, session)

            if error_list:
                all_passed = False
            else:
                passed_validation.append(record_id)
            logging.info(f'validation result: {error_list}')

        if not all_passed:
            logging.error('validation errors were found')
        else:
            with open('g2p_consent_validation.csv', 'w') as output_file:
                dict_writer = csv.DictWriter(output_file, result_export_fields)
                dict_writer.writeheader()
                for record_id in passed_validation:
                    dict_writer.writerow({
                        'record_id': record_id,
                        'consent_validation': 1
                    })

        logging.info(f'found {total_count} records ({already_validated_count} were already validated)')

    def _record_result(self, data, error_message_list, session: Session):
        consent_record = PrsConsentResponse(
            participant_id=data['participant_id'],
            signed_date=data['date'],
            consent_type='G2P'
        )
        session.add(consent_record)

        validation_result = PrsConsentValidationResult(
            consent_response=consent_record,
            is_valid=len(error_message_list) == 0
        )
        session.add(validation_result)

        for error in error_message_list:
            session.add(PrsConsentValidationError(
                validation_result=validation_result,
                error_message=error
            ))

    def _validate_consent_pdf(self, record_id, expected_data: G2pConsentExpectedData):
        # pdf_response = self._redcap_client.get_pdf(
        #     self._api_key, record_id, 'data_sharing_consent'
        # )
        pdf_response = self._redcap_client.get_file(
            self._api_key, record_id, 'full_consent'
        )
        try:
            return G2pConsentValidator.validate(
                BytesIO(pdf_response.content),
                expected_data
            )
        except PdfParsingError as e:
            logging.error(f'Unable to parse consent PDF for {record_id}: {e}')
            return None
