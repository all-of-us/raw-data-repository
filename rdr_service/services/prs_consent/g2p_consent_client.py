import logging
from datetime import datetime
from io import BytesIO

from dateutil.parser import parse

from rdr_service.services.prs_consent.validation import G2pConsentValidator, G2pConsentExpectedData, PdfParsingError
from rdr_service.services.redcap_client import RedcapClient


class G2pConsentClient:
    def __init__(self, redcap_api_key: str):
        self._redcap_client = RedcapClient()
        self._api_key = redcap_api_key

    def sync_new_consents(self):
        # todo: find latest date using the database
        since_datetime = datetime(2026, 1, 1)

        recent_records = self._redcap_client.get_records(
            self._api_key, since_datetime
        )

        field_name_map = {
            'date': 'date',
            'received_help': 'please_check_the_box_below___1',
            'helper_name': 'name_of_the_person_who_hel',
            'completion_status': 'data_sharing_consent_complete',
            'consent_status': 'if_you_agree___1'
        }

        record_data = {}
        for record in recent_records:
            mapped_data = {}
            for mapped_name, redcap_name in field_name_map.items():
                if redcap_name in record:
                    mapped_data[mapped_name] = record[redcap_name]

            record_data[record['record_id']] = mapped_data

        for record_id, metadata in record_data.items():
            if (
                metadata['completion_status'] != '2'  # skip any records not set as complete
                or metadata['consent_status'] != '1'  # skip any records that don't provide consent
            ):
                continue

            logging.info(f'validating consent for {record_id}...')
            expected_values = G2pConsentExpectedData(
                signed_date=parse(metadata['date']).date(),
                received_help=metadata['received_help'] == '1',
                helper_name=metadata['helper_name'] or None
            )
            error_list = self._validate_consent_pdf(record_id, expected_values)
            logging.info(f'validation result: {error_list}')

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
