from datetime import datetime
import logging
import requests


class RedcapClient:
    @staticmethod
    def send_request(project_api_token, content_type, additional_request_parameters=None, return_raw_response=False):
        # https://precisionmedicineinitiative.atlassian.net/browse/PD-5404
        headers = {
            'User-Agent': 'RDR code sync tool',
            'Accept': None,
            'Connection': None,
        }

        request_body = {
            'token': project_api_token,
            'content': content_type,
            'format': 'json',
            'returnFormat': 'json'
        }
        if additional_request_parameters:
            request_body.update(additional_request_parameters)
        response = requests.post('https://redcap.pmi-ops.org/api/', data=request_body, headers=headers)

        if response.status_code != 200:
            logging.error(f'ERROR: Received status code {response.status_code} from REDCap API')
            return None
        else:
            if return_raw_response:
                return response
            else:
                return response.json()

    def get_data_dictionary(self, project_api_token):
        return self.send_request(project_api_token, 'metadata')

    def get_project_info(self, project_api_token):
        return self.send_request(project_api_token, 'project')

    def get_records(self, project_api_token, datetime_range_begin: datetime = None, forms=None, fields=None):
        """
        Get REDCap records (responses to the REDCap survey)
        :param project_api_token: Access token for the project
        :param datetime_range_begin: Specify that only records created or modified after the datetime should be returned
        :param fields: List of specific field names to export
        :return: An array of records
        """
        request_parameters = {
            'exportSurveyFields': True  # so that record timestamps are retrieved too
        }
        if forms:
            request_parameters['forms'] = forms
        if fields:
            request_parameters['fields'] = fields
        if datetime_range_begin:
            request_parameters['dateRangeBegin'] = datetime_range_begin.strftime('%Y-%m-%d %H:%M:%S')
        return self.send_request(project_api_token, 'record', request_parameters)

    def get_pdf(self, project_api_token, record_id, instrument_name):
        return self.send_request(project_api_token, 'pdf', {
            'record': record_id,
            'instrument': instrument_name
        }, return_raw_response=True)

    def get_file(self, project_api_token, record_id, field_name):
        return self.send_request(project_api_token, 'file', {
            'record': record_id,
            'action': 'export',
            'field': field_name,
            'event': ''
        }, return_raw_response=True)
