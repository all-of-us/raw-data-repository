from datetime import datetime
import logging
import requests


class RedcapClient:
    @staticmethod
    def send_request(project_api_token, content_type, additional_request_parameters=None):
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
            return response.json()

    def get_data_dictionary(self, project_api_token):
        return self.send_request(project_api_token, 'metadata')

    def get_project_info(self, project_api_token):
        return self.send_request(project_api_token, 'project')

    def get_records(self, project_api_token, datetime_range_begin: datetime = None):
        """
        Get REDCap records (responses to the REDCap survey)
        :param project_api_token: Access token for the project
        :param datetime_range_begin: Specify that only records created or modified after the datetime should be returned
        :return: An array of records
        """
        request_parameters = {
            'exportSurveyFields': True  # so that record timestamps are retrieved too
        }
        if datetime_range_begin:
            request_parameters['dateRangeBegin'] = datetime_range_begin.strftime('%Y-%m-%d %H:%M:%S')
        return self.send_request(project_api_token, 'record', request_parameters)
