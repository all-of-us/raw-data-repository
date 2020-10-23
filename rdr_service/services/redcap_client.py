import logging
import requests


class RedcapClient:
    @staticmethod
    def send_request(project_api_token, content_type):
        # https://precisionmedicineinitiative.atlassian.net/browse/PD-5404
        headers = {
            'User-Agent': 'RDR code sync tool',
            'Accept': None,
            'Connection': None,
        }

        response = requests.post('https://redcap.pmi-ops.org/api/', data={
            'token': project_api_token,
            'content': content_type,
            'format': 'json',
            'returnFormat': 'json'
        }, headers=headers)

        if response.status_code != 200:
            logging.error(f'ERROR: Received status code {response.status_code} from REDCap API')
            return None
        else:
            return response.json()

    def get_data_dictionary(self, project_api_token):
        return self.send_request(project_api_token, 'metadata')

    def get_records(self, project_api_token):
        return self.send_request(project_api_token, 'record')
