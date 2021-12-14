from datetime import date

import requests


class PtscClient:
    def __init__(self, client_id, auth_url, client_secret, request_url):
        super(PtscClient, self).__init__()
        self.client_id = client_id
        self.secret = client_secret
        self.auth_url = auth_url
        self.request_url = request_url
        self._current_token = None

    def get_access_token(self):
        token_response = requests.post(
            url=self.auth_url,
            data=f'grant_type=client_credentials&client_id={self.client_id}&client_secret={self.secret}',
            headers={'Content-type': 'application/x-www-form-urlencoded'}
        ).json()
        return token_response['access_token']

    def make_request(self, url):
        if not self._current_token:
            self._current_token = self.get_access_token()
        headers = {
            'Authorization': f'Bearer {self._current_token}'
        }

        response = requests.get(url=url, headers=headers)
        if response.status_code == 401:
            # Re-attempt with new token, use the new result even if the new token fails
            self._current_token = self.get_access_token()
            response = requests.get(
                url=url,
                headers={
                    'Authorization': f'Bearer {self._current_token}'
                }
            )

        if response.status_code == 200:
            return response.json()
        else:
            raise Exception(f'got status code {response.status_code}. Message: {response.content}')

    def request_next_page(self, current_response):
        if 'link' in current_response:
            next_page_url = current_response['link'].get('nextPageQuery')
            if next_page_url:
                return self.make_request(next_page_url)

        return None

    def get_participant_lookup(self, participant_id: int = None, start_date: date = None):
        url_params = ''
        if participant_id:
            url_params += f'drcId=P{participant_id}'
        if start_date:
            url_params += f'startDate={start_date.strftime("%Y-%m-%d")}&pageSize=1000'

        response_json = self.make_request(f'{self.request_url}participantLookup?{url_params}')

        if participant_id:
            participant_list = response_json['participants']
            return participant_list[0] if participant_list else None
        else:
            return response_json
