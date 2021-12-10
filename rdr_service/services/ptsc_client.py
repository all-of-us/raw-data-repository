
import requests


class PtscClient:
    def __init__(self):
        super(PtscClient, self).__init__()
        self.client_id = 'drc-service'
        self.secret = '5f6a2626-3578-484e-be24-120bf37c5cac'
        self.auth_url = 'https://accounts-stb.joinallofus.org/auth/realms/stb_participant_realm' \
                        '/protocol/openid-connect/token'

    def get_access_token(self):
        token_response = requests.post(
            url=self.auth_url,
            data=f'grant_type=client_credentials&client_id={self.client_id}&client_secret={self.secret}',
            headers={'Content-type': 'application/x-ww-form-urlencoded'}
        ).json()
        return token_response['access_token']

    def get_participant_lookup(self, participant_id):
        ...
