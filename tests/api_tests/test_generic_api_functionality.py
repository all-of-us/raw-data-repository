from rdr_service import main
from tests.helpers.unittest_base import BaseTestCase


class GenericApiFunctionalityTest(BaseTestCase):

    def test_server_graceful_with_invalid_json(self):
        response = self.app.open(
            main.API_PREFIX + 'Participant/P1234/Observation',
            method='POST',
            data="{invalid json",
            content_type="application/json"
        )
        self.assertNotEqual(500, response.status_code, 'Server crashed when given invalid JSON')
