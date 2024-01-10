from datetime import datetime

from rdr_service.clock import FakeClock
from tests.helpers.unittest_base import BaseTestCase


class RelatedPersonApiTest(BaseTestCase):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.uses_database = False

    def setUp(self, *args, **kwargs):
        super().setUp(*args, **kwargs)
        self.dao_mock = self.mock('rdr_service.api.related_person_api.AccountLinkDao')

    def test_sending_link(self):
        now_timestamp = datetime.now()
        child_pid = 987654321
        guardian_pid = 123456789

        with FakeClock(now_timestamp):
            self.send_post(
                'RelatedPerson',
                request_data={
                    "resourceType": "RelatedPerson",
                    "identifier": [
                        {"value": f"P{guardian_pid}"}
                    ],
                    "period": {
                        "start": "2023-08-01"
                    },
                    "patient": {
                        "reference": f"Patient/P{child_pid}"
                    }
                }
            )

        saved_link = self.dao_mock.save_account_link.call_args[0][0]
        self.assertEqual(now_timestamp, saved_link.created)
        self.assertEqual(now_timestamp, saved_link.modified)
        self.assertEqual('2023-08-01', saved_link.start)
        self.assertIsNone(saved_link.end)
        self.assertEqual(child_pid, saved_link.participant_id)
        self.assertEqual(guardian_pid, saved_link.related_id)

    def test_updating_with_end(self):
        now_timestamp = datetime.now()
        child_pid = 987654321
        guardian_pid = 123456789

        with FakeClock(now_timestamp):
            self.send_post(
                'RelatedPerson',
                request_data={
                    "resourceType": "RelatedPerson",
                    "identifier": [
                        {"value": f"P{guardian_pid}"}
                    ],
                    "period": {
                        "start": "2023-08-01",
                        "end": "2023-10-01"
                    },
                    "patient": {
                        "reference": f"Patient/P{child_pid}"
                    }
                }
            )

        saved_link = self.dao_mock.save_account_link.call_args[0][0]
        self.assertEqual('2023-10-01', saved_link.end)
