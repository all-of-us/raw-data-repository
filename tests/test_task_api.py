from datetime import datetime
import mock

from tests.helpers.unittest_base import BaseTestCase


class TaskApiTest(BaseTestCase):
    def setUp(self, *args, **kwargs):
        super(TaskApiTest, self).setUp(*args, **kwargs)

        from rdr_service.resource.main import app
        self.test_client = app.test_client()

    @mock.patch('rdr_service.api.cloud_tasks_api.ParticipantDataValidation')
    def test_date_of_birth_check(self, validation_mock):
        self._call_task_endpoint(
            task_path='ValidateDateOfBirth',
            json={
                'participant_id': 1234,
                'date_of_birth': '2000-3-19'
            }
        )

        validation_mock.analyze_date_of_birth.assert_called_with(
            participant_id=1234,
            date_of_birth=datetime(2000, 3, 19)
        )

    def _call_task_endpoint(self, task_path, json):
        response = self.send_post(
            f'/resource/task/{task_path}',
            json,
            test_client=self.test_client,
            prefix=''
        )
        self.assertEqual('{"success": "true"}', response)
