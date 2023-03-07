from datetime import datetime
import mock

from tests.helpers.unittest_base import BaseTestCase


class TaskApiTest(BaseTestCase):
    def setUp(self, *args, **kwargs):
        super(TaskApiTest, self).setUp(*args, **kwargs)

        from rdr_service.resource.main import app
        self.test_client = app.test_client()
        self.done = True

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
    @mock.patch('rdr_service.api.cloud_tasks_api.onsite_id_verification_build_task')
    def test_onsite_id_verification_build(self, onsite_build_task_mock):
        self._call_task_endpoint(
            task_path='OnSiteIdVerificationBuildTaskApi',
            json={'onsite_verification_id': 1}
        )
        onsite_build_task_mock.assert_called_with(1)

    @mock.patch('rdr_service.api.cloud_tasks_api.onsite_id_verification_batch_rebuild_task')
    def test_onsite_id_verification_build(self, onsite_batch_rebuild_task_mock):
        self._call_task_endpoint(
            task_path='OnSiteIdVerificationBatchRebuildTaskApi',
            json={'onsite_verification_id_list': [1, 2, 3]}
        )
        onsite_batch_rebuild_task_mock.assert_called_with([1, 2, 3])

    @mock.patch('rdr_service.api.cloud_tasks_api.bq_hpo_update_all')
    def test_hpo_rebuild_task(self, hpo_rebuild_task_mock):
        self._call_task_endpoint(
            task_path='RebuildHpoAllTaskApi',
            json={}
        )
        self.assertEqual(hpo_rebuild_task_mock.call_count, 1)

    @mock.patch('rdr_service.api.cloud_tasks_api.bq_organization_update_all')
    def test_organization_rebuild_task(self, org_rebuild_task_mock):
        self._call_task_endpoint(
            task_path='RebuildOrganizationAllTaskApi',
            json={}
        )
        self.assertEqual(org_rebuild_task_mock.call_count, 1)

    @mock.patch('rdr_service.api.cloud_tasks_api.bq_site_update_all')
    def test_site_rebuild_task(self, site_rebuild_task_mock):
        self._call_task_endpoint(
            task_path='RebuildSiteAllTaskApi',
            json={}
        )
        self.assertEqual(site_rebuild_task_mock.call_count, 1)

    def _call_task_endpoint(self, task_path, json):
        response = self.send_post(
            f'/resource/task/{task_path}',
            json,
            test_client=self.test_client,
            prefix=''
        )
        self.assertEqual('{"success": "true"}', response)
