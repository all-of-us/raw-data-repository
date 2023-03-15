import mock

from rdr_service import config
from rdr_service.dao.participant_dao import ParticipantDao
from rdr_service.model.consent_file import ConsentFile
from rdr_service.offline.sync_consent_files import ConsentSyncController, DEFAULT_GOOGLE_GROUP, DEFAULT_ORG_NAME
from rdr_service.storage import GoogleCloudStorageProvider
from tests.helpers.unittest_base import BaseTestCase


@mock.patch('rdr_service.offline.sync_consent_files.dispatch_rebuild_consent_metrics_tasks')
class ConsentSyncControllerTest(BaseTestCase):
    def __init__(self, *args, **kwargs):
        super(ConsentSyncControllerTest, self).__init__(*args, **kwargs)
        self.uses_database = False

    def setUp(self, *args, **kwargs) -> None:
        super(ConsentSyncControllerTest, self).setUp(*args, **kwargs)

        self.consent_dao_mock = mock.MagicMock()
        self.participant_dao_mock = mock.MagicMock(spec=ParticipantDao)

        self.storage_provider_mock = mock.MagicMock(spec=GoogleCloudStorageProvider)
        patcher = mock.patch('rdr_service.storage.GoogleCloudStorageProvider')
        provider_class_mock = patcher.start()
        provider_class_mock.return_value = self.storage_provider_mock
        self.addCleanup(patcher.stop)

        zip_patcher = mock.patch('rdr_service.storage.ZipFile')
        self.zip_mock = zip_patcher.start()
        self.addCleanup(zip_patcher.stop)

        self.sync_controller = ConsentSyncController(
            consent_dao=self.consent_dao_mock,
            participant_dao=self.participant_dao_mock,
            storage_provider=self.storage_provider_mock
        )

        self.test_hpo_name = 'TEST'
        self.bob_bucket_name = 'bob_dest_bucket'
        self.bob_org_name = 'BOB_CARE'
        self.foo_bucket_name = 'foo_dest_bucket'
        self.foo_org_name = 'FOO_CLINIC'
        self.bar_bucket_name = 'bar_dest_bucket'
        self.bar_hpo_name = 'BAR_CORP'
        self.temporarily_override_config_setting(
            key=config.CONSENT_SYNC_BUCKETS,
            value={
                'orgs': {
                    self.bob_org_name: {'bucket': self.bob_bucket_name, 'zip_consents': False},
                    self.foo_org_name: {'bucket': self.foo_bucket_name, 'zip_consents': False}
                },
                'hpos': {
                    self.bar_hpo_name: {'bucket': self.bar_bucket_name, 'zip_consents': False}
                }
            }
        )

        self.bob_participant_id = 1234
        self.foo_participant_id = 4567
        self.bar_participant_id = 7890
        self.participant_dao_mock.get_pairing_data_for_ids.return_value = [
            (self.bob_participant_id, self.test_hpo_name, self.bob_org_name, 'test-site-group'),
            (self.foo_participant_id, self.test_hpo_name, self.foo_org_name, None),
            (self.bar_participant_id, self.bar_hpo_name, None, None)
        ]

        self.bob_file = ConsentFile(id=1, file_path='/source_bucket_a/bob.pdf', participant_id=self.bob_participant_id)
        self.foo_file = ConsentFile(id=2, file_path='/source_bucket_b/foo.pdf', participant_id=self.foo_participant_id)
        self.bar_file = ConsentFile(id=3, file_path='/source_bucket_b/bar.pdf', participant_id=self.bar_participant_id)
        self.consent_dao_mock.get_files_ready_to_sync.return_value = [self.bob_file, self.foo_file, self.bar_file]

    def test_sync_of_ready_files(self, mock_dispatch_rebuild):
        """Test that files ready to sync are copied"""
        first_file = ConsentFile(id=4, file_path='/source_bucket/test/one.pdf', participant_id=self.bob_participant_id)
        second_file = ConsentFile(id=5, file_path='/source_bucket/test/two.pdf', participant_id=self.bob_participant_id)
        third_file = ConsentFile(id=6, file_path='/source_bucket/test/three.pdf',
                                 participant_id=self.bob_participant_id)
        self.consent_dao_mock.get_files_ready_to_sync.return_value = [first_file, second_file, third_file]

        self.sync_controller.sync_ready_files()
        self.storage_provider_mock.copy_blob.assert_has_calls(
            calls=[
                mock.call(source_path=first_file.file_path, destination_path=mock.ANY),
                mock.call(source_path=second_file.file_path, destination_path=mock.ANY),
                mock.call(source_path=third_file.file_path, destination_path=mock.ANY),
            ],
            any_order=True
        )
        mock_dispatch_rebuild.assert_called_once_with([first_file.id, second_file.id, third_file.id])

    def test_file_destinations(self, mock_dispatch_rebuild):
        """Test that consent files sync to the correct destinations based on participant data"""

        self.sync_controller.sync_ready_files()
        self.storage_provider_mock.copy_blob.assert_has_calls(
            calls=[
                mock.call(
                    source_path=self.bob_file.file_path,
                    destination_path=self._build_expected_dest_path(
                        bucket_name=self.bob_bucket_name,
                        org_id=self.bob_org_name,
                        site_group='test-site-group',
                        participant_id=self.bob_participant_id,
                        file_name='bob.pdf'
                    )
                ),
                mock.call(
                    source_path=self.foo_file.file_path,
                    destination_path=self._build_expected_dest_path(
                        bucket_name=self.foo_bucket_name,
                        org_id=self.foo_org_name,
                        site_group=DEFAULT_GOOGLE_GROUP,
                        participant_id=self.foo_participant_id,
                        file_name='foo.pdf'
                    )
                ),
                mock.call(
                    source_path=self.bar_file.file_path,
                    destination_path=self._build_expected_dest_path(
                        bucket_name=self.bar_bucket_name,
                        org_id=DEFAULT_ORG_NAME,
                        site_group=DEFAULT_GOOGLE_GROUP,
                        participant_id=self.bar_participant_id,
                        file_name='bar.pdf'
                    )
                )
            ],
            any_order=True
        )
        mock_dispatch_rebuild.assert_has_calls(
            [
                mock.call([self.bob_file.id]),
                mock.call([self.foo_file.id]),
                mock.call([self.bar_file.id]),
            ], any_order=True
        )

    def test_zipping_specified_orgs(self, mock_dispatch_rebuild):
        """Test that the controller zips consents for organizations that give that they should be zipped"""
        self.temporarily_override_config_setting(
            key=config.CONSENT_SYNC_BUCKETS,
            value={
                'orgs': {
                    self.bob_org_name: {'bucket': self.bob_bucket_name, 'zip_consents': False},
                    self.foo_org_name: {'bucket': self.foo_bucket_name, 'zip_consents': True}
                },
                'hpos': {}
            }
        )

        self.sync_controller.sync_ready_files()
        self.storage_provider_mock.copy_blob.assert_called_once_with(
            source_path=self.bob_file.file_path,
            destination_path=self._build_expected_dest_path(
                bucket_name=self.bob_bucket_name,
                org_id=self.bob_org_name,
                site_group='test-site-group',
                participant_id=self.bob_participant_id,
                file_name='bob.pdf'
            )
        )

        # check that the PDF was added to the zip file
        self.zip_mock.return_value.writestr.assert_any_call(
            zinfo_or_arcname='P4567/foo.pdf',
            data=mock.ANY
        )
        # check that the zip file was uploaded
        self.storage_provider_mock.open.assert_any_call(
            'foo_dest_bucket/Participant/FOO_CLINIC/no-site-assigned.zip',
            mode='w'
        )

        mock_dispatch_rebuild.assert_has_calls(
            [
                mock.call([self.bob_file.id]),
                mock.call([self.foo_file.id]),
            ], any_order=True
        )

    def test_unpaired_participants(self, mock_dispatch_rebuild):
        """Test that any participants that aren't paired are ignored"""
        # Return empty list, indicating that the participants are not paired to organizations
        self.participant_dao_mock.get_pairing_data_for_ids.return_value = []

        self.sync_controller.sync_ready_files()

        # If no participants are paired, then nothing should sync
        self.storage_provider_mock.copy_blob.assert_not_called()
        self.assertEqual(mock_dispatch_rebuild.call_count, 0)

    def test_ignore_unrecognized_orgs(self, mock_dispatch_rebuild):
        """Test that the sync ignores participants paired to an organization that isn't specified in the config"""
        # Return empty list, indicating that the participants are not paired to organizations
        self.participant_dao_mock.get_pairing_data_for_ids.return_value = [
            (self.bob_participant_id, self.test_hpo_name, 'org_not_in_config', None),
        ]

        self.sync_controller.sync_ready_files()

        # If no participants are paired, then nothing should sync
        self.storage_provider_mock.copy_blob.assert_not_called()
        self.assertEqual(mock_dispatch_rebuild.call_count, 0)

    def test_only_loading_consents_that_will_sync(self, mock_dispatch_rebuild):
        """
        Currently there are only a few organizations in the config to sync.
        For performance, we just load the files that will be copied.
        """
        self.sync_controller.sync_ready_files()

        # Check that the config was used to filter the consent files loaded
        org_name_keys = self.consent_dao_mock.get_files_ready_to_sync.call_args.kwargs['org_names']
        for expected_key in [self.bob_org_name, self.foo_org_name]:
            self.assertIn(expected_key, org_name_keys)

        # All files still had their sync_status updated
        mock_dispatch_rebuild.assert_has_calls(
            [
                mock.call([self.bob_file.id]),
                mock.call([self.foo_file.id]),
                mock.call([self.bar_file.id]),
            ], any_order=True
        )

    @classmethod
    def _build_expected_dest_path(cls, bucket_name, org_id, site_group, participant_id, file_name):
        return f'{bucket_name}/Participant/{org_id}/{site_group}/P{participant_id}/{file_name}'
