import mock

from rdr_service import config
from rdr_service.dao.participant_dao import ParticipantDao
from rdr_service.model.consent_file import ConsentFile
from rdr_service.offline.sync_consent_files import ConsentSyncController, DEFAULT_GOOGLE_GROUP
from rdr_service.storage import GoogleCloudStorageProvider
from tests.helpers.unittest_base import BaseTestCase


class ConsentSyncControllerTest(BaseTestCase):
    def __init__(self, *args, **kwargs):
        super(ConsentSyncControllerTest, self).__init__(*args, **kwargs)
        self.uses_database = False

    def setUp(self, *args, **kwargs) -> None:
        super(ConsentSyncControllerTest, self).setUp(*args, **kwargs)

        self.consent_dao_mock = mock.MagicMock()
        self.participant_dao_mock = mock.MagicMock(spec=ParticipantDao)
        self.storage_provider_mock = mock.MagicMock(spec=GoogleCloudStorageProvider)
        self.sync_controller = ConsentSyncController(
            consent_dao=self.consent_dao_mock,
            participant_dao=self.participant_dao_mock,
            storage_provider=self.storage_provider_mock
        )

        self.bob_bucket_name = 'bob_dest_bucket'
        self.bob_org_name = 'BOB_CARE'
        self.foo_bucket_name = 'foo_dest_bucket'
        self.foo_org_name = 'FOO_CLINIC'
        self.temporarily_override_config_setting(
            key=config.CONSENT_SYNC_BUCKETS,
            value={
                self.bob_org_name: {'bucket': self.bob_bucket_name, 'zip_consents': False},
                self.foo_org_name: {'bucket': self.foo_bucket_name, 'zip_consents': False}
            }
        )

        self.bob_participant_id = 1234
        self.foo_participant_id = 4567
        self.participant_dao_mock.get_org_and_site_for_ids.return_value = [
            (self.bob_participant_id, self.bob_org_name, 'test-site-group'),
            (self.foo_participant_id, self.foo_org_name, None)
        ]

        self.bob_file = ConsentFile(file_path='/source_bucket_a/bob.pdf', participant_id=self.bob_participant_id)
        self.foo_file = ConsentFile(file_path='/source_bucket_b/foo.pdf', participant_id=self.foo_participant_id)
        self.consent_dao_mock.get_files_ready_to_sync.return_value = [self.bob_file, self.foo_file]

    def test_sync_of_ready_files(self):
        """Test that files ready to sync are copied"""
        first_file = ConsentFile(file_path='/source_bucket/test/one.pdf', participant_id=self.bob_participant_id)
        second_file = ConsentFile(file_path='/source_bucket/test/two.pdf', participant_id=self.bob_participant_id)
        third_file = ConsentFile(file_path='/source_bucket/test/three.pdf', participant_id=self.bob_participant_id)
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

    def test_file_destinations(self):
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
                )
            ],
            any_order=True
        )

    def test_zipping_specified_orgs(self):
        """Test that the controller zips consents for organizations that give that they should be zipped"""
        self.temporarily_override_config_setting(
            key=config.CONSENT_SYNC_BUCKETS,
            value={
                self.bob_org_name: {'bucket': self.bob_bucket_name, 'zip_consents': False},
                self.foo_org_name: {'bucket': self.foo_bucket_name, 'zip_consents': True}
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

        self.storage_provider_mock.download_blob.assert_called_once_with(
            source_path=self.foo_file.file_path,
            destination_path=mock.ANY  # Downloaded to a temp directory, don't really need to know specifically where
        )
        self.storage_provider_mock.upload_from_file.assert_called_once_with(
            source_file=mock.ANY,  # Uploading archive generated from temp directory
            path=f'{self.foo_bucket_name}/Participant/{self.foo_org_name}/{DEFAULT_GOOGLE_GROUP}.zip'
        )

    def test_unpaired_participants(self):
        """Test that any participants that aren't paired are ignored"""
        # Return empty list, indicating that the participants are not paired to organizations
        self.participant_dao_mock.get_org_and_site_for_ids.return_value = []

        self.sync_controller.sync_ready_files()

        # If no participants are paired, then nothing should sync
        self.storage_provider_mock.copy_blob.assert_not_called()

    @classmethod
    def _build_expected_dest_path(cls, bucket_name, org_id, site_group, participant_id, file_name):
        return f'{bucket_name}/Participant/{org_id}/{site_group}/P{participant_id}/{file_name}'
