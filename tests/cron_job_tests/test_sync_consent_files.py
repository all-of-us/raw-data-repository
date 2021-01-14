from collections import namedtuple
import datetime
import mock
import os
from pathlib import Path
import tempfile

from google.cloud.storage import Blob
from rdr_service import config
from rdr_service.api_util import upload_from_string, open_cloud_file, list_blobs
from rdr_service.dao.organization_dao import OrganizationDao
from rdr_service.dao.participant_dao import ParticipantDao
from rdr_service.dao.participant_summary_dao import ParticipantSummaryDao
from rdr_service.dao.site_dao import SiteDao
from rdr_service.offline import sync_consent_files
from tests.helpers.unittest_base import BaseTestCase

EXPECTED_CLOUD_DESTINATION_PATTERN =\
    '{org_bucket_name}/Participant/{org_id}/{site_name}/P{participant_id}/{file_name}'
EXPECTED_DOWNLOAD_DESTINATION_PATTERN = os.path.join(
    tempfile.gettempdir(),
    'temp_consents/{org_bucket_name}/{org_id}/{site_name}/P{participant_id}/{file_name}')

FakeConsentFile = namedtuple('FakeConsentFile', ['name', 'updated'], defaults=['consent.pdf', None])


@mock.patch('rdr_service.dao.participant_dao.get_account_origin_id', lambda: 'vibrent')
class SyncConsentFilesTest(BaseTestCase):
    """Tests behavior of sync_consent_files
  """

    mock_bucket_paths = []

    def setUp(self):
        super(SyncConsentFilesTest, self).setUp()
        self.org_dao = OrganizationDao()
        self.site_dao = SiteDao()
        self.participant_dao = ParticipantDao()
        self.summary_dao = ParticipantSummaryDao()

        self.org_buckets = {
            "test_one": "testbucket123",
            "test_two": "testbucket456"
        }
        config.override_setting(config.CONSENT_SYNC_BUCKETS, self.org_buckets)

        self.org1 = self.data_generator.create_database_organization(externalId='test_one')
        self.site1 = self.data_generator.create_database_site(googleGroup="group1")

        self.source_consent_bucket = sync_consent_files.SOURCE_BUCKET['vibrent']

    def tearDown(self):
        super(SyncConsentFilesTest, self).tearDown()

    def _create_participant(self, id_, org_id, site_id, consents=False, ghost=None, email=None, null_email=False,
                            consent_time=None):
        participant = self.data_generator.create_database_participant(participantId=id_, organizationId=org_id,
                                                                      siteId=site_id, isGhostId=ghost)
        summary_data = {'participant': participant}
        summary = self.data_generator.create_database_participant_summary(participant=participant)
        if consents:
            summary_data.update(consentForElectronicHealthRecords=1,
                                consentForStudyEnrollment=1,
                                consentForStudyEnrollmentTime=consent_time)
        if email:
            summary.email = email
            summary_data['email'] = email
        if null_email:
            summary_data['email'] = None
        self.data_generator.create_database_participant_summary(**summary_data)
        return participant

    @staticmethod
    def _make_blob(name, bucket=None, updated=None):
        if updated is None:
            updated = datetime.datetime(2020, 1, 1)

        blob = Blob(name, bucket=bucket)
        blob._properties['updated'] = updated.isoformat() + '.000Z'
        return blob

    @staticmethod
    def _mock_files_for_participants(mock_list_blobs, fake_files):
        def consent_files_for_participant(bucket, prefix):
            return [SyncConsentFilesTest._make_blob(f'{prefix}/{file.name}', bucket=bucket, updated=file.updated)
                    for file in fake_files]
        mock_list_blobs.side_effect = consent_files_for_participant

    @mock.patch('rdr_service.offline.sync_consent_files.list_blobs')
    @mock.patch('rdr_service.offline.sync_consent_files.copy_cloud_file')
    def test_basic_consent_file_copy(self, mock_copy_cloud_file, mock_list_blobs):
        self._mock_files_for_participants(mock_list_blobs, [
            FakeConsentFile(),
            FakeConsentFile(name='addendum.pdf')
        ])
        self._create_participant(1, self.org1.organizationId, self.site1.siteId, consents=True)
        sync_consent_files.do_sync_consent_files()

        pattern_args = {
            'org_bucket_name': self.org_buckets[self.org1.externalId],
            'org_id': self.org1.externalId,
            'site_name': self.site1.googleGroup,
            'participant_id': 1
        }
        mock_copy_cloud_file.assert_has_calls([
            mock.call(f'/{self.source_consent_bucket}/Participant/P1/consent.pdf',
                      EXPECTED_CLOUD_DESTINATION_PATTERN.format(**pattern_args, file_name='consent.pdf')),
            mock.call(f'/{self.source_consent_bucket}/Participant/P1/addendum.pdf',
                      EXPECTED_CLOUD_DESTINATION_PATTERN.format(**pattern_args, file_name='addendum.pdf'))
        ])

    @mock.patch('rdr_service.offline.sync_consent_files.list_blobs')
    @mock.patch('rdr_service.offline.sync_consent_files.copy_cloud_file')
    def test_sync_date_cutoff(self, mock_copy_cloud_file, mock_list_blobs):
        self._mock_files_for_participants(mock_list_blobs, [
            FakeConsentFile(updated=datetime.datetime(2020, 2, 20)),
            # Neither of the following files should try to be copied since they're outside the date range
            FakeConsentFile(updated=datetime.datetime(2019, 10, 20)),
            FakeConsentFile(updated=datetime.datetime(2020, 4, 20))
        ])

        self._create_participant(1, self.org1.organizationId, None, consent_time=datetime.datetime(2020, 1, 12),
                                 consents=True)
        self._create_participant(2, self.org1.organizationId, self.site1.siteId,
                                 consent_time=datetime.datetime(2020, 2, 3), consents=True)
        self._create_participant(3, self.org1.organizationId, None, consent_time=datetime.datetime(2020, 3, 10),
                                 consents=True)

        sync_consent_files.do_sync_consent_files(start_date='2020-02-01', end_date='2020-03-01')

        org_bucket_name = self.org_buckets[self.org1.externalId]
        mock_copy_cloud_file.called_once_with(f'/{self.source_consent_bucket}/Participant/P2/consent.pdf',
                                              f'/{org_bucket_name}/Participant/{self.site1.googleGroup}/P2/consent.pdf')
        self.assertEqual(1, mock_copy_cloud_file.call_count, 'One file should be copied for one participant')

    @mock.patch('rdr_service.offline.sync_consent_files.list_blobs')
    @mock.patch('rdr_service.offline.sync_consent_files.copy_cloud_file')
    def test_sync_from_manual_trigger(self, mock_copy_cloud_file, mock_list_blobs):
        # Set up test data
        self._mock_files_for_participants(mock_list_blobs, [
            FakeConsentFile(updated=datetime.datetime(2020, 1, 12))
        ])
        self._create_participant(1, self.org1.organizationId, None, consent_time=datetime.datetime(2020, 1, 12),
                                 consents=True)

        # Call manual sync endpoint in offline app
        from rdr_service.offline.main import app, OFFLINE_PREFIX
        offline_test_client = app.test_client()
        self.send_post(
            'ManuallySyncConsentFiles',
            test_client=offline_test_client,
            prefix=OFFLINE_PREFIX,
            request_data={
                'start_date': '2020-01-01'
            }
        )

        org_bucket_name = self.org_buckets[self.org1.externalId]
        self.assertEqual(1, mock_copy_cloud_file.call_count, 'File should be copied for the participant')
        mock_copy_cloud_file.called_once_with(f'/{self.source_consent_bucket}/Participant/P1/consent.pdf',
                                              f'/{org_bucket_name}/Participant/{self.site1.googleGroup}/P1/consent.pdf')

    @mock.patch('rdr_service.offline.sync_consent_files.list_blobs')
    @mock.patch('rdr_service.offline.sync_consent_files.copy_cloud_file')
    def test_file_date_check(self, mock_copy_cloud_file, mock_list_blobs):
        self._mock_files_for_participants(mock_list_blobs, [
            FakeConsentFile(updated=datetime.datetime(2020, 1, 13)),
            FakeConsentFile(updated=datetime.datetime(2020, 2, 27))
        ])

        self._create_participant(1, self.org1.organizationId, self.site1.siteId, consents=True,
                                 consent_time=datetime.datetime(2020, 2, 3))
        sync_consent_files.do_sync_consent_files(start_date='2020-02-01')

        org_bucket_name = self.org_buckets[self.org1.externalId]
        mock_copy_cloud_file.called_once_with(f'/{self.source_consent_bucket}/Participant/P1/consent.pdf',
                                              f'/{org_bucket_name}/Participant/{self.site1.googleGroup}/P1/consent.pdf')
        self.assertEqual(1, mock_copy_cloud_file.call_count, 'One file should be copied')

    @mock.patch('rdr_service.offline.sync_consent_files.list_blobs')
    @mock.patch('rdr_service.offline.sync_consent_files.copy_cloud_file')
    def test_default_time_frame(self, mock_copy_cloud_file, mock_list_blobs):
        today = datetime.datetime.today()
        today_without_timestamp = datetime.datetime(today.year, today.month, today.day)
        self._mock_files_for_participants(mock_list_blobs, [
            FakeConsentFile(updated=today_without_timestamp)
        ])

        days_ago = datetime.datetime.now() - datetime.timedelta(days=7)
        more_than_a_month_ago = datetime.datetime.now() - datetime.timedelta(days=82)
        self._create_participant(1, self.org1.organizationId, self.site1.siteId, consents=True,
                                 consent_time=more_than_a_month_ago)
        self._create_participant(2, self.org1.organizationId, self.site1.siteId, consents=True,
                                 consent_time=days_ago)

        sync_consent_files.do_sync_recent_consent_files()

        org_bucket_name = self.org_buckets[self.org1.externalId]
        mock_copy_cloud_file.called_once_with(f'/{self.source_consent_bucket}/Participant/P2/consent.pdf',
                                              f'/{org_bucket_name}/Participant/{self.site1.googleGroup}/P2/consent.pdf')
        self.assertEqual(1, mock_copy_cloud_file.call_count, 'Files should be copied for one participant')

    @mock.patch('rdr_service.offline.sync_consent_files.list_blobs')
    @mock.patch('rdr_service.offline.sync_consent_files.copy_cloud_file')
    def test_default_file_filter(self, mock_copy_cloud_file, mock_list_blobs):
        self._mock_files_for_participants(mock_list_blobs, [
            FakeConsentFile(),
            FakeConsentFile(name='extra.doc'),
            FakeConsentFile(name='another.bin')
        ])

        self._create_participant(1, self.org1.organizationId, self.site1.siteId, consents=True,
                                 consent_time=datetime.datetime(2020, 2, 3))
        sync_consent_files.do_sync_consent_files()

        org_bucket_name = self.org_buckets[self.org1.externalId]
        mock_copy_cloud_file.called_once_with(f'/{self.source_consent_bucket}/Participant/P1/consent.pdf',
                                              f'/{org_bucket_name}/Participant/{self.site1.googleGroup}/P1/consent.pdf')
        self.assertEqual(1, mock_copy_cloud_file.call_count, 'Only PDF files should be copied')

    @mock.patch('rdr_service.offline.sync_consent_files.list_blobs')
    @mock.patch('rdr_service.offline.sync_consent_files.copy_cloud_file')
    def test_all_file_filter(self, mock_copy_cloud_file, mock_list_blobs):
        self._mock_files_for_participants(mock_list_blobs, [
            FakeConsentFile(),
            FakeConsentFile(name='extra.doc'),
            FakeConsentFile(name='another.bin')
        ])

        self._create_participant(1, self.org1.organizationId, self.site1.siteId, consents=True,
                                 consent_time=datetime.datetime(2020, 2, 3))
        sync_consent_files.do_sync_consent_files(file_filter=None)

        pattern_args = {
            'org_bucket_name': self.org_buckets[self.org1.externalId],
            'org_id': self.org1.externalId,
            'site_name': self.site1.googleGroup,
            'participant_id': 1
        }
        mock_copy_cloud_file.assert_has_calls([
            mock.call(f'/{self.source_consent_bucket}/Participant/P1/consent.pdf',
                      EXPECTED_CLOUD_DESTINATION_PATTERN.format(**pattern_args, file_name='consent.pdf')),
            mock.call(f'/{self.source_consent_bucket}/Participant/P1/extra.doc',
                      EXPECTED_CLOUD_DESTINATION_PATTERN.format(**pattern_args, file_name='extra.doc')),
            mock.call(f'/{self.source_consent_bucket}/Participant/P1/another.bin',
                      EXPECTED_CLOUD_DESTINATION_PATTERN.format(**pattern_args, file_name='another.bin'))
        ])

    @mock.patch('rdr_service.offline.sync_consent_files.list_blobs')
    @mock.patch('rdr_service.offline.sync_consent_files.copy_cloud_file')
    def test_all_file_filter(self, mock_copy_cloud_file, mock_list_blobs):
        today = datetime.datetime.today()
        today_without_timestamp = datetime.datetime(today.year, today.month, today.day)
        self._mock_files_for_participants(mock_list_blobs, [
            FakeConsentFile(updated=today_without_timestamp)
        ])

        va_org = self.data_generator.create_database_organization(externalId='VA_TEST')
        self._create_participant(1, va_org.organizationId, self.site1.siteId, consents=True,
                                 consent_time=datetime.datetime(2020, 2, 3))
        sync_consent_files.do_sync_consent_files(all_va=True)

        pattern_args = {
            'org_bucket_name': 'aou179',
            'org_id': 'VA_TEST',
            'site_name': self.site1.googleGroup,
            'participant_id': 1
        }
        mock_copy_cloud_file.assert_has_calls([
            mock.call(f'/{self.source_consent_bucket}/Participant/P1/consent.pdf',
                      EXPECTED_CLOUD_DESTINATION_PATTERN.format(**pattern_args, file_name='consent.pdf'))
        ])

    @staticmethod
    def _create_local_files_with_download(mock_download_cloud_file):
        def create_local_file(_, destination):
            os.makedirs(os.path.dirname(destination), exist_ok=True)
            Path(destination).touch()
        mock_download_cloud_file.side_effect = create_local_file

    @mock.patch('rdr_service.storage.GoogleCloudStorageProvider.upload_from_file')
    @mock.patch('rdr_service.offline.sync_consent_files.list_blobs')
    @mock.patch('rdr_service.offline.sync_consent_files.download_cloud_file')
    def test_file_download(self, mock_download_cloud_file, mock_list_blobs, _):
        self._mock_files_for_participants(mock_list_blobs, [
            FakeConsentFile()
        ])

        self._create_local_files_with_download(mock_download_cloud_file)

        self._create_participant(1, self.org1.organizationId, self.site1.siteId, consents=True)
        sync_consent_files.do_sync_consent_files(zip_files=True)

        pattern_args = {
            'org_bucket_name': self.org_buckets[self.org1.externalId],
            'org_id': self.org1.externalId,
            'site_name': self.site1.googleGroup,
            'participant_id': 1
        }
        mock_download_cloud_file.assert_called_once_with(
            f'/{self.source_consent_bucket}/Participant/P1/consent.pdf',
            EXPECTED_DOWNLOAD_DESTINATION_PATTERN.format(**pattern_args, file_name='consent.pdf')
        )

    @mock.patch('rdr_service.storage.GoogleCloudStorageProvider.upload_from_file')
    @mock.patch('rdr_service.offline.sync_consent_files.list_blobs')
    @mock.patch('rdr_service.offline.sync_consent_files.download_cloud_file')
    def test_zip_upload_destinations(self, mock_download_cloud_file, mock_list_blobs, mock_file_upload):
        # Test that the zip files are uploaded to the correct buckets
        org2 = self.data_generator.create_database_organization(externalId='test_two')
        site2 = self.data_generator.create_database_site(googleGroup="group2")

        self._mock_files_for_participants(mock_list_blobs, [
            FakeConsentFile()
        ])

        self._create_local_files_with_download(mock_download_cloud_file)

        self._create_participant(1, self.org1.organizationId, self.site1.siteId, consents=True)
        self._create_participant(2, org2.organizationId, site2.siteId, consents=True)
        sync_consent_files.do_sync_consent_files(zip_files=True)

        mock_file_upload.assert_has_calls([
            mock.call(f'{tempfile.gettempdir()}/temp_consents/testbucket123/test_one/group1.zip',
                      'testbucket123/Participant/test_one/group1.zip'),
            mock.call(f'{tempfile.gettempdir()}/temp_consents/testbucket456/test_two/group2.zip',
                      'testbucket456/Participant/test_two/group2.zip')
        ], any_order=True)

    def test_iter_participants_data(self):
        """should list consenting participants
    """
        org2 = self.data_generator.create_database_organization(externalId='test_two')
        site2 = self.data_generator.create_database_site(googleGroup="group2")
        self._create_participant(1, self.org1.organizationId, self.site1.siteId, consents=True, null_email=True)
        self._create_participant(2, org2.organizationId, site2.siteId)
        self._create_participant(3, self.org1.organizationId, None, consents=True, ghost=False)
        self._create_participant(4, self.org1.organizationId, None, consents=True, ghost=True)
        self._create_participant(5, self.org1.organizationId, None, consents=True, email="foo@example.com")
        self._create_participant(6, org2.organizationId, site2.siteId, consents=True)
        participant_data_list = list(sync_consent_files._iter_participants_data(['test_one', 'test_two']))
        participant_ids = [d.participant_id for d in participant_data_list]
        self.assertEqual(len(participant_ids), 3, "finds correct number of results")
        self.assertEqual(participant_ids, [1, 3, 6], "finds valid participants")
        self.assertEqual(participant_data_list[0].google_group, "group1", "Includes google group")
        self.assertEqual(participant_data_list[1].google_group, None, "allows None for google group")

    @mock.patch("rdr_service.offline.sync_consent_files.list_blobs")
    @mock.patch("rdr_service.offline.sync_consent_files.copy_cloud_file")
    def test_cloudstorage_copy_objects_api_calls(self, mock_copy_cloud_file, mock_list_blobs):
        """Makes the proper google cloudstorage API calls
    """
        mock_list_blobs.return_value = iter([
            self._make_blob('/prefix1/foo', bucket='fake_bucket1'),
            self._make_blob('/prefix1/bar', bucket='fake_bucket1')
        ])

        # with trailing slashes
        sync_consent_files.cloudstorage_copy_objects_task("/fake_bucket1/prefix1/", "/fake_bucket2/prefix2/",
                                                          file_filter=None)
        mock_copy_cloud_file.assert_has_calls(
            [
                mock.call("/fake_bucket1/prefix1/foo", "/fake_bucket2/prefix2/foo"),
                mock.call("/fake_bucket1/prefix1/bar", "/fake_bucket2/prefix2/bar"),
            ]
        )
        # without trailing slashes
        sync_consent_files.cloudstorage_copy_objects_task("/fake_bucket1/prefix1", "/fake_bucket2/prefix2")
        mock_copy_cloud_file.assert_has_calls(
            [
                mock.call("/fake_bucket1/prefix1/foo", "/fake_bucket2/prefix2/foo"),
                mock.call("/fake_bucket1/prefix1/bar", "/fake_bucket2/prefix2/bar"),
            ]
        )

    @staticmethod
    def _write_cloud_object(cloud_file_path, contents_str):
        upload_from_string(contents_str, cloud_file_path)

    def test_cloudstorage_copy_objects_actual(self):

        mock_bucket_paths = ['fake_bucket1',
                             'fake_bucket1/prefix',
                             'fake_bucket1/prefix/x1',
                             'fake_bucket1/prefix/x1/y1',
                             'fake_bucket2',
                             'fake_bucket2/prefix',
                             'fake_bucket2/prefix/z',
                             'fake_bucket2/prefix/z/x1'
                             ]

        self.clear_default_storage()
        self.create_mock_buckets(mock_bucket_paths)
        self._write_cloud_object("/fake_bucket1/prefix/x1/foo.txt", "foo")
        self._write_cloud_object("/fake_bucket1/prefix/x1/bar.txt", "bar")
        self._write_cloud_object("/fake_bucket1/prefix/x1/y1/foo.txt", "foo")
        with open_cloud_file("/fake_bucket1/prefix/x1/y1/foo.txt") as f:
            self.assertEqual(f.read(), "foo", "Wrote to cloud storage")
        sync_consent_files.cloudstorage_copy_objects_task("/fake_bucket1/prefix/x1/", "/fake_bucket2/prefix/z/x1/",
                                                          file_filter='txt')

        self.assertEqual(
            sorted([
                "prefix/z/x1/bar.txt",
                "prefix/z/x1/foo.txt",
                "prefix/z/x1/y1/foo.txt",
            ]),
            sorted([
                file_stat.name
                for file_stat in list_blobs("fake_bucket2", "/prefix/z/x1")
            ]),
            "copied all objects",
        )
        with open_cloud_file("/fake_bucket2/prefix/z/x1/foo.txt") as f:
            self.assertEqual(f.read(), "foo", "copied contents")

    @mock.patch("rdr_service.offline.sync_consent_files.copy_cloud_file")
    def test_cloudstorage_copy_objects_only_new_and_changed(self, mock_copy_cloud_file):
        mock_bucket_paths = ['fake_bucket1',
                             'fake_bucket1/prefix',
                             'fake_bucket1/prefix/x1',
                             'fake_bucket2',
                             'fake_bucket2/prefix',
                             'fake_bucket2/prefix/z',
                             'fake_bucket2/prefix/z/x1'
                             ]

        self.clear_default_storage()
        self.create_mock_buckets(mock_bucket_paths)
        self._write_cloud_object("/fake_bucket1/prefix/x1/foo.txt", "foo")
        self._write_cloud_object("/fake_bucket1/prefix/x1/bar.txt", "bar")
        self._write_cloud_object("/fake_bucket2/prefix/z/x1/foo.txt", "foo")
        self._write_cloud_object("/fake_bucket2/prefix/z/x1/bar.txt", "baz")
        sync_consent_files.cloudstorage_copy_objects_task("/fake_bucket1/prefix/x1/", "/fake_bucket2/prefix/z/x1/",
                                                          file_filter='txt')

        mock_copy_cloud_file.assert_called_once_with("/fake_bucket1/prefix/x1/bar.txt",
                                                     "/fake_bucket2/prefix/z/x1/bar.txt")
