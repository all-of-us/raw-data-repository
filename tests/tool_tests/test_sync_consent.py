from collections import namedtuple
from datetime import datetime
import mock
import os
from pathlib import Path
import pytz
import tempfile

from rdr_service import config
from rdr_service.participant_enums import QuestionnaireStatus
from rdr_service.tools.tool_libs.sync_consent import SyncConsentClass
from tests.helpers.unittest_base import BaseTestCase
from tests.helpers.tool_test_mixin import ToolTestMixin

FakeFile = namedtuple('FakeFile', ['name', 'updated'])


@mock.patch('rdr_service.storage.GoogleCloudStorageProvider.upload_from_file')
@mock.patch("rdr_service.offline.sync_consent_files.gcp_cp")
class SyncConsentTest(ToolTestMixin, BaseTestCase):
    def setUp(self):
        super().setUp()

        config.override_setting(config.CONSENT_SYNC_BUCKETS, {
            'test_org': 'test_dest_bucket'
        })

        site = self.data_generator.create_database_site(googleGroup='test_site_google_group')
        org = self.data_generator.create_database_organization(externalId='test_org')
        self.participant = self.data_generator.create_database_participant(organizationId=org.organizationId, siteId=site.siteId)
        self.data_generator.create_database_participant_summary(
            participant=self.participant,
            consentForStudyEnrollment=QuestionnaireStatus.SUBMITTED
        )

    @staticmethod
    def setup_local_file_creation(mock_gcp_cp):
        # Actually create the files locally so the zipping code will have something to work with
        def create_local_file(source, destination, **_):
            if destination.startswith(tempfile.gettempdir()):
                Path(os.path.join(destination, Path(source).name)).touch()
        mock_gcp_cp.side_effect = create_local_file

    @staticmethod
    def run_sync(date_limit='2020-05-01', end_date=None, org_id='test_org', destination_bucket=None,
                 all_files=False, zip_files=False, dry_run=None, consent_files=[], all_va=False):
        with mock.patch.dict('rdr_service.tools.tool_libs.sync_consent.SOURCE_BUCKET', {
                    'example': "gs://uploads_bucket/Participant/P{p_id}/*{file_ext}"
                }),\
                mock.patch('rdr_service.tools.tool_libs.sync_consent.GoogleCloudStorageProvider.list',
                           return_value=consent_files):
            SyncConsentTest.run_tool(SyncConsentClass, tool_args={
                'date_limit': date_limit,
                'end_date': end_date,
                'org_id': None if all_va else org_id,
                'destination_bucket': destination_bucket,
                'all_files': all_files,
                'zip_files': zip_files,
                'dry_run': dry_run,
                'all_va': all_va,
                'pid_file': None
            })

    @staticmethod
    def _fake_file(participant, file_name):
        return FakeFile(name=f'Participant/P{participant.participantId}/{file_name}',
                        updated=datetime.now(pytz.timezone('Etc/Greenwich')))

    @staticmethod
    def assertZipFilesCreated(mock_zip_file, directory, relative_path):
        zip_instance = mock_zip_file.return_value
        zip_instance_context = zip_instance.__enter__.return_value
        mock_zip_write = zip_instance_context.write
        mock_zip_write.assert_any_call(os.path.join(directory, relative_path), arcname=f'/{relative_path}')

    def test_zip_file_download(self, mock_gcp_cp, _):
        self.run_sync(zip_files=True, consent_files=[self._fake_file(self.participant, 'one.pdf')])

        # Assert that the files were copied locally for zipping
        zip_path = '{temp_dir}/temp_consents/{bucket}/{org_id}/{site}/P{participant_id}/'.format(
            temp_dir=tempfile.gettempdir(),
            bucket='test_dest_bucket',
            org_id='test_org',
            site='test_site_google_group',
            participant_id=self.participant.participantId
        )
        mock_gcp_cp.assert_any_call(
            f'gs://uploads_bucket/Participant/P{self.participant.participantId}/one.pdf',
            zip_path,
            flags='-m')

    @mock.patch('rdr_service.offline.sync_consent_files.ZipFile')
    def test_zip_file_write(self, mock_zip_file, mock_gcp_cp, _):
        self.setup_local_file_creation(mock_gcp_cp)

        self.run_sync(zip_files=True, consent_files=[self._fake_file(self.participant, 'one.pdf')])

        # Assert that the correct files were written into the zip
        zip_path = f'{tempfile.gettempdir()}/temp_consents/test_dest_bucket/test_org/test_site_google_group/'
        self.assertZipFilesCreated(mock_zip_file,
                                   zip_path,
                                   f'P{self.participant.participantId}/one.pdf')

    def test_zip_file_upload(self, mock_gcp_cp, mock_upload_file):
        self.setup_local_file_creation(mock_gcp_cp)

        self.run_sync(zip_files=True, consent_files=[self._fake_file(self.participant, 'one.pdf')])

        # Assert that the zip was uploaded to the correct location
        mock_upload_file.assert_any_call(
            f'{tempfile.gettempdir()}/temp_consents/test_dest_bucket/test_org/test_site_google_group.zip',
            'test_dest_bucket/Participant/test_org/test_site_google_group.zip'
        )

    def test_moving_cloud_file(self, mock_gcp_cp, _):
        self.run_sync(consent_files=[self._fake_file(self.participant, 'one.pdf')])

        # Make sure the file was moved on the cloud if we aren't zipping
        mock_gcp_cp.assert_any_call(
            f'gs://uploads_bucket/Participant/P{self.participant.participantId}/one.pdf',
            f'gs://test_dest_bucket/Participant/test_org/test_site_google_group/P{self.participant.participantId}/',
            flags='-m', args='-r')

    # There's a switch that targets the VA upload bucket for all organizations that belong under the VA hpo
    def test_va_zip_upload(self, mock_gcp_cp, mock_upload_file):
        self.setup_local_file_creation(mock_gcp_cp)
        site = self.data_generator.create_database_site(googleGroup='boston_site')
        org = self.data_generator.create_database_organization(externalId='VA_BOSTON')
        va_participant = self.data_generator.create_database_participant(organizationId=org.organizationId, siteId=site.siteId)
        self.data_generator.create_database_participant_summary(
            participant=va_participant,
            consentForStudyEnrollment=QuestionnaireStatus.SUBMITTED
        )

        self.run_sync(zip_files=True, all_va=True, consent_files=[self._fake_file(va_participant, 'consent.pdf')])

        # Assert that the zip was uploaded to the VA bucket
        mock_upload_file.assert_any_call(
            f'{tempfile.gettempdir()}/temp_consents/aou179/VA_BOSTON/boston_site.zip',
            'aou179/Participant/VA_BOSTON/boston_site.zip'
        )

    def test_loading_only_va_participants(self, mock_gcp_cp, _):
        # The test setup creates a participant that should have a file downloaded if they were loaded from the database.
        # But they're not in a VA organization, so we shouldn't see a call for them.

        self.run_sync(zip_files=True, all_va=True, consent_files=[self._fake_file(self.participant, 'consent.pdf')])
        mock_gcp_cp.assert_not_called()
