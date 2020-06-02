from collections import namedtuple
from datetime import datetime
import mock
import os
from pathlib import Path
import pytz

from rdr_service import config
from rdr_service.participant_enums import QuestionnaireStatus
from rdr_service.tools.tool_libs.sync_consent import SyncConsentClass
from tests.helpers.unittest_base import BaseTestCase

FakeFile = namedtuple('FakeFile', ['name', 'updated'])


@mock.patch("rdr_service.tools.tool_libs.sync_consent.gcp_cp")
class SyncConsentTest(BaseTestCase):
    def setUp(self):
        super().setUp()

        config.override_setting(config.CONSENT_SYNC_ORGANIZATIONS, [{
            'test_org': {
                'bucket_name': 'test_bucket'
            }
        }])

        site = self.create_database_site(googleGroup='test_site_google_group')
        org = self.create_database_organization(externalId='test_org')
        self.participant = self.create_database_participant(organizationId=org.organizationId, siteId=site.siteId)
        self.create_database_participant_summary(
            participant=self.participant,
            consentForStudyEnrollment=QuestionnaireStatus.SUBMITTED
        )

        self.default_copy_args = {
            'flags': '-m'
        }

    @staticmethod
    def _greenwich_time_now():
        return datetime.now(pytz.timezone('Etc/Greenwich'))

    def config_copy_mock(self, mock_gcp_cp):
        # Actually create the files locally so the zipping code will have something to work with
        def create_local_file(source, destination, **_):
            if destination.startswith('./'):
                print('creating', os.path.join(destination, Path(source).name))
                Path(os.path.join(destination, Path(source).name)).touch()
        mock_gcp_cp.side_effect = create_local_file

    @staticmethod
    def run_sync(date_limit='2020-05-01', end_date=None, org_id='test_org', destination_bucket=None,
                 all_files=False, zip_files=False, dry_run=None, files=[], all_va=False):
        environment = mock.MagicMock()
        environment.project = 'unit_test'

        args = mock.MagicMock()
        args.date_limit = date_limit
        args.end_date = end_date
        args.org_id = org_id
        args.destination_bucket = destination_bucket
        args.all_files = all_files
        args.zip_files = zip_files
        args.dry_run = dry_run
        args.all_va = all_va

        # Patching things to keep tool from trying to call GAE, to provide test data
        with mock.patch('rdr_service.tools.tool_libs.sync_consent.make_api_request',
                        return_value=(200, {'rdr_db_password': 'test'})),\
                mock.patch.dict('rdr_service.tools.tool_libs.sync_consent.SOURCE_BUCKET', {
                    'example': "gs://uploads_bucket/Participant/P{p_id}/*{file_ext}"
                }),\
                mock.patch('rdr_service.tools.tool_libs.sync_consent.GoogleCloudStorageProvider.list',
                           return_value=files):

            sync_consent_tool = SyncConsentClass(args, environment)
            sync_consent_tool.run()

    ## @mock.patch('rdr_service.tools.tool_libs.sync_consent.ZipFile')
    def test_zip_file_upload(self, mock_gcp_cp):
        self.config_copy_mock(mock_gcp_cp)

        self.run_sync(zip_files=True, files=[
            FakeFile(name=f'Participant/P{self.participant.participantId}/one.pdf',
                     updated=SyncConsentTest._greenwich_time_now())
        ])
        mock_gcp_cp.assert_called_with(
            f'gs://uploads_bucket/Participant/P{self.participant.participantId}/one.pdf',
            f'./temp_consents/test_bucket/test_org/test_site_google_group/P{self.participant.participantId}/',
            **self.default_copy_args)
