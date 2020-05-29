from collections import namedtuple
from datetime import datetime
import mock
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
        participant = self.create_database_participant(organizationId=org.organizationId, siteId=site.siteId)
        self.create_database_participant_summary(
            participant=participant,
            consentForStudyEnrollment=QuestionnaireStatus.SUBMITTED
        )

        self.default_copy_args = {
            'args': '-r',
            'flags': '-m'
        }

    @staticmethod
    def _greenwich_time_now():
        return datetime.now(pytz.timezone('Etc/Greenwich'))

    @staticmethod
    def run_sync(date_limit='2020-05-01', end_date=None, org_id='test_org', destination_bucket=None,
                 all_files=False, zip_files=False, dry_run=None):
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

        # Patching things to keep tool from trying to call GAE, to provide test data, and to keep it from making
        # consent directories on the test machine.
        with mock.patch('rdr_service.tools.tool_libs.sync_consent.make_api_request',
                        return_value=(200, {'rdr_db_password': 'test'})),\
                mock.patch.dict('rdr_service.tools.tool_libs.sync_consent.SOURCE_BUCKET', {
                    'example': "gs://fake/Participant/P{p_id}/*{file_ext}"
                }),\
                mock.patch('rdr_service.tools.tool_libs.sync_consent.GoogleCloudStorageProvider.list', return_value=[
                    FakeFile(name='one.pdf', updated=SyncConsentTest._greenwich_time_now())
                ]),\
                mock.patch('rdr_service.tools.tool_libs.sync_consent.os.makedirs'):

            sync_consent_tool = SyncConsentClass(args, environment)
            sync_consent_tool.run()

    def test_file_upload(self, mock_gcp_cp):
        self.run_sync(zip_files=True)
        mock_gcp_cp.assert_called_with('one.pdf',
                                       './temp_consents/test_bucket/test_org/test_site_google_group/P900000000/',
                                       **self.default_copy_args)
