import mock

from rdr_service import config
from rdr_service.participant_enums import QuestionnaireStatus
from rdr_service.tools.tool_libs.sync_consent import SyncConsentClass
from tests.helpers.unittest_base import BaseTestCase


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
        participant = self.create_database_participant(organizationId=org.id, siteId=site.id)
        self.create_database_participant_summary(
            participantId=participant.id,
            consentForStudyEnrollment=QuestionnaireStatus.SUBMITTED
        )

    @staticmethod
    def run_sync(date_limit='2020-05-01', end_date=None, org_id='test_org'):
        environment = mock.MagicMock()
        environment.project = 'unit_test'

        args = mock.MagicMock()
        args.date_limit = date_limit
        args.end_date = end_date
        args.org_id = org_id

        with mock.patch('rdr_service.tools.tool_libs.sync_consent.make_api_request') as mock_api_request,\
                mock.patch.dict('rdr_service.tools.tool_libs.sync_consent.SOURCE_BUCKET', {
                    'example': "gs://fake/Participant/P{p_id}/*{file_ext}"
                }):
            mock_api_request.return_value = (200, {'rdr_db_password': 'test'})

            sync_consent_tool = SyncConsentClass(args, environment)
            sync_consent_tool.run()

    def test_file_upload(self):
        self.run_sync()
        self.assertTrue(True)
