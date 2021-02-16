from collections import namedtuple
from datetime import datetime
import mock
import pytz

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

        site = self.data_generator.create_database_site(googleGroup='test_site_google_group')
        org = self.data_generator.create_database_organization(externalId='test_org')
        self.participant = self.data_generator.create_database_participant(organizationId=org.organizationId, siteId=site.siteId)
        self.data_generator.create_database_participant_summary(
            participant=self.participant,
            consentForStudyEnrollment=QuestionnaireStatus.SUBMITTED
        )

    @classmethod
    def run_sync(cls, date_limit='2020-05-01', end_date=None, org_id='test_org', destination_bucket=None,
                 all_files=False, zip_files=False, dry_run=None, consent_files=[], all_va=False):
        with mock.patch.dict('rdr_service.tools.tool_libs.sync_consent.SOURCE_BUCKET', {
                    'example': "gs://uploads_bucket/Participant/P{p_id}/*{file_ext}"
                }),\
                mock.patch('rdr_service.tools.tool_libs.sync_consent.GoogleCloudStorageProvider.list',
                           return_value=consent_files):
            cls.run_tool(SyncConsentClass, tool_args={
                'date_limit': date_limit,
                'end_date': end_date,
                'org_id': None if all_va else org_id,
                'destination_bucket': destination_bucket,
                'all_files': all_files,
                'zip_files': zip_files,
                'dry_run': dry_run,
                'all_va': all_va,
                'pid_file': None
            }, server_config={
                config.CONSENT_SYNC_BUCKETS: {
                    'test_org': 'test_dest_bucket'
                }
            })

    @staticmethod
    def _fake_file(participant, file_name):
        return FakeFile(name=f'Participant/P{participant.participantId}/{file_name}',
                        updated=datetime.now(pytz.timezone('Etc/Greenwich')))

    def test_moving_cloud_file(self, mock_gcp_cp, _):
        self.run_sync(consent_files=[self._fake_file(self.participant, 'one.pdf')])

        # Make sure the file was moved on the cloud if we aren't zipping
        mock_gcp_cp.assert_any_call(
            f'gs://uploads_bucket/Participant/P{self.participant.participantId}/one.pdf',
            f'gs://test_dest_bucket/Participant/test_org/test_site_google_group/P{self.participant.participantId}/',
            flags='-m', args='-r')
