import mock

from google.cloud.storage import Blob
from rdr_service import config
from rdr_service.api_util import upload_from_string, open_cloud_file, list_blobs
from rdr_service.dao.organization_dao import OrganizationDao
from rdr_service.dao.participant_dao import ParticipantDao
from rdr_service.dao.participant_summary_dao import ParticipantSummaryDao
from rdr_service.dao.site_dao import SiteDao
from rdr_service.model.organization import Organization
from rdr_service.model.participant import Participant
from rdr_service.model.site import Site
from rdr_service.offline import sync_consent_files
from rdr_service.participant_enums import UNSET_HPO_ID
from tests.helpers.unittest_base import BaseTestCase


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

        self.org_map_data = {
            "test_one": {
                "hpo_id": "t_1",
                "bucket_name": "testbucket123"
            },
            "test_two": {
                "hpo_id": "t_2",
                "bucket_name": "testbucket456"
            }
        }
        config.override_setting(config.CONSENT_SYNC_ORGANIZATIONS, [self.org_map_data])

    def tearDown(self):
        super(SyncConsentFilesTest, self).tearDown()

    def _create_org(self, id_, external_id):
        org = Organization(organizationId=id_, externalId=external_id, displayName=id_, hpoId=UNSET_HPO_ID)
        self.org_dao.insert(org)
        return org

    def _create_site(self, id_, google_group):
        site = Site(siteId=id_, siteName=id_, googleGroup=google_group)
        self.site_dao.insert(site)
        return site

    def _create_participant(self, id_, org_id, site_id, consents=False, ghost=None, email=None, null_email=False):
        participant = Participant(
            participantId=id_, biobankId=id_, organizationId=org_id, siteId=site_id, isGhostId=ghost
        )
        self.participant_dao.insert(participant)
        summary = self.participant_summary(participant)
        if consents:
            summary.consentForElectronicHealthRecords = 1
            summary.consentForStudyEnrollment = 1
        if email:
            summary.email = email
        if null_email:
            summary.email = None
        self.summary_dao.insert(summary)
        return participant

    @mock.patch("rdr_service.offline.sync_consent_files.list_blobs")
    @mock.patch('rdr_service.offline.sync_consent_files.copy_cloud_file')
    def test_basic_consent_file_copy(self, mock_copy_cloud_file, mock_list_blobs):
        mock_copy_cloud_file.return_value = True

        source_consent_bucket = sync_consent_files.SOURCE_BUCKET
        mock_list_blobs.return_value = iter([
            Blob('/consent.pdf', bucket=source_consent_bucket),
            Blob('/addendum.pdf', bucket=source_consent_bucket)
        ])

        org1 = self._create_org(1, 'test_one')
        site1 = self._create_site(1001, "group1")
        self._create_participant(1, org1.organizationId, site1.siteId, consents=True)
        sync_consent_files.do_sync_consent_files()

        org_bucket_name = self.org_map_data[org1.externalId]['bucket_name']
        mock_copy_cloud_file.assert_has_calls(
            [
                mock.call("/{}/Participant/P1/consent.pdf".format(source_consent_bucket),
                          "/{}/Participant/{}/P1/".format(org_bucket_name, site1.googleGroup)),
                mock.call("/{}/Participant/P1/addendum.pdf".format(source_consent_bucket),
                          "/{}/Participant/{}/P1/".format(org_bucket_name, site1.googleGroup))
            ]
        )

    def test_iter_participants_data(self):
        """should list consenting participants
    """
        org1 = self._create_org(1, 'test_one')
        org2 = self._create_org(2, 'test_two')
        site1 = self._create_site(1001, "group1")
        site2 = self._create_site(1002, "group2")
        self._create_participant(1, org1.organizationId, site1.siteId, consents=True, null_email=True)
        self._create_participant(2, org2.organizationId, site2.siteId)
        self._create_participant(3, org1.organizationId, None, consents=True, ghost=False)
        self._create_participant(4, org1.organizationId, None, consents=True, ghost=True)
        self._create_participant(5, org1.organizationId, None, consents=True, email="foo@example.com")
        participant_data_list = list(sync_consent_files._iter_participants_data(['test_one', 'test_two']))
        participant_ids = [d.participant_id for d in participant_data_list]
        self.assertEqual(len(participant_ids), 2, "finds correct number of results")
        self.assertEqual(participant_ids, [1, 3], "finds valid participants")
        self.assertEqual(participant_data_list[0].google_group, "group1", "Includes google group")
        self.assertEqual(participant_data_list[1].google_group, None, "allows None for google group")

    @mock.patch("rdr_service.offline.sync_consent_files.list_blobs")
    @mock.patch("rdr_service.offline.sync_consent_files.copy_cloud_file")
    def test_cloudstorage_copy_objects_api_calls(self, mock_copy_cloud_file, mock_list_blobs):
        """Makes the proper google cloudstorage API calls
    """
        mock_list_blobs.return_value = iter([
            Blob('/prefix1/foo', bucket='fake_bucket1'),
            Blob('/prefix1/bar', bucket='fake_bucket1')
        ])

        # with trailing slashes
        sync_consent_files.cloudstorage_copy_objects_task("/fake_bucket1/prefix1/", "/fake_bucket2/prefix2/")
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
        sync_consent_files.cloudstorage_copy_objects_task("/fake_bucket1/prefix/x1/", "/fake_bucket2/prefix/z/x1/")
        self.assertEqual(
            sorted([
                file_stat.name
                for file_stat in list_blobs("fake_bucket2", "/prefix/z/x1")
            ]),
            sorted([
                "prefix/z/x1/bar.txt",
                "prefix/z/x1/foo.txt",
                "prefix/z/x1/y1/foo.txt",
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
        sync_consent_files.cloudstorage_copy_objects_task("/fake_bucket1/prefix/x1/", "/fake_bucket2/prefix/z/x1/")

        mock_copy_cloud_file.assert_called_once_with("/fake_bucket1/prefix/x1/bar.txt",
                                                     "/fake_bucket2/prefix/z/x1/bar.txt")
