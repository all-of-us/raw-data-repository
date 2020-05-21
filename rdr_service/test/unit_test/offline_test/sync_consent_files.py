import mock

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
from rdr_service.test.unit_test.unit_test_util import CloudStorageSqlTestBase, NdbTestBase, TestBase


class SyncConsentFilesTest(CloudStorageSqlTestBase, NdbTestBase):
    """Tests behavior of sync_consent_files
  """

    def setUp(self, **kwargs):
        super(SyncConsentFilesTest, self).setUp(use_mysql=True, **kwargs)
        NdbTestBase.doSetUp(self)
        TestBase.setup_fake(self)
        self.org_dao = OrganizationDao()
        self.site_dao = SiteDao()
        self.participant_dao = ParticipantDao()
        self.summary_dao = ParticipantSummaryDao()

    def tearDown(self):
        super(SyncConsentFilesTest, self).tearDown()

    def _create_org(self, id_):
        org = Organization(organizationId=id_, externalId=id_, displayName=id_, hpoId=UNSET_HPO_ID)
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

    def test_iter_participants_data(self):
        """should list consenting participants
    """
        org1 = self._create_org(1)
        org2 = self._create_org(2)
        site1 = self._create_site(1001, "group1")
        site2 = self._create_site(1002, "group2")
        self._create_participant(1, org1.organizationId, site1.siteId, consents=True, null_email=True)
        self._create_participant(2, org2.organizationId, site2.siteId)
        self._create_participant(3, org1.organizationId, None, consents=True, ghost=False)
        self._create_participant(4, org1.organizationId, None, consents=True, ghost=True)
        self._create_participant(5, org1.organizationId, None, consents=True, email="foo@example.com")
        participant_data_list = list(sync_consent_files._iter_participants_data())
        participant_ids = [d.participant_id for d in participant_data_list]
        self.assertEqual(len(participant_ids), 2, "finds correct number of results")
        self.assertEqual(participant_ids, [1, 3], "finds valid participants")
        self.assertEqual(participant_data_list[0].google_group, "group1", "Includes google group")
        self.assertEqual(participant_data_list[1].google_group, None, "allows None for google group")

    @mock.patch("cloudstorage.listbucket")
    @mock.patch("cloudstorage.copy2")
    def test_cloudstorage_copy_objects_api_calls(self, mock_copy2, mock_listbucket):
        """Makes the proper google cloudstorage API calls
    """
        mock_listbucket.return_value = [  # @todo: find an alternative
            # cloudstorage.common.GCSFileStat("/fake_bucket1/prefix1/foo", 0, "x", 0),
            # cloudstorage.common.GCSFileStat("/fake_bucket1/prefix1/bar", 0, "x", 0),
        ]
        # with trailing slashes
        sync_consent_files.cloudstorage_copy_objects("/fake_bucket1/prefix1/", "/fake_bucket2/prefix2/")
        mock_copy2.assert_has_calls(
            [
                mock.call("/fake_bucket1/prefix1/foo", "/fake_bucket2/prefix2/foo"),
                mock.call("/fake_bucket1/prefix1/bar", "/fake_bucket2/prefix2/bar"),
            ]
        )
        # without trailing slashes
        sync_consent_files.cloudstorage_copy_objects("/fake_bucket1/prefix1", "/fake_bucket2/prefix2")
        mock_copy2.assert_has_calls(
            [
                mock.call("/fake_bucket1/prefix1/foo", "/fake_bucket2/prefix2/foo"),
                mock.call("/fake_bucket1/prefix1/bar", "/fake_bucket2/prefix2/bar"),
            ]
        )

    @staticmethod
    def _write_cloud_object(cloud_filename, contents_str):
        upload_from_string(contents_str, "/rdr-fake-bucket/" + cloud_filename)

    def test_cloudstorage_copy_objects_actual(self):
        self._write_cloud_object("/fake_bucket1/prefix/x1/foo.txt", "foo")
        self._write_cloud_object("/fake_bucket1/prefix/x1/bar.txt", "bar")
        self._write_cloud_object("/fake_bucket1/prefix/x1/y1/foo.txt", "foo")
        f = open_cloud_file("/fake_bucket1/prefix/x1/foo.txt")
        self.assertEqual(f.read(), "foo", "Wrote to cloud storage")
        sync_consent_files.cloudstorage_copy_objects("/fake_bucket1/prefix/x1/", "/fake_bucket2/prefix/z/x1/")
        self.assertEqual(
            [
                file_stat.filename
                for file_stat in list_blobs("/fake_bucket2/prefix/z/x1/")
            ],
            [
                "/fake_bucket2/prefix/z/x1/bar.txt",
                "/fake_bucket2/prefix/z/x1/foo.txt",
                "/fake_bucket2/prefix/z/x1/y1/foo.txt",
            ],
            "copied all objects",
        )
        f = open_cloud_file("/fake_bucket2/prefix/z/x1/foo.txt")
        self.assertEqual(f.read(), "foo", "copied contents")

    @mock.patch("cloudstorage.copy2")
    def test_cloudstorage_copy_objects_only_new_and_changed(self, copy2):
        self._write_cloud_object("/fake_bucket1/prefix/x1/foo.txt", "foo")
        self._write_cloud_object("/fake_bucket1/prefix/x1/bar.txt", "bar")
        self._write_cloud_object("/fake_bucket2/prefix/z/x1/foo.txt", "foo")
        self._write_cloud_object("/fake_bucket2/prefix/z/x1/bar.txt", "baz")
        sync_consent_files.cloudstorage_copy_objects("/fake_bucket1/prefix/x1/", "/fake_bucket2/prefix/z/x1/")
        copy2.assert_called_once_with("/fake_bucket1/prefix/x1/bar.txt", "/fake_bucket2/prefix/z/x1/bar.txt")
