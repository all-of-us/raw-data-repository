import collections
import datetime

import mock
import pytz

from rdr_service import config
from rdr_service.clock import FakeClock
from rdr_service.dao.ehr_dao import EhrReceiptDao
from rdr_service.dao.hpo_dao import HPODao
from rdr_service.dao.organization_dao import OrganizationDao
from rdr_service.dao.participant_dao import ParticipantDao
from rdr_service.dao.participant_summary_dao import ParticipantSummaryDao
from rdr_service.model.hpo import HPO
from rdr_service.model.organization import Organization
from rdr_service.offline import update_ehr_status
from rdr_service.participant_enums import EhrStatus
from tests.helpers.unittest_base import BaseTestCase


class UpdateEhrStatusMakeJobsTestCase(BaseTestCase):
    # pylint: disable=unused-argument
    def setUp(self, use_mysql=False, with_data=False, with_consent_codes=False):
        super(UpdateEhrStatusMakeJobsTestCase, self).setUp()

    @mock.patch("rdr_service.cloud_utils.bigquery.build")
    @mock.patch("rdr_service.cloud_utils.bigquery.GAE_PROJECT")
    def test_make_update_participant_summaries_job(self, mock_build, mock_gae_project):
        mock_build.return_value = "foo"
        mock_gae_project.return_value = "app_id"

        config.override_setting(config.EHR_STATUS_BIGQUERY_VIEW_PARTICIPANT, ["some_view"])
        job = update_ehr_status.make_update_participant_summaries_job()
        self.assertNotEqual(job, None)

        config.override_setting(config.EHR_STATUS_BIGQUERY_VIEW_PARTICIPANT, ["a", "b"])
        job = update_ehr_status.make_update_participant_summaries_job()
        self.assertEqual(job, None)

        config.override_setting(config.EHR_STATUS_BIGQUERY_VIEW_PARTICIPANT, [""])
        job = update_ehr_status.make_update_participant_summaries_job()
        self.assertEqual(job, None)

        config.override_setting(config.EHR_STATUS_BIGQUERY_VIEW_PARTICIPANT, [None])
        job = update_ehr_status.make_update_participant_summaries_job()
        self.assertEqual(job, None)

        config.override_setting(config.EHR_STATUS_BIGQUERY_VIEW_PARTICIPANT, [])
        job = update_ehr_status.make_update_participant_summaries_job()
        self.assertEqual(job, None)

        config.override_setting(config.EHR_STATUS_BIGQUERY_VIEW_PARTICIPANT, None)
        job = update_ehr_status.make_update_participant_summaries_job()
        self.assertEqual(job, None)

    @mock.patch("rdr_service.cloud_utils.bigquery.build")
    @mock.patch("rdr_service.cloud_utils.bigquery.GAE_PROJECT")
    def test_make_update_organizations_job(self, mock_build, mock_gae_project):
        mock_build.return_value = "foo"
        mock_gae_project.return_value = "app_id"

        config.override_setting(config.EHR_STATUS_BIGQUERY_VIEW_ORGANIZATION, ["some_view"])
        job = update_ehr_status.make_update_organizations_job()
        self.assertNotEqual(job, None)

        config.override_setting(config.EHR_STATUS_BIGQUERY_VIEW_ORGANIZATION, ["a", "b"])
        job = update_ehr_status.make_update_organizations_job()
        self.assertEqual(job, None)

        config.override_setting(config.EHR_STATUS_BIGQUERY_VIEW_ORGANIZATION, [""])
        job = update_ehr_status.make_update_organizations_job()
        self.assertEqual(job, None)

        config.override_setting(config.EHR_STATUS_BIGQUERY_VIEW_ORGANIZATION, [None])
        job = update_ehr_status.make_update_organizations_job()
        self.assertEqual(job, None)

        config.override_setting(config.EHR_STATUS_BIGQUERY_VIEW_PARTICIPANT, [])
        job = update_ehr_status.make_update_participant_summaries_job()
        self.assertEqual(job, None)

        config.override_setting(config.EHR_STATUS_BIGQUERY_VIEW_PARTICIPANT, None)
        job = update_ehr_status.make_update_participant_summaries_job()
        self.assertEqual(job, None)


class UpdateEhrStatusUpdatesTestCase(BaseTestCase):
    def setUp(self, **kwargs):
        # pylint: disable=unused-argument
        super(UpdateEhrStatusUpdatesTestCase, self).setUp()
        self.hpo_dao = HPODao()
        self.org_dao = OrganizationDao()
        self.participant_dao = ParticipantDao()
        self.summary_dao = ParticipantSummaryDao()
        self.ehr_receipt_dao = EhrReceiptDao()

        self.hpo_foo = self._make_hpo(int_id=10, string_id="hpo_foo")
        self.hpo_bar = self._make_hpo(int_id=11, string_id="hpo_bar")

        self.org_foo_a = self._make_org(hpo=self.hpo_foo, int_id=10, external_id="FOO_A")
        self.org_foo_b = self._make_org(hpo=self.hpo_foo, int_id=11, external_id="FOO_B")
        self.org_bar_a = self._make_org(hpo=self.hpo_bar, int_id=12, external_id="BAR_A")

        self.participants = [
            self._make_participant(hpo=self.hpo_foo, org=self.org_foo_a, int_id=11),
            self._make_participant(hpo=self.hpo_foo, org=self.org_foo_b, int_id=12),
            self._make_participant(hpo=self.hpo_bar, org=self.org_bar_a, int_id=13),
            self._make_participant(hpo=self.hpo_bar, org=self.org_bar_a, int_id=14),
        ]

    def _make_hpo(self, int_id, string_id):
        hpo = HPO(hpoId=int_id, name=string_id)
        self.hpo_dao.insert(hpo)
        return hpo

    def _make_org(self, hpo, int_id, external_id):
        org = Organization(organizationId=int_id, externalId=external_id, displayName="SOME ORG", hpoId=hpo.hpoId)
        self.org_dao.insert(org)
        return org

    def _make_participant(self, hpo, org, int_id):
        participant = self.data_generator._participant_with_defaults(participantId=int_id, biobankId=int_id)
        participant.hpoId = hpo.hpoId
        participant.organizationId = org.organizationId
        self.participant_dao.insert(participant)
        summary = self.participant_summary(participant)
        self.summary_dao.insert(summary)
        return participant, summary

    # Mock BigQuery result types
    EhrUpdatePidRow = collections.namedtuple("EhrUpdatePidRow", ["person_id"])
    TableCountsRow = collections.namedtuple("TableCountsRow", ["org_id", "person_upload_time"])

    @mock.patch("rdr_service.offline.update_ehr_status.update_organizations_from_job")
    @mock.patch("rdr_service.offline.update_ehr_status.update_participant_summaries_from_job")
    @mock.patch("rdr_service.offline.update_ehr_status.make_update_organizations_job")
    @mock.patch("rdr_service.offline.update_ehr_status.make_update_participant_summaries_job")
    def test_skips_when_no_job(
        self, mock_summary_job, mock_organization_job, mock_update_summaries, mock_update_organizations
    ):
        mock_summary_job.return_value = None
        mock_organization_job.return_value = None

        with FakeClock(datetime.datetime(2019, 1, 1)):
            update_ehr_status.update_ehr_status()

        self.assertFalse(mock_update_summaries.called)
        self.assertFalse(mock_update_organizations.called)

    @mock.patch("rdr_service.offline.update_ehr_status.make_update_organizations_job")
    @mock.patch("rdr_service.offline.update_ehr_status.make_update_participant_summaries_job")
    def test_updates_participant_summaries(self, mock_summary_job, mock_organization_job):
        mock_summary_job.return_value.__iter__.return_value = [[self.EhrUpdatePidRow(11)]]
        mock_organization_job.return_value.__iter__.return_value = []
        with FakeClock(datetime.datetime(2019, 1, 1)):
            update_ehr_status.update_ehr_status()

        mock_summary_job.return_value.__iter__.return_value = [[self.EhrUpdatePidRow(11), self.EhrUpdatePidRow(12)]]
        mock_organization_job.return_value.__iter__.return_value = []
        with FakeClock(datetime.datetime(2019, 1, 2)):
            update_ehr_status.update_ehr_status()

        summary = self.summary_dao.get(11)
        self.assertEqual(summary.ehrStatus, EhrStatus.PRESENT)
        self.assertEqual(summary.ehrReceiptTime, datetime.datetime(2019, 1, 1))
        self.assertEqual(summary.ehrUpdateTime, datetime.datetime(2019, 1, 2))

        summary = self.summary_dao.get(12)
        self.assertEqual(summary.ehrStatus, EhrStatus.PRESENT)
        self.assertEqual(summary.ehrReceiptTime, datetime.datetime(2019, 1, 2))
        self.assertEqual(summary.ehrUpdateTime, datetime.datetime(2019, 1, 2))

    @mock.patch("rdr_service.offline.update_ehr_status.make_update_organizations_job")
    @mock.patch("rdr_service.offline.update_ehr_status.make_update_participant_summaries_job")
    def test_creates_receipts(self, mock_summary_job, mock_organization_job):
        mock_summary_job.return_value.__iter__.return_value = []
        mock_organization_job.return_value.__iter__.return_value = [
            [
                self.TableCountsRow(
                    org_id="FOO_A", person_upload_time=datetime.datetime(2019, 1, 1).replace(tzinfo=pytz.UTC)
                )
            ]
        ]
        with FakeClock(datetime.datetime(2019, 1, 1)):
            update_ehr_status.update_ehr_status()

        foo_a_receipts = self.ehr_receipt_dao.get_by_organization_id(self.org_foo_a.organizationId)
        self.assertEqual(len(foo_a_receipts), 1)
        self.assertEqual(foo_a_receipts[0].receiptTime, datetime.datetime(2019, 1, 1))

        foo_b_receipts = self.ehr_receipt_dao.get_by_organization_id(self.org_foo_b.organizationId)
        self.assertEqual(len(foo_b_receipts), 0)

        mock_summary_job.return_value.__iter__.return_value = []
        mock_organization_job.return_value.__iter__.return_value = [
            [
                self.TableCountsRow(
                    org_id="FOO_A", person_upload_time=datetime.datetime(2019, 1, 1).replace(tzinfo=pytz.UTC)
                ),
                self.TableCountsRow(
                    org_id="FOO_A", person_upload_time=datetime.datetime(2019, 1, 2).replace(tzinfo=pytz.UTC)
                ),
                self.TableCountsRow(
                    org_id="FOO_B", person_upload_time=datetime.datetime(2019, 1, 2).replace(tzinfo=pytz.UTC)
                ),
            ]
        ]
        with FakeClock(datetime.datetime(2019, 1, 2)):
            update_ehr_status.update_ehr_status()

        foo_a_receipts = self.ehr_receipt_dao.get_by_organization_id(self.org_foo_a.organizationId)
        self.assertEqual(len(foo_a_receipts), 2)
        self.assertEqual(foo_a_receipts[0].receiptTime, datetime.datetime(2019, 1, 1))
        self.assertEqual(foo_a_receipts[1].receiptTime, datetime.datetime(2019, 1, 2))

        foo_b_receipts = self.ehr_receipt_dao.get_by_organization_id(self.org_foo_b.organizationId)
        self.assertEqual(len(foo_b_receipts), 1)
        self.assertEqual(foo_b_receipts[0].receiptTime, datetime.datetime(2019, 1, 2))

    @mock.patch("rdr_service.offline.update_ehr_status.make_update_organizations_job")
    @mock.patch("rdr_service.offline.update_ehr_status.make_update_participant_summaries_job")
    def test_ignores_bad_data(self, mock_summary_job, mock_organization_job):
        invalid_participant_id = -1
        mock_summary_job.return_value.__iter__.return_value = [[self.EhrUpdatePidRow(invalid_participant_id)]]
        mock_organization_job.return_value.__iter__.return_value = [
            [
                self.TableCountsRow(org_id="FOO_A", person_upload_time="an invalid date string"),
                self.TableCountsRow(
                    org_id="AN_ORG_THAT_DOESNT_EXIST",
                    person_upload_time=datetime.datetime(2019, 1, 1).replace(tzinfo=pytz.UTC),
                ),
                self.TableCountsRow(org_id="AN_ORG_THAT_DOESNT_EXIST", person_upload_time=None),
            ]
        ]
        with FakeClock(datetime.datetime(2019, 1, 1)):
            update_ehr_status.update_ehr_status()

        foo_a_receipts = self.ehr_receipt_dao.get_all()
        self.assertEqual(len(foo_a_receipts), 0)
