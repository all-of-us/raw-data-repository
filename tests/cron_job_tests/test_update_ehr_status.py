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
from rdr_service.model.ehr import ParticipantEhrReceipt
from rdr_service.model.hpo import HPO
from rdr_service.model.organization import Organization
from rdr_service.offline import update_ehr_status
from rdr_service.participant_enums import EhrStatus
from rdr_service.resource.generators.participant import ParticipantSummaryGenerator
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
    EhrUpdatePidRow = collections.namedtuple("EhrUpdatePidRow", ["person_id", "latest_upload_time"])
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
            update_ehr_status.update_ehr_status_participant()
            update_ehr_status.update_ehr_status_organization()

        self.assertFalse(mock_update_summaries.called)
        self.assertFalse(mock_update_organizations.called)

    def assert_ehr_data_matches(self, had_ehr_status: EhrStatus, currently_has_ehr, first_ehr_time, latest_ehr_time,
                                participant_id):
        # Check participant summary
        participant_summary = self.summary_dao.get(participant_id)
        self.assertEqual(had_ehr_status, participant_summary.ehrStatus)
        self.assertEqual(currently_has_ehr, participant_summary.isEhrDataAvailable)
        self.assertEqual(first_ehr_time, participant_summary.ehrReceiptTime)
        self.assertEqual(latest_ehr_time, participant_summary.ehrUpdateTime)

        # Check generated data
        gen = ParticipantSummaryGenerator()
        ps_data = gen.make_resource(participant_id).get_data()
        self.assertEqual(str(participant_summary.ehrStatus), ps_data['ehr_status'])
        self.assertEqual(participant_summary.ehrReceiptTime, ps_data['ehr_receipt'])
        self.assertEqual(participant_summary.ehrUpdateTime, ps_data['ehr_update'])

    def assert_has_participant_ehr_record(self, participant_id, file_timestamp, first_seen, last_seen):
        record = self.session.query(ParticipantEhrReceipt).filter(
            ParticipantEhrReceipt.participantId == participant_id,
            ParticipantEhrReceipt.fileTimestamp == file_timestamp
        ).one_or_none()
        self.assertIsNotNone(record, f'EHR receipt was not recorded for participant {participant_id}')

        # The first_seen and last_seen fields are set with mysql's NOW function,
        #   so check that the time is close to what is expected

        # mysql isn't storing the microseconds
        first_seen = first_seen.replace(microsecond=0)
        last_seen = last_seen.replace(microsecond=0)

        self.assertLessEqual(first_seen, record.firstSeen,
                             "The record found has a firstSeen time earlier than expected")
        self.assertGreaterEqual(1, (record.firstSeen - first_seen).seconds,
                                "The record found has a firstSeen time much later than expected")
        self.assertLessEqual(last_seen, record.lastSeen,
                             "The record found has a lastSeen time earlier than expected")
        self.assertGreaterEqual(1, (record.lastSeen - last_seen).seconds,
                                "The record found has a lastSeen time much later than expected")

    @mock.patch("rdr_service.offline.update_ehr_status.make_update_participant_summaries_job")
    def test_updates_participant_summaries(self, mock_summary_job):

        # Run job with data for participants 11 and 14
        p_eleven_first_upload = self.EhrUpdatePidRow(11, datetime.datetime(2020, 3, 12, 8))
        p_fourteen_upload = self.EhrUpdatePidRow(14, datetime.datetime(2020, 3, 12, 10))
        mock_summary_job.return_value.__iter__.return_value = [[p_eleven_first_upload, p_fourteen_upload]]

        first_job_run_time = datetime.datetime.utcnow()
        update_ehr_status.update_ehr_status_participant()

        # Run job with data for participants 11 and 12 (leaving 14 out)
        new_p_eleven_upload = self.EhrUpdatePidRow(11, datetime.datetime(2020, 3, 30, 2))
        p_twelve_upload = self.EhrUpdatePidRow(12, datetime.datetime(2020, 3, 27, 18))
        mock_summary_job.return_value.__iter__.return_value = [
            [p_eleven_first_upload, new_p_eleven_upload, p_twelve_upload]
        ]

        second_job_run_time = datetime.datetime.utcnow()
        update_ehr_status.update_ehr_status_participant()

        self.assert_ehr_data_matches(
            participant_id=11,
            had_ehr_status=EhrStatus.PRESENT,
            currently_has_ehr=True,
            first_ehr_time=p_eleven_first_upload.latest_upload_time,
            latest_ehr_time=new_p_eleven_upload.latest_upload_time
        )
        self.assert_has_participant_ehr_record(
            participant_id=11,
            file_timestamp=p_eleven_first_upload.latest_upload_time,
            first_seen=first_job_run_time,
            last_seen=second_job_run_time
        )
        self.assert_has_participant_ehr_record(
            participant_id=11,
            file_timestamp=new_p_eleven_upload.latest_upload_time,
            first_seen=second_job_run_time,
            last_seen=second_job_run_time
        )

        self.assert_ehr_data_matches(
            participant_id=12,
            had_ehr_status=EhrStatus.PRESENT,
            currently_has_ehr=True,
            first_ehr_time=p_twelve_upload.latest_upload_time,
            latest_ehr_time=p_twelve_upload.latest_upload_time
        )
        self.assert_has_participant_ehr_record(
            participant_id=12,
            file_timestamp=p_twelve_upload.latest_upload_time,
            first_seen=second_job_run_time,
            last_seen=second_job_run_time
        )

        self.assert_ehr_data_matches(
            participant_id=14,
            had_ehr_status=EhrStatus.PRESENT,
            currently_has_ehr=False,
            first_ehr_time=p_fourteen_upload.latest_upload_time,
            latest_ehr_time=p_fourteen_upload.latest_upload_time
        )
        self.assert_has_participant_ehr_record(
            participant_id=14,
            file_timestamp=p_fourteen_upload.latest_upload_time,
            first_seen=first_job_run_time,
            last_seen=first_job_run_time
        )


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
            update_ehr_status.update_ehr_status_participant()
            update_ehr_status.update_ehr_status_organization()

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
            update_ehr_status.update_ehr_status_participant()
            update_ehr_status.update_ehr_status_organization()

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
        mock_summary_job.return_value.__iter__.return_value = [[
            self.EhrUpdatePidRow(invalid_participant_id, datetime.datetime(2020, 10, 10))
        ]]
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
            update_ehr_status.update_ehr_status_participant()
            update_ehr_status.update_ehr_status_organization()

        foo_a_receipts = self.ehr_receipt_dao.get_all()
        self.assertEqual(len(foo_a_receipts), 0)
