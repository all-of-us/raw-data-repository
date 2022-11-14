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
from tests.helpers.unittest_base import BaseTestCase, PDRGeneratorTestMixin


class UpdateEhrStatusMakeJobsTestCase(BaseTestCase):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.uses_database = False

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


class UpdateEhrStatusUpdatesTestCase(BaseTestCase, PDRGeneratorTestMixin):
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
        ps_data = self.make_participant_resource(participant_id)
        self.assertEqual(str(participant_summary.ehrStatus), ps_data['ehr_status'])
        self.assertEqual(participant_summary.ehrReceiptTime, ps_data['ehr_receipt'])
        self.assertEqual(participant_summary.ehrUpdateTime, ps_data['ehr_update'])
        self.assertEqual(participant_summary.isEhrDataAvailable, ps_data['is_ehr_data_available'])
        self.assertEqual(first_ehr_time, ps_data['first_ehr_receipt_time'])
        self.assertEqual(latest_ehr_time, ps_data['latest_ehr_receipt_time'])

    def assert_has_participant_ehr_record(self, participant_id, file_timestamp, first_seen, last_seen):
        record = self.session.query(ParticipantEhrReceipt).filter(
            ParticipantEhrReceipt.participantId == participant_id,
            ParticipantEhrReceipt.fileTimestamp == file_timestamp
        ).one_or_none()
        self.assertIsNotNone(record, f'EHR receipt was not recorded for participant {participant_id}')

        # The first_seen and last_seen fields are set with mysql's NOW function,
        #   so check that the time is close to what is expected
        self.assertAlmostEquals(first_seen, record.firstSeen, delta=datetime.timedelta(seconds=1))
        self.assertAlmostEquals(last_seen, record.lastSeen, delta=datetime.timedelta(seconds=1))

        # Check generated data.
        ps_data = self.make_participant_resource(participant_id)
        self.assertIsNotNone(ps_data['ehr_receipts'], f'PDR EHR receipt data not generated for pid {participant_id}')

        # Look for a matching dict entry in the ps_data['ehr_receipts'] list, since it may also contain other entries
        # depending on the test construction
        def ehr_receipt_matches_expected(generated_ehr_receipt):
            first_seen_timedelta = generated_ehr_receipt['first_seen'] - first_seen
            last_seen_timedelta = generated_ehr_receipt['last_seen'] - last_seen
            return all([
                generated_ehr_receipt['file_timestamp'] == file_timestamp,
                first_seen_timedelta <= datetime.timedelta(seconds=1),
                last_seen_timedelta <= datetime.timedelta(seconds=1)
            ])
        self.assertTrue(any([ehr_receipt_matches_expected(ehr_receipt) for ehr_receipt in ps_data['ehr_receipts']]))

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

    @staticmethod
    def build_expected_patch_data(participant_id, ehr_status: EhrStatus, is_ehr_available,
                                  first_ehr_time, latest_ehr_time):
        return {
            'pid': participant_id,
            'patch': {
                'ehr_status': str(ehr_status),
                'is_ehr_data_available': int(is_ehr_available),
                'ehr_status_id': int(ehr_status),
                'ehr_receipt': first_ehr_time,
                'ehr_update': latest_ehr_time
            }
        }

    def assert_patch_rebuilds_match(self, expected_patch_data_list, mock_rebuild_tasks):
        self.assertIsNotNone(mock_rebuild_tasks.call_args, "Rebuild wasn't called")

        actual_patch_data = []
        for call_args in mock_rebuild_tasks.call_args_list:
            call_patch_data, *_ = call_args.args
            actual_patch_data += call_patch_data

        self.assertEqual(len(expected_patch_data_list), len(actual_patch_data),
                         "Unexpected number of participants were rebuilt")

        for expected_data in expected_patch_data_list:
            found_expected_pid = False
            for actual_data in actual_patch_data:
                if expected_data['pid'] == actual_data['pid']:
                    found_expected_pid = True
                    self.assertDictEqual(expected_data, actual_data)
                    break
            if not found_expected_pid:
                self.fail(f"Did not find a rebuild call for {expected_data['pid']}")

    @mock.patch('rdr_service.offline.update_ehr_status.dispatch_participant_rebuild_tasks')
    @mock.patch('rdr_service.offline.update_ehr_status.make_update_participant_summaries_job')
    def test_participant_pdr_patch_requests(self, mock_summary_job, mock_rebuild_tasks):
        """Checking that participant ehr data gets patched under different scenarios"""

        # There are four different scenarios tested here, each of them requiring that different data be sent to PDR:
        #   1 - participants that still appear in the view, but don't have a new file upload timestamp should not be
        #       patched again because their participant summary data hasn't changed since the last time they were
        #       patched.
        #   2 - participants that have appeared in the view previously, but have a new file upload should be patched
        #       with the new ehr_update file time
        #   3 - participants that newly appear in the view should be patched
        #   4 - participants that have had EHR data available but are no longer in the view need to have PDR patched
        #       to set is_ehr_data_available to False

        # set up data for first scenario (appear in the view with previously patched data)
        first_upload_datetime = datetime.datetime(2020, 3, 12, 8)
        first_pid = self.data_generator.create_database_participant_summary(
            ehrStatus=EhrStatus.PRESENT,
            isEhrDataAvailable=True,
            ehrReceiptTime=first_upload_datetime,
            ehrUpdateTime=first_upload_datetime
        ).participantId
        first_view_data = self.EhrUpdatePidRow(first_pid, first_upload_datetime)
        self.data_generator.create_database_participant_ehr_receipt(
            participantId=first_pid,
            fileTimestamp=first_upload_datetime,
            firstSeen=datetime.datetime(2020, 1, 23)
        )

        # set up data for second scenario (appear in the view with new data to be patched)
        seconds_first_upload_time = datetime.datetime(2020, 2, 1)
        second_pid = self.data_generator.create_database_participant_summary(
            ehrStatus=EhrStatus.PRESENT,
            isEhrDataAvailable=True,
            ehrReceiptTime=seconds_first_upload_time,
            ehrUpdateTime=seconds_first_upload_time
        ).participantId
        second_view_data = self.EhrUpdatePidRow(second_pid, datetime.datetime(2020, 3, 12, 10))
        self.data_generator.create_database_participant_ehr_receipt(
            participantId=second_pid,
            fileTimestamp=seconds_first_upload_time,
            firstSeen=datetime.datetime(2020, 2, 6)
        )

        # initialize data for the third scenario (new participant appears in the view)
        third_pid = self.data_generator.create_database_participant_summary().participantId
        third_view_data = self.EhrUpdatePidRow(third_pid, datetime.datetime(2020, 3, 14, 10))

        # set up data for the fourth scenario (participant is no longer in the view)
        fourth_upload_time = datetime.datetime(2020, 5, 10)
        fourth_pid = self.data_generator.create_database_participant_summary(
            ehrStatus=EhrStatus.PRESENT,
            isEhrDataAvailable=True,
            ehrReceiptTime=fourth_upload_time,
            ehrUpdateTime=fourth_upload_time
        ).participantId

        mock_summary_job.return_value.__iter__.return_value = [[first_view_data, second_view_data, third_view_data]]
        update_ehr_status.update_ehr_status_participant()

        self.assert_patch_rebuilds_match([
            self.build_expected_patch_data(
                second_pid,
                EhrStatus.PRESENT,
                True,
                seconds_first_upload_time,
                second_view_data.latest_upload_time
            ),
            self.build_expected_patch_data(
                third_pid,
                EhrStatus.PRESENT,
                True,
                third_view_data.latest_upload_time,
                third_view_data.latest_upload_time
            ),
            self.build_expected_patch_data(
                fourth_pid,
                EhrStatus.PRESENT,
                False,
                fourth_upload_time,
                fourth_upload_time
            )
        ], mock_rebuild_tasks)

    @mock.patch('rdr_service.offline.update_ehr_status.make_update_participant_summaries_job')
    @mock.patch('rdr_service.dao.participant_summary_dao.ParticipantSummaryDao.update_enrollment_status')
    def test_ehr_receipt_updates_enrollment_status(self, update_status_mock, mock_summary_job):
        """Checking that a participant's enrollment status gets updated when they have EHR submitted for them"""

        test_participant_id = self.data_generator.create_database_participant_summary().participantId
        view_data = self.EhrUpdatePidRow(test_participant_id, datetime.datetime(2020, 3, 12, 10))

        mock_summary_job.return_value.__iter__.return_value = [[view_data]]
        update_ehr_status.update_ehr_status_participant()

        # Check that the participant was handed off to the status update method
        checked_participant_id = update_status_mock.call_args.kwargs['summary'].participantId
        self.assertEqual(test_participant_id, checked_participant_id)

        self.clear_table_after_test(ParticipantEhrReceipt.__tablename__)


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
