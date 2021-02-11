import datetime
from unittest import mock

import pytz
from dateutil import parser

from rdr_service.services.system_utils import JSONObject
from tests.helpers.unittest_base import BaseTestCase
from rdr_service import clock
from rdr_service.dao.participant_dao import ParticipantDao
from rdr_service.dao.participant_summary_dao import ParticipantSummaryDao
from rdr_service.dao.genomics_dao import (
    GenomicSetDao,
    GenomicSetMemberDao,
    GenomicJobRunDao,
    GenomicGCValidationMetricsDao,
    GenomicManifestFileDao)
from rdr_service.model.participant import Participant

from rdr_service.model.genomics import (
    GenomicSet,
    GenomicSetMember,
    GenomicJobRun,
)

from rdr_service.participant_enums import (
    GenomicJob,
    SampleStatus,
    WithdrawalStatus,
    GenomicWorkflowState
)


class GenomicApiTestBase(BaseTestCase):
    def setUp(self):

        self.participant_incrementer = 1

        super(GenomicApiTestBase, self).setUp()
        self.participant_dao = ParticipantDao()
        self.ps_dao = ParticipantSummaryDao()
        self.member_dao = GenomicSetMemberDao()
        self.job_run_dao = GenomicJobRunDao()
        self.metrics_dao = GenomicGCValidationMetricsDao()
        self.set_dao = GenomicSetDao()

        self._setup_data()

    def _setup_data(self):
        self._make_job_run()
        self._make_genomic_set()
        p = self._make_participant()
        self._make_summary(p)
        self._make_set_member(p)

    def _make_job_run(self, job_id=GenomicJob.UNSET):
        new_run = GenomicJobRun(
            jobId=job_id,
            startTime=clock.CLOCK.now(),
            endTime=clock.CLOCK.now(),
            runStatus=1,
            runResult=1,
        )
        return self.job_run_dao.insert(new_run)

    def _make_genomic_set(self):
        genomic_test_set = GenomicSet(
            genomicSetName="genomic-test-set",
            genomicSetCriteria=".",
            genomicSetVersion=1
        )
        return self.set_dao.insert(genomic_test_set)

    def _make_participant(self):
        i = self.participant_incrementer
        self.participant_incrementer += 1
        participant = Participant(participantId=i, biobankId=i)
        self.participant_dao.insert(participant)
        return participant

    def _make_summary(self, participant, **override_kwargs):
        """
        Make a summary with custom settings.
        default creates a valid summary.
        """
        valid_kwargs = dict(
            participantId=participant.participantId,
            biobankId=participant.biobankId,
            withdrawalStatus=participant.withdrawalStatus,
            dateOfBirth=datetime.datetime(2000, 1, 1),
            firstName="TestFN",
            lastName="TestLN",
            zipCode="12345",
            sampleStatus1ED04=SampleStatus.RECEIVED,
            sampleStatus1SAL2=SampleStatus.RECEIVED,
            samplesToIsolateDNA=SampleStatus.RECEIVED,
            consentForStudyEnrollmentTime=datetime.datetime(2019, 1, 1),
            consentForGenomicsROR=1,
        )
        kwargs = dict(valid_kwargs, **override_kwargs)
        summary = self.data_generator._participant_summary_with_defaults(**kwargs)
        self.ps_dao.insert(summary)
        return summary

    def _make_set_member(self, participant, **override_kwargs):
        valid_kwargs = dict(
            genomicSetId=1,
            participantId=participant.participantId,
            gemPass='Y',
            biobankId=participant.biobankId,
            sexAtBirth='F',
            genomicWorkflowState=GenomicWorkflowState.GEM_RPT_READY)

        kwargs = dict(valid_kwargs, **override_kwargs)
        new_member = GenomicSetMember(**kwargs)

        return self.member_dao.insert(new_member)


class GemApiTest(GenomicApiTestBase):
    def setUp(self):
        super(GemApiTest, self).setUp()

    def test_get_pii_valid_pid(self):
        p1_pii = self.send_get("GenomicPII/GEM/P1")
        self.assertEqual(p1_pii['biobank_id'], '1')
        self.assertEqual(p1_pii['first_name'], 'TestFN')
        self.assertEqual(p1_pii['last_name'], 'TestLN')
        self.assertEqual(p1_pii['sex_at_birth'], 'F')

    def test_get_pii_invalid_pid(self):
        p = self._make_participant()
        self._make_summary(p, withdrawalStatus=WithdrawalStatus.NO_USE)
        self._make_set_member(p)
        self.send_get("GenomicPII/GEM/P2", expected_status=404)

    def test_get_pii_no_gror_consent(self):
        p = self._make_participant()
        self._make_summary(p, consentForGenomicsROR=0)
        self._make_set_member(p)
        p2_pii = self.send_get("GenomicPII/GEM/P2")
        self.assertEqual(p2_pii['message'], "No RoR consent.")

    def test_get_pii_bad_request(self):
        self.send_get("GenomicPII/GEM/", expected_status=404)
        self.send_get("GenomicPII/GEM/P8", expected_status=404)
        self.send_get("GenomicPII/CVL/P2", expected_status=400)


class RhpApiTest(GenomicApiTestBase):
    def setUp(self):
        super(RhpApiTest, self).setUp()

    def test_get_pii_valid_pid(self):
        p1_pii = self.send_get("GenomicPII/RHP/P1")
        self.assertEqual(p1_pii['biobank_id'], '1')
        self.assertEqual(p1_pii['first_name'], 'TestFN')
        self.assertEqual(p1_pii['last_name'], 'TestLN')
        self.assertEqual(p1_pii['date_of_birth'], '2000-01-01')

    def test_get_pii_invalid_pid(self):
        p = self._make_participant()
        self._make_summary(p, withdrawalStatus=WithdrawalStatus.NO_USE)
        self._make_set_member(p)
        self.send_get("GenomicPII/RHP/P2", expected_status=404)

    def test_get_pii_no_gror_consent(self):
        p = self._make_participant()
        self._make_summary(p, consentForGenomicsROR=0)
        self._make_set_member(p)
        p2_pii = self.send_get("GenomicPII/RHP/P2")
        self.assertEqual(p2_pii['message'], "No RoR consent.")

    def test_get_pii_bad_request(self):
        self.send_get("GenomicPII/RHP/", expected_status=404)
        self.send_get("GenomicPII/RHP/P8", expected_status=404)
        self.send_get("GenomicPII/CVL/P2", expected_status=400)


class GenomicOutreachApiTest(GenomicApiTestBase):
    def setUp(self):
        super(GenomicOutreachApiTest, self).setUp()

    def test_get_date_lookup(self):
        p2 = self._make_participant()
        p3 = self._make_participant()

        fake_gror_date = parser.parse('2020-05-29T08:00:01-05:00')
        fake_rpt_update_date = parser.parse('2020-09-01T08:00:01-05:00')
        fake_now = clock.CLOCK.now().replace(microsecond=0)

        self._make_summary(p2, consentForGenomicsRORAuthored=fake_gror_date,
                           consentForStudyEnrollmentAuthored=fake_gror_date)

        self._make_summary(p3, consentForGenomicsRORAuthored=fake_gror_date,
                           consentForStudyEnrollmentAuthored=fake_gror_date)

        self._make_set_member(p2, genomicWorkflowState=GenomicWorkflowState.GEM_RPT_READY,
                              genomicWorkflowStateModifiedTime=fake_rpt_update_date)
        self._make_set_member(p3, genomicWorkflowState=GenomicWorkflowState.GEM_RPT_PENDING_DELETE,
                              genomicWorkflowStateModifiedTime=fake_rpt_update_date)

        with clock.FakeClock(fake_now):
            resp = self.send_get("GenomicOutreach/GEM?start_date=2020-08-30T08:00:01-05:00")

        expected_response = {
            "participant_report_statuses": [
                {
                    "participant_id": "P2",
                    "report_status": "ready"
                },
                {
                    "participant_id": "P3",
                    "report_status": "pending_delete"
                }
            ],
            "timestamp": fake_now.replace(microsecond=0, tzinfo=pytz.UTC).isoformat()
        }

        self.assertEqual(expected_response, resp)

    def test_get_date_range(self):
        p2 = self._make_participant()
        p3 = self._make_participant()

        fake_gror_date_1 = parser.parse('2020-05-01T08:00:01-05:00')
        fake_gror_date_2 = parser.parse('2020-06-01T08:00:01-05:00')
        fake_rpt_update_date = parser.parse('2020-05-29T08:00:01-05:00')
        fake_now = clock.CLOCK.now().replace(microsecond=0)

        self._make_summary(p2, consentForGenomicsRORAuthored=fake_gror_date_1,
                           consentForStudyEnrollmentAuthored=fake_gror_date_1)

        self._make_summary(p3, consentForGenomicsRORAuthored=fake_gror_date_2,
                           consentForStudyEnrollmentAuthored=fake_gror_date_2)

        self._make_set_member(p2, genomicWorkflowState=GenomicWorkflowState.GEM_RPT_READY,
                              genomicWorkflowStateModifiedTime=fake_rpt_update_date)

        self._make_set_member(p3, genomicWorkflowState=GenomicWorkflowState.GEM_RPT_READY,
                              genomicWorkflowStateModifiedTime=fake_gror_date_2)

        with clock.FakeClock(fake_now):
            uri = "GenomicOutreach/GEM?start_date=2020-05-27T08:00:01-05:00&end_date=2020-05-30T08:00:01-05:00"
            resp = self.send_get(uri)

        expected_response = {
            "participant_report_statuses": [
                {
                    "participant_id": "P2",
                    "report_status": "ready"
                }
            ],
            "timestamp": fake_now.replace(microsecond=0, tzinfo=pytz.UTC).isoformat()
        }

        self.assertEqual(expected_response, resp)

    def test_get_participant_lookup(self):
        p2 = self._make_participant()

        fake_date = parser.parse('2020-05-29T08:00:01-05:00')
        fake_now = clock.CLOCK.now().replace(microsecond=0)

        self._make_summary(p2, consentForGenomicsRORAuthored=fake_date,
                           consentForStudyEnrollmentAuthored=fake_date)

        self._make_set_member(p2, genomicWorkflowState=GenomicWorkflowState.GEM_RPT_READY)

        with clock.FakeClock(fake_now):
            resp = self.send_get("GenomicOutreach/GEM?participant_id=P2")

        expected_response = {
            "participant_report_statuses": [
                {
                    "participant_id": "P2",
                    "report_status": "ready"
                }
            ],
            "timestamp": fake_now.replace(microsecond=0, tzinfo=pytz.UTC).isoformat()
        }

        self.assertEqual(expected_response, resp)

    def test_get_no_participant(self):
        fake_now = clock.CLOCK.now().replace(microsecond=0)
        with clock.FakeClock(fake_now):
            self.send_get("GenomicOutreach/GEM?participant_id=P13", expected_status=404)

    def test_genomic_test_participant_created(self):
        p = self._make_participant()

        self._make_summary(p)

        local_path = f"GenomicOutreach/GEM/Participant/P{p.participantId}"

        # Test Payload for participant report status
        payload = {
            "status": "pending_delete",
            "date": "2020-09-13T20:52:12+00:00"
        }

        expected_response = {
            "participant_report_statuses": [
                {
                    "participant_id": "P2",
                    "report_status": "pending_delete"
                }
            ],
            "timestamp": "2020-09-13T20:52:12+00:00"
        }

        resp = self.send_post(local_path, request_data=payload)
        member = self.member_dao.get(2)

        self.assertEqual(expected_response, resp)
        self.assertEqual(GenomicWorkflowState.GEM_RPT_PENDING_DELETE, member.genomicWorkflowState)

    def test_genomic_test_participant_not_found(self):
        # P2001 doesn't exist in participant
        local_path = f"GenomicOutreach/GEM/Participant/P2001"

        # Test Payload for participant report status
        payload = {
            "status": "pending_delete",
            "date": "2020-09-13T20:52:12+00:00"
        }

        self.send_post(local_path, request_data=payload, expected_status=404)


class GenomicCloudTasksApiTest(BaseTestCase):
    def setUp(self):
        super(GenomicCloudTasksApiTest, self).setUp()

        self.manifest_file_dao = GenomicManifestFileDao()

    @mock.patch('rdr_service.offline.genomic_pipeline.dispatch_genomic_job_from_task')
    def test_calculate_record_count_task_api(self, dispatch_job_mock):

        manifest = self.data_generator.create_database_genomic_manifest_file()

        # Payload for caculate record count task endpoint
        data = {"manifest_file_id": manifest.id}

        from rdr_service.resource import main as resource_main
        resource_main.app.testing = True
        self.send_post(
            local_path='CalculateRecordCountTaskApi',
            request_data=data,
            prefix="/resource/task/",
            test_client=resource_main.app.test_client(),
        )

        # Expected payload from task API to dispatch_genomic_job_from_task
        expected_payload = {
            "job": GenomicJob.CALCULATE_RECORD_COUNT_AW1,
            "manifest_file": manifest
        }

        expected_payload = JSONObject(expected_payload)

        called_json_obj = dispatch_job_mock.call_args[0][0]

        self.assertEqual(
            expected_payload.manifest_file.id,
            called_json_obj.manifest_file.id
        )
        self.assertEqual(
            expected_payload.job,
            called_json_obj.job)

    @mock.patch('rdr_service.offline.genomic_pipeline.load_aw1_manifest_into_raw_table')
    def test_load_aw1_raw_data_task_api(self, load_raw_aw1_data_mock):

        # Payload for caculate record count task endpoint
        test_file_path = "test-bucket-name/test_aw1_file.csv"
        data = {"file_path": test_file_path}

        from rdr_service.resource import main as resource_main
        resource_main.app.testing = True
        self.send_post(
            local_path='LoadRawAW1ManifestDataAPI',
            request_data=data,
            prefix="/resource/task/",
            test_client=resource_main.app.test_client(),
        )

        load_raw_aw1_data_mock.assert_called_with(test_file_path)
