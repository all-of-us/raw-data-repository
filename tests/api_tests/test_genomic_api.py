import datetime
import http.client
import pytz
import random

from copy import deepcopy
from dateutil import parser
from unittest import mock

from rdr_service.api_util import PTC, HEALTHPRO, GEM, RDR
from rdr_service.dao.database_utils import format_datetime
from rdr_service.services.system_utils import JSONObject
from rdr_service import clock, config
from rdr_service.dao.participant_dao import ParticipantDao
from rdr_service.dao.participant_summary_dao import ParticipantSummaryDao
from rdr_service.dao.genomics_dao import (
    GenomicSetDao,
    GenomicSetMemberDao,
    GenomicJobRunDao,
    GenomicGCValidationMetricsDao,
    GenomicCloudRequestsDao,
    GenomicInformingLoopDao,
    GenomicMemberReportStateDao,
    GenomicGcDataFileDao,
    GenomicResultViewedDao,
    GenomicAppointmentEventDao
)
from rdr_service.genomic_enums import GenomicJob, GenomicReportState, GenomicWorkflowState, GenomicManifestTypes, \
    GenomicQcStatus, GenomicSampleSwapCategory
from rdr_service.model.participant import Participant
from rdr_service.model.genomics import (
    GenomicSet,
    GenomicSetMember,
    GenomicJobRun,
)
from rdr_service.participant_enums import (
    SampleStatus,
    WithdrawalStatus
)

from tests.helpers.unittest_base import BaseTestCase
from tests.service_tests.test_genomic_datagen import GenomicDataGenMixin


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
        self.report_state_dao = GenomicMemberReportStateDao()

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
            biobankId=participant.biobankId,
            sexAtBirth='F',
            genomicWorkflowState=GenomicWorkflowState.GEM_RPT_READY
        )

        kwargs = dict(valid_kwargs, **override_kwargs)
        new_member = GenomicSetMember(**kwargs)

        return self.member_dao.insert(new_member)

    def overwrite_test_user_roles(self, roles):
        new_user_info = deepcopy(config.getSettingJson(config.USER_INFO))
        new_user_info['example@example.com']['roles'] = roles
        self.temporarily_override_config_setting(config.USER_INFO, new_user_info)


class GPGenomicPIIApiTest(GenomicApiTestBase):
    def setUp(self):
        super(GPGenomicPIIApiTest, self).setUp()

    def test_full_participant_validation_lookup(self):

        gen_set = self.data_generator.create_database_genomic_set(
            genomicSetName=".",
            genomicSetCriteria=".",
            genomicSetVersion=1
        )
        participant = self.data_generator.create_database_participant()

        # no summary / no set member
        resp = self.send_get(
            f"GenomicPII/GP/P{participant.participantId}",
            expected_status=http.client.NOT_FOUND
        )
        self.assertEqual(
            resp.json['message'],
            f'Participant with ID P{participant.participantId} not found in RDR'
        )
        self.assertEqual(resp.status_code, 404)

        # summary / no set member
        self.data_generator.create_database_participant_summary(
            participant=participant
        )
        resp = self.send_get(
            f"GenomicPII/GP/P{participant.participantId}",
            expected_status=http.client.NOT_FOUND
        )
        self.assertEqual(resp.json['message'],
                         f'Participant with ID P{participant.participantId} '
                         f'not found in Genomics system'
                         )
        self.assertEqual(resp.status_code, 404)

        # summary / set member / failed validation in query
        self.data_generator.create_database_genomic_set_member(
            genomicSetId=gen_set.id,
            participantId=participant.participantId,
            biobankId=participant.biobankId,
            sampleId="21042005280",
            genomeType="aou_array",
            genomicWorkflowState=GenomicWorkflowState.AW0
        )

        resp = self.send_get(
            f"GenomicPII/GP/P{participant.participantId}",
            expected_status=http.client.NOT_FOUND
        )

        self.assertEqual(resp.json['message'], f'Participant with ID P{participant.participantId} '
                                               f'did not pass validation check')
        self.assertEqual(resp.status_code, 404)

        # add consent correct validation
        summaries = self.ps_dao.get_all()
        current_summary = list(filter(lambda x: x.participantId == participant.participantId, summaries))[0]
        current_summary.consentForGenomicsROR = 1
        self.ps_dao.update(current_summary)

        resp = self.send_get(
            f"GenomicPII/GP/P{participant.participantId}"
        )

        self.assertIsNotNone(resp)
        self.assertEqual(resp.get('biobank_id'), str(participant.biobankId))

    def test_get_pii_valid_pid(self):
        p1_pii = self.send_get("GenomicPII/GP/P1")
        self.assertEqual(p1_pii['biobank_id'], '1')
        self.assertEqual(p1_pii['first_name'], 'TestFN')
        self.assertEqual(p1_pii['last_name'], 'TestLN')
        self.assertEqual(p1_pii['sex_at_birth'], 'F')
        self.assertEqual(p1_pii['hgm_informing_loop'], False)

    def test_get_pii_invalid_pid(self):
        p = self._make_participant()
        self._make_summary(p, withdrawalStatus=WithdrawalStatus.NO_USE)
        self._make_set_member(p)
        resp = self.send_get(f"GenomicPII/GP/P{p.participantId}", expected_status=404)
        self.assertEqual(resp.status_code, 404)

    def test_get_pii_no_gror_consent(self):
        p = self._make_participant()
        self._make_summary(p, consentForGenomicsROR=0)
        self._make_set_member(p)
        resp = self.send_get(f"GenomicPII/GP/P{p.participantId}", expected_status=404)
        self.assertEqual(resp.status_code, 404)
        self.assertEqual(resp.json['message'], f"Participant with ID P{p.participantId}"
                                                 f" did not pass validation check")

    def test_get_pii_bad_request(self):
        self.send_get("GenomicPII/GP/", expected_status=404)
        self.send_get("GenomicPII/GP/P8", expected_status=404)
        self.send_get("GenomicPII/CVL/P2", expected_status=400)

    def test_sex_at_birth_equals_na(self):
        sex_type = 'NA'
        p = self._make_participant()
        self._make_set_member(p, sexAtBirth=sex_type)
        self._make_summary(p)
        response = self.send_get(f"GenomicPII/GP/P{p.participantId}")
        self.assertIsNotNone(response)
        self.assertEqual(response.get('sex_at_birth'), sex_type)

    def test_hgm_informing_loop_statuses(self):
        p = self._make_participant()
        self._make_set_member(p)
        self._make_summary(p, consentForStudyEnrollment=1)

        response = self.send_get(f"GenomicPII/GP/P{p.participantId}")
        self.assertIsNotNone(response)
        self.assertEqual(response.get('hgm_informing_loop'), False)

        # build informing_loop_ready criteria
        wgs_member = self._make_set_member(
            p,
            genomeType=config.GENOME_TYPE_WGS,
            qcStatus=GenomicQcStatus.PASS,
            gcManifestSampleSource='Whole Blood',
            informingLoopReadyFlag=1,
            informingLoopReadyFlagModified=clock.CLOCK.now()
        )
        self.data_generator.create_database_genomic_gc_validation_metrics(
            genomicSetMemberId=wgs_member.id,
            sexConcordance='True',
            drcFpConcordance='Pass',
            drcSexConcordance='Pass',
            processingStatus='Pass'
        )

        response = self.send_get(f"GenomicPII/GP/P{p.participantId}")
        self.assertIsNotNone(response)
        self.assertEqual(response.get('hgm_informing_loop'), True)

        wgs_member = self.member_dao.get(wgs_member.id)
        wgs_member.informingLoopReadyFlag = 0
        self.member_dao.update(wgs_member)

        response = self.send_get(f"GenomicPII/GP/P{p.participantId}")
        self.assertIsNotNone(response)
        self.assertEqual(response.get('hgm_informing_loop'), False)

        wgs_member = self.member_dao.get(wgs_member.id)
        wgs_member.informingLoopReadyFlag = 1
        self.member_dao.update(wgs_member)

        response = self.send_get(f"GenomicPII/GP/P{p.participantId}")
        self.assertIsNotNone(response)
        self.assertEqual(response.get('hgm_informing_loop'), True)

        update_summary = self.ps_dao.get_by_participant_id(p.participantId)
        update_summary.participantOrigin = 'careevolution'
        self.ps_dao.update(update_summary)

        response = self.send_get(f"GenomicPII/GP/P{p.participantId}")
        self.assertIsNotNone(response)
        self.assertEqual(response.get('hgm_informing_loop'), False)


class RhpGenomicPIIApiTest(GenomicApiTestBase):
    def setUp(self):
        super(RhpGenomicPIIApiTest, self).setUp()

    def test_full_participant_validation_lookup(self):

        gen_set = self.data_generator.create_database_genomic_set(
            genomicSetName=".",
            genomicSetCriteria=".",
            genomicSetVersion=1
        )
        participant = self.data_generator.create_database_participant()

        # no summary / no set member
        resp = self.send_get(
            f"GenomicPII/RHP/A{participant.biobankId}",
            expected_status=http.client.NOT_FOUND
        )
        self.assertEqual(
            resp.json['message'],
            f'Participant with ID A{participant.biobankId} not found in RDR'
        )
        self.assertEqual(resp.status_code, 404)

        # summary / no set member
        self.data_generator.create_database_participant_summary(
            participant=participant,
            consentForGenomicsROR=1
        )
        resp = self.send_get(
            f"GenomicPII/RHP/A{participant.biobankId}",
            expected_status=http.client.NOT_FOUND
        )
        self.assertEqual(resp.json['message'],
                         f'Participant with ID A{participant.biobankId} '
                         f'not found in Genomics system'
                         )
        self.assertEqual(resp.status_code, 404)

        # summary / set member / failed validation in query
        self.data_generator.create_database_genomic_set_member(
            genomicSetId=gen_set.id,
            participantId=participant.participantId,
            biobankId=participant.biobankId,
            collectionTubeId=11111,
            cvlW4wrHdrManifestJobRunID=1,
            gcManifestSampleSource='Whole Blood'
        )

        resp = self.send_get(
            f"GenomicPII/RHP/A{participant.biobankId}",
            expected_status=http.client.NOT_FOUND
        )

        self.assertEqual(resp.json['message'], f'Participant with ID A{participant.biobankId} '
                                               f'did not pass validation check')
        self.assertEqual(resp.status_code, 404)

        # add biobank stored sample fix validation
        self.data_generator.create_database_biobank_stored_sample(
            biobankId=participant.biobankId,
            biobankOrderIdentifier=self.fake.pyint(),
            biobankStoredSampleId=11111,
            confirmed=clock.CLOCK.now()
        )

        resp = self.send_get(
            f"GenomicPII/RHP/A{participant.biobankId}"
        )

        self.assertIsNotNone(resp)
        self.assertTrue(str(participant.participantId) in resp.get('participant_id'))

    def test_get_pii_valid_pid(self):
        participant = self._make_participant()
        self._make_summary(participant)
        self._make_set_member(
            participant=participant,
            biobankId=participant.biobankId,
            collectionTubeId=11111,
            cvlW4wrHdrManifestJobRunID=1,
            gcManifestSampleSource='Whole Blood'
        )
        self.data_generator.create_database_biobank_stored_sample(
            biobankId=participant.biobankId,
            biobankOrderIdentifier=self.fake.pyint(),
            biobankStoredSampleId=11111,
            confirmed=clock.CLOCK.now()
        )
        response = self.send_get(f"GenomicPII/RHP/A{participant.biobankId}")
        self.assertIsNotNone(response)

        needed_keys = ['participant_id', 'first_name', 'last_name', 'date_of_birth', 'sample_source', 'collection_date']
        all_keys_values = all(not len(response.keys() - needed_keys) and
                              response.values())
        self.assertTrue(all_keys_values)

    def test_get_pii_invalid_pid(self):
        p = self._make_participant()
        self._make_summary(
            p,
            withdrawalStatus=WithdrawalStatus.NO_USE
        )
        self._make_set_member(p)
        resp = self.send_get("GenomicPII/RHP/A2", expected_status=404)
        self.assertEqual(resp.status_code, 404)

    def test_get_pii_no_gror_consent(self):
        participant = self._make_participant()
        self._make_summary(
            participant,
            consentForGenomicsROR=0
        )
        self._make_set_member(
            participant=participant,
            biobankId=participant.biobankId,
            collectionTubeId=11111,
            cvlW4wrHdrManifestJobRunID=1,
            gcManifestSampleSource='Whole Blood'
        )
        self.data_generator.create_database_biobank_stored_sample(
            biobankId=participant.biobankId,
            biobankOrderIdentifier=self.fake.pyint(),
            biobankStoredSampleId=11111,
            confirmed=clock.CLOCK.now()
        )
        resp = self.send_get(f"GenomicPII/RHP/A{participant.biobankId}", expected_status=404)
        self.assertEqual(resp.status_code, 404)
        self.assertEqual(resp.json['message'], f"Participant with ID A{participant.biobankId}"
                                               f" did not pass validation check")

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
        report_state_member = self.report_state_dao.get_from_member_id(member.id)

        self.assertEqual(report_state_member.genomic_report_state, GenomicReportState.GEM_RPT_PENDING_DELETE)
        self.assertEqual(report_state_member.module, 'gem')
        self.assertEqual(report_state_member.genomic_set_member_id, member.id)
        self.assertEqual(report_state_member.event_authored_time, member.genomicWorkflowStateModifiedTime)

    def test_genomic_test_participant_not_found(self):
        # P2001 doesn't exist in participant
        local_path = f"GenomicOutreach/GEM/Participant/P2001"

        # Test Payload for participant report status
        payload = {
            "status": "pending_delete",
            "date": "2020-09-13T20:52:12+00:00"
        }

        self.send_post(local_path, request_data=payload, expected_status=404)

    def test_get_roles_return_response(self):
        participant = self._make_participant()

        fake_date = parser.parse('2020-05-29T08:00:01-05:00')
        self._make_summary(
            participant,
            consentForGenomicsRORAuthored=fake_date,
            consentForStudyEnrollmentAuthored=fake_date
        )
        self._make_set_member(
            participant,
            genomicWorkflowState=GenomicWorkflowState.GEM_RPT_READY
        )

        accepted_roles = [PTC, GEM, RDR]

        self.overwrite_test_user_roles(
            [random.choice(accepted_roles)]
        )

        resp = self.send_get(
            f'GenomicOutreach/GEM?participant_id=P{participant.participantId}'
        )

        self.assertTrue(resp.get('participant_report_statuses') is not None)

        self.overwrite_test_user_roles([HEALTHPRO])

        resp = self.send_get(
            f'GenomicOutreach/GEM?participant_id=P{participant.participantId}',
            expected_status=403
        )

        self.assertTrue(resp.status_code== 403)


class GenomicOutreachApiV2Test(GenomicApiTestBase, GenomicDataGenMixin):
    def setUp(self):
        super(GenomicOutreachApiV2Test, self).setUp()
        self.loop_dao = GenomicInformingLoopDao()
        self.report_dao = GenomicMemberReportStateDao()
        self.result_dao = GenomicResultViewedDao()
        self.member_dao = GenomicSetMemberDao()
        self.num_participants = 5

    # GET
    def test_full_participant_validation_lookup(self):

        gen_set = self.data_generator.create_database_genomic_set(
            genomicSetName=".",
            genomicSetCriteria=".",
            genomicSetVersion=1
        )
        participant = self.data_generator.create_database_participant()

        # no summary / no set member
        resp = self.send_get(
            f"GenomicOutreachV2?participant_id=P{participant.participantId}",
            expected_status=http.client.NOT_FOUND
        )
        self.assertEqual(
            resp.json['message'],
            f'Participant with ID P{participant.participantId} not found in RDR'
        )
        self.assertEqual(resp.status_code, 404)

        # summary / no set member
        self.data_generator.create_database_participant_summary(
            participant=participant,
            consentForGenomicsROR=1
        )
        resp = self.send_get(
            f"GenomicOutreachV2?participant_id=P{participant.participantId}",
            expected_status=http.client.NOT_FOUND
        )
        self.assertEqual(resp.json['message'],
                         f'Participant with ID P{participant.participantId} '
                         f'not found in Genomics system'
                         )
        self.assertEqual(resp.status_code, 404)

        # summary / set member / failed validation in query
        gen_member = self.data_generator.create_database_genomic_set_member(
            genomicSetId=gen_set.id,
            participantId=participant.participantId,
            genomeType='aou_array'
        )

        resp = self.send_get(
            f"GenomicOutreachV2?participant_id=P{participant.participantId}",
            expected_status=http.client.NOT_FOUND
        )

        self.assertEqual(resp.json['message'], f'Participant with ID P{participant.participantId} '
                                               f'did not pass validation check')
        self.assertEqual(resp.status_code, 404)

        # add result ready for gem fix validation
        self.data_generator.create_database_genomic_member_report_state(
            genomic_set_member_id=gen_member.id,
            participant_id=participant.participantId,
            module='gem',
            genomic_report_state=GenomicReportState.GEM_RPT_READY,
            event_authored_time=clock.CLOCK.now()
        )

        resp = self.send_get(
            f"GenomicOutreachV2?participant_id=P{participant.participantId}"
        )

        self.assertIsNotNone(resp)
        self.assertEqual(len(resp.get('data')), 1)
        self.assertTrue(str(participant.participantId) in resp.get('data')[0]['participant_id'])

    def test_validate_params(self):
        bad_response = 'GenomicOutreachV2 GET accepted params: start_date | end_date | participant_id | module | type'

        response_one = self.send_get(
            "GenomicOutreachV2?wwqwqw=ewewe",
            expected_status=http.client.BAD_REQUEST
        )
        self.assertEqual(response_one.json['message'], bad_response)
        self.assertEqual(response_one.status_code, 400)

        response_two = self.send_get(
            "GenomicOutreachV2?wwqwqw=ewewe&participant_id=P2",
            expected_status=http.client.BAD_REQUEST
        )

        self.assertEqual(response_two.json['message'], bad_response)
        self.assertEqual(response_two.status_code, 400)

        bad_response = 'Participant ID or Start Date is required for GenomicOutreach lookup.'

        response_three = self.send_get(
            "GenomicOutreachV2?participant_id=",
            expected_status=http.client.BAD_REQUEST
        )

        self.assertEqual(response_three.json['message'], bad_response)
        self.assertEqual(response_three.status_code, 400)

        bad_response = 'GenomicOutreachV2 GET accepted modules: gem | hdr | pgx'

        response_four = self.send_get(
            "GenomicOutreachV2?module=ewewewew",
            expected_status=http.client.BAD_REQUEST
        )

        self.assertEqual(response_four.json['message'], bad_response)
        self.assertEqual(response_four.status_code, 400)

        bad_response = 'GenomicOutreachV2 GET accepted types: result | informingLoop'

        response_five = self.send_get(
            "GenomicOutreachV2?type=ewewewew",
            expected_status=http.client.BAD_REQUEST
        )

        self.assertEqual(response_five.json['message'], bad_response)
        self.assertEqual(response_five.status_code, 400)

    def test_get_participant_lookup(self):
        first_participant = None
        second_participant = None
        third_participant = None
        fake_date = parser.parse('2020-05-29T08:00:01-05:00')
        fake_now = clock.CLOCK.now().replace(microsecond=0)

        gen_set = self.data_generator.create_database_genomic_set(
            genomicSetName=".",
            genomicSetCriteria=".",
            genomicSetVersion=1
        )

        for num in range(self.num_participants):
            participant = self.data_generator.create_database_participant()

            self.data_generator.create_database_participant_summary(
                participant=participant,
                consentForGenomicsRORAuthored=fake_date,
                consentForStudyEnrollmentAuthored=fake_date
            )

            module = 'gem'
            report_state = GenomicReportState.GEM_RPT_READY
            genome_type = 'aou_array'
            report_revision_number = None

            if num == 0:
                first_participant = participant
            elif num == 1:
                second_participant = participant
                module = 'pgx_v1'
                genome_type = 'aou_wgs'
                report_state = GenomicReportState.PGX_RPT_READY
                report_revision_number = 1

            gen_member = self.data_generator.create_database_genomic_set_member(
                genomicSetId=gen_set.id,
                biobankId="100153482",
                sampleId="21042005280",
                genomeType=genome_type,
                genomicWorkflowState=GenomicWorkflowState.GEM_RPT_READY,
                participantId=participant.participantId
            )

            self.data_generator.create_database_genomic_member_report_state(
                genomic_set_member_id=gen_member.id,
                participant_id=participant.participantId,
                module=module,
                genomic_report_state=report_state,
                report_revision_number=report_revision_number,
                event_authored_time=fake_date
            )

            if num == 3:
                third_participant = participant
                self.data_generator.create_database_genomic_informing_loop(
                    message_record_id=1,
                    event_type='informing_loop_decision',
                    module_type=module,
                    participant_id=participant.participantId,
                    decision_value='maybe_later',
                    event_authored_time=fake_date
                )

        total_num_set = self.loop_dao.get_all() + self.report_dao.get_all()
        self.assertEqual(len(total_num_set), 6)

        with clock.FakeClock(fake_now):
            resp = self.send_get(f'GenomicOutreachV2?participant_id={first_participant.participantId}')

        expected = {
            'data': [
                {
                    'module': 'gem',
                    'type': 'result',
                    'status': 'ready',
                    'participant_id': f'P{first_participant.participantId}'
                }
            ],
            'timestamp': fake_now.replace(microsecond=0, tzinfo=pytz.UTC).isoformat()
        }
        self.assertEqual(expected, resp)

        with clock.FakeClock(fake_now):
            resp = self.send_get(f'GenomicOutreachV2?participant_id={second_participant.participantId}')

        expected = {
            'data': [
                {
                    'module': 'pgx',
                    'report_revision_number': 1,
                    'type': 'result',
                    'status': 'ready',
                    'participant_id': f'P{second_participant.participantId}'
                }
            ],
            'timestamp': fake_now.replace(microsecond=0, tzinfo=pytz.UTC).isoformat()
        }
        self.assertEqual(expected, resp)

        with clock.FakeClock(fake_now):
            resp = self.send_get(f'GenomicOutreachV2?participant_id={third_participant.participantId}')

        expected = {
            'data': [
                {
                    'module': 'gem',
                    'type': 'informingLoop',
                    'status': 'completed',
                    'decision': 'maybe_later',
                    'participant_id': f'P{third_participant.participantId}'
                },
                {
                    'module': 'gem',
                    'type': 'result',
                    'status': 'ready',
                    'participant_id': f'P{third_participant.participantId}'
                },
            ],
            'timestamp': fake_now.replace(microsecond=0, tzinfo=pytz.UTC).isoformat()
        }

        self.assertEqual(len(resp['data']), 2)
        self.assertEqual(len(resp['data'][0]), 5)
        self.assertEqual(expected, resp)

    def test_get_by_type(self):
        self.num_participants = 10
        fake_date_one = parser.parse('2020-05-29T08:00:01-05:00')
        fake_date_two = parser.parse('2020-05-30T08:00:01-05:00')
        fake_date_three = parser.parse('2020-05-31T08:00:01-05:00')
        fake_now = clock.CLOCK.now().replace(microsecond=0)
        informing_loop_type = 'informingLoop'
        result_type = 'result'

        gen_set = self.data_generator.create_database_genomic_set(
            genomicSetName=".",
            genomicSetCriteria=".",
            genomicSetVersion=1
        )

        for num in range(self.num_participants):
            participant = self.data_generator.create_database_participant()

            self.data_generator.create_database_participant_summary(
                participant=participant,
                consentForGenomicsRORAuthored=fake_date_one,
                consentForStudyEnrollmentAuthored=fake_date_one
            )

            module = 'gem'
            report_state = GenomicReportState.GEM_RPT_READY

            gen_member = self.data_generator.create_database_genomic_set_member(
                genomicSetId=gen_set.id,
                biobankId="100153482",
                sampleId="21042005280",
                genomeType="aou_array",
                genomicWorkflowState=GenomicWorkflowState.GEM_RPT_READY,
                participantId=participant.participantId
            )

            self.data_generator.create_database_genomic_member_report_state(
                genomic_set_member_id=gen_member.id,
                participant_id=participant.participantId,
                module=module,
                genomic_report_state=report_state,
                event_authored_time=fake_date_two if num > 4 else fake_date_one
            )

            if num % 2 == 0:
                self.data_generator.create_database_genomic_informing_loop(
                    message_record_id=1,
                    event_type='informing_loop_decision',
                    module_type=module,
                    participant_id=participant.participantId,
                    decision_value='maybe_later',
                    event_authored_time=fake_date_two
                )

        total_num_set = self.loop_dao.get_all() + self.report_dao.get_all()
        self.assertEqual(len(total_num_set), 15)

        bad_response = 'Participant ID or Start Date is required for GenomicOutreach lookup.'

        resp = self.send_get(
            f'GenomicOutreachV2?type={informing_loop_type}',
            expected_status=http.client.BAD_REQUEST
        )

        self.assertEqual(resp.json['message'], bad_response)
        self.assertEqual(resp.status_code, 400)

        resp = self.send_get(
            f'GenomicOutreachV2?type={result_type}',
            expected_status=http.client.BAD_REQUEST
        )

        self.assertEqual(resp.json['message'], bad_response)
        self.assertEqual(resp.status_code, 400)

        with clock.FakeClock(fake_now):
            resp = self.send_get(
                f'GenomicOutreachV2?start_date={fake_date_three}&type={informing_loop_type}'
            )

        self.assertEqual(resp['data'], [])

        with clock.FakeClock(fake_now):
            resp = self.send_get(
                f'GenomicOutreachV2?start_date={fake_date_one}&type={informing_loop_type}'
            )

        decision_loop_keys = ['module', 'type', 'status', 'decision', 'participant_id']
        all_loop_keys_data = all(not len(obj.keys() - decision_loop_keys) and obj.values() for obj in resp['data'])
        self.assertTrue(all_loop_keys_data)

        self.assertEqual(len(resp['data']), 5)
        all_loops = all(obj['type'] == informing_loop_type for obj in resp['data'])
        self.assertTrue(all_loops)

        with clock.FakeClock(fake_now):
            resp = self.send_get(
                f'GenomicOutreachV2?start_date={fake_date_one}&type={result_type}'
            )

        result_keys = ['module', 'type', 'status', 'participant_id']
        all_result_keys_data = all(not len(obj.keys() - result_keys) and obj.values() for obj in resp['data'])
        self.assertTrue(all_result_keys_data)

        self.assertEqual(len(resp['data']), 5)
        all_results = all(obj['type'] == result_type for obj in resp['data'])
        self.assertTrue(all_results)

    def test_get_by_module(self):
        self.num_participants = 10
        fake_date_one = parser.parse('2020-05-30T08:00:01-05:00')
        fake_date_two = parser.parse('2020-05-31T08:00:01-05:00')
        fake_now = clock.CLOCK.now().replace(microsecond=0)

        gen_set = self.data_generator.create_database_genomic_set(
            genomicSetName=".",
            genomicSetCriteria=".",
            genomicSetVersion=1
        )

        for num in range(self.num_participants):
            if num % 2 == 0:
                result_module = 'pgx_v1'
                loop_module = 'pgx'
                report_state = GenomicReportState.PGX_RPT_READY
                genome_type = config.GENOME_TYPE_WGS
            else:
                result_module = 'gem'
                loop_module = 'gem'
                report_state = GenomicReportState.GEM_RPT_READY
                genome_type = config.GENOME_TYPE_ARRAY

            participant = self.data_generator.create_database_participant()

            self.data_generator.create_database_participant_summary(
                participant=participant,
                consentForGenomicsRORAuthored=fake_date_two,
                consentForStudyEnrollmentAuthored=fake_date_two
            )

            gen_member = self.data_generator.create_database_genomic_set_member(
                genomicSetId=gen_set.id,
                biobankId="100153482",
                sampleId="21042005280",
                genomeType=genome_type,
                genomicWorkflowState=GenomicWorkflowState.GEM_RPT_READY,
                participantId=participant.participantId
            )

            self.data_generator.create_database_genomic_member_report_state(
                genomic_set_member_id=gen_member.id,
                participant_id=participant.participantId,
                module=result_module,
                genomic_report_state=report_state,
                event_authored_time=fake_date_two
            )

            self.data_generator.create_database_genomic_informing_loop(
                message_record_id=1,
                event_type='informing_loop_decision',
                module_type=loop_module,
                participant_id=participant.participantId,
                decision_value='maybe_later',
                event_authored_time=fake_date_two
            )

        total_num_set = self.loop_dao.get_all() + self.report_dao.get_all()
        self.assertEqual(len(total_num_set), 20)

        bad_response = 'Participant ID or Start Date is required for GenomicOutreach lookup.'

        resp = self.send_get(
            'GenomicOutreachV2?module=GEM',
            expected_status=http.client.BAD_REQUEST
        )

        self.assertEqual(resp.json['message'], bad_response)
        self.assertEqual(resp.status_code, 400)

        with clock.FakeClock(fake_now):
            resp = self.send_get(
                f'GenomicOutreachV2?start_date={fake_date_one}&module=GEM'
            )

        self.assertEqual(len(resp['data']), len(total_num_set) // 2)

        all_gem = all(obj['module'] == 'gem' for obj in resp['data'])
        gem_result = any(obj['type'] == 'result' for obj in resp['data'])
        gem_loop = any(obj['type'] == 'informingLoop' for obj in resp['data'])

        self.assertTrue(all_gem)
        self.assertTrue(gem_result)
        self.assertTrue(gem_loop)

        with clock.FakeClock(fake_now):
            resp = self.send_get(
                f'GenomicOutreachV2?start_date={fake_date_one}&module=PGX'
            )

        self.assertEqual(len(resp['data']), len(total_num_set) // 2)

        all_pgx = all(obj['module'] == 'pgx' for obj in resp['data'])
        pgx_result = any(obj['type'] == 'result' for obj in resp['data'])
        pgx_loop = any(obj['type'] == 'informingLoop' for obj in resp['data'])

        self.assertTrue(all_pgx)
        self.assertTrue(pgx_result)
        self.assertTrue(pgx_loop)

    def test_hdr_result_payload(self):
        module = 'hdr_v1'
        report_state = GenomicReportState.HDR_RPT_UNINFORMATIVE

        gen_set = self.data_generator.create_database_genomic_set(
            genomicSetName=".",
            genomicSetCriteria=".",
            genomicSetVersion=1
        )

        participant = self.data_generator.create_database_participant()

        self.data_generator.create_database_participant_summary(
            participant=participant,
            consentForGenomicsRORAuthored=clock.CLOCK.now(),
            consentForStudyEnrollmentAuthored=clock.CLOCK.now()
        )

        gen_member = self.data_generator.create_database_genomic_set_member(
            genomicSetId=gen_set.id,
            biobankId="100153482",
            sampleId="21042005280",
            genomeType="aou_wgs",
            genomicWorkflowState=GenomicWorkflowState.CVL_READY,
            participantId=participant.participantId,
            genomicWorkflowStateModifiedTime=clock.CLOCK.now()
        )

        self.data_generator.create_database_genomic_member_report_state(
            genomic_set_member_id=gen_member.id,
            participant_id=participant.participantId,
            module=module,
            genomic_report_state=report_state,
            report_revision_number=1,
            event_authored_time=clock.CLOCK.now()
        )

        resp = self.send_get(f'GenomicOutreachV2?participant_id={participant.participantId}')

        self.assertTrue(len(resp['data']), 1)
        hdr_result_keys = ['module', 'type', 'status', 'participant_id', 'hdr_result_status',
                           'report_revision_number']
        all_hdr_result_keys_data = all(not len(obj.keys() - hdr_result_keys) and obj.values() for obj in resp['data'])
        self.assertTrue(all_hdr_result_keys_data)

        self.assertTrue(all(obj['hdr_result_status'] == 'uninformative' for obj in resp['data']))
        self.assertTrue(all(obj['report_revision_number'] == 1 for obj in resp['data']))

    def test_get_by_date_range(self):
        self.num_participants = 10
        fake_date_one = parser.parse('2020-05-30T08:00:01-05:00')
        fake_date_two = parser.parse('2020-05-31T08:00:01-05:00')
        fake_now = clock.CLOCK.now().replace(microsecond=0)
        module = 'gem'
        report_state = GenomicReportState.GEM_RPT_READY

        gen_set = self.data_generator.create_database_genomic_set(
            genomicSetName=".",
            genomicSetCriteria=".",
            genomicSetVersion=1
        )

        for num in range(self.num_participants):
            participant = self.data_generator.create_database_participant()

            self.data_generator.create_database_participant_summary(
                participant=participant,
                consentForGenomicsRORAuthored=fake_date_two,
                consentForStudyEnrollmentAuthored=fake_date_two
            )

            gen_member = self.data_generator.create_database_genomic_set_member(
                genomicSetId=gen_set.id,
                biobankId="100153482",
                sampleId="21042005280",
                genomeType="aou_array",
                genomicWorkflowState=GenomicWorkflowState.GEM_RPT_READY,
                participantId=participant.participantId,
            )

            self.data_generator.create_database_genomic_member_report_state(
                genomic_set_member_id=gen_member.id,
                participant_id=participant.participantId,
                module=module,
                genomic_report_state=report_state,
                event_authored_time=fake_date_two if num % 2 == 0 else fake_date_one
            )

            self.data_generator.create_database_genomic_informing_loop(
                message_record_id=1,
                event_type='informing_loop_decision',
                module_type=module,
                participant_id=participant.participantId,
                decision_value='maybe_later',
                event_authored_time=fake_date_two if num % 2 == 0 else fake_date_one
            )

        total_num_set = self.loop_dao.get_all() + self.report_dao.get_all()
        self.assertEqual(len(total_num_set), 20)

        with clock.FakeClock(fake_now):
            resp = self.send_get(
                f'GenomicOutreachV2?start_date={fake_date_two}'
            )

        self.assertEqual(resp['data'], [])

        with clock.FakeClock(fake_now):
            resp = self.send_get(
                f'GenomicOutreachV2?start_date={fake_date_one}'
            )

        self.assertEqual(len(resp['data']), len(total_num_set) / 2)

        loop_and_result = all(obj['type'] == 'informingLoop' or obj['type'] == 'result' for obj in resp['data'])
        self.assertTrue(loop_and_result)

    def test_get_last_updated_informing_loop_decision(self):
        self.num_loops = 3
        fake_date_one = parser.parse('2020-05-30T08:00:01-05:00')
        fake_date_two = parser.parse('2020-05-31T08:00:01-05:00')
        fake_now = clock.CLOCK.now().replace(microsecond=0)
        module = 'gem'
        decisions = ['yes', 'no', 'maybe_later']

        gen_set = self.data_generator.create_database_genomic_set(
            genomicSetName=".",
            genomicSetCriteria=".",
            genomicSetVersion=1
        )

        participant_one = self.data_generator.create_database_participant()

        self.data_generator.create_database_participant_summary(
            participant=participant_one,
            consentForGenomicsRORAuthored=fake_date_two,
            consentForStudyEnrollmentAuthored=fake_date_two
        )

        self.data_generator.create_database_genomic_set_member(
            genomicSetId=gen_set.id,
            biobankId="100153482",
            sampleId="21042005280",
            genomeType="aou_array",
            genomicWorkflowState=GenomicWorkflowState.GEM_RPT_READY,
            participantId=participant_one.participantId,
            genomicWorkflowStateModifiedTime=fake_date_two
        )

        for i, _ in enumerate(decisions):
            # maybe_later => newest date
            self.data_generator.create_database_genomic_informing_loop(
                message_record_id=i + 1,
                event_type='informing_loop_decision',
                module_type=module,
                participant_id=participant_one.participantId,
                decision_value=decisions[i],
                event_authored_time=fake_date_one + datetime.timedelta(days=i, minutes=i+31)
            )

        participant_two = self.data_generator.create_database_participant()

        self.data_generator.create_database_participant_summary(
            participant=participant_two,
            consentForGenomicsRORAuthored=fake_date_two,
            consentForStudyEnrollmentAuthored=fake_date_two
        )

        self.data_generator.create_database_genomic_set_member(
            genomicSetId=gen_set.id,
            biobankId="100153482",
            sampleId="21042005280",
            genomeType="aou_array",
            genomicWorkflowState=GenomicWorkflowState.GEM_RPT_READY,
            participantId=participant_two.participantId,
            genomicWorkflowStateModifiedTime=fake_date_two
        )

        for i, _ in enumerate(decisions):
            # maybe_later => newest date
            self.data_generator.create_database_genomic_informing_loop(
                message_record_id=i + 2,
                event_type='informing_loop_decision',
                module_type=module,
                participant_id=participant_two.participantId,
                decision_value=decisions[i],
                event_authored_time=fake_date_one + datetime.timedelta(days=i, minutes=i+32)
            )

        with clock.FakeClock(fake_now):
            resp = self.send_get(
                f'GenomicOutreachV2?start_date={fake_date_one}'
            )

        response_data = resp.get('data')
        self.assertTrue(all(obj['decision'] == decisions[2] for obj in response_data))

        all_loops = self.loop_dao.get_all()
        self.assertEqual(
            len([obj['participant_id'] for obj in response_data]),
            len(all_loops) / len(decisions)
        )

    def test_get_only_ready_informing_loop(self):
        self.num_participants = 10
        first_participant = None
        fake_date_one = parser.parse('2020-05-30T08:00:01-05:00')
        fake_date_two = parser.parse('2020-05-31T08:00:01-05:00')
        fake_now = clock.CLOCK.now().replace(microsecond=0)
        ready_modules = ['hdr', 'pgx']

        gen_set = self.data_generator.create_database_genomic_set(
            genomicSetName=".",
            genomicSetCriteria=".",
            genomicSetVersion=1
        )

        for num in range(self.num_participants):
            participant = self.data_generator.create_database_participant()
            loop_modified_date = fake_date_two if num % 2 == 0 else fake_date_one
            if num == 0:
                first_participant = participant

            self.data_generator.create_database_participant_summary(
                participant=participant,
                consentForStudyEnrollment=1,
                consentForGenomicsROR=1
            )

            member = self.data_generator.create_database_genomic_set_member(
                genomicSetId=gen_set.id,
                participantId=participant.participantId,
                genomeType=config.GENOME_TYPE_WGS,
                qcStatus=GenomicQcStatus.PASS,
                gcManifestSampleSource='Whole Blood',
                informingLoopReadyFlag=1,
                informingLoopReadyFlagModified=loop_modified_date
            )

            self.data_generator.create_database_genomic_gc_validation_metrics(
                genomicSetMemberId=member.id,
                sexConcordance='True',
                drcFpConcordance='Pass',
                drcSexConcordance='Pass',
                processingStatus='Pass'
            )

        resp = self.send_get(f'GenomicOutreachV2?participant_id={first_participant.participantId}')

        # 2 ready objects based on module type
        self.assertEqual(len(resp['data']), 2)
        self.assertTrue(all(obj['participant_id'] == f'P{first_participant.participantId}'
                            for obj in resp['data']))

        self.assertTrue(all(obj['type'] == 'informingLoop' for obj in resp['data']))
        self.assertTrue(all(obj['module'] in ready_modules for obj in resp['data']))
        self.assertTrue(all(obj['status'] == 'ready' for obj in resp['data']))

        with clock.FakeClock(fake_now):
            resp = self.send_get(
                f'GenomicOutreachV2?start_date={fake_date_one}'
            )

        current_members = self.member_dao.get_all()
        # 5 qualify based on data * 2 for each ready module
        self.assertEqual(
            len(resp['data']),
            (len(current_members) // 2) * len(ready_modules)
        )

        self.assertTrue(all(obj['type'] == 'informingLoop' for obj in resp['data']))
        self.assertTrue(all(obj['module'] in ready_modules for obj in resp['data']))
        self.assertTrue(all(obj['status'] == 'ready' for obj in resp['data']))

        # with module param passed | ready modules are only hdr | pgx
        with clock.FakeClock(fake_now):
            resp = self.send_get(
                f'GenomicOutreachV2?start_date={fake_date_one}&module=GEM'
            )

        self.assertEqual(resp['data'], [])

        with clock.FakeClock(fake_now):
            resp = self.send_get(
                f'GenomicOutreachV2?start_date={fake_date_one}&module=HDR'
            )

        self.assertEqual(
            len(resp['data']),
            (len(current_members) // 2)
        )

        self.assertTrue(all(obj['type'] == 'informingLoop' for obj in resp['data']))
        self.assertTrue(all(obj['module'] == 'hdr' for obj in resp['data']))
        self.assertTrue(all(obj['status'] == 'ready' for obj in resp['data']))

    def test_get_only_ready_informing_loop_data_updates(self):
        fake_date_one = parser.parse('2020-05-30T08:00:01-05:00')
        ready_modules = ['hdr', 'pgx']

        gen_set = self.data_generator.create_database_genomic_set(
            genomicSetName=".",
            genomicSetCriteria=".",
            genomicSetVersion=1
        )

        participant = self.data_generator.create_database_participant()

        self.data_generator.create_database_participant_summary(
            participant=participant,
            consentForStudyEnrollment=1,
            consentForGenomicsROR=1
        )

        member = self.data_generator.create_database_genomic_set_member(
            genomicSetId=gen_set.id,
            participantId=participant.participantId,
            genomeType=config.GENOME_TYPE_WGS,
            qcStatus=GenomicQcStatus.PASS,
            gcManifestSampleSource='Whole Blood',
            informingLoopReadyFlag=1,
            informingLoopReadyFlagModified=fake_date_one
        )

        self.data_generator.create_database_genomic_gc_validation_metrics(
            genomicSetMemberId=member.id,
            sexConcordance='True',
            drcFpConcordance='Pass',
            drcSexConcordance='Pass',
            processingStatus='Pass'
        )

        # correct data
        resp = self.send_get(f'GenomicOutreachV2?participant_id={participant.participantId}')

        # 2 ready objects based on module type
        self.assertEqual(len(resp['data']), 2)
        self.assertTrue(all(obj['participant_id'] == f'P{participant.participantId}'
                            for obj in resp['data']))

        self.assertTrue(all(obj['type'] == 'informingLoop' for obj in resp['data']))
        self.assertTrue(all(obj['module'] in ready_modules for obj in resp['data']))
        self.assertTrue(all(obj['status'] == 'ready' for obj in resp['data']))

        members = self.member_dao.get_all()

        # ready loop flag changed => no data
        member = list(filter(lambda x: x.participantId == participant.participantId, members))[0]
        member.informingLoopReadyFlag = 0
        self.member_dao.update(member)

        resp = self.send_get(f'GenomicOutreachV2?participant_id={participant.participantId}', expected_status=404)
        self.assertEqual(resp.status_code, 404)

        # ready loop flag is correct, qc status is wrong => no data
        member = list(filter(lambda x: x.participantId == participant.participantId, members))[0]
        member.informingLoopReadyFlag = 1
        member.qcStatus = GenomicQcStatus.FAIL
        self.member_dao.update(member)

        resp = self.send_get(f'GenomicOutreachV2?participant_id={participant.participantId}', expected_status=404)
        self.assertEqual(resp.status_code, 404)

        # qc status is good => should have data
        member = list(filter(lambda x: x.participantId == participant.participantId, members))[0]
        member.qcStatus = GenomicQcStatus.PASS
        self.member_dao.update(member)

        resp = self.send_get(f'GenomicOutreachV2?participant_id={participant.participantId}')
        self.assertEqual(len(resp['data']), 2)

    def test_get_multi_module_last_decision_loop(self):
        fake_date_one = parser.parse('2020-05-30T08:00:01-05:00')
        fake_date_two = parser.parse('2020-05-31T08:00:01-05:00')
        fake_now = clock.CLOCK.now().replace(microsecond=0)
        first_participant = None
        loop_modules = ['gem', 'hdr', 'pgx']

        gen_set = self.data_generator.create_database_genomic_set(
            genomicSetName=".",
            genomicSetCriteria=".",
            genomicSetVersion=1
        )

        for num in range(self.num_participants):
            participant = self.data_generator.create_database_participant()
            genome_type = config.GENOME_TYPE_ARRAY if num % 2 == 0 else config.GENOME_TYPE_WGS

            if num == 0:
                first_participant = participant

            self.data_generator.create_database_participant_summary(
                participant=participant,
                consentForGenomicsRORAuthored=fake_date_two,
                consentForStudyEnrollmentAuthored=fake_date_two
            )

            self.data_generator.create_database_genomic_set_member(
                genomicSetId=gen_set.id,
                biobankId="100153482",
                sampleId="21042005280",
                genomeType=genome_type,
                genomicWorkflowState=GenomicWorkflowState.GEM_RPT_READY,
                participantId=participant.participantId,
                genomicWorkflowStateModifiedTime=fake_date_two
            )

            # gem decision
            self.data_generator.create_database_genomic_informing_loop(
                message_record_id=1,
                event_type='informing_loop_decision',
                module_type='gem',
                participant_id=participant.participantId,
                decision_value='yes',
                event_authored_time=fake_date_one
            )

            # 2nd gem decision
            self.data_generator.create_database_genomic_informing_loop(
                message_record_id=1,
                event_type='informing_loop_decision',
                module_type='gem',
                participant_id=participant.participantId,
                decision_value='no',
                event_authored_time=fake_date_one + datetime.timedelta(days=1, minutes=23)
            )

            # 3rd gem decision
            self.data_generator.create_database_genomic_informing_loop(
                message_record_id=1,
                event_type='informing_loop_decision',
                module_type='gem',
                participant_id=participant.participantId,
                decision_value='maybe_later',
                event_authored_time=fake_date_one + datetime.timedelta(days=1, minutes=24)
            )

            # hdr decision
            self.data_generator.create_database_genomic_informing_loop(
                message_record_id=1,
                event_type='informing_loop_decision',
                module_type='hdr',
                participant_id=participant.participantId,
                decision_value='yes',
                event_authored_time=fake_date_one
            )

            # 2nd hdr decision
            self.data_generator.create_database_genomic_informing_loop(
                message_record_id=1,
                event_type='informing_loop_decision',
                module_type='hdr',
                participant_id=participant.participantId,
                decision_value='no',
                event_authored_time=fake_date_one + datetime.timedelta(days=1, minutes=26)
            )

            # pgx decision
            self.data_generator.create_database_genomic_informing_loop(
                message_record_id=1,
                event_type='informing_loop_decision',
                module_type='pgx',
                participant_id=participant.participantId,
                decision_value='yes',
                event_authored_time=fake_date_one
            )

            # 2nd pgx decision
            self.data_generator.create_database_genomic_informing_loop(
                message_record_id=1,
                event_type='informing_loop_decision',
                module_type='pgx',
                participant_id=participant.participantId,
                decision_value='no',
                event_authored_time=fake_date_one + datetime.timedelta(days=1, minutes=30)
            )

        resp = self.send_get(f'GenomicOutreachV2?participant_id={first_participant.participantId}')

        self.assertEqual(len(resp['data']), 3)
        self.assertTrue(all(obj['module'] in loop_modules for obj in resp['data']))
        self.assertTrue(all(obj['decision'] != 'yes' for obj in resp['data']))
        self.assertTrue(all(obj['decision'] == 'no' for obj in resp['data'] if obj['module'] in ['pgx', 'hdr']))
        self.assertTrue(all(obj['decision'] == 'maybe_later' for obj in resp['data'] if obj['module'] in ['gem']))

        with clock.FakeClock(fake_now):
            resp = self.send_get(
                f'GenomicOutreachV2?start_date={fake_date_one}'
            )

        self.assertEqual(len(resp['data']), 15)
        self.assertTrue(all(obj['module'] in loop_modules for obj in resp['data']))
        members_pid_resp_set = {obj['participant_id'] for obj in resp['data']}
        self.assertEqual(len(members_pid_resp_set), 5)
        self.assertTrue(all(obj['decision'] == 'no' for obj in resp['data'] if obj['module'] in ['pgx', 'hdr']))
        self.assertTrue(all(obj['decision'] == 'maybe_later' for obj in resp['data'] if obj['module'] in ['gem']))

        for pid in members_pid_resp_set:
            loops = list(filter(lambda x: x['participant_id'] == pid, resp['data']))
            self.assertEqual(len(loops), 3)

    def test_get_sample_swap_result(self):
        module = 'gem'
        fake_date_one = parser.parse('2020-05-30T08:00:01-05:00')
        report_state = GenomicReportState.GEM_RPT_READY
        genome_type = config.GENOME_TYPE_ARRAY

        gen_set = self.data_generator.create_database_genomic_set(
            genomicSetName=".",
            genomicSetCriteria=".",
            genomicSetVersion=1
        )

        sample_swap = self.data_generator.create_database_genomic_sample_swap(
            name='daSwap'
        )

        participant = self.data_generator.create_database_participant()

        self.data_generator.create_database_participant_summary(
            participant=participant,
            consentForGenomicsRORAuthored=fake_date_one,
            consentForStudyEnrollmentAuthored=fake_date_one
        )

        gen_member = self.data_generator.create_database_genomic_set_member(
            genomicSetId=gen_set.id,
            biobankId="100153482",
            sampleId="21042005280",
            genomeType=genome_type,
            genomicWorkflowState=GenomicWorkflowState.GEM_RPT_READY,
            participantId=participant.participantId,
            genomicWorkflowStateModifiedTime=fake_date_one
        )

        self.data_generator.create_database_genomic_member_report_state(
            genomic_set_member_id=gen_member.id,
            participant_id=participant.participantId,
            module=module,
            genomic_report_state=report_state,
            event_authored_time=fake_date_one
        )

        # initial result ready
        resp = self.send_get(f'GenomicOutreachV2?participant_id={participant.participantId}')

        result_keys = ['module', 'type', 'status', 'participant_id']

        all_result_keys_data = all(not len(obj.keys() - result_keys) and obj.values() for obj in resp['data'])
        self.assertTrue(all_result_keys_data)
        self.assertEqual(len(resp['data']), 1)
        self.assertTrue(all(obj['type'] == 'result' for obj in resp['data']))
        self.assertTrue(all(obj['module'] == module for obj in resp['data']))

        # denoted as being part of sample swap
        self.data_generator.create_database_genomic_sample_swap_member(
            genomic_sample_swap=sample_swap.id,
            genomic_set_member_id=gen_member.id,
            category=GenomicSampleSwapCategory.RESULT_READY_NOT_VIEWED
        )

        resp = self.send_get(f'GenomicOutreachV2?participant_id={participant.participantId}')

        self.assertTrue(all_result_keys_data)
        self.assertEqual(len(resp['data']), 1)
        self.assertTrue(all(obj['type'] == 'result' for obj in resp['data']))
        swap_module_name = f'{module}_{sample_swap.name}_' \
                           f'{GenomicSampleSwapCategory.RESULT_READY_NOT_VIEWED.name}'.lower()
        self.assertTrue(all(obj['module'] == swap_module_name for obj in resp['data']))

    def test_get_result_viewed(self):
        fake_date_one = parser.parse('2020-05-30T08:00:01-05:00')
        fake_date_two = parser.parse('2020-05-31T08:00:01-05:00')
        fake_now = clock.CLOCK.now().replace(microsecond=0)

        gem_module = 'gem'
        gem_report_state = GenomicReportState.GEM_RPT_READY
        gem_result_keys = ['module', 'type', 'status', 'participant_id']

        hdr_module = 'hdr_v1'
        hdr_report_state = GenomicReportState.HDR_RPT_POSITIVE
        hdr_result_keys = ['module', 'type', 'status', 'participant_id', 'hdr_result_status', 'report_revision_number']

        gen_set = self.data_generator.create_database_genomic_set(
            genomicSetName=".",
            genomicSetCriteria=".",
            genomicSetVersion=1
        )

        participant = self.data_generator.create_database_participant()

        self.data_generator.create_database_participant_summary(
            participant=participant,
            consentForGenomicsRORAuthored=fake_date_one,
            consentForStudyEnrollmentAuthored=fake_date_one
        )

        # GEM
        gem_member = self.data_generator.create_database_genomic_set_member(
            genomicSetId=gen_set.id,
            biobankId="100153482",
            sampleId="21042005280",
            genomeType=config.GENOME_TYPE_ARRAY,
            genomicWorkflowState=GenomicWorkflowState.GEM_RPT_READY,
            participantId=participant.participantId,
        )

        self.data_generator.create_database_genomic_member_report_state(
            genomic_set_member_id=gem_member.id,
            participant_id=participant.participantId,
            module=gem_module,
            genomic_report_state=gem_report_state,
            event_authored_time=fake_date_one,
            sample_id=gem_member.sampleId
        )

        self.data_generator.create_genomic_result_viewed(
            participant_id=participant.participantId,
            event_type='result_viewed',
            event_authored_time=fake_date_two,
            module_type=gem_module,
            sample_id=gem_member.sampleId
        )

        resp = self.send_get(f'GenomicOutreachV2?participant_id={participant.participantId}')

        all_gem_keys_data = all(not len(obj.keys() - gem_result_keys) and obj.values() for obj in resp['data'])

        self.assertTrue(all_gem_keys_data)
        self.assertEqual(len(resp['data']), 2)
        self.assertTrue(all(obj['type'] == 'result' for obj in resp['data']))
        self.assertTrue(all(obj['module'] == gem_module for obj in resp['data']))
        # should be one ready
        self.assertTrue(any(obj['status'] == 'ready' for obj in resp['data']))
        # should be one viewed
        self.assertTrue(any(obj['status'] == 'viewed' for obj in resp['data']))

        # HDR
        hdr_member = self.data_generator.create_database_genomic_set_member(
            genomicSetId=gen_set.id,
            biobankId="100153482",
            sampleId="21042005280",
            genomeType=config.GENOME_TYPE_WGS,
            genomicWorkflowState=GenomicWorkflowState.CVL_READY,
            participantId=participant.participantId,
        )

        self.data_generator.create_database_genomic_member_report_state(
            genomic_set_member_id=hdr_member.id,
            participant_id=participant.participantId,
            module=hdr_module,
            genomic_report_state=hdr_report_state,
            event_authored_time=fake_date_one,
            sample_id=hdr_member.sampleId,
            report_revision_number=0
        )

        self.data_generator.create_genomic_result_viewed(
            participant_id=participant.participantId,
            event_type='result_viewed',
            event_authored_time=fake_date_two,
            module_type=hdr_module,
            sample_id=hdr_member.sampleId
        )

        resp = self.send_get(f'GenomicOutreachV2?participant_id={participant.participantId}')

        self.assertEqual(len(resp['data']), 4)
        self.assertTrue(all(obj['type'] == 'result' for obj in resp['data']))

        hdr_objs = list(filter(lambda x: x['module'] == 'hdr', resp['data']))

        self.assertEqual(len(hdr_objs), 2)
        # should be one ready
        self.assertTrue(any(obj['status'] == 'ready' for obj in hdr_objs))
        # should be one viewed
        self.assertTrue(any(obj['status'] == 'viewed' for obj in hdr_objs))

        all_hdr_keys_data = all(not len(obj.keys() - hdr_result_keys) and obj.values() for obj in hdr_objs)
        self.assertTrue(all_hdr_keys_data)

        with clock.FakeClock(fake_now):
            resp = self.send_get(
                f'GenomicOutreachV2?start_date={fake_date_one}'
            )

        # should only be viewed states * 2
        self.assertEqual(len(resp['data']), 2)

        self.assertTrue(all(obj['type'] == 'result' for obj in resp['data']))
        # should all be viewed
        self.assertTrue(all(obj['status'] == 'viewed' for obj in resp['data']))

        self.assertTrue(all(obj['module'] in ['gem', 'hdr'] for obj in resp['data']))

    # POST/PUT
    def test_validate_post_put_data(self):

        resp = self.send_post(
            'GenomicOutreachV2',
            request_data={'bad_key': ''},
            expected_status=400
        )
        self.assertEqual(resp.status_code, 400)
        self.assertEqual(resp.json['message'], 'GenomicOutreachV2 POST accepted data/params: '
                                               'informing_loop_eligible | eligibility_date_utc | participant_id')

        resp = self.send_put(
            'GenomicOutreachV2',
            request_data={'bad_key': ''},
            expected_status=400
        )
        self.assertEqual(resp.status_code, 400)
        self.assertEqual(resp.json['message'], 'GenomicOutreachV2 PUT accepted data/params: '
                                               'informing_loop_eligible | eligibility_date_utc | participant_id')

        resp = self.send_post(
            'GenomicOutreachV2?bad_arg=bad',
            request_data={
                'informing_loop_eligible': '',
                'eligibility_date_utc': ''
            },
            expected_status=400
        )
        self.assertEqual(resp.status_code, 400)
        self.assertEqual(resp.json['message'], 'GenomicOutreachV2 POST accepted data/params: '
                                               'informing_loop_eligible | eligibility_date_utc | participant_id')

        resp = self.send_put(
            'GenomicOutreachV2?bad_arg=bad',
            request_data={
                'informing_loop_eligible': '',
                'eligibility_date_utc': ''
            },
            expected_status=400
        )
        self.assertEqual(resp.status_code, 400)
        self.assertEqual(resp.json['message'], 'GenomicOutreachV2 PUT accepted data/params: '
                                               'informing_loop_eligible | eligibility_date_utc | participant_id')

        resp = self.send_post(
            'GenomicOutreachV2',
            request_data={},
            expected_status=400
        )
        self.assertEqual(resp.status_code, 400)
        self.assertEqual(resp.json['message'], 'Missing request data/params in POST')

        resp = self.send_put(
            'GenomicOutreachV2',
            request_data={},
            expected_status=400
        )
        self.assertEqual(resp.status_code, 400)
        self.assertEqual(resp.json['message'], 'Missing request data/params in PUT')

    def build_ready_loop_template_data(self):
        post_data_map = {
            'participant_summary': {
                'participant_id': 'external_participant_id',
                'consent_for_genomics_ror': 1,
                'consent_for_study_enrollment': 1,
                'withdrawal_status': 1,
                'suspension_status': 1,
                'deceased_status': 0,
            },
            'genomic_set_member': {
                'genomic_set_id': 'system',
                'participant_id': '%participant_summary.participant_id%',
                'genome_type': 'aou_wgs',
                'qc_status': 1,
                'gc_manifest_sample_source': 'Whole Blood',
                'informing_loop_ready_flag': 'external_informing_loop_ready_flag',
                'informing_loop_ready_flag_modified': 'external_informing_loop_ready_flag_modified'
            },
            'genomic_gc_validation_metrics': {
                'genomic_set_member_id': '%genomic_set_member.id%',
                'sex_concordance': 'True',
                'processing_status': 'PASS',
                'drc_sex_concordance': 'PASS',
                'drc_fp_concordance': 'PASS',
            }
        }

        # build template datagen w1il template data
        self.build_cvl_template_based_data(
            template_name='default',
            _dict=post_data_map,
            project_name='cvl_il'
        )

    def test_post_put_checks_for_participant(self):

        resp = self.send_post(
            'GenomicOutreachV2?participant_id=P12234312',
            request_data={
                'informing_loop_eligible': 'yes',
                'eligibility_date_utc': '2022-03-23T20:52:12+00:00'
            },
            expected_status=404
        )
        self.assertIsNotNone(resp)
        self.assertEqual(resp.status_code, 404)
        self.assertEqual(resp.json['message'], 'Participant with id P12234312 was not found')

        resp = self.send_put(
            'GenomicOutreachV2?participant_id=P12234312',
            request_data={
                'informing_loop_eligible': 'yes',
                'eligibility_date_utc': '2022-03-23T20:52:12+00:00'
            },
            expected_status=404
        )
        self.assertIsNotNone(resp)
        self.assertEqual(resp.status_code, 404)
        self.assertEqual(resp.json['message'], 'Participant with id P12234312 was not found')

    def test_post_with_yes_inserts_via_template_returns_correctly(self):
        ready_modules = ['hdr', 'pgx']
        self.build_ready_loop_template_data()

        participant = self.data_generator.create_database_participant()

        resp = self.send_post(
            f'GenomicOutreachV2?participant_id=P{participant.participantId}',
            request_data={
                'informing_loop_eligible': 'yes',
                'eligibility_date_utc': '2022-03-23T20:52:12+00:00'
            }
        )

        self.assertIsNotNone(resp)
        self.assertEqual(len(resp['data']), 2)
        self.assertTrue(all(obj['participant_id'] == f'P{participant.participantId}'
                            for obj in resp['data']))

        self.assertTrue(all(obj['module'] in ready_modules for obj in resp['data']))
        self.assertTrue(all(obj['status'] == 'ready' for obj in resp['data']))

        self.clear_table_after_test('genomic_datagen_member_run')

    def test_post_twice_same_pid_validation(self):
        self.build_ready_loop_template_data()

        participant = self.data_generator.create_database_participant()

        resp = self.send_post(
            f'GenomicOutreachV2?participant_id=P{participant.participantId}',
            request_data={
                'informing_loop_eligible': 'yes',
                'eligibility_date_utc': '2022-03-23T20:52:12+00:00'
            }
        )

        self.assertIsNotNone(resp)
        self.assertEqual(len(resp['data']), 2)

        resp = self.send_post(
            f'GenomicOutreachV2?participant_id=P{participant.participantId}',
            request_data={
                'informing_loop_eligible': 'yes',
                'eligibility_date_utc': '2022-03-23T20:52:12+00:00'
            },
            expected_status=400
        )

        self.assertEqual(resp.status_code, 400)
        self.assertEqual(resp.json['message'], f'Participant with id P{participant.participantId} and WGS sample '
                                               f'already exists. Please use PUT to update.')

    def test_post_with_no_inserts_via_template_returns_correctly(self):
        self.build_ready_loop_template_data()

        participant = self.data_generator.create_database_participant()

        resp = self.send_post(
            f'GenomicOutreachV2?participant_id=P{participant.participantId}',
            request_data={
                'informing_loop_eligible': 'no',
                'eligibility_date_utc': '2022-03-23T20:52:12+00:00'
            }
        )

        self.assertIsNotNone(resp)
        self.assertEqual(len(resp['data']), 0)
        self.assertEqual(resp['data'], [])

        self.clear_table_after_test('genomic_datagen_member_run')

    def test_put_validates_updates_and_returns(self):

        # PUT for no set member
        resp = self.send_put(
            'GenomicOutreachV2?participant_id=P2121232',
            request_data={
                'informing_loop_eligible': 'no',
                'eligibility_date_utc': '2022-03-23T20:52:12+00:00'
            },
            expected_status=404
        )

        self.assertIsNotNone(resp)
        self.assertEqual(resp.status_code, 404)
        self.assertEqual(resp.json['message'], 'Participant with id P2121232 was not found')

        self.build_ready_loop_template_data()

        participant = self.data_generator.create_database_participant()

        # POST to create set member
        resp = self.send_post(
            f'GenomicOutreachV2?participant_id=P{participant.participantId}',
            request_data={
                'informing_loop_eligible': 'no',
                'eligibility_date_utc': '2022-03-23T20:52:12+00:00'
            }
        )

        self.assertIsNotNone(resp)
        self.assertEqual(len(resp['data']), 0)
        self.assertEqual(resp['data'], [])

        # PUT to update set member
        resp = self.send_put(
            f'GenomicOutreachV2?participant_id=P{participant.participantId}',
            request_data={
                'informing_loop_eligible': 'yes',
                'eligibility_date_utc': '2022-03-23T20:52:12+00:00'
            }
        )

        self.assertIsNotNone(resp)
        self.assertEqual(len(resp['data']), 2)

        resp = self.send_put(
            f'GenomicOutreachV2?participant_id=P{participant.participantId}',
            request_data={
                'informing_loop_eligible': 'no',
                'eligibility_date_utc': '2022-03-23T20:52:12+00:00'
            }
        )

        self.assertIsNotNone(resp)
        self.assertEqual(len(resp['data']), 0)
        self.assertEqual(resp['data'], [])

        self.clear_table_after_test('genomic_datagen_member_run')


class GenomicSchedulingApiTest(GenomicApiTestBase):
    def setUp(self):
        super().setUp()
        self.appointment_dao = GenomicAppointmentEventDao()
        self.num_participants = 4

        self.gen_set = self.data_generator.create_database_genomic_set(
            genomicSetName=".",
            genomicSetCriteria=".",
            genomicSetVersion=1
        )

    def build_base_participant_data(self):

        participant = self.data_generator.create_database_participant()

        self.data_generator.create_database_participant_summary(
            participant=participant,
            consentForGenomicsROR=1
        )

        self.data_generator.create_database_genomic_set_member(
            genomicSetId=self.gen_set.id,
            participantId=participant.participantId,
            genomeType='aou_array'
        )

        return participant

    def test_full_participant_validation_appointment_lookup(self):

        gen_set = self.data_generator.create_database_genomic_set(
            genomicSetName="..",
            genomicSetCriteria="..",
            genomicSetVersion=1
        )
        participant = self.data_generator.create_database_participant()

        # no appointments / no set member
        resp = self.send_get(
            f"GenomicScheduling?participant_id=P{participant.participantId}",
            expected_status=http.client.NOT_FOUND
        )
        self.assertEqual(
            resp.json['message'],
            f'Participant with ID P{participant.participantId} not found in RDR'
        )
        self.assertEqual(resp.status_code, 404)

        # summary / no set member
        self.data_generator.create_database_participant_summary(
            participant=participant,
            consentForGenomicsROR=1
        )

        resp = self.send_get(
            f"GenomicScheduling?participant_id=P{participant.participantId}",
            expected_status=http.client.NOT_FOUND
        )
        self.assertEqual(resp.json['message'],
                         f'Participant with ID P{participant.participantId} '
                         f'not found in Genomics system'
                         )
        self.assertEqual(resp.status_code, 404)

        # summary / set member / failed validation in query
        self.data_generator.create_database_genomic_set_member(
            genomicSetId=gen_set.id,
            participantId=participant.participantId,
            genomeType='aou_array'
        )

        resp = self.send_get(
            f"GenomicScheduling?participant_id=P{participant.participantId}",
            expected_status=http.client.NOT_FOUND
        )

        self.assertEqual(resp.json['message'], f'Participant with ID P{participant.participantId} '
                                               f'did not pass validation check')
        self.assertEqual(resp.status_code, 404)

        # add appointment record
        appointment_record = self.data_generator.create_database_genomic_appointment(
            message_record_id=1,
            appointment_id=1,
            event_type='appointment_scheduled',
            module_type='hdr',
            participant_id=participant.participantId,
            event_authored_time=clock.CLOCK.now(),
            source='Color',
            appointment_timestamp=format_datetime(clock.CLOCK.now()),
            appointment_timezone='America/Los_Angeles',
            location='123 address st',
            contact_number='17348675309',
            language='EN'
        )

        resp = self.send_get(
            f"GenomicScheduling?participant_id=P{participant.participantId}"
        )

        self.assertIsNotNone(resp)
        self.assertEqual(len(resp.get('data')), 1)

        self.assertTrue(all(obj['participant_id'] == f'P{participant.participantId}' for obj in resp['data']))
        self.assertTrue(all(obj['appointment_id'] == appointment_record.appointment_id for obj in resp['data']))
        self.assertTrue(all(obj['type'] == 'appointment' for obj in resp['data']))
        self.assertTrue(all(obj['module'] == appointment_record.module_type for obj in resp['data']))
        self.assertTrue(all(obj['status'] == appointment_record.event_type.split('_')[-1] for obj in resp['data']))
        self.assertTrue(all(obj['appointment_timestamp'] is not None and '+00:00' in obj['appointment_timestamp'] for
                            obj in resp['data']))
        self.assertTrue(all(obj['appointment_timezone'] == appointment_record.appointment_timezone for obj in
                            resp['data']))
        self.assertTrue(all(obj['source'] == appointment_record.source for obj in resp['data']))
        self.assertTrue(all(obj['contact_number'] == appointment_record.contact_number for obj in resp['data']))
        self.assertTrue(all(obj['location'] == appointment_record.location for obj in resp['data']))
        self.assertTrue(all(obj['language'] == appointment_record.language for obj in resp['data']))

        self.assertTrue(all(obj['note_available'] is False for obj in resp['data']))

    def test_validate_params(self):
        bad_response = 'GenomicScheduling GET accepted params: start_date | end_date | participant_id | module'

        response = self.send_get(
            "GenomicScheduling?wwqwqw=ewewe",
            expected_status=http.client.BAD_REQUEST
        )
        self.assertEqual(response.json['message'], bad_response)
        self.assertEqual(response.status_code, 400)

        response = self.send_get(
            "GenomicScheduling?wwqwqw=ewewe&participant_id=P2",
            expected_status=http.client.BAD_REQUEST
        )

        self.assertEqual(response.json['message'], bad_response)
        self.assertEqual(response.status_code, 400)

        bad_response = 'Participant ID or Start Date parameter is required for use with GenomicScheduling API.'

        response = self.send_get(
            "GenomicScheduling?participant_id=",
            expected_status=http.client.BAD_REQUEST
        )

        self.assertEqual(response.json['message'], bad_response)
        self.assertEqual(response.status_code, 400)

        bad_response = 'GenomicScheduling GET accepted modules: hdr | pgx'

        response = self.send_get(
            "GenomicScheduling?module=ewewewew",
            expected_status=http.client.BAD_REQUEST
        )

        self.assertEqual(response.json['message'], bad_response)
        self.assertEqual(response.status_code, 400)

    def test_get_note_available_on_last_appointment_id(self):

        participant_data = self.build_base_participant_data()

        # add appointment record
        self.data_generator.create_database_genomic_appointment(
            message_record_id=1,
            appointment_id=1,
            event_type='appointment_scheduled',
            module_type='hdr',
            participant_id=participant_data.participantId,
            event_authored_time=clock.CLOCK.now()
        )

        resp = self.send_get(
            f"GenomicScheduling?participant_id=P{participant_data.participantId}"
        )

        self.assertTrue(all(obj['status'] == 'scheduled' for obj in resp['data']))
        self.assertTrue(all(obj['note_available'] is False for obj in resp['data']))

        # add note event on appointment_id: 1
        self.data_generator.create_database_genomic_appointment(
            message_record_id=2,
            appointment_id=1,
            event_type='appointment_note_available',
            module_type='hdr',
            participant_id=participant_data.participantId,
            event_authored_time=clock.CLOCK.now() + datetime.timedelta(days=1)
        )

        resp = self.send_get(
            f"GenomicScheduling?participant_id=P{participant_data.participantId}"
        )

        # should still be scheduled as the status, note event should be filtered out
        self.assertTrue(all(obj['status'] == 'scheduled' for obj in resp['data']))
        self.assertTrue(all(obj['note_available'] is True for obj in resp['data']))

    def test_module_params(self):

        participant_data = self.build_base_participant_data()

        # add hdr appointment record
        self.data_generator.create_database_genomic_appointment(
            message_record_id=1,
            appointment_id=1,
            event_type='appointment_scheduled',
            module_type='hdr',
            participant_id=participant_data.participantId,
            event_authored_time=clock.CLOCK.now()
        )

        # add hdr appointment record
        self.data_generator.create_database_genomic_appointment(
            message_record_id=2,
            appointment_id=2,
            event_type='appointment_scheduled',
            module_type='pgx',
            participant_id=participant_data.participantId,
            event_authored_time=clock.CLOCK.now()
        )

        resp = self.send_get(
            f"GenomicScheduling?participant_id=P{participant_data.participantId}"
        )

        current_appointments = self.appointment_dao.get_all()

        all_module_appointments = list(filter(lambda x: x.participant_id == participant_data.participantId,
                                              current_appointments))
        self.assertTrue(len(resp['data']), len(all_module_appointments))  # 2
        self.assertTrue(any(obj['module'] == 'hdr' for obj in resp['data']))  # 1
        self.assertTrue(any(obj['module'] == 'pgx' for obj in resp['data']))  # 1

        resp = self.send_get(
            f"GenomicScheduling?participant_id=P{participant_data.participantId}&module=HDR"
        )

        hdr_appointments = list(
            filter(lambda x: x.participant_id == participant_data.participantId and x.module_type == 'hdr',
                   current_appointments))
        self.assertTrue(len(resp['data']), len(hdr_appointments))  # 1
        self.assertTrue(all(obj['module'] == 'hdr' for obj in resp['data']))  # 1

        resp = self.send_get(
            f"GenomicScheduling?participant_id=P{participant_data.participantId}&module=PGX"
        )

        pgx_appointments = list(
            filter(lambda x: x.participant_id == participant_data.participantId and x.module_type == 'pgx',
                   current_appointments))
        self.assertTrue(len(resp['data']), len(pgx_appointments))  # 1
        self.assertTrue(all(obj['module'] == 'pgx' for obj in resp['data']))  # 1

    def test_get_last_appointment_id_stored_for_participant(self):

        participant_data = self.build_base_participant_data()

        # add hdr appointment record
        self.data_generator.create_database_genomic_appointment(
            message_record_id=1,
            appointment_id=1,
            event_type='appointment_scheduled',
            module_type='hdr',
            participant_id=participant_data.participantId,
            event_authored_time=clock.CLOCK.now()
        )

        # add another hdr appointment record
        self.data_generator.create_database_genomic_appointment(
            message_record_id=2,
            appointment_id=2,
            event_type='appointment_scheduled',
            module_type='hdr',
            participant_id=participant_data.participantId,
            event_authored_time=clock.CLOCK.now()
        )

        resp = self.send_get(
            f"GenomicScheduling?participant_id=P{participant_data.participantId}"
        )

        current_appointments = self.appointment_dao.get_all()
        hdr_appointments = list(filter(lambda x: x.participant_id == participant_data.participantId,
                                       current_appointments))
        self.assertTrue(len(hdr_appointments), 2)

        self.assertTrue(len(resp['data']), len(hdr_appointments) - 1)  # 1
        self.assertTrue(all(obj['module'] == 'hdr' for obj in resp['data']))
        # greatest appointment id value
        self.assertTrue(all(obj['appointment_id'] == 2 for obj in resp['data']))
        self.assertTrue(all(obj['module'] == 'hdr' for obj in resp['data']))
        self.assertTrue(all(obj['status'] == 'scheduled' for obj in resp['data']))

    def test_status_updates_same_appointment_id_greatest_event_authored_time(self):

        participant_data = self.build_base_participant_data()

        # add hdr appointment record
        self.data_generator.create_database_genomic_appointment(
            message_record_id=1,
            appointment_id=1,
            event_type='appointment_scheduled',
            module_type='hdr',
            participant_id=participant_data.participantId,
            event_authored_time=clock.CLOCK.now()
        )

        resp = self.send_get(
            f"GenomicScheduling?participant_id=P{participant_data.participantId}"
        )

        current_appointments = self.appointment_dao.get_all()
        hdr_appointments = list(filter(lambda x: x.participant_id == participant_data.participantId,
                                       current_appointments))
        self.assertTrue(len(hdr_appointments), 1)
        self.assertTrue(len(resp['data']), len(hdr_appointments))  # 1
        self.assertTrue(all(obj['module'] == 'hdr' for obj in resp['data']))
        # greatest appointment id
        self.assertTrue(all(obj['appointment_id'] == 1 for obj in resp['data']))
        # should be scheduled
        self.assertTrue(all(obj['status'] == 'scheduled' for obj in resp['data']))

        # add another hdr appointment record
        self.data_generator.create_database_genomic_appointment(
            message_record_id=2,
            appointment_id=1,  # same id
            event_type='appointment_updated',
            module_type='hdr',
            participant_id=participant_data.participantId,
            event_authored_time=clock.CLOCK.now() + datetime.timedelta(days=1)
        )

        resp = self.send_get(
            f"GenomicScheduling?participant_id=P{participant_data.participantId}"
        )

        current_appointments = self.appointment_dao.get_all()
        hdr_appointments = list(filter(lambda x: x.participant_id == participant_data.participantId,
                                       current_appointments))
        self.assertTrue(len(hdr_appointments), 1)
        self.assertTrue(len(resp['data']), len(hdr_appointments))  # 1
        self.assertTrue(all(obj['module'] == 'hdr' for obj in resp['data']))
        # greatest appointment id
        self.assertTrue(all(obj['appointment_id'] == 1 for obj in resp['data']))
        # should be updated | greatest appointment id and greatest event_authored_time
        self.assertTrue(all(obj['status'] == 'updated' for obj in resp['data']))

    def test_pass_start_date_params(self):
        fake_date_one = parser.parse('2020-05-30T08:00:01-05:00')
        fake_now = clock.CLOCK.now().replace(microsecond=0)
        participant_ids = []

        for i in range(self.num_participants):
            participant_data = self.build_base_participant_data()
            participant_ids.append(participant_data.participantId)

            self.data_generator.create_database_genomic_appointment(
                message_record_id=1,
                appointment_id=i+1,
                event_type='appointment_scheduled',
                module_type='hdr',
                participant_id=participant_data.participantId,
                event_authored_time=fake_date_one
            )

            self.data_generator.create_database_genomic_appointment(
                message_record_id=1,
                appointment_id=i+1,
                event_type='appointment_updated',
                module_type='hdr',
                participant_id=participant_data.participantId,
                event_authored_time=fake_date_one + datetime.timedelta(days=1)
            )

        with clock.FakeClock(fake_now):
            resp = self.send_get(
                f'GenomicScheduling?start_date={fake_date_one}'
            )

        current_appointments = self.appointment_dao.get_all()
        self.assertTrue(len(current_appointments), self.num_participants * 2)  # 8

        self.assertTrue(len(current_appointments) // 2 == len(resp['data']))  # 4
        self.assertTrue(obj['status'] == 'updated' for obj in resp['data'])
        self.assertTrue(obj['module'] == 'hdr' for obj in resp['data'])
        self.assertTrue(all(int(obj['participant_id'].split('P')[-1]) in participant_ids for obj in resp['data']))

        with clock.FakeClock(fake_now):
            resp = self.send_get(
                f'GenomicScheduling?start_date={fake_date_one}&module=HDR'
            )

        self.assertTrue(len(current_appointments) // 2 == len(resp['data']))  # 4
        self.assertTrue(obj['status'] == 'updated' for obj in resp['data'])
        self.assertTrue(obj['module'] == 'hdr' for obj in resp['data'])

        with clock.FakeClock(fake_now):
            resp = self.send_get(
                f'GenomicScheduling?start_date={fake_date_one}&module=PGX'
            )

        self.assertTrue(resp['data'] == [])

    def test_appointment_cancelled_payload(self):
        participant_data = self.build_base_participant_data()

        # add hdr appointment record
        self.data_generator.create_database_genomic_appointment(
            message_record_id=1,
            appointment_id=1,
            event_type='appointment_scheduled',
            module_type='hdr',
            participant_id=participant_data.participantId,
            event_authored_time=clock.CLOCK.now()
        )

        resp = self.send_get(
            f"GenomicScheduling?participant_id=P{participant_data.participantId}"
        )

        current_appointments = self.appointment_dao.get_all()
        hdr_appointments = list(filter(lambda x: x.participant_id == participant_data.participantId,
                                       current_appointments))
        self.assertTrue(len(hdr_appointments), 1)
        self.assertTrue(len(resp['data']), len(hdr_appointments))  # 1
        self.assertTrue(all(obj['module'] == 'hdr' for obj in resp['data']))
        # greatest appointment id
        self.assertTrue(all(obj['appointment_id'] == 1 for obj in resp['data']))
        # should be scheduled
        self.assertTrue(all(obj['status'] == 'scheduled' for obj in resp['data']))

        # add another hdr appointment: cancelled
        self.data_generator.create_database_genomic_appointment(
            message_record_id=2,
            appointment_id=1,  # same id
            event_type='appointment_cancelled',
            module_type='hdr',
            participant_id=participant_data.participantId,
            event_authored_time=clock.CLOCK.now() + datetime.timedelta(days=1),
            cancellation_reason='participant_initiated',
            source='Color'
        )

        resp = self.send_get(
            f"GenomicScheduling?participant_id=P{participant_data.participantId}"
        )

        current_appointments = self.appointment_dao.get_all()
        hdr_appointments = list(filter(lambda x: x.participant_id == participant_data.participantId,
                                       current_appointments))
        self.assertTrue(len(hdr_appointments), 2) # 2 appointment records
        self.assertTrue(len(resp['data']), 1)  # 1
        self.assertTrue(all(obj['module'] == 'hdr' for obj in resp['data']))
        # greatest appointment id
        self.assertTrue(all(obj['appointment_id'] == 1 for obj in resp['data']))
        # should be cancelled | greatest appointment id and greatest event_authored_time
        self.assertTrue(all(obj['status'] == 'cancelled' for obj in resp['data']))
        self.assertTrue(all(obj['cancellation_reason'] == 'participant_initiated' for obj in resp['data']))
        self.assertTrue(all(obj['source'] == 'Color' for obj in resp['data']))


class GenomicCloudTasksApiTest(BaseTestCase):
    def setUp(self):
        super(GenomicCloudTasksApiTest, self).setUp()
        self.data_file_dao = GenomicGcDataFileDao()
        self.cloud_req_dao = GenomicCloudRequestsDao()

    @mock.patch('rdr_service.offline.genomic_pipeline.dispatch_genomic_job_from_task')
    def test_calculate_record_count_task_api(self, dispatch_job_mock):

        manifest = self.data_generator.create_database_genomic_manifest_file()

        # Payload for caculate record count task endpoint
        data = {"manifest_file_id": manifest.id}

        from rdr_service.resource import main as resource_main

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

    @mock.patch('rdr_service.offline.genomic_pipeline.load_awn_manifest_into_raw_table')
    def test_load_manifests_raw_data_task_api(self, load_raw_awn_data_mock):

        from rdr_service.resource import main as resource_main
        raw_manifest_keys = ['aw1', 'aw2', 'aw4', 'w2sc', 'w3ns', 'w3sc', 'w4wr']

        for key in raw_manifest_keys:
            test_file_path = f"test-bucket-name/test_{key}_file.csv"
            data = {
                "file_path": test_file_path,
                "file_type": key
            }

            self.send_post(
                local_path='LoadRawAWNManifestDataAPI',
                request_data=data,
                prefix="/resource/task/",
                test_client=resource_main.app.test_client(),
            )

            load_raw_awn_data_mock.assert_called_with(test_file_path, key)

        self.assertEqual(load_raw_awn_data_mock.call_count, len(raw_manifest_keys))

    def test_load_samples_from_raw_data_task_api(self):

        data = {'job': 'AW1_MANIFEST',
                'server_config': {'biobank_id_prefix': ['S']},
                'member_ids': [1, 2, 3]}

        from rdr_service.resource import main as resource_main

        sample_results = self.send_post(
            local_path='IngestSamplesFromRawTaskAPI',
            request_data=data,
            prefix="/resource/task/",
            test_client=resource_main.app.test_client(),
        )

        self.assertIsNotNone(sample_results)
        self.assertEqual(sample_results['success'], True)

    @mock.patch('rdr_service.offline.genomic_pipeline.execute_genomic_manifest_file_pipeline')
    def test_ingest_aw1_data_task_api(self, ingest_aw1_mock):
        aw1_file_path = "AW1_sample_manifests/test_aw1_file.csv"
        aw1_failure_file_path = "AW1_sample_manifests/test_aw1_FAILURE_file.csv"

        data = {
            "file_path": aw1_file_path,
            "bucket_name": aw1_file_path.split('/')[0],
            "upload_date": '2020-09-13T20:52:12+00:00'
        }

        from rdr_service.resource import main as resource_main

        self.send_post(
            local_path='IngestAW1ManifestTaskApi',
            request_data=data,
            prefix="/resource/task/",
            test_client=resource_main.app.test_client(),
        )

        call_json = ingest_aw1_mock.call_args[0][0]

        self.assertEqual(ingest_aw1_mock.called, True)
        self.assertEqual(call_json['bucket'], data['bucket_name'])
        self.assertEqual(call_json['job'], GenomicJob.AW1_MANIFEST)
        self.assertIsNotNone(call_json['file_data'])

        data = {
            "file_path": aw1_failure_file_path,
            "bucket_name": aw1_failure_file_path.split('/')[0],
            "upload_date": '2020-09-13T20:52:12+00:00'
        }

        self.send_post(
            local_path='IngestAW1ManifestTaskApi',
            request_data=data,
            prefix="/resource/task/",
            test_client=resource_main.app.test_client(),
        )

        call_json = ingest_aw1_mock.call_args[0][0]

        self.assertEqual(ingest_aw1_mock.called, True)
        self.assertEqual(call_json['bucket'], data['bucket_name'])
        self.assertEqual(call_json['job'], GenomicJob.AW1F_MANIFEST)
        self.assertIsNotNone(call_json['file_data'])

    @mock.patch('rdr_service.offline.genomic_pipeline.execute_genomic_manifest_file_pipeline')
    def test_ingest_aw2_data_task_api(self, ingest_aw1_mock):
        aw2_file_path = "AW2_data_manifests/test_aw2_file.csv"

        data = {
            "file_path": aw2_file_path,
            "bucket_name": aw2_file_path.split('/')[0],
            "upload_date": '2020-09-13T20:52:12+00:00'
        }

        from rdr_service.resource import main as resource_main

        self.send_post(
            local_path='IngestAW2ManifestTaskApi',
            request_data=data,
            prefix="/resource/task/",
            test_client=resource_main.app.test_client(),
        )

        call_json = ingest_aw1_mock.call_args[0][0]

        self.assertEqual(ingest_aw1_mock.called, True)
        self.assertEqual(call_json['bucket'], data['bucket_name'])
        self.assertEqual(call_json['job'], GenomicJob.METRICS_INGESTION)
        self.assertIsNotNone(call_json['file_data'])

    @mock.patch('rdr_service.offline.genomic_pipeline.execute_genomic_manifest_file_pipeline')
    def test_ingest_aw4_data_task_api(self, ingest_aw4_mock):
        aw4_wgs_file_path = "AW4_wgs_manifest/test_aw4_file.csv"
        aw4_array_file_path = "AW4_array_manifest/test_aw4_file.csv"

        data = {
            "file_path": aw4_array_file_path,
            "bucket_name": aw4_array_file_path.split('/')[0],
            "upload_date": '2020-09-13T20:52:12+00:00'
        }

        from rdr_service.resource import main as resource_main

        self.send_post(
            local_path='IngestAW4ManifestTaskApi',
            request_data=data,
            prefix="/resource/task/",
            test_client=resource_main.app.test_client(),
        )

        call_json = ingest_aw4_mock.call_args[0][0]

        self.assertEqual(ingest_aw4_mock.called, True)
        self.assertEqual(call_json['bucket'], data['bucket_name'])
        self.assertEqual(call_json['job'], GenomicJob.AW4_ARRAY_WORKFLOW)
        self.assertIsNotNone(call_json['file_data'])
        self.assertEqual(call_json['subfolder'], 'AW4_array_manifest')

        data = {
            "file_path": aw4_wgs_file_path,
            "bucket_name": aw4_wgs_file_path.split('/')[0],
            "upload_date": '2020-09-13T20:52:12+00:00'
        }

        self.send_post(
            local_path='IngestAW4ManifestTaskApi',
            request_data=data,
            prefix="/resource/task/",
            test_client=resource_main.app.test_client(),
        )

        call_json = ingest_aw4_mock.call_args[0][0]

        self.assertEqual(ingest_aw4_mock.called, True)
        self.assertEqual(call_json['bucket'], data['bucket_name'])
        self.assertEqual(call_json['job'], GenomicJob.AW4_WGS_WORKFLOW)
        self.assertIsNotNone(call_json['file_data'])
        self.assertEqual(call_json['subfolder'], 'AW4_wgs_manifest')

    @mock.patch('rdr_service.offline.genomic_pipeline.execute_genomic_manifest_file_pipeline')
    def test_ingest_aw5_data_task_api(self, ingest_aw5_mock):
        aw5_wgs_file_path = "AW5_wgs_manifest/test_aw5_file.csv"
        aw5_array_file_path = "AW5_array_manifest/test_aw5_file.csv"

        data = {
            "file_path": aw5_array_file_path,
            "bucket_name": aw5_array_file_path.split('/')[0],
            "upload_date": '2020-09-13T20:52:12+00:00'
        }

        from rdr_service.resource import main as resource_main

        self.send_post(
            local_path='IngestAW5ManifestTaskApi',
            request_data=data,
            prefix="/resource/task/",
            test_client=resource_main.app.test_client(),
        )

        call_json = ingest_aw5_mock.call_args[0][0]

        self.assertEqual(ingest_aw5_mock.called, True)
        self.assertEqual(call_json['bucket'], data['bucket_name'])
        self.assertEqual(call_json['job'], GenomicJob.AW5_ARRAY_MANIFEST)
        self.assertIsNotNone(call_json['file_data'])

        data = {
            "file_path": aw5_wgs_file_path,
            "bucket_name": aw5_wgs_file_path.split('/')[0],
            "upload_date": '2020-09-13T20:52:12+00:00'
        }

        self.send_post(
            local_path='IngestAW5ManifestTaskApi',
            request_data=data,
            prefix="/resource/task/",
            test_client=resource_main.app.test_client(),
        )

        call_json = ingest_aw5_mock.call_args[0][0]

        self.assertEqual(ingest_aw5_mock.called, True)
        self.assertEqual(call_json['bucket'], data['bucket_name'])
        self.assertEqual(call_json['job'], GenomicJob.AW5_WGS_MANIFEST)
        self.assertIsNotNone(call_json['file_data'])

    @mock.patch('rdr_service.offline.genomic_pipeline.execute_genomic_manifest_file_pipeline')
    def test_ingest_cvl_job_task_api(self, ingest_mock):

        from rdr_service.resource import main as resource_main

        cvl_map = {
            'w2sc': {
                'job': GenomicJob.CVL_W2SC_WORKFLOW,
                'manifest_type': GenomicManifestTypes.CVL_W2SC
            },
            'w3ns': {
                'job': GenomicJob.CVL_W3NS_WORKFLOW,
                'manifest_type': GenomicManifestTypes.CVL_W3NS
            },
            'w3sc': {
                'job': GenomicJob.CVL_W3SC_WORKFLOW,
                'manifest_type': GenomicManifestTypes.CVL_W3SC
            },
            'w3ss': {
                'job': GenomicJob.CVL_W3SS_WORKFLOW,
                'manifest_type': GenomicManifestTypes.CVL_W3SS
            },
            'w4wr': {
                'job': GenomicJob.CVL_W4WR_WORKFLOW,
                'manifest_type': GenomicManifestTypes.CVL_W4WR
            },
            'w5nf': {
                'job': GenomicJob.CVL_W5NF_WORKFLOW,
                'manifest_type': GenomicManifestTypes.CVL_W5NF
            }
        }

        test_bucket = 'test_cvl_bucket'

        for cvl_key, cvl_data in cvl_map.items():

            cvl_type_file_path = f"{test_bucket}/test_cvl_{cvl_key}_file.csv"

            data = {
                "file_path": cvl_type_file_path,
                "bucket_name": cvl_type_file_path.split('/')[0],
                "upload_date": '2020-09-13T20:52:12+00:00',
                "file_type": cvl_key
            }

            self.send_post(
                local_path='IngestCVLManifestTaskApi',
                request_data=data,
                prefix="/resource/task/",
                test_client=resource_main.app.test_client(),
            )

            call_json = ingest_mock.call_args[0][0]

            self.assertEqual(ingest_mock.called, True)
            self.assertEqual(call_json['bucket'], data['bucket_name'])
            self.assertEqual(call_json['job'], cvl_data['job'])
            self.assertIsNotNone(call_json['file_data'])
            self.assertEqual(
                call_json['file_data']['manifest_type'],
                cvl_data['manifest_type']
            )

    def test_ingest_data_files_task_api(self):

        data = {
            "file_path": "test_file_path",
            "bucket_name": "test_bucket",
        }

        from rdr_service.resource import main as resource_main

        insert_files_result = self.send_post(
            local_path='IngestDataFilesTaskApi',
            request_data=data,
            prefix="/resource/task/",
            test_client=resource_main.app.test_client(),
        )

        self.assertIsNotNone(insert_files_result)
        self.assertEqual(insert_files_result['success'], True)

    @mock.patch('rdr_service.offline.genomic_pipeline.execute_genomic_manifest_file_pipeline')
    def test_create_cloud_record(self, ingest_mock):

        base_payload = {
            "@type": "type.googleapis.com/google.pubsub.v1.PubsubMessage",
            "attributes": {
                "bucketId": "aou-rdr-sandbox-mock-data",
                "eventTime": "2021-04-19T16:02:41.919922Z",
                "eventType": "OBJECT_FINALIZE",
                "notificationConfig": "projects/_/buckets/aou-rdr-sandbox-mock-data/notificationConfigs/34",
                "objectGeneration": "1618848161894414",
                "objectId": "AW1_genotyping_sample_manifests/RDR_AoU_GEN_PKG-1908-218054.csv",
                "overwroteGeneration": "1618605912794149",
                "payloadFormat": "JSON_API_V1"
            }
        }

        mappings = {
            "aw1": {
                'route': 'IngestAW1ManifestTaskApi',
                'file_path': 'AW1_sample_manifests/test_aw1_file.csv'
            },
            "aw2": {
                'route': 'IngestAW2ManifestTaskApi',
                'file_path': 'AW2_data_manifests/test_aw2_file.csv'
            },
            "aw4": {
                'route': 'IngestAW4ManifestTaskApi',
                'file_path': 'AW4_array_manifest/test_aw4_file.csv"'
            },
            "aw5": {
                'route': 'IngestAW5ManifestTaskApi',
                'file_path': 'AW5_array_manifest/test_aw5_file.csv'
            },
        }

        from rdr_service.resource import main as resource_main

        current_id = 0
        for key, value in mappings.items():
            current_id += 1
            data = {
                "file_type": key,
                "filename": 'test_file_name',
                "file_path": f'{base_payload["attributes"]["bucketId"]}/{value["file_path"]}',
                "bucket_name": base_payload['attributes']['bucketId'],
                "topic": "genomic_manifest_upload",
                "event_payload": base_payload,
                "upload_date": '2020-09-13T20:52:12+00:00',
                "task": f'{key}_manifest',
                "api_route": f'/resource/task/{value["route"]}',
                "cloud_function": True,
            }

            self.send_post(
                local_path=value["route"],
                request_data=data,
                prefix="/resource/task/",
                test_client=resource_main.app.test_client(),
            )

            self.assertEqual(ingest_mock.called, True)

            cloud_record = self.cloud_req_dao.get(current_id)

            self.assertEqual(cloud_record.api_route, data['api_route'])
            self.assertIsNotNone(cloud_record.event_payload)
            self.assertEqual(cloud_record.bucket_name, data['bucket_name'])
            self.assertEqual(cloud_record.topic, data['topic'])
            self.assertEqual(cloud_record.task, data['task'])
            self.assertEqual(cloud_record.file_path, data['file_path'])

        self.assertEqual(len(mappings), len(self.cloud_req_dao.get_all()))

    @mock.patch('rdr_service.offline.genomic_pipeline.execute_genomic_manifest_file_pipeline')
    def test_batching_manifest_task_api(self, ingest_mock):
        mappings = {
            "aw1": {
                'route': 'IngestAW1ManifestTaskApi',
                'file_path': ['AW1_sample_manifests/test_aw1_file_1.csv',
                              'AW1_sample_manifests/test_aw1_file_2.csv',
                              'AW1_sample_manifests/test_aw1_file_3.csv',
                              'AW1_sample_manifests/test_aw1_file_4.csv',
                              'AW1_sample_manifests/test_aw1_file_5.csv']
            },
            "aw2": {
                'route': 'IngestAW2ManifestTaskApi',
                'file_path': 'AW2_data_manifests/test_aw2_file.csv'
            },
            "aw4": {
                'route': 'IngestAW4ManifestTaskApi',
                'file_path': ['AW4_array_manifest/test_aw4_file.csv',
                              'AW4_array_manifest/test_aw4_file.csv'
                              'AW4_array_manifest/test_aw4_file.csv'
                              'AW4_array_manifest/test_aw4_file.csv',
                              'AW4_array_manifest/test_aw4_file.csv']
            },
            "aw5": {
                'route': 'IngestAW5ManifestTaskApi',
                'file_path': 'AW5_array_manifest/test_aw5_file.csv'
            },
        }

        from rdr_service.resource import main as resource_main

        path_count = 0
        for value in mappings.values():
            path_count = path_count + (len(value['file_path']) if type(value['file_path']) is list else 1)

            data = {
                "file_path": value['file_path'],
                "bucket_name": value['file_path'].split('/')[0]
                if type(value['file_path']) is not list
                else value['file_path'][0].split('/')[0],
                "upload_date": '2020-09-13T20:52:12+00:00'
            }

            self.send_post(
                local_path=value["route"],
                request_data=data,
                prefix="/resource/task/",
                test_client=resource_main.app.test_client(),
            )

            self.assertEqual(ingest_mock.called, True)

        self.assertEqual(ingest_mock.call_count, path_count)

    @mock.patch('rdr_service.genomic.genomic_job_controller.GenomicJobController'
                '.ingest_records_from_message_broker_data')
    def test_ingest_message_broker_ingest_data_api(self, ingest_called):

        from rdr_service.resource import main as resource_main

        data = {
            'message_record_id': [],
            'event_type': ''
        }

        bad_data_post = self.send_post(
            local_path='IngestGenomicMessageBrokerDataApi',
            request_data=data,
            prefix="/resource/task/",
            test_client=resource_main.app.test_client(),
        )

        self.assertIsNotNone(bad_data_post)
        self.assertEqual(bad_data_post['success'], False)
        self.assertEqual(ingest_called.call_count, 0)

        data = {
            'message_record_id': 2,
            'event_type': 'informing_loop_decision'
        }

        informing_loop_post = self.send_post(
            local_path='IngestGenomicMessageBrokerDataApi',
            request_data=data,
            prefix="/resource/task/",
            test_client=resource_main.app.test_client(),
        )

        self.assertIsNotNone(informing_loop_post)
        self.assertEqual(informing_loop_post['success'], True)
        self.assertEqual(ingest_called.call_count, 1)

        data = {
            'message_record_id': 2,
            'event_type': 'informing_loop_started'
        }

        informing_loop_post_two = self.send_post(
            local_path='IngestGenomicMessageBrokerDataApi',
            request_data=data,
            prefix="/resource/task/",
            test_client=resource_main.app.test_client(),
        )

        self.assertIsNotNone(informing_loop_post_two)
        self.assertEqual(informing_loop_post_two['success'], True)
        self.assertEqual(ingest_called.call_count, 2)

        data = {
            'message_record_id': 2,
            'event_type': 'result_viewed'
        }

        result_viewed_post = self.send_post(
            local_path='IngestGenomicMessageBrokerDataApi',
            request_data=data,
            prefix="/resource/task/",
            test_client=resource_main.app.test_client(),
        )

        self.assertIsNotNone(result_viewed_post)
        self.assertEqual(result_viewed_post['success'], True)
        self.assertEqual(ingest_called.call_count, 3)

        data = {
            'message_record_id': 2,
            'event_type': 'result_ready'
        }

        result_viewed_post = self.send_post(
            local_path='IngestGenomicMessageBrokerDataApi',
            request_data=data,
            prefix="/resource/task/",
            test_client=resource_main.app.test_client(),
        )

        self.assertIsNotNone(result_viewed_post)
        self.assertEqual(result_viewed_post['success'], True)
        self.assertEqual(ingest_called.call_count, 4)

    @mock.patch('rdr_service.genomic.genomic_job_controller.GenomicJobController'
                '.ingest_records_from_message_broker_data')
    def test_ingest_message_broker_ingest_appointment_api(self, ingest_called):

        from rdr_service.resource import main as resource_main

        data = {
            'message_record_id': [],
        }

        bad_data_post = self.send_post(
            local_path='IngestGenomicMessageBrokerAppointmentApi',
            request_data=data,
            prefix="/resource/task/",
            test_client=resource_main.app.test_client(),
        )

        self.assertIsNotNone(bad_data_post)
        self.assertEqual(bad_data_post['success'], False)
        self.assertEqual(ingest_called.call_count, 0)

        data = {
            'message_record_id': 2,
        }

        appointment_post = self.send_post(
            local_path='IngestGenomicMessageBrokerAppointmentApi',
            request_data=data,
            prefix="/resource/task/",
            test_client=resource_main.app.test_client(),
        )

        self.assertIsNotNone(appointment_post)
        self.assertEqual(appointment_post['success'], True)
        self.assertEqual(ingest_called.call_count, 1)

    def test_batch_data_file_task_api(self):

        test_bucket_baylor = "fake-data-bucket-baylor"
        test_idat_file = "fake-data-bucket-baylor/Genotyping_sample_raw_data/204027270091_R02C01_Grn.idat"
        test_vcf_file = "fake-data-bucket-baylor/Genotyping_sample_raw_data/204027270091_R02C01.vcf.gz"
        test_cram_file = "fake-data-bucket-baylor/Wgs_sample_raw_data/" \
                         "CRAMs_CRAIs/BCM_A100134256_21063006771_SIA0017196_1.cram"

        test_file_paths = [test_idat_file, test_vcf_file, test_cram_file]

        data = {
            "file_path": test_file_paths,
            "bucket_name": test_bucket_baylor
        }

        from rdr_service.resource import main as resource_main

        self.send_post(
            local_path='IngestDataFilesTaskApi',
            request_data=data,
            prefix="/resource/task/",
            test_client=resource_main.app.test_client(),
        )

        inserted_files = self.data_file_dao.get_all()

        self.assertEqual(len(inserted_files), len(test_file_paths))
        self.assertTrue(all([file for file in inserted_files if file.bucket_name == test_bucket_baylor]))
        self.assertTrue(all([file for file in inserted_files if file.file_path in test_file_paths]))

    @mock.patch('rdr_service.dao.genomics_dao.GenomicSetMemberDao.batch_update_member_field')
    def test_set_member_update_api(self, update_mock):

        from rdr_service.resource import main as resource_main

        data = {
            'member_ids': [],
        }

        update_member = self.send_post(
            local_path='GenomicSetMemberUpdateApi',
            request_data=data,
            prefix="/resource/task/",
            test_client=resource_main.app.test_client(),
        )

        self.assertFalse(update_member['success'])

        data['member_ids'] = [1, 2, 3]

        update_member = self.send_post(
            local_path='GenomicSetMemberUpdateApi',
            request_data=data,
            prefix="/resource/task/",
            test_client=resource_main.app.test_client(),
        )

        self.assertFalse(update_member['success'])

        data['field'] = 'testFieldKey'
        data['value'] = 1

        update_member = self.send_post(
            local_path='GenomicSetMemberUpdateApi',
            request_data=data,
            prefix="/resource/task/",
            test_client=resource_main.app.test_client(),
        )

        self.assertTrue(update_member['success'])
        self.assertTrue(update_mock.called)

    @mock.patch('rdr_service.offline.genomic_pipeline.execute_genomic_manifest_file_pipeline')
    def test_manifest_execute_in_manifest_ingestions(self, pipeline_mock):

        from rdr_service.resource import main as resource_main

        path_mappings = {
            "aw1": {
                'route': 'IngestAW1ManifestTaskApi',
                'file_path': ['AW1_sample_manifests/test_aw1_file_1.csv',
                              'AW1_sample_manifests/test_aw1_file_2.csv',
                              'AW1_sample_manifests/test_aw1_file_3.csv',
                              'AW1_sample_manifests/test_aw1_file_4.csv',
                              'AW1_sample_manifests/test_aw1_file_5.csv']
            },
            "aw2": {
                'route': 'IngestAW2ManifestTaskApi',
                'file_path': 'AW2_data_manifests/test_aw2_file.csv'
            },
            "aw4_array": {
                'route': 'IngestAW4ManifestTaskApi',
                'file_path': ['AW4_array_manifest/test_aw4_file.csv',
                              'AW4_array_manifest/test_aw4_file.csv'
                              'AW4_array_manifest/test_aw4_file.csv'
                              'AW4_array_manifest/test_aw4_file.csv',
                              'AW4_array_manifest/test_aw4_file.csv']
            },
            "aw4_wgs": {
                'route': 'IngestAW4ManifestTaskApi',
                'file_path': 'AW4_wgs_manifest/test_aw4_file.csv',
            },
            "aw5_array": {
                'route': 'IngestAW5ManifestTaskApi',
                'file_path': 'AW5_array_manifest/test_aw5_file.csv'
            },
            "aw5_wgs": {
                'route': 'IngestAW5ManifestTaskApi',
                'file_path': 'AW5_wgs_manifest/test_aw5_file.csv'
            },
        }

        path_count = 0
        for value in path_mappings.values():

            path_count = path_count + (len(value['file_path']) if type(value['file_path']) is list else 1)

            self.send_post(
                local_path=value.get('route'),
                request_data={
                    'file_path': value.get('file_path'),
                    'bucket_name': 'test_bucket_name',
                    'upload_date': '2020-09-13T20:52:12+00:00',
                },
                prefix="/resource/task/",
                test_client=resource_main.app.test_client(),
            )

        # from base_config all True => call count == all path count
        self.assertEqual(pipeline_mock.call_count, path_count)

        manifest_config = {
            "aw1_manifest": 0,
            "aw2_manifest": 0,
            "aw4_array_manifest": 0,
            "aw4_wgs_manifest": 1,
            "aw5_array_manifest": 1,
            "aw5_wgs_manifest": 1
        }

        config.override_setting(config.GENOMIC_INGESTIONS, manifest_config)

        # add 3 calls tp path_count
        true_calls = [val for val in manifest_config.values() if val == 1]

        for value in path_mappings.values():
            self.send_post(
                local_path=value.get('route'),
                request_data={
                    'file_path': value.get('file_path'),
                    'bucket_name': 'test_bucket_name',
                    'upload_date': '2020-09-13T20:52:12+00:00',
                },
                prefix="/resource/task/",
                test_client=resource_main.app.test_client(),
            )

        self.assertEqual(
            pipeline_mock.call_count,
            path_count + len(true_calls)
        )

    @mock.patch('rdr_service.genomic.genomic_job_controller.GenomicJobController.ingest_metrics_file')
    def test_ingest_user_metrics_api(self, ingest_mock):

        from rdr_service.resource import main as resource_main

        data = {}

        user_metrics = self.send_post(
            local_path='IngestUserEventMetricsApi',
            request_data=data,
            prefix="/resource/task/",
            test_client=resource_main.app.test_client(),
        )

        self.assertIsNotNone(user_metrics)
        self.assertEqual(user_metrics['success'], False)
        self.assertEqual(ingest_mock.call_count, 0)

        data = {
            'file_path': 'test_file_path'
        }

        user_metrics = self.send_post(
            local_path='IngestUserEventMetricsApi',
            request_data=data,
            prefix="/resource/task/",
            test_client=resource_main.app.test_client(),
        )

        self.assertIsNotNone(user_metrics)
        self.assertEqual(user_metrics['success'], True)
        self.assertEqual(ingest_mock.call_count, 1)

    @mock.patch('rdr_service.genomic.genomic_job_controller.GenomicJobController.ingest_appointment_metrics_file')
    def test_ingest_appointment_metrics_api(self, ingest_mock):

        from rdr_service.resource import main as resource_main

        data = {}

        appointment_metrics = self.send_post(
            local_path='IngestAppointmentMetricsApi',
            request_data=data,
            prefix="/resource/task/",
            test_client=resource_main.app.test_client(),
        )

        self.assertIsNotNone(appointment_metrics)
        self.assertEqual(appointment_metrics['success'], False)
        self.assertEqual(ingest_mock.call_count, 0)

        data = {
            'file_path': 'test_file_path'
        }

        appointment_metrics = self.send_post(
            local_path='IngestAppointmentMetricsApi',
            request_data=data,
            prefix="/resource/task/",
            test_client=resource_main.app.test_client(),
        )

        self.assertIsNotNone(appointment_metrics)
        self.assertEqual(appointment_metrics['success'], True)
        self.assertEqual(ingest_mock.call_count, 1)

    @mock.patch('rdr_service.api.genomic_cloud_tasks_api.bq_genomic_set_member_batch_update')
    @mock.patch('rdr_service.api.genomic_cloud_tasks_api.genomic_set_member_batch_update')
    def test_genomic_rebuild_task_api(self, bq_batch_mock, batch_mock):

        from rdr_service.resource import main as resource_main

        gen_set = self.data_generator.create_database_genomic_set(
            genomicSetName=".",
            genomicSetCriteria=".",
            genomicSetVersion=1
        )

        self.data_generator.create_database_genomic_set_member(
            genomicSetId=gen_set.id,
            biobankId="100153482",
            sampleId="21042005280",
            genomeType="aou_array",
            genomicWorkflowState=GenomicWorkflowState.AW0
        )

        data = {}
        call_ids = [1]

        rebuild_task = self.send_post(
            local_path='RebuildGenomicTableRecordsApi',
            request_data=data,
            prefix="/resource/task/",
            test_client=resource_main.app.test_client(),
        )

        self.assertIsNotNone(rebuild_task)
        self.assertEqual(rebuild_task['success'], False)
        self.assertEqual(bq_batch_mock.call_count, 0)
        self.assertEqual(batch_mock.call_count, 0)

        data = {
            'table': 'bad_table',
            'ids': call_ids
        }

        rebuild_task = self.send_post(
            local_path='RebuildGenomicTableRecordsApi',
            request_data=data,
            prefix="/resource/task/",
            test_client=resource_main.app.test_client(),
        )

        self.assertIsNotNone(rebuild_task)
        self.assertEqual(rebuild_task['success'], False)
        self.assertEqual(bq_batch_mock.call_count, 0)
        self.assertEqual(batch_mock.call_count, 0)

        data = {
            'table': 'genomic_set_member',
            'ids': call_ids
        }

        rebuild_task = self.send_post(
            local_path='RebuildGenomicTableRecordsApi',
            request_data=data,
            prefix="/resource/task/",
            test_client=resource_main.app.test_client(),
        )

        self.assertIsNotNone(rebuild_task)
        self.assertEqual(rebuild_task['success'], True)
        self.assertEqual(bq_batch_mock.call_count, 1)
        self.assertEqual(batch_mock.call_count, 1)

