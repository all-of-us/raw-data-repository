import datetime

import mock

from tests.helpers.unittest_base import BaseTestCase
from rdr_service import clock
from rdr_service.dao.participant_dao import ParticipantDao
from rdr_service.dao.participant_summary_dao import ParticipantSummaryDao
from rdr_service.dao.genomics_dao import (
    GenomicSetDao,
    GenomicSetMemberDao,
    GenomicJobRunDao,
    GenomicGCValidationMetricsDao,
)
from rdr_service.model.participant_summary import ParticipantSummary
from rdr_service.model.participant import Participant

from rdr_service.model.genomics import (
    GenomicSet,
    GenomicSetMember,
    GenomicJobRun,
    GenomicGCValidationMetrics,
)

from rdr_service.participant_enums import (
    GenomicSubProcessResult,
    GenomicJob,
    SampleStatus)


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
        summary = self._participant_summary_with_defaults(**kwargs)
        self.ps_dao.insert(summary)
        return summary

    def _make_set_member(self, participant):
        new_member = GenomicSetMember(genomicSetId=1,
                                      participantId=participant.participantId,
                                      gemPass='Y',
                                      biobankId=participant.biobankId)
        return self.member_dao.insert(new_member)


class GemApiTest(GenomicApiTestBase):
    def setUp(self):
        super(GemApiTest, self).setUp()

    def test_get_pii_valid_pid(self):
        p1_pii = self.send_get("GemPII/P1")
        self.assertEqual(p1_pii['biobank_id'], 1)
        self.assertEqual(p1_pii['first_name'], 'TestFN')
        self.assertEqual(p1_pii['last_name'], 'TestLN')
