import datetime

import mock

from rdr_service import clock
from rdr_service.dao.genomics_dao import GenomicSetDao, GenomicSetMemberDao
from rdr_service.dao.participant_dao import ParticipantDao
from rdr_service.dao.participant_summary_dao import ParticipantSummaryDao
from rdr_service.genomic.validation import validate_and_update_genomic_set_by_id
from rdr_service.model.genomics import (
    GenomicSet,
    GenomicSetMember,
)
from rdr_service.model.participant import Participant
from rdr_service.participant_enums import SampleStatus, WithdrawalStatus
from rdr_service.genomic_enums import GenomicSetStatus, GenomicSetMemberStatus, GenomicValidationFlag
from tests.helpers.unittest_base import BaseTestCase


class GenomicSetValidationBaseTestCase(BaseTestCase):
    def setUp(self):
        super(GenomicSetValidationBaseTestCase, self).setUp()
        self.participant_dao = ParticipantDao()
        self.summary_dao = ParticipantSummaryDao()
        self.genomic_set_dao = GenomicSetDao()
        self.genomic_member_dao = GenomicSetMemberDao()
        self._participant_i = 0
        self.setup_data()

    def setup_data(self):
        pass

    def make_participant(self, **kwargs):
        """
    Make a participant with custom settings.
    default should create a valid participant.
    """
        i = self._participant_i
        self._participant_i += 1
        participant = Participant(participantId=i, biobankId=i, **kwargs)
        self.participant_dao.insert(participant)
        return participant

    def make_summary(self, participant, **override_kwargs):
        """
    Make a summary with custom settings.
    default should create a valid summary.
    """
        valid_kwargs = dict(
            participantId=participant.participantId,
            biobankId=participant.biobankId,
            withdrawalStatus=participant.withdrawalStatus,
            dateOfBirth=datetime.datetime(2000, 1, 1),
            firstName="foo",
            lastName="bar",
            zipCode="12345",
            sampleStatus1ED04=SampleStatus.RECEIVED,
            sampleStatus1SAL2=SampleStatus.RECEIVED,
            samplesToIsolateDNA=SampleStatus.RECEIVED,
            consentForStudyEnrollmentTime=datetime.datetime(2019, 1, 1),
            participantOrigin='example'
        )
        kwargs = dict(valid_kwargs, **override_kwargs)
        summary = self.data_generator._participant_summary_with_defaults(**kwargs)
        self.summary_dao.insert(summary)
        return summary

    def make_genomic_set(self, **override_kwargs):
        """
    Make a genomic set with custom settings.
    default should create a valid set.
    """
        valid_kwargs = dict(
            genomicSetName="foo",
            genomicSetCriteria="something",
            genomicSetVersion=1,
            genomicSetStatus=GenomicSetStatus.UNSET,
        )
        kwargs = dict(valid_kwargs, **override_kwargs)
        genomic_set = GenomicSet(**kwargs)
        self.genomic_set_dao.insert(genomic_set)
        return genomic_set

    def make_genomic_member(self, genomic_set, participant, **override_kwargs):
        """
    Make a genomic member with custom settings.
    default should create a valid member.
    """
        valid_kwargs = dict(
            genomicSetId=genomic_set.id,
            participantId=participant.participantId,
            sexAtBirth="F",
            biobankId=participant.biobankId,
        )
        kwargs = dict(valid_kwargs, **override_kwargs)
        member = GenomicSetMember(**kwargs)
        self.genomic_member_dao.insert(member)
        return member


# TODO: represent in new test suite
class GenomicSetMemberValidationTestCase(GenomicSetValidationBaseTestCase):
    def test_test_defaults_are_valid(self):
        participant = self.make_participant()
        self.make_summary(participant)
        genomic_set = self.make_genomic_set()
        member = self.make_genomic_member(genomic_set, participant)
        validate_and_update_genomic_set_by_id(genomic_set.id)
        current_member = self.genomic_member_dao.get(member.id)
        self.assertEqual(current_member.validationStatus, GenomicSetMemberStatus.VALID)
        current_set = self.genomic_set_dao.get(genomic_set.id)
        self.assertEqual(current_set.genomicSetStatus, GenomicSetStatus.VALID)

    def test_duplicate(self):
        participant = self.make_participant()
        self.make_summary(participant)
        genomic_set_a = self.make_genomic_set(genomicSetName="A", genomicSetStatus=GenomicSetStatus.VALID)
        self.make_genomic_member(genomic_set_a, participant)
        genomic_set_b = self.make_genomic_set(genomicSetName="B")
        member_b = self.make_genomic_member(genomic_set_b, participant)
        validate_and_update_genomic_set_by_id(genomic_set_b.id)
        current_member = self.genomic_member_dao.get(member_b.id)
        self.assertEqual(current_member.validationStatus, GenomicSetMemberStatus.VALID)
        current_set = self.genomic_set_dao.get(genomic_set_b.id)
        self.assertEqual(current_set.genomicSetStatus, GenomicSetStatus.VALID)

    def test_consent(self):
        participant = self.make_participant()
        self.make_summary(participant, consentForStudyEnrollmentTime=datetime.datetime(2017, 1, 1))
        genomic_set = self.make_genomic_set()
        member = self.make_genomic_member(genomic_set, participant)
        validate_and_update_genomic_set_by_id(genomic_set.id)
        current_member = self.genomic_member_dao.get(member.id)
        self.assertEqual(current_member.validationStatus, GenomicSetMemberStatus.INVALID)
        self.assertIn(GenomicValidationFlag.INVALID_CONSENT, current_member.validationFlags)
        current_set = self.genomic_set_dao.get(genomic_set.id)
        self.assertEqual(current_set.genomicSetStatus, GenomicSetStatus.INVALID)

    def test_consent_null(self):
        participant = self.make_participant()
        self.make_summary(participant, consentForStudyEnrollmentTime=None)
        genomic_set = self.make_genomic_set()
        member = self.make_genomic_member(genomic_set, participant)
        validate_and_update_genomic_set_by_id(genomic_set.id)
        current_member = self.genomic_member_dao.get(member.id)
        self.assertEqual(current_member.validationStatus, GenomicSetMemberStatus.INVALID)
        self.assertIn(GenomicValidationFlag.INVALID_CONSENT, current_member.validationFlags)
        current_set = self.genomic_set_dao.get(genomic_set.id)
        self.assertEqual(current_set.genomicSetStatus, GenomicSetStatus.INVALID)

    def test_withdrawn(self):
        participant = self.make_participant(withdrawalStatus=WithdrawalStatus.NO_USE)
        self.make_summary(participant)
        genomic_set = self.make_genomic_set()
        member = self.make_genomic_member(genomic_set, participant)
        validate_and_update_genomic_set_by_id(genomic_set.id)
        current_member = self.genomic_member_dao.get(member.id)
        self.assertEqual(current_member.validationStatus, GenomicSetMemberStatus.INVALID)
        self.assertIn(GenomicValidationFlag.INVALID_WITHDRAW_STATUS, current_member.validationFlags)
        current_set = self.genomic_set_dao.get(genomic_set.id)
        self.assertEqual(current_set.genomicSetStatus, GenomicSetStatus.INVALID)

    def test_sexatbirth(self):
        participant = self.make_participant()
        self.make_summary(participant)
        genomic_set = self.make_genomic_set()
        member = self.make_genomic_member(genomic_set, participant, sexAtBirth="foo")
        validate_and_update_genomic_set_by_id(genomic_set.id)
        current_member = self.genomic_member_dao.get(member.id)
        self.assertEqual(current_member.validationStatus, GenomicSetMemberStatus.INVALID)
        self.assertIn(GenomicValidationFlag.INVALID_SEX_AT_BIRTH, current_member.validationFlags)
        current_set = self.genomic_set_dao.get(genomic_set.id)
        self.assertEqual(current_set.genomicSetStatus, GenomicSetStatus.INVALID)

    def test_age(self):
        now = datetime.datetime(2019, 1, 1)
        valid_date_of_birth = datetime.datetime(now.year - 18, now.month, now.day)
        invalid_date_of_birth = datetime.datetime(now.year - 17, now.month, now.day)
        participant_a = self.make_participant()
        self.make_summary(participant_a, dateOfBirth=valid_date_of_birth)
        participant_b = self.make_participant()
        self.make_summary(participant_b, dateOfBirth=invalid_date_of_birth)
        genomic_set = self.make_genomic_set()
        member_a = self.make_genomic_member(genomic_set, participant_a)
        member_b = self.make_genomic_member(genomic_set, participant_b)
        with clock.FakeClock(datetime.datetime(2019, 1, 1)):
            validate_and_update_genomic_set_by_id(genomic_set.id)
        current_member_a = self.genomic_member_dao.get(member_a.id)
        current_member_b = self.genomic_member_dao.get(member_b.id)
        self.assertEqual(current_member_a.validationStatus, GenomicSetMemberStatus.VALID)
        self.assertEqual(current_member_b.validationStatus, GenomicSetMemberStatus.INVALID)
        self.assertIn(GenomicValidationFlag.INVALID_AGE, current_member_b.validationFlags)
        current_set = self.genomic_set_dao.get(genomic_set.id)
        self.assertEqual(current_set.genomicSetStatus, GenomicSetStatus.INVALID)

    def test_biobank_status(self):
        def make_member(genomic_set, **summary_kwargs):
            participant = self.make_participant()
            self.make_summary(participant, **summary_kwargs)
            return self.make_genomic_member(genomic_set, participant)

        kwargs_with_expected_status_and_flags = [
            (
                {
                    "sampleStatus1ED04": SampleStatus.UNSET,
                    "sampleStatus1SAL2": SampleStatus.UNSET,
                    "samplesToIsolateDNA": SampleStatus.UNSET,
                },
                GenomicSetMemberStatus.INVALID,
                [GenomicValidationFlag.INVALID_BIOBANK_ORDER],
            ),
            (
                {
                    "sampleStatus1ED04": SampleStatus.RECEIVED,
                    "sampleStatus1SAL2": SampleStatus.UNSET,
                    "samplesToIsolateDNA": SampleStatus.UNSET,
                },
                GenomicSetMemberStatus.INVALID,
                [GenomicValidationFlag.INVALID_BIOBANK_ORDER],
            ),
            (
                {
                    "sampleStatus1ED04": SampleStatus.UNSET,
                    "sampleStatus1SAL2": SampleStatus.RECEIVED,
                    "samplesToIsolateDNA": SampleStatus.UNSET,
                },
                GenomicSetMemberStatus.INVALID,
                [GenomicValidationFlag.INVALID_BIOBANK_ORDER],
            ),
            (
                {
                    "sampleStatus1ED04": SampleStatus.UNSET,
                    "sampleStatus1SAL2": SampleStatus.UNSET,
                    "samplesToIsolateDNA": SampleStatus.RECEIVED,
                },
                GenomicSetMemberStatus.INVALID,
                [GenomicValidationFlag.INVALID_BIOBANK_ORDER],
            ),
            (
                {
                    "sampleStatus1ED04": SampleStatus.RECEIVED,
                    "sampleStatus1SAL2": SampleStatus.UNSET,
                    "samplesToIsolateDNA": SampleStatus.RECEIVED,
                },
                GenomicSetMemberStatus.VALID,
                [],
            ),
            (
                {
                    "sampleStatus1ED04": SampleStatus.UNSET,
                    "sampleStatus1SAL2": SampleStatus.RECEIVED,
                    "samplesToIsolateDNA": SampleStatus.RECEIVED,
                },
                GenomicSetMemberStatus.VALID,
                [],
            ),
        ]

        genomic_set = self.make_genomic_set()
        runs = [
            (make_member(genomic_set, **kwargs), kwargs, status, flags)
            for kwargs, status, flags in kwargs_with_expected_status_and_flags
        ]

        validate_and_update_genomic_set_by_id(genomic_set.id)
        for member, kwargs, expected_status, expected_flags in runs:
            current_member = self.genomic_member_dao.get(member.id)
            self.assertEqual(current_member.validationStatus, expected_status)
            for flag in expected_flags:
                self.assertIn(flag, current_member.validationFlags)
        current_set = self.genomic_set_dao.get(genomic_set.id)
        self.assertEqual(current_set.genomicSetStatus, GenomicSetStatus.INVALID)

    def test_ny_zip_code(self):
        participant_a = self.make_participant()
        self.make_summary(participant_a, zipCode=None)
        participant_b = self.make_participant()
        self.make_summary(participant_b, zipCode="")
        participant_c = self.make_participant()
        self.make_summary(participant_c, zipCode="12345")
        genomic_set = self.make_genomic_set()
        member_a = self.make_genomic_member(genomic_set, participant_a)
        member_b = self.make_genomic_member(genomic_set, participant_b)
        member_c = self.make_genomic_member(genomic_set, participant_c)
        with clock.FakeClock(datetime.datetime(2019, 1, 1)):
            validate_and_update_genomic_set_by_id(genomic_set.id)
        current_member_a = self.genomic_member_dao.get(member_a.id)
        current_member_b = self.genomic_member_dao.get(member_b.id)
        current_member_c = self.genomic_member_dao.get(member_c.id)
        self.assertEqual(current_member_a.validationStatus, GenomicSetMemberStatus.INVALID)
        self.assertIn(GenomicValidationFlag.INVALID_NY_ZIPCODE, current_member_a.validationFlags)
        self.assertEqual(current_member_b.validationStatus, GenomicSetMemberStatus.INVALID)
        self.assertIn(GenomicValidationFlag.INVALID_NY_ZIPCODE, current_member_a.validationFlags)
        self.assertEqual(current_member_c.validationStatus, GenomicSetMemberStatus.VALID)
        current_set = self.genomic_set_dao.get(genomic_set.id)
        self.assertEqual(current_set.genomicSetStatus, GenomicSetStatus.INVALID)


class GenomicSetValidationSafetyTestCase(GenomicSetValidationBaseTestCase):
    def test_transaction(self):
        participant = self.make_participant()
        self.make_summary(participant)
        genomic_set = self.make_genomic_set()
        member = self.make_genomic_member(genomic_set, participant)
        with mock.patch("rdr_service.genomic.validation.GenomicSetDao.update_with_session") as mocked_set_update:
            mocked_set_update.side_effect = Exception("baz")
            with clock.FakeClock(datetime.datetime(2019, 1, 1)):
                with self.assertRaises(Exception):
                    validate_and_update_genomic_set_by_id(genomic_set.id)
        current_member = self.genomic_member_dao.get(member.id)
        self.assertEqual(current_member.validationStatus, None)
        current_set = self.genomic_set_dao.get(genomic_set.id)
        self.assertEqual(current_set.genomicSetStatus, None)

    def test_invalid_does_not_update_validated_time(self):
        participant = self.make_participant(withdrawalStatus=WithdrawalStatus.NO_USE)
        self.make_summary(participant)
        genomic_set = self.make_genomic_set()
        member = self.make_genomic_member(genomic_set, participant)
        validate_and_update_genomic_set_by_id(genomic_set.id)
        current_member = self.genomic_member_dao.get(member.id)
        self.assertEqual(current_member.validatedTime, None)
        current_set = self.genomic_set_dao.get(genomic_set.id)
        self.assertEqual(current_set.validatedTime, None)

    def test_valid_does_update_validated_time(self):
        participant = self.make_participant()
        self.make_summary(participant)
        genomic_set = self.make_genomic_set()
        member = self.make_genomic_member(genomic_set, participant)
        now = datetime.datetime(2019, 1, 1)
        with clock.FakeClock(now):
            validate_and_update_genomic_set_by_id(genomic_set.id)
        current_member = self.genomic_member_dao.get(member.id)
        self.assertEqual(current_member.validatedTime, now)
        current_set = self.genomic_set_dao.get(genomic_set.id)
        self.assertEqual(current_set.validatedTime, now)
