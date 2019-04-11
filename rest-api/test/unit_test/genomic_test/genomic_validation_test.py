import datetime
import itertools

import mock

import clock
from dao.genomics_dao import GenomicSetDao, GenomicSetMemberDao
from dao.participant_dao import ParticipantDao
from dao.participant_summary_dao import ParticipantSummaryDao
from genomic.validation import validate_and_update_genomic_set_by_id
from model.genomics import GenomicSet, GenomicSetMember, GenomicSetStatus, GenomicValidationStatus
from model.participant import Participant
from participant_enums import WithdrawalStatus, SampleStatus
from unit_test_util import SqlTestBase


class GenomicSetValidationBaseTestCase(SqlTestBase):
  def setUp(self, with_data=True, use_mysql=False):
    super(GenomicSetValidationBaseTestCase, self).setUp(with_data=with_data, use_mysql=use_mysql)
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
    participant = Participant(
      participantId = i,
      biobankId = i,
      **kwargs
    )
    self.participant_dao.insert(participant)
    return participant

  def make_summary(self, participant, **override_kwargs):
    """
    Make a summary with custom settings.
    default should create a valid summary.
    """
    valid_kwargs = dict(
      participantId = participant.participantId,
      biobankId=participant.biobankId,
      withdrawalStatus=participant.withdrawalStatus,
      dateOfBirth=datetime.datetime(2000, 1, 1),
      firstName='foo',
      lastName='bar',
      zipCode='12345',
      sampleStatus1ED04=SampleStatus.RECEIVED,
      sampleStatus1SAL2=SampleStatus.RECEIVED,
      consentForStudyEnrollmentTime=datetime.datetime(2019, 1, 1)
    )
    kwargs = dict(valid_kwargs, **override_kwargs)
    summary = self._participant_summary_with_defaults(**kwargs)
    self.summary_dao.insert(summary)
    return summary

  def make_genomic_set(self, **override_kwargs):
    """
    Make a genomic set with custom settings.
    default should create a valid set.
    """
    valid_kwargs = dict(
      genomicSetName='foo',
      genomicSetCriteria='something',
      genomicSetVersion=1,
      genomicSetStatus=GenomicSetStatus.UNSET
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
      genomicSetId = genomic_set.id,
      participantId = participant.participantId,
      sexAtBirth = 'F',
    )
    kwargs = dict(valid_kwargs, **override_kwargs)
    member = GenomicSetMember(**kwargs)
    self.genomic_member_dao.insert(member)
    return member


class GenomicSetMemberValidationTestCase(GenomicSetValidationBaseTestCase):

  def test_test_defaults_are_valid(self):
    participant = self.make_participant()
    self.make_summary(participant)
    genomic_set = self.make_genomic_set()
    member = self.make_genomic_member(genomic_set, participant)
    validate_and_update_genomic_set_by_id(genomic_set.id)
    current_member = self.genomic_member_dao.get(member.id)
    self.assertEqual(current_member.validationStatus, GenomicValidationStatus.VALID)
    current_set = self.genomic_set_dao.get(genomic_set.id)
    self.assertEqual(current_set.genomicSetStatus, GenomicSetStatus.VALID)

  def test_duplicate(self):
    participant = self.make_participant()
    self.make_summary(participant)
    genomic_set_a = self.make_genomic_set(genomicSetName='A',
                                          genomicSetStatus=GenomicSetStatus.VALID)
    self.make_genomic_member(genomic_set_a, participant)
    genomic_set_b = self.make_genomic_set(genomicSetName='B')
    member_b = self.make_genomic_member(genomic_set_b, participant)
    validate_and_update_genomic_set_by_id(genomic_set_b.id)
    current_member = self.genomic_member_dao.get(member_b.id)
    self.assertEqual(current_member.validationStatus,
                     GenomicValidationStatus.INVALID_DUP_PARTICIPANT)
    current_set = self.genomic_set_dao.get(genomic_set_b.id)
    self.assertEqual(current_set.genomicSetStatus, GenomicSetStatus.INVALID)

  def test_consent(self):
    participant = self.make_participant()
    self.make_summary(participant, consentForStudyEnrollmentTime=datetime.datetime(2017, 1, 1))
    genomic_set = self.make_genomic_set()
    member = self.make_genomic_member(genomic_set, participant)
    validate_and_update_genomic_set_by_id(genomic_set.id)
    current_member = self.genomic_member_dao.get(member.id)
    self.assertEqual(current_member.validationStatus,
                     GenomicValidationStatus.INVALID_CONSENT)
    current_set = self.genomic_set_dao.get(genomic_set.id)
    self.assertEqual(current_set.genomicSetStatus, GenomicSetStatus.INVALID)

  def test_consen_null(self):
    participant = self.make_participant()
    self.make_summary(participant, consentForStudyEnrollmentTime=None)
    genomic_set = self.make_genomic_set()
    member = self.make_genomic_member(genomic_set, participant)
    validate_and_update_genomic_set_by_id(genomic_set.id)
    current_member = self.genomic_member_dao.get(member.id)
    self.assertEqual(current_member.validationStatus,
                     GenomicValidationStatus.INVALID_CONSENT)
    current_set = self.genomic_set_dao.get(genomic_set.id)
    self.assertEqual(current_set.genomicSetStatus, GenomicSetStatus.INVALID)

  def test_withdrawn(self):
    participant = self.make_participant(withdrawalStatus=WithdrawalStatus.NO_USE)
    self.make_summary(participant)
    genomic_set = self.make_genomic_set()
    member = self.make_genomic_member(genomic_set, participant)
    validate_and_update_genomic_set_by_id(genomic_set.id)
    current_member = self.genomic_member_dao.get(member.id)
    self.assertEqual(current_member.validationStatus,
                     GenomicValidationStatus.INVALID_WITHDRAW_STATUS)
    current_set = self.genomic_set_dao.get(genomic_set.id)
    self.assertEqual(current_set.genomicSetStatus, GenomicSetStatus.INVALID)

  def test_sexatbirth(self):
    participant = self.make_participant()
    self.make_summary(participant)
    genomic_set = self.make_genomic_set()
    member = self.make_genomic_member(genomic_set, participant, sexAtBirth='foo')
    validate_and_update_genomic_set_by_id(genomic_set.id)
    current_member = self.genomic_member_dao.get(member.id)
    self.assertEqual(current_member.validationStatus, GenomicValidationStatus.INVALID_SEX_AT_BIRTH)
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
    self.assertNotEqual(current_member_a.validationStatus, GenomicValidationStatus.INVALID_AGE)
    self.assertEqual(current_member_b.validationStatus, GenomicValidationStatus.INVALID_AGE)
    current_set = self.genomic_set_dao.get(genomic_set.id)
    self.assertEqual(current_set.genomicSetStatus, GenomicSetStatus.INVALID)

  def test_biobank_status(self):
    sample_names = ['1ED04', '1SAL2']
    valid_sample_statuses = {SampleStatus.RECEIVED,}
    def make_member(genomic_set, **summary_kwargs):
      participant = self.make_participant()
      self.make_summary(participant, **summary_kwargs)
      return self.make_genomic_member(genomic_set, participant)

    genomic_set = self.make_genomic_set()
    members_kwargs_and_statuses = [
      (make_member(genomic_set, **kwargs), kwargs, status)
      for kwargs, status
      in (
        (
          {'sampleStatus' + sample_name: status},
          GenomicValidationStatus.VALID if status in valid_sample_statuses else
          GenomicValidationStatus.INVALID_BIOBANK_ORDER
        )
        for sample_name, status
        in itertools.product(sample_names, SampleStatus)
      )
    ]
    validate_and_update_genomic_set_by_id(genomic_set.id)
    for member, kwargs, expected_validation_status in members_kwargs_and_statuses:
      current_member = self.genomic_member_dao.get(member.id)
      self.assertEqual(
        current_member.validationStatus,
        expected_validation_status,
        "Expected ParticipantSummary(**{}) to have {} but got {}".format(
          kwargs,
          expected_validation_status,
          current_member.validationStatus
        )
      )
    current_set = self.genomic_set_dao.get(genomic_set.id)
    self.assertEqual(current_set.genomicSetStatus, GenomicSetStatus.INVALID)

  def test_ny_zip_code(self):
    participant_a = self.make_participant()
    self.make_summary(participant_a, zipCode=None)
    participant_b = self.make_participant()
    self.make_summary(participant_b, zipCode='')
    participant_c = self.make_participant()
    self.make_summary(participant_c, zipCode='12345')
    genomic_set = self.make_genomic_set()
    member_a = self.make_genomic_member(genomic_set, participant_a)
    member_b = self.make_genomic_member(genomic_set, participant_b)
    member_c = self.make_genomic_member(genomic_set, participant_c)
    with clock.FakeClock(datetime.datetime(2019, 1, 1)):
      validate_and_update_genomic_set_by_id(genomic_set.id)
    current_member_a = self.genomic_member_dao.get(member_a.id)
    current_member_b = self.genomic_member_dao.get(member_b.id)
    current_member_c = self.genomic_member_dao.get(member_c.id)
    self.assertEqual(current_member_a.validationStatus, GenomicValidationStatus.INVALID_NY_ZIPCODE)
    self.assertEqual(current_member_b.validationStatus, GenomicValidationStatus.INVALID_NY_ZIPCODE)
    self.assertEqual(current_member_c.validationStatus, GenomicValidationStatus.VALID)
    current_set = self.genomic_set_dao.get(genomic_set.id)
    self.assertEqual(current_set.genomicSetStatus, GenomicSetStatus.INVALID)


class GenomicSetValidationSafetyTestCase(GenomicSetValidationBaseTestCase):

  def test_transaction(self):
    participant = self.make_participant()
    self.make_summary(participant)
    genomic_set = self.make_genomic_set()
    member = self.make_genomic_member(genomic_set, participant)
    with mock.patch('genomic.validation.GenomicSetDao.update_with_session') as mocked_set_update:
      mocked_set_update.side_effect = Exception('baz')
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

