from __future__ import print_function

import collections
import datetime
import functools
import operator

import clock
from dao.genomics_dao import GenomicSetDao
from model.genomics import GenomicValidationStatus, GenomicSetStatus
from participant_enums import WithdrawalStatus, SampleStatus


GENOMIC_VALID_SEX_AT_BIRTH_VALUES = ['F', 'M']
GENOMIC_VALID_AGE = 18
GENOMIC_VALID_CONSENT_CUTOFF = datetime.datetime(2018, 4, 24)
GENOMIC_VALID_SAMPLE_STATUSES = [SampleStatus.RECEIVED]


def validate_and_update_genomic_set_by_id(genomic_set_id, dao=None):
  now = clock.CLOCK.now()
  dob_cutoff = datetime.date(year=now.year - GENOMIC_VALID_AGE, month=now.month, day=now.day)
  dao = dao or GenomicSetDao()

  MemberUpdateTask = collections.namedtuple('MemberUpdateTask', [
    'member_id',
    'new_status',
  ])
  update_queue = collections.deque()

  for row in dao.iter_validation_data_for_genomic_set_id(genomic_set_id):
    update_queue.append(MemberUpdateTask(
      row.id,
      _get_validation_status(row, dob_cutoff),
    ))

  genomic_set = dao.get(genomic_set_id)
  for task in update_queue:  # TODO: replace with single query batch update
    member = dao.member_dao.get(task.member_id)
    member.validationStatus = task.new_status
    member.validationTime = now
    dao.member_dao.update(member)
    if member.validationStatus != GenomicValidationStatus.VALID:
      genomic_set.genomicSetStatus = GenomicSetStatus.INVALID
  if genomic_set.genomicSetStatus != GenomicSetStatus.INVALID:
    genomic_set.genomicSetStatus = GenomicSetStatus.VALID
  dao.update(genomic_set)


def _get_validation_status(row, dob_cutoff):
  """
  invalid if ANY of the following fail
  - Previous consent (prior to 3/1/18)
  - Withdrawn(withdraw date populated)
  - Sex at Birth not SexAtBirth_Female or SexAtBirth_Male
  - Participant younger than 18 yo (DOB)
  - Missing test order (1ED04 or 1SAL2)
  - Missing zip code

  :rtype: GenomicValidationStatus
  """
  if row.existing_valid_genomic_count != 0:
    return GenomicValidationStatus.INVALID_DUP_PARTICIPANT
  if not row.consent_time or row.consent_time < GENOMIC_VALID_CONSENT_CUTOFF:
    return GenomicValidationStatus.INVALID_CONSENT
  if row.withdrawal_status != WithdrawalStatus.NOT_WITHDRAWN:
    return GenomicValidationStatus.INVALID_WITHDRAW_STATUS
  if row.sex_at_birth not in GENOMIC_VALID_SEX_AT_BIRTH_VALUES:
    return GenomicValidationStatus.INVALID_SEX_AT_BIRTH
  if not row.birth_date or row.birth_date > dob_cutoff:
    return GenomicValidationStatus.INVALID_AGE
  if not all(map(
    functools.partial(operator.contains, GENOMIC_VALID_SAMPLE_STATUSES),
    [row.sample_status_1ED04, row.sample_status_1SAL2]
  )):
    return GenomicValidationStatus.INVALID_BIOBANK_ORDER
  if row.ny_flag and not row.zip_code:
    return GenomicValidationStatus.INVALID_NY_ZIPCODE
  return GenomicValidationStatus.VALID
