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
  """
  Determine and write validation statuses and times for the specified GenomicSet and all of it's
  GenomicSetMembers in a single transaction.

  :param genomic_set_id: The id of the GenomicSet to validate
  :param dao: (optional)
  :type dao: GenomicSetDao or None
  """
  now = clock.CLOCK.now()
  date_of_birth_cutoff = datetime.date(year=now.year - GENOMIC_VALID_AGE, month=now.month,
                                       day=now.day)
  dao = dao or GenomicSetDao()

  MemberIdStatusPair = collections.namedtuple('MemberIdStatusPair', [
    'member_id',
    'status',
  ])
  update_queue = collections.deque()

  with dao.member_dao.session() as session:
    try:
      for row in dao.iter_validation_data_for_genomic_set_id_with_session(session, genomic_set_id):
        update_queue.append(MemberIdStatusPair(
          row.id,
          _get_validation_status(row, date_of_birth_cutoff),
        ))

      dao.member_dao.bulk_update_validation_status_with_session(session, update_queue)

      genomic_set = dao.get_with_session(session, genomic_set_id)
      for task in update_queue:
        if task.status != GenomicValidationStatus.VALID:
          genomic_set.genomicSetStatus = GenomicSetStatus.INVALID
      if genomic_set.genomicSetStatus != GenomicSetStatus.INVALID:
        genomic_set.genomicSetStatus = GenomicSetStatus.VALID
        genomic_set.validatedTime = now
      dao.update_with_session(session, genomic_set)
    except Exception:
      session.rollback()
      raise


def _get_validation_status(row, date_of_birth_cutoff):
  """
  Get the correct GenomicValidationStatus for the given row.

  invalid if ANY of the following fail
  - Previous consent (prior to 3/1/18)
  - Withdrawn(withdraw date populated)
  - Sex at Birth not SexAtBirth_Female or SexAtBirth_Male
  - Participant younger than 18 yo (DOB)
  - Missing test order (1ED04 or 1SAL2)
  - Missing zip code

  :param row: a Row from GenomicSet.iter_validation_data_for_genomic_set_id_with_session().
              Rows contain all fields of GenomicSetMember along with any calculated fields necessary
              for this validation.
  :param date_of_birth_cutoff: any birth date before dob_cutoff will be invalid
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
  if not row.birth_date or row.birth_date > date_of_birth_cutoff:
    return GenomicValidationStatus.INVALID_AGE
  if not all(map(
    functools.partial(operator.contains, GENOMIC_VALID_SAMPLE_STATUSES),
    [row.sample_status_1ED04, row.sample_status_1SAL2]
  )):
    return GenomicValidationStatus.INVALID_BIOBANK_ORDER
  if not row.zip_code:
    return GenomicValidationStatus.INVALID_NY_ZIPCODE
  return GenomicValidationStatus.VALID
