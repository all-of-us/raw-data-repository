import collections
import datetime
import functools
import operator

from rdr_service import clock
from rdr_service.dao.genomics_dao import GenomicSetDao
from rdr_service.participant_enums import SampleStatus, WithdrawalStatus
from rdr_service.genomic_enums import GenomicSetStatus, GenomicSetMemberStatus, GenomicValidationFlag

GENOMIC_VALID_SEX_AT_BIRTH_VALUES = ["F", "M"]
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
    date_of_birth_cutoff = datetime.date(year=now.year - GENOMIC_VALID_AGE, month=now.month, day=now.day)
    dao = dao or GenomicSetDao()

    update_queue = collections.deque()

    with dao.member_dao.session() as session:
        try:
            for row in dao.iter_validation_data_for_genomic_set_id_with_session(session, genomic_set_id):
                flags = list(_iter_validation_flags(row, date_of_birth_cutoff))
                status = GenomicSetMemberStatus.INVALID if len(flags) > 0 else GenomicSetMemberStatus.VALID
                update_queue.append(dao.member_dao.BulkUpdateValidationParams(row.id, status, flags))

            dao.member_dao.bulk_update_validation_status_with_session(session, update_queue)

            genomic_set = dao.get_with_session(session, genomic_set_id)
            if any([task.status == GenomicSetMemberStatus.INVALID for task in update_queue]):
                genomic_set.genomicSetStatus = GenomicSetStatus.INVALID
            else:
                genomic_set.genomicSetStatus = GenomicSetStatus.VALID
                genomic_set.validatedTime = now
            dao.update_with_session(session, genomic_set)
        except Exception:
            session.rollback()
            raise


def _iter_validation_flags(row, date_of_birth_cutoff):
    """
  Iterate all GenomicValidationFlag that apply to the given row

  :param row: a Row from GenomicSet.iter_validation_data_for_genomic_set_id_with_session().
              Rows contain all fields of GenomicSetMember along with any calculated fields necessary
              for this validation.
  :param date_of_birth_cutoff: any birth date before dob_cutoff will be invalid
  """
    if not row.consent_time or row.consent_time < GENOMIC_VALID_CONSENT_CUTOFF:
        yield GenomicValidationFlag.INVALID_CONSENT
    if row.withdrawal_status != WithdrawalStatus.NOT_WITHDRAWN:
        yield GenomicValidationFlag.INVALID_WITHDRAW_STATUS
    if row.sex_at_birth not in GENOMIC_VALID_SEX_AT_BIRTH_VALUES:
        yield GenomicValidationFlag.INVALID_SEX_AT_BIRTH
    if not row.birth_date or row.birth_date > date_of_birth_cutoff:
        yield GenomicValidationFlag.INVALID_AGE
    if not (
        row.samples_to_isolate_dna in GENOMIC_VALID_SAMPLE_STATUSES
        and any(
            map(
                functools.partial(operator.contains, GENOMIC_VALID_SAMPLE_STATUSES),
                [row.sample_status_1ED04, row.sample_status_1SAL2],
            )
        )
    ):
        yield GenomicValidationFlag.INVALID_BIOBANK_ORDER
    if not row.zip_code:
        yield GenomicValidationFlag.INVALID_NY_ZIPCODE
