import collections

import sqlalchemy

import clock
from dao.base_dao import UpdatableDao
from model.genomics import GenomicSet, GenomicSetMember, GenomicSetStatus, GenomicValidationStatus
from model.participant import Participant
from model.participant_summary import ParticipantSummary
from query import Query, Operator, FieldFilter, OrderBy

class GenomicSetDao(UpdatableDao):
  """ Stub for GenomicSet model """

  validate_version_match = False

  def __init__(self):
    super(GenomicSetDao, self).__init__(GenomicSet, order_by_ending=['id'])
    self.member_dao = GenomicSetMemberDao()

  def get_id(self, obj):
    return obj.id

  def get_one_by_file_name(self, filename):
    return super(GenomicSetDao, self) \
      .query(Query([FieldFilter('genomicSetFile', Operator.EQUALS, filename)], None, 1, None)).items

  def get_new_version_number(self, genomic_set_name):
    genomic_sets = super(GenomicSetDao, self)\
      .query(Query([FieldFilter('genomicSetName', Operator.EQUALS, genomic_set_name)],
                   OrderBy('genomicSetVersion', False), 1, None)).items
    if genomic_sets:
      return genomic_sets[0].genomicSetVersion + 1
    else:
      return 1

  def iter_validation_data_for_genomic_set_id(self, genomic_set_id):
    """
    Iterate over validation data rows.

    :type genomic_set_id: int
    :rtype: collections.Iterable
    """
    with self.session() as session:
      return self.iter_validation_data_for_genomic_set_id_with_session(session, genomic_set_id)

  def iter_validation_data_for_genomic_set_id_with_session(self, session, genomic_set_id):
    """
    Iterate over validation data rows using the given session.

    :param session: sqlalchemy session
    :type genomic_set_id: int
    :rtype: collections.Iterable
    """
    query = self._get_validation_data_query_for_genomic_set_id(genomic_set_id)
    cursor = session.execute(query)
    Row = collections.namedtuple('Row', cursor.keys())
    for row in cursor:
      yield Row(*row)

  def _get_validation_data_query_for_genomic_set_id(self, genomic_set_id):
    """
    Build a sqlalchemy query for validation data.

    :type genomic_set_id: int
    :return: sqlalchemy query
    """
    existing_valid_query = (
      sqlalchemy
        .select([
          sqlalchemy.func.count().label('existing_count'),
        ])
        .select_from(
          sqlalchemy.join(
            GenomicSet, GenomicSetMember,
            GenomicSetMember.genomicSetId == GenomicSet.id
          )
        )
        .where(
          (GenomicSet.genomicSetStatus == GenomicSetStatus.VALID)
          & (GenomicSetMember.participantId == Participant.participantId)
        )
    )

    return(
      sqlalchemy
        .select([
          GenomicSetMember,
          Participant.withdrawalStatus.label('withdrawal_status'),
          ParticipantSummary.dateOfBirth.label('birth_date'),
          ParticipantSummary.consentForStudyEnrollmentTime.label('consent_time'),
          ParticipantSummary.sampleStatus1ED04.label('sample_status_1ED04'),
          ParticipantSummary.sampleStatus1SAL2.label('sample_status_1SAL2'),
          ParticipantSummary.zipCode.label('zip_code'),
          existing_valid_query.label('existing_valid_genomic_count'),
        ])
        .select_from(
          sqlalchemy.join(
            sqlalchemy.join(
              sqlalchemy.join(
                GenomicSet, GenomicSetMember,
                GenomicSetMember.genomicSetId == GenomicSet.id
              ),
              Participant,
              Participant.participantId == GenomicSetMember.participantId
            ),
            ParticipantSummary,
            ParticipantSummary.participantId == Participant.participantId
          )
        )
          .where(
          (GenomicSet.id == genomic_set_id)
        )
    )

class GenomicSetMemberDao(UpdatableDao):
  """ Stub for GenomicSetMember model """

  validate_version_match = False

  def __init__(self):
    super(GenomicSetMemberDao, self).__init__(GenomicSetMember, order_by_ending=['id'])

  def get_id(self, obj):
    return obj.id

  def upsert_all(self, genomic_set_members):
    """Inserts/updates members. """
    members = list(genomic_set_members)

    def upsert(session):
      written = 0
      for member in members:
        session.merge(member)
        written += 1
      return written
    return self._database.autoretry(upsert)

  def bulk_update_validation_status(self, member_id_status_pair_iterable):
    """
    Perform a bulk update of validation statuses.

    :param member_id_status_pair_iterable: pairs of GenomicSetMember.id and GenomicValidationStatus
                                           to include in this update
    :type member_id_status_pair_iterable: collections.Iterable of (int, GenomicValidationStatus)
    :rtype: sqlalchemy.engine.ResultProxy
    """
    with self.session() as session:
      return self.bulk_update_validation_status_with_session(session,
                                                             member_id_status_pair_iterable)

  def bulk_update_validation_status_with_session(self, session, member_id_status_pair_iterable):
    """
    Perform a bulk update of validation statuses in a given session.

    :param session: sqlalchemy session
    :param member_id_status_pair_iterable: pairs of GenomicSetMember.id and GenomicValidationStatus
                                           to include in this update
    :type member_id_status_pair_iterable: collections.Iterable of (int, GenomicValidationStatus)
    :rtype: sqlalchemy.engine.ResultProxy
    """
    now = clock.CLOCK.now()
    status_case = sqlalchemy.case(
      {int(GenomicValidationStatus.VALID): now},
      value=sqlalchemy.bindparam('status'),
      else_=None
    )
    query = (
      sqlalchemy
        .update(GenomicSetMember)
        .where(GenomicSetMember.id == sqlalchemy.bindparam('member_id'))
        .values({
          GenomicSetMember.validationStatus.name: sqlalchemy.bindparam('status'),
          GenomicSetMember.validatedTime.name: status_case
        })
    )
    parameter_sets = [
      {
        'member_id': member_id,
        'status': int(status),
        'time': now if status == GenomicValidationStatus.VALID else None,
      }
      for member_id, status in member_id_status_pair_iterable
    ]
    return session.execute(query, parameter_sets)
