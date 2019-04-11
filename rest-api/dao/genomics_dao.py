import collections

import sqlalchemy
from dao.base_dao import UpdatableDao
from model.genomics import GenomicSet, GenomicSetMember, GenomicSetStatus
from model.participant import Participant
from model.participant_summary import ParticipantSummary
from query import Query, Operator, FieldFilter, OrderBy


class GenomicSetDao(UpdatableDao):
  """ Stub for GenomicSet model """

  def __init__(self):
    super(GenomicSetDao, self).__init__(GenomicSet, order_by_ending=['id'])
    self.member_dao = GenomicSetMemberDao()

  def get_id(self, obj):
    return obj.id

  def get_members(self, genomic_set, option_args=None):
    return self.member_dao.get_members_by_genomic_set_id(genomic_set.id, option_args)

  def iter_validation_data_for_genomic_set_id(self, genomic_set_id):
    with self.session() as session:
      query = self._get_validation_data_query_for_genomic_set_id(genomic_set_id)
      cursor = session.execute(query)
      Row = collections.namedtuple('Row', cursor.keys())
      for row in cursor:
        yield Row(*row)

  def _get_validation_data_query_for_genomic_set_id(self, genomic_set_id):
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

  def get_valid_membership_counts_for_participant_ids(self, participant_ids):
    """
    :type participant_id: int
    :rtype: dict
    """
    with self.session() as session:
      cursor = session.execute(
        sqlalchemy.select([GenomicSetMember.participantId, sqlalchemy.func.count()])
        .select_from(
          sqlalchemy.join(
            GenomicSet,
            GenomicSetMember,
            GenomicSetMember.genomicSetId == GenomicSet.id
          )
        )
        .where(
          GenomicSetMember.participantId.in_(participant_ids)
          & (GenomicSet.genomicSetStatus == GenomicSetStatus.VALID)
        )
        .group_by(GenomicSetMember.participantId)
      )
      return {
        row[0]: row[1] for row in cursor
      }

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


class GenomicSetMemberDao(UpdatableDao):
  """ Stub for GenomicSetMember model """

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

  def get_members_by_genomic_set_id(self, genomic_set_id, option_args=None):
    """
    :type genomic_set_id: int
    :rtype: collections.Iterable[GenomicSetMember]
    """
    with self.session() as session:
      query = (
        session.query(GenomicSetMember)
          .filter(GenomicSetMember.genomicSetId == genomic_set_id)
      )
      if option_args is not None:
        query = query.options(*option_args)
      return query
