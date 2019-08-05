import collections

import sqlalchemy

import clock
from dao.base_dao import UpdatableDao
from model.genomics import GenomicSet, GenomicSetMember, GenomicSetStatus, GenomicSetMemberStatus
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
          ParticipantSummary.samplesToIsolateDNA.label('samples_to_isolate_dna'),
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

  def update_biobank_id(self, genomic_set_id):
    with self.session() as session:
      self.update_biobank_id_with_session(session, genomic_set_id)

  def update_biobank_id_with_session(self, session, genomic_set_id):

    query = (
      sqlalchemy
        .update(GenomicSetMember)
        .where(GenomicSetMember.genomicSetId == sqlalchemy.bindparam('genomic_set_id_param'))
        .values({
        GenomicSetMember.biobankId.name:
          sqlalchemy.select([Participant.biobankId])
            .where(Participant.participantId == GenomicSetMember.participantId)
            .limit(1)
      })
    )

    return session.execute(query, {'genomic_set_id_param': genomic_set_id})

  BulkUpdateValidationParams = collections.namedtuple('BulkUpdateValidationParams', [
    'member_id',
    'status',
    'flags'
  ])

  def bulk_update_validation_status(self, member_update_params_iterable):
    """
    Perform a bulk update of validation statuses.

    :param member_update_params_iterable: iterable of BulkUpdateValidationParams objects
    :type member_update_params_iterable: collections.Iterable of BulkUpdateValidationParams
    :rtype: sqlalchemy.engine.ResultProxy
    """
    with self.session() as session:
      return self.bulk_update_validation_status_with_session(session,
                                                             member_update_params_iterable)

  def bulk_update_validation_status_with_session(self, session, member_update_params_iterable):
    """
    Perform a bulk update of validation statuses in a given session.

    :param session: sqlalchemy session
    :param member_update_params_iterable: iterable of BulkUpdateValidationParams objects
    :type member_update_params_iterable: collections.Iterable of BulkUpdateValidationParams
    :rtype: sqlalchemy.engine.ResultProxy
    """
    now = clock.CLOCK.now()
    status_case = sqlalchemy.case(
      {int(GenomicSetMemberStatus.VALID): now},
      value=sqlalchemy.bindparam('status'),
      else_=None
    )
    query = (
      sqlalchemy
        .update(GenomicSetMember)
        .where(GenomicSetMember.id == sqlalchemy.bindparam('member_id'))
        .values({
          GenomicSetMember.validationStatus.name: sqlalchemy.bindparam('status'),
          GenomicSetMember.validationFlags.name: sqlalchemy.bindparam('flags'),
          GenomicSetMember.validatedTime.name: status_case
        })
    )
    parameter_sets = [
      {
        'member_id': member_id,
        'status': int(status),
        'flags': flags,
        'time': now,
      }
      for member_id, status, flags in member_update_params_iterable
    ]
    return session.execute(query, parameter_sets)

  def bulk_update_package_id(self, genomic_set_id, client_id_package_id_pair_iterable):
    """
    Perform a bulk update of package id.

    :param genomic_set_id
    :param client_id_package_id_pair_iterable: pairs of GenomicSetMember.biobankOrderClientId and
                                               package_id
    :type client_id_package_id_pair_iterable: collections.Iterable of (string, string)
    :rtype: sqlalchemy.engine.ResultProxy
    """
    with self.session() as session:
      return self.bulk_update_package_id_with_session(session, genomic_set_id,
                                                      client_id_package_id_pair_iterable)

  def bulk_update_package_id_with_session(self, session, genomic_set_id, client_id_package_id_pair_iterable):
    """
    Perform a bulk update of package id in a given session.

    :param session: sqlalchemy session
    :param genomic_set_id
    :param client_id_package_id_pair_iterable: pairs of GenomicSetMember.biobankOrderClientId and
                                               package_id
    :type client_id_package_id_pair_iterable: collections.Iterable of (string, string)
    :rtype: sqlalchemy.engine.ResultProxy
    """

    query = (
      sqlalchemy
        .update(GenomicSetMember)
        .where(
                (GenomicSetMember.genomicSetId == genomic_set_id) &
                (GenomicSetMember.biobankId == sqlalchemy.bindparam('biobank_id_param')) &
                (GenomicSetMember.genomeType == sqlalchemy.bindparam('genome_type_param'))
              )
        .values({
          GenomicSetMember.packageId.name: sqlalchemy.bindparam('package_id_param'),
          GenomicSetMember.biobankOrderClientId.name: sqlalchemy.bindparam('client_id_param')
        })
    )

    parameter_sets = [
      {
        'biobank_id_param': biobank_id,
        'genome_type_param': genome_type,
        'client_id_param': client_id,
        'package_id_param': package_id
      }
      for biobank_id, genome_type, client_id, package_id in client_id_package_id_pair_iterable
    ]
    return session.execute(query, parameter_sets)
