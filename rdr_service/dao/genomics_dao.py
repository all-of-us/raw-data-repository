import collections

import pytz
import sqlalchemy
import logging
from dateutil import parser

from sqlalchemy.exc import OperationalError
from sqlalchemy.sql import functions
from werkzeug.exceptions import BadRequest, NotFound

from rdr_service import clock, config
from rdr_service.dao.base_dao import UpdatableDao, BaseDao, UpsertableDao
from rdr_service.dao.bq_genomics_dao import bq_genomic_set_member_update, bq_genomic_manifest_feedback_update, \
    bq_genomic_manifest_file_update
from rdr_service.dao.participant_dao import ParticipantDao
from rdr_service.model.genomics import (
    GenomicSet,
    GenomicSetMember,
    GenomicJobRun,
    GenomicFileProcessed,
    GenomicGCValidationMetrics,
    GenomicManifestFile, GenomicManifestFeedback, GenomicAW1Raw, GenomicAW2Raw, GenomicIncident)
from rdr_service.participant_enums import (
    GenomicSetStatus,
    GenomicSetMemberStatus,
    GenomicSubProcessResult,
    QuestionnaireStatus,
    WithdrawalStatus,
    SuspensionStatus,
    GenomicWorkflowState,
    GenomicManifestTypes)
from rdr_service.model.participant import Participant
from rdr_service.model.participant_summary import ParticipantSummary
from rdr_service.query import FieldFilter, Operator, OrderBy, Query
from rdr_service.resource.generators.genomics import genomic_set_member_update, genomic_manifest_feedback_update, \
    genomic_manifest_file_update


class GenomicSetDao(UpdatableDao):
    """ Stub for GenomicSet model """

    validate_version_match = False

    def __init__(self):
        super(GenomicSetDao, self).__init__(GenomicSet, order_by_ending=["id"])
        self.member_dao = GenomicSetMemberDao()

    def get_id(self, obj):
        return obj.id

    def get_one_by_file_name(self, filename):
        return (
            super(GenomicSetDao, self)
                .query(Query([FieldFilter("genomicSetFile", Operator.EQUALS, filename)], None, 1, None))
                .items
        )

    def get_new_version_number(self, genomic_set_name):
        genomic_sets = (
            super(GenomicSetDao, self)
                .query(
                Query(
                    [FieldFilter("genomicSetName", Operator.EQUALS, genomic_set_name)],
                    OrderBy("genomicSetVersion", False),
                    1,
                    None,
                )
            )
                .items
        )
        if genomic_sets:
            return genomic_sets[0].genomicSetVersion + 1
        else:
            return 1

    def get_max_set(self):
        with self.session() as s:
            return s.query(functions.max(GenomicSet.id)).one()[0]

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
        Row = collections.namedtuple("Row", list(cursor.keys()))
        for row in cursor:
            yield Row(*row)

    def _get_validation_data_query_for_genomic_set_id(self, genomic_set_id):
        """
    Build a sqlalchemy query for validation data.

    :type genomic_set_id: int
    :return: sqlalchemy query
    """
        existing_valid_query = (
            sqlalchemy.select([sqlalchemy.func.count().label("existing_count")])
                .select_from(
                sqlalchemy.join(GenomicSet, GenomicSetMember, GenomicSetMember.genomicSetId == GenomicSet.id))
                .where(
                (GenomicSet.genomicSetStatus == GenomicSetStatus.VALID)
                & (GenomicSetMember.participantId == Participant.participantId)
            )
        )

        return (
            sqlalchemy.select(
                [
                    GenomicSetMember,
                    Participant.withdrawalStatus.label("withdrawal_status"),
                    ParticipantSummary.dateOfBirth.label("birth_date"),
                    ParticipantSummary.consentForStudyEnrollmentTime.label("consent_time"),
                    ParticipantSummary.sampleStatus1ED04.label("sample_status_1ED04"),
                    ParticipantSummary.sampleStatus1SAL2.label("sample_status_1SAL2"),
                    ParticipantSummary.samplesToIsolateDNA.label("samples_to_isolate_dna"),
                    ParticipantSummary.zipCode.label("zip_code"),
                    existing_valid_query.label("existing_valid_genomic_count"),
                ]
            )
                .select_from(
                sqlalchemy.join(
                    sqlalchemy.join(
                        sqlalchemy.join(GenomicSet, GenomicSetMember, GenomicSetMember.genomicSetId == GenomicSet.id),
                        Participant,
                        Participant.participantId == GenomicSetMember.participantId,
                    ),
                    ParticipantSummary,
                    ParticipantSummary.participantId == Participant.participantId,
                )
            )
                .where((GenomicSet.id == genomic_set_id))
        )


class GenomicSetMemberDao(UpdatableDao):
    """ Stub for GenomicSetMember model """

    validate_version_match = False

    def __init__(self):
        super(GenomicSetMemberDao, self).__init__(GenomicSetMember, order_by_ending=["id"])
        self.valid_job_id_fields = ('reconcileMetricsBBManifestJobRunId',
                                    'reconcileMetricsSequencingJobRunId',
                                    'reconcileCvlJobRunId',
                                    'cvlW1ManifestJobRunId',
                                    'gemA1ManifestJobRunId',
                                    'reconcileGCManifestJobRunId',
                                    'gemA3ManifestJobRunId',
                                    'cvlW3ManifestJobRunID',
                                    'aw3ManifestJobRunID',
                                    'aw4ManifestJobRunID',)

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
            sqlalchemy.update(GenomicSetMember)
                .where(GenomicSetMember.genomicSetId == sqlalchemy.bindparam("genomic_set_id_param"))
                .values(
                {
                    GenomicSetMember.biobankId.name: sqlalchemy.select([Participant.biobankId])
                        .where(Participant.participantId == GenomicSetMember.participantId)
                        .limit(1)
                }
            )
        )

        return session.execute(query, {"genomic_set_id_param": genomic_set_id})

    BulkUpdateValidationParams = collections.namedtuple("BulkUpdateValidationParams", ["member_id", "status", "flags"])

    def bulk_update_validation_status(self, member_update_params_iterable):
        """
    Perform a bulk update of validation statuses.

    :param member_update_params_iterable: iterable of BulkUpdateValidationParams objects
    :type member_update_params_iterable: collections.Iterable of BulkUpdateValidationParams
    :rtype: sqlalchemy.engine.ResultProxy
    """
        with self.session() as session:
            return self.bulk_update_validation_status_with_session(session, member_update_params_iterable)

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
            {int(GenomicSetMemberStatus.VALID): now}, value=sqlalchemy.bindparam("status"), else_=None
        )
        query = (
            sqlalchemy.update(GenomicSetMember)
                .where(GenomicSetMember.id == sqlalchemy.bindparam("member_id"))
                .values(
                {
                    GenomicSetMember.validationStatus.name: sqlalchemy.bindparam("status"),
                    GenomicSetMember.validationFlags.name: sqlalchemy.bindparam("flags"),
                    GenomicSetMember.validatedTime.name: status_case,
                }
            )
        )
        parameter_sets = [
            {"member_id": member_id, "status": int(status), "flags": flags, "time": now}
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
            return self.bulk_update_package_id_with_session(
                session, genomic_set_id, client_id_package_id_pair_iterable
            )

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
            sqlalchemy.update(GenomicSetMember)
                .where(
                (GenomicSetMember.genomicSetId == genomic_set_id)
                & (GenomicSetMember.biobankId == sqlalchemy.bindparam("biobank_id_param"))
                & (GenomicSetMember.genomeType == sqlalchemy.bindparam("genome_type_param"))
            )
                .values(
                {
                    GenomicSetMember.packageId.name: sqlalchemy.bindparam("package_id_param"),
                    GenomicSetMember.biobankOrderClientId.name: sqlalchemy.bindparam("client_id_param"),
                }
            )
        )

        parameter_sets = [
            {
                "biobank_id_param": biobank_id,
                "genome_type_param": genome_type,
                "client_id_param": client_id,
                "package_id_param": package_id,
            }
            for biobank_id, genome_type, client_id, package_id in client_id_package_id_pair_iterable
        ]
        return session.execute(query, parameter_sets)

    def bulk_update_genotyping_sample_manifest_data(self, genotyping_data_iterable):
        with self.session() as session:
            return self.bulk_update_genotyping_sample_manifest_data_with_session(session,
                                                                                 genotyping_data_iterable)

    def bulk_update_genotyping_sample_manifest_data_with_session(self, session,
                                                                 genotyping_data_iterable):
        query = (
            sqlalchemy
                .update(GenomicSetMember)
                .where(
                (GenomicSetMember.biobankId == sqlalchemy.bindparam('biobank_id_param')) &
                (GenomicSetMember.genomeType == sqlalchemy.bindparam('genome_type_param'))
            )
                .values({
                GenomicSetMember.sampleId.name: sqlalchemy.bindparam('sample_id_param'),
                GenomicSetMember.sampleType.name: sqlalchemy.bindparam('sample_type_param')
            })
        )

        parameter_sets = [
            {
                'biobank_id_param': biobank_id,
                'genome_type_param': genome_type,
                'sample_id_param': sample_id,
                'sample_type_param': sample_type
            }
            for biobank_id, genome_type, sample_id, sample_type in genotyping_data_iterable
        ]
        return session.execute(query, parameter_sets)

    def get_member_from_biobank_id(self, biobank_id, genome_type):
        """
        Retrieves a genomic set member record matching the biobank Id
        :param biobank_id:
        :param genome_type:
        :return: a GenomicSetMember object
        """
        with self.session() as session:
            member = session.query(GenomicSetMember).filter(
                GenomicSetMember.biobankId == biobank_id,
                GenomicSetMember.genomeType == genome_type,
            ).first()
        return member

    def get_member_from_biobank_id_and_sample_id(self, biobank_id, sample_id, genome_type):
        """
        Retrieves a genomic set member record matching the biobank Id
        :param biobank_id:
        :param sample_id:
        :param genome_type:
        :return: a GenomicSetMember object
        """
        with self.session() as session:
            member = session.query(GenomicSetMember).filter(
                GenomicSetMember.biobankId == biobank_id,
                GenomicSetMember.sampleId == sample_id,
                GenomicSetMember.genomeType == genome_type,
            ).first()
        return member

    def get_member_from_biobank_id_in_state(self, biobank_id, genome_type, state):
        """
        Retrieves a genomic set member record matching the biobank Id
        :param biobank_id:
        :param genome_type:
        :param state: genomic_workflow_state
        :return: a GenomicSetMember object
        """
        with self.session() as session:
            member = session.query(GenomicSetMember).filter(
                GenomicSetMember.biobankId == biobank_id,
                GenomicSetMember.genomeType == genome_type,
                GenomicSetMember.genomicWorkflowState == state
            ).one_or_none()
        return member

    def get_member_from_sample_id(self, sample_id, genome_type):
        """
        Retrieves a genomic set member record matching the sample_id
        The sample_id is supplied in AW1 manifest, not biobank_stored_sample_id
        Needs a genome type
        :param genome_type: aou_wgs, aou_array, aou_cvl
        :param sample_id:
        :return: a GenomicSetMember object
        """
        with self.session() as session:
            member = session.query(GenomicSetMember).filter(
                GenomicSetMember.sampleId == sample_id,
                GenomicSetMember.genomeType == genome_type,
                GenomicSetMember.genomicWorkflowState != GenomicWorkflowState.IGNORE,
            ).first()
        return member

    def get_member_from_sample_id_with_state(self, sample_id, genome_type, state):
        """
        Retrieves a genomic set member record matching the sample_id
        The sample_id is supplied in AW1 manifest, not biobank_stored_sample_id
        Needs a genome type and GenomicWorkflowState
        :param genome_type: aou_wgs, aou_array, aou_cvl
        :param sample_id:
        :param state: GenomicWorkflowState
        :return: a GenomicSetMember object
        """
        with self.session() as session:
            member = session.query(GenomicSetMember).filter(
                GenomicSetMember.sampleId == sample_id,
                GenomicSetMember.genomeType == genome_type,
                GenomicSetMember.genomicWorkflowState != GenomicWorkflowState.IGNORE,
                GenomicSetMember.genomicWorkflowState == state,
            ).first()
        return member

    def get_member_from_aw3_sample(self, sample_id, genome_type):
        """
        Retrieves a genomic set member record matching the sample_id
        The sample_id is supplied in AW1 manifest, not biobank_stored_sample_id
        Needs a genome type.
        :param genome_type: aou_wgs, aou_array, aou_cvl
        :param sample_id:
        :return: a GenomicSetMember object
        """
        with self.session() as session:
            member = session.query(GenomicSetMember).filter(
                GenomicSetMember.sampleId == sample_id,
                GenomicSetMember.genomeType == genome_type,
                GenomicSetMember.genomicWorkflowState != GenomicWorkflowState.IGNORE,
                GenomicSetMember.aw3ManifestJobRunID != None,
            ).one_or_none()
        return member

    def get_member_from_collection_tube(self, tube_id, genome_type):
        """
        Retrieves a genomic set member record matching the collection_tube_id
        Needs a genome type
        :param genome_type: aou_wgs, aou_array, aou_cvl
        :param tube_id:
        :return: a GenomicSetMember object
        """
        with self.session() as session:
            member = session.query(GenomicSetMember).filter(
                GenomicSetMember.collectionTubeId == tube_id,
                GenomicSetMember.genomeType == genome_type,
                GenomicSetMember.genomicWorkflowState != GenomicWorkflowState.IGNORE,
            ).first()
        return member

    def get_members_from_set_id(self, set_id, bids=None):
        """
        Retrieves all genomic set member records matching the set_id
        :param set_id
        :param bids
        :return: result set of GenomicSetMembers
        """
        with self.session() as session:
            members_query = session.query(GenomicSetMember).filter(
                GenomicSetMember.genomicSetId == set_id,
                GenomicSetMember.genomicWorkflowState != GenomicWorkflowState.IGNORE
            )
            if bids:
                members_query = members_query.filter(
                    GenomicSetMember.biobankId.in_(bids)
                )
            return members_query.all()

    def get_gem_consent_removal_date(self, member):
        """
        Calculates the earliest removal date between GROR or Primary Consent
        :param participant_id
        :return: datetime
        """
        # get both Primary and GROR dates and consent statuses
        with self.session() as session:
            consent_status = session.query(ParticipantSummary.consentForStudyEnrollment,
                                           ParticipantSummary.consentForStudyEnrollmentAuthored,
                                           ParticipantSummary.consentForGenomicsROR,
                                           ParticipantSummary.consentForGenomicsRORAuthored,
                                           ).filter(
                ParticipantSummary.participantId == member.participantId,
            ).first()

        # Calculate gem consent removal date
        # Earliest date between GROR or Primary if both,
        withdraw_dates = []
        if consent_status.consentForGenomicsROR != QuestionnaireStatus.SUBMITTED:
            withdraw_dates.append(consent_status.consentForGenomicsRORAuthored)

        if consent_status.consentForStudyEnrollment != QuestionnaireStatus.SUBMITTED:
            withdraw_dates.append(consent_status.consentForStudyEnrollmentAuthored)

        return min([d for d in withdraw_dates if d is not None])

    def get_collection_tube_max_set_id(self):
        """
        Retrieves the maximum genomic_set_id for a control sample
        :return: integer
        """
        with self.session() as s:
            return s.query(
                functions.max(GenomicSetMember.genomicSetId)
            ).filter(
                GenomicSetMember.genomicWorkflowState == GenomicWorkflowState.CONTROL_SAMPLE
            ).one()

    def get_member_count_from_manifest_path(self, filepath):
        """
        Retrieves the count of members based on file path
        :return: integer
        """
        with self.session() as s:
            return s.query(
                functions.count(GenomicSetMember.id)
            ).join(
                GenomicFileProcessed,
                GenomicFileProcessed.id == GenomicSetMember.aw1FileProcessedId
            ).join(
                GenomicManifestFile,
                GenomicManifestFile.id == GenomicFileProcessed.genomicManifestFileId
            ).filter(
                GenomicManifestFile.filePath == filepath
            ).one_or_none()

    def update_report_consent_removal_date(self, member, date):
        """
        Updates the reportConsentRemovalDate on the genomic set member
        :param member:
        :param date:
        :return: query result or result code of error
        """
        try:
            logging.info(f'Updating reportConsentRemovalDate for member ID {member.id}.')
            member.reportConsentRemovalDate = date
            updated_member = self.update(member)

            # Update member for PDR
            bq_genomic_set_member_update(member.id)
            genomic_set_member_update(member.id)

            return updated_member

        except OperationalError:
            logging.error(f'Error updating member id: {member.id}.')
            return GenomicSubProcessResult.ERROR

    def update_member_job_run_id(self, member, job_run_id, field, project_id=None):
        """
        Updates the GenomicSetMember with a job_run_id for an arbitrary workflow
        :param project_id:
        :param member: the GenomicSetMember object to update
        :param job_run_id:
        :param field: the field for the job-run workflow (i.e. reconciliation, cvl, etc.)
        :return: query result or result code of error
        """
        if field not in self.valid_job_id_fields:
            logging.error(f'{field} is not a valid job ID field.')
            return GenomicSubProcessResult.ERROR
        setattr(member, field, job_run_id)
        try:
            logging.info(f'Updating {field} with run ID.')
            updated_member = self.update(member)

            # Update member for PDR
            bq_genomic_set_member_update(member.id, project_id=project_id)
            genomic_set_member_update(member.id)

            return updated_member

        except OperationalError:
            logging.error(f'Error updating member id: {member.id}.')
            return GenomicSubProcessResult.ERROR

        except Exception as e:  # pylint: disable=broad-except
            logging.error(e)
            return GenomicSubProcessResult.ERROR

    def update_member_state(self, member, new_state, project_id=None):
        """
        Sets the member's state to a new state
        :param project_id:
        :param member: GenomicWorkflowState
        :param new_state:
        """

        member.genomicWorkflowState = new_state
        member.genomicWorkflowStateModifiedTime = clock.CLOCK.now()
        updated_member = self.update(member)

        # Update member for PDR
        bq_genomic_set_member_update(member.id, project_id)
        genomic_set_member_update(member.id)

        return updated_member

    def update_member_sequencing_file(self, member, job_run_id, filename):
        """
        Updates the sequencing filename of the GenomicSetMember
        :param member:
        :param filename:
        :param job_run_id:
        :return: query result or result code of error
        """
        member.reconcileMetricsSequencingJobRunId = job_run_id
        member.sequencingFileName = filename
        try:
            return self.update(member)
        except OperationalError:
            return GenomicSubProcessResult.ERROR

    def get_members_for_cvl_reconciliation(self):
        """
        Simple select from GSM
        :return: unreconciled GenomicSetMembers with ror consent and seq. data
        """
        with self.session() as session:
            members = session.query(GenomicSetMember).filter(
                GenomicSetMember.reconcileCvlJobRunId == None,
                GenomicSetMember.sequencingFileName != None,
                GenomicSetMember.genomicWorkflowState != GenomicWorkflowState.IGNORE,
            ).all()
        return members

    def get_unconsented_gror_since_date(self, _date):
        """
        Get the genomic set members with GROR updated to No Consent since date
        :param _date:
        :return: GenomicSetMember list
        """
        with self.session() as session:
            members = session.query(GenomicSetMember).join(
                (ParticipantSummary,
                 GenomicSetMember.participantId == ParticipantSummary.participantId)
            ).filter(
                GenomicSetMember.genomicWorkflowState != GenomicWorkflowState.IGNORE,
                GenomicSetMember.genomicWorkflowState.in_((
                    GenomicWorkflowState.GEM_RPT_READY,
                    GenomicWorkflowState.A1,
                    GenomicWorkflowState.A2
                )) &
                (
                    (
                        (ParticipantSummary.consentForGenomicsROR != QuestionnaireStatus.SUBMITTED) &
                        (ParticipantSummary.consentForGenomicsRORAuthored > _date)
                    ) |

                    (
                        (ParticipantSummary.consentForStudyEnrollment != QuestionnaireStatus.SUBMITTED) &
                        (ParticipantSummary.consentForStudyEnrollmentAuthored > _date) &
                        (ParticipantSummary.withdrawalStatus == WithdrawalStatus.NO_USE)
                    )
                )
            ).all()
        return members

    def get_reconsented_gror_since_date(self, _date):
        """
        Get the genomic set members with GROR updated to Yes Consent since date
        after having report marked for deletion
        :param _date:
        :return: GenomicSetMember list
        """
        with self.session() as session:
            members = session.query(GenomicSetMember).join(
                (ParticipantSummary,
                 GenomicSetMember.participantId == ParticipantSummary.participantId)
            ).filter(
                GenomicSetMember.genomicWorkflowState != GenomicWorkflowState.IGNORE,
                GenomicSetMember.genomicWorkflowState.in_((
                    GenomicWorkflowState.GEM_RPT_PENDING_DELETE,
                    GenomicWorkflowState.GEM_RPT_DELETED,
                )) &
                (
                    (
                        (ParticipantSummary.consentForGenomicsROR == QuestionnaireStatus.SUBMITTED) &
                        (ParticipantSummary.consentForGenomicsRORAuthored > _date)
                    ) |
                    (
                        (ParticipantSummary.consentForStudyEnrollment == QuestionnaireStatus.SUBMITTED) &
                        (ParticipantSummary.consentForStudyEnrollmentAuthored > _date)
                    )
                )
            ).all()
        return members

    def get_control_sample_parent(self, genome_type, sample_id):
        """
        Returns the GenomicSetMember parent record for a control sample
        :param genome_type:
        :param sample_id:
        :return: GenomicSetMember
        """
        with self.session() as session:
            return session.query(
                GenomicSetMember
            ).filter(
                GenomicSetMember.genomicWorkflowState == GenomicWorkflowState.CONTROL_SAMPLE,
                GenomicSetMember.sampleId == sample_id,
                GenomicSetMember.genomeType == genome_type
            ).one_or_none()

    def get_control_sample_for_gc_and_genome_type(self, _site, genome_type, biobank_id,
                                                  collection_tube_id, sample_id):
        """
        Returns the GenomicSetMember record for a control sample based on
        GC site, genome type, biobank ID, and collection tube ID.

        :param collection_tube_id:
        :param biobank_id:
        :param genome_type:
        :param sample_id:
        :return: GenomicSetMember
        """
        with self.session() as session:
            return session.query(
                GenomicSetMember
            ).filter(
                GenomicSetMember.sampleId == sample_id,
                GenomicSetMember.genomeType == genome_type,
                GenomicSetMember.gcSiteId == _site,
                GenomicSetMember.biobankId == biobank_id,
                GenomicSetMember.collectionTubeId == collection_tube_id,
                GenomicSetMember.genomicWorkflowState != GenomicWorkflowState.IGNORE
            ).one_or_none()


class GenomicJobRunDao(UpdatableDao):
    """ Stub for GenomicJobRun model """

    def from_client_json(self):
        """As of 2019-11-15 There is no API requirement"""
        pass

    validate_version_match = False

    def __init__(self):
        super(GenomicJobRunDao, self).__init__(GenomicJobRun, order_by_ending=['id'])
        self.member_dao = GenomicSetMemberDao()

    def get_id(self, obj):
        return obj.id

    def get_last_successful_runtime(self, job_id):
        with self.session() as session:
            return self._get_last_runtime_with_session(session, job_id)

    def _get_last_runtime_with_session(self, session, job_id):
        return session.query(functions.max(GenomicJobRun.startTime))\
            .filter(GenomicJobRun.jobId == job_id,
                    GenomicJobRun.runResult == GenomicSubProcessResult.SUCCESS)\
            .one()[0]

    def insert_run_record(self, job_id):
        """
        Inserts the job_run record.
        :param job_id: the ID of the current genomic job
        :return: the object inserted
        """
        job_run = GenomicJobRun()
        job_run.jobId = job_id
        job_run.startTime = clock.CLOCK.now()

        return self.insert(job_run)

    def update_run_record(self, run_id, result, status):
        with self.session() as session:
            return self._update_run_record_with_session(session, run_id, result, status)

    def _update_run_record_with_session(self, session, run_id, result, status):
        """
        UPDATES the job_run record.
        :param run_id: the ID of the current genomic's job
        :param result: the result dict of the run.
        """
        query = (
            sqlalchemy.update(GenomicJobRun)
                .where(GenomicJobRun.id == sqlalchemy.bindparam("run_id_param"))
                .values(
                {
                    GenomicJobRun.runResult: sqlalchemy.bindparam("run_result_param"),
                    GenomicJobRun.runStatus: sqlalchemy.bindparam("run_status_param"),
                    GenomicJobRun.endTime: sqlalchemy.bindparam("end_time_param"),
                }
            )
        )
        query_params = {
            "run_id_param": run_id,
            "run_result_param": result,
            "run_status_param": status,
            "end_time_param": clock.CLOCK.now()
        }
        return session.execute(query, query_params)


class GenomicFileProcessedDao(UpdatableDao):
    """ Stub for GenomicFileProcessed model """

    def from_client_json(self):
        """As of 2019-11-15 There is no API requirement"""
        pass

    validate_version_match = False

    def __init__(self):
        super(GenomicFileProcessedDao, self).__init__(
            GenomicFileProcessed, order_by_ending=['id'])
        self.member_dao = GenomicSetMemberDao()

    def get_id(self, obj):
        return obj.id

    def get_max_file_processed_for_filepath(self, filepath):
        """
        Looks up the latest GenomicFileProcessed object associated to the filepath
        :param filepath:
        :return: GenomicFileProcessed object
        """
        with self.session() as session:
            return session.query(GenomicFileProcessed).filter(
                GenomicFileProcessed.filePath == filepath
            ).order_by(GenomicFileProcessed.id.desc()).first()

    def get_files_for_run(self, run_id):
        """
        Returns the list of GenomicFileProcessed objects for a run ID.
        :param run_id:
        :return: list of GenomicFileProcessed objects
        """
        with self.session() as session:
            file_list = session.query(GenomicFileProcessed).filter(
                GenomicFileProcessed.runId == run_id).all()
        return file_list

    def get_files_for_job_id(self, job_id):
        """
        Returns the list of GenomicFileProcessed objects for a job ID.
        :param job_id: from participant_enums.GenomicJob
        :return: list of GenomicFileProcessed objects
        """
        with self.session() as session:
            file_list = session.query(GenomicFileProcessed).join(
                GenomicJobRun,
                GenomicJobRun.id == GenomicFileProcessed.runId
            ).filter(
                GenomicJobRun.jobId == job_id
            ).all()
        return file_list

    def insert_file_record(self, run_id,
                           path,
                           bucket_name,
                           file_name,
                           end_time=None,
                           file_result=None,
                           upload_date=None,
                           manifest_file_id=None):
        """
        Inserts the file record
        :param run_id: the id of the current genomics_job_run
        :param path: the path of the current file to be inserted
        :param bucket_name: name of Google Cloud bucket being processed
        :param file_name: name of file being processed
        :param upload_date: the date the file was uploaded to the bucket
        :return: the inserted GenomicFileProcessed object
        """
        processing_file = GenomicFileProcessed()
        processing_file.runId = run_id
        processing_file.filePath = path
        processing_file.bucketName = bucket_name
        processing_file.fileName = file_name
        processing_file.startTime = clock.CLOCK.now()
        processing_file.endTime = end_time
        processing_file.fileResult = file_result
        processing_file.uploadDate = upload_date
        processing_file.genomicManifestFileId = manifest_file_id

        return self.insert(processing_file)

    def update_file_record(self, file_id, file_status, file_result):
        with self.session() as session:
            return self._update_file_record_with_session(session, file_id,
                                                         file_status, file_result)

    def _update_file_record_with_session(self, session, file_id,
                                         file_status, file_result):
        """
        updates the file record with session
        :param file_id: id of file to update
        :param file_status: status of file processing
        :param file_result: result of file processing
        :return:
        """
        query = (
            sqlalchemy.update(GenomicFileProcessed)
                .where(GenomicFileProcessed.id == sqlalchemy.bindparam("file_id_param"))
                .values(
                {
                    GenomicFileProcessed.fileStatus: sqlalchemy.bindparam("file_status_param"),
                    GenomicFileProcessed.fileResult: sqlalchemy.bindparam("file_result_param"),
                    GenomicFileProcessed.endTime: sqlalchemy.bindparam("end_time_param"),
                }
            )
        )
        query_params = {
            "file_id_param": file_id,
            "file_status_param": file_status,
            "file_result_param": file_result,
            "end_time_param": clock.CLOCK.now()
        }
        return session.execute(query, query_params)


class GenomicGCValidationMetricsDao(UpsertableDao):
    """ Stub for GenomicGCValidationMetrics model """

    def from_client_json(self):
        """As of 2019-11-15 There is no API requirement"""
        pass

    validate_version_match = False

    def __init__(self):
        super(GenomicGCValidationMetricsDao, self).__init__(
            GenomicGCValidationMetrics, order_by_ending=['id'])
        self.member_dao = GenomicSetMemberDao()

        self.data_mappings = {
            'genomicSetMemberId': 'member_id',
            'genomicFileProcessedId': 'file_id',
            'limsId': 'limsid',
            'chipwellbarcode': 'chipwellbarcode',
            'callRate': 'callrate',
            'meanCoverage': 'meancoverage',
            'genomeCoverage': 'genomecoverage',
            'aouHdrCoverage': 'aouhdrcoverage',
            'contamination': 'contamination',
            'contaminationCategory': 'contamination_category',
            'sexConcordance': 'sexconcordance',
            'sexPloidy': 'sexploidy',
            'alignedQ30Bases': 'alignedq30bases',
            'arrayConcordance': 'arrayconcordance',
            'processingStatus': 'processingstatus',
            'notes': 'notes',
            'siteId': 'siteid',
        }
        # The mapping between the columns in the DB and the data to ingest

        self.deleted_flag_mappings = {
            'idatRedDeleted': 'redidat',
            'idatRedMd5Deleted': 'redidatmd5',
            'idatGreenDeleted': 'greenidat',
            'idatGreenMd5Deleted': 'greenidatmd5',
            'vcfDeleted': 'vcf',
            'vcfTbiDeleted': 'vcfindex',
            'vcfMd5Deleted': 'vcfmd5',
            'hfVcfDeleted': 'vcfhf',
            'hfVcfTbiDeleted': 'vcfhfindex',
            'hfVcfMd5Deleted': 'vcfhfmd5',
            'rawVcfDeleted': 'vcfraw',
            'rawVcfTbiDeleted': 'vcfrawindex',
            'rawVcfMd5Deleted': 'vcfrawmd5',
            'cramDeleted': 'cram',
            'cramMd5Deleted': 'crammd5',
            'craiDeleted': 'crai',
        }

    def get_id(self, obj):
        return obj.id

    def upsert_gc_validation_metrics_from_dict(self, data_to_upsert, existing_id=None):
        """
        Upsert a GC validation metrics object
        :param data_to_upsert: dictionary of row-data from AW2 file to insert
        :param existing_id: an existing metrics ID if it is available
        :return: upserted metrics object
        """
        gc_metrics_obj = GenomicGCValidationMetrics()
        gc_metrics_obj.id = existing_id
        for key in self.data_mappings.keys():
            try:
                gc_metrics_obj.__setattr__(key, data_to_upsert[self.data_mappings[key]])
            except KeyError:
                gc_metrics_obj.__setattr__(key, None)

        logging.info(f'Inserting GC Metrics for member ID {gc_metrics_obj.genomicSetMemberId}.')
        upserted_metrics_obj = self.upsert(gc_metrics_obj)

        return upserted_metrics_obj

    def update_gc_validation_metrics_deleted_flags_from_dict(self, data_to_upsert, existing_id):
        """
        Upsert a GC validation metrics object
        :param data_to_upsert: dictionary of row-data from AW2 file to insert
        :param existing_id: an existing metrics ID
        :return: upserted metrics object
        """
        gc_metrics_obj = GenomicGCValidationMetrics()
        gc_metrics_obj.id = existing_id
        for key in self.deleted_flag_mappings.keys():
            try:
                gc_metrics_obj.__setattr__(key, 1 if data_to_upsert[self.deleted_flag_mappings[key]] == 'D' else 0)
            except KeyError:
                # if the key is not in the file, do nothing
                pass

        logging.info(f'Updating deletion flag of GC Metrics for member ID {gc_metrics_obj.genomicSetMemberId}.')
        upserted_metrics_obj = self.upsert(gc_metrics_obj)

        return upserted_metrics_obj

    def get_null_set_members(self):
        """
        Retrieves all gc metrics with a null genomic_set_member_id
        :return: list of returned GenomicGCValidationMetrics objects
        """
        with self.session() as session:
            return (
                session.query(GenomicGCValidationMetrics)
                .filter(GenomicGCValidationMetrics.genomicSetMemberId == None,
                        GenomicGCValidationMetrics.ignoreFlag != 1)
                .all()
            )

    def get_with_missing_gen_files(self, _date, _gc_site_id):
        """
        Retrieves all gc metrics with missing genotyping files for a gc
        :param: _date: last run time
        :param: _gc_site_id: 'uw', 'bcm', 'jh', 'bi', etc.
        :return: list of returned GenomicGCValidationMetrics objects
        """
        with self.session() as session:
            return (
                session.query(GenomicGCValidationMetrics)
                .join(
                    (GenomicSetMember,
                     GenomicSetMember.id == GenomicGCValidationMetrics.genomicSetMemberId)
                )
                .filter(
                    GenomicSetMember.genomicWorkflowState != GenomicWorkflowState.IGNORE,
                    GenomicSetMember.genomeType == config.GENOME_TYPE_ARRAY,
                    GenomicSetMember.gcSiteId == _gc_site_id,
                    GenomicGCValidationMetrics.genomicFileProcessedId != None,
                    sqlalchemy.func.lower(GenomicGCValidationMetrics.processingStatus) == "pass",
                    GenomicGCValidationMetrics.modified > _date,
                    ((GenomicGCValidationMetrics.ignoreFlag != 1) |
                     (GenomicGCValidationMetrics.ignoreFlag is not None)),
                    (GenomicGCValidationMetrics.idatRedReceived == 0) |
                    (GenomicGCValidationMetrics.idatGreenReceived == 0) |
                    (GenomicGCValidationMetrics.idatRedMd5Received == 0) |
                    (GenomicGCValidationMetrics.idatGreenMd5Received == 0) |
                    (GenomicGCValidationMetrics.vcfReceived == 0) |
                    (GenomicGCValidationMetrics.vcfTbiReceived == 0) |
                    (GenomicGCValidationMetrics.vcfMd5Received == 0)
                )
                .all()
            )

    def get_with_missing_seq_files(self, _date, _gc_site_id):
        """
        Retrieves all gc metrics with missing sequencing files
        :param: _date: last run time
        :param: _gc_site_id: 'uw', 'bcm', 'jh', 'bi', etc.
        :return: list of returned GenomicGCValidationMetrics objects
        """
        with self.session() as session:
            return (
                session.query(GenomicGCValidationMetrics,
                              GenomicSetMember.biobankId,
                              GenomicSetMember.sampleId,)
                .join(
                    (GenomicSetMember,
                     GenomicSetMember.id == GenomicGCValidationMetrics.genomicSetMemberId)
                )
                .filter(
                    GenomicSetMember.genomicWorkflowState != GenomicWorkflowState.IGNORE,
                    GenomicSetMember.genomeType == config.GENOME_TYPE_WGS,
                    GenomicSetMember.gcSiteId == _gc_site_id,
                    GenomicGCValidationMetrics.genomicFileProcessedId != None,
                    sqlalchemy.func.lower(GenomicGCValidationMetrics.processingStatus) == "pass",
                    GenomicGCValidationMetrics.modified > _date,
                    ((GenomicGCValidationMetrics.ignoreFlag != 1) | (
                            GenomicGCValidationMetrics.ignoreFlag is not None)),
                    (GenomicGCValidationMetrics.hfVcfReceived == 0) |
                    (GenomicGCValidationMetrics.hfVcfTbiReceived == 0) |
                    (GenomicGCValidationMetrics.hfVcfMd5Received == 0) |
                    (GenomicGCValidationMetrics.rawVcfReceived == 0) |
                    (GenomicGCValidationMetrics.rawVcfTbiReceived == 0) |
                    (GenomicGCValidationMetrics.rawVcfMd5Received == 0) |
                    (GenomicGCValidationMetrics.cramReceived == 0) |
                    (GenomicGCValidationMetrics.cramMd5Received == 0) |
                    (GenomicGCValidationMetrics.craiReceived == 0)
                )
                .all()
            )

    def get_metrics_by_member_id(self, member_id):
        """
        Retrieves gc metric record with the member_id
        :param: member_id
        :return: GenomicGCValidationMetrics object
        """
        with self.session() as session:
            return (
                session.query(GenomicGCValidationMetrics)
                .filter(GenomicGCValidationMetrics.genomicSetMemberId == member_id,
                        GenomicGCValidationMetrics.ignoreFlag != 1)
                .one_or_none()
            )

    def get_metric_record_counts_from_filepath(self, filepath):
        with self.session() as session:
            return session.query(
                functions.count(GenomicGCValidationMetrics.id)
            ).join(
                GenomicManifestFile,
                GenomicManifestFile.id == GenomicGCValidationMetrics.genomicFileProcessedId
            ).filter(
                GenomicManifestFile.filePath == filepath
            ).one_or_none()

    def update_metric_set_member_id(self, metric_obj, member_id):
        """
        Updates the record with the reconciliation data.
        :param metric_obj:
        :param member_id:
        :return: query result or result code of error
        """
        metric_obj.genomicSetMemberId = member_id
        try:
            return self.update(metric_obj)
        except OperationalError:
            return GenomicSubProcessResult.ERROR


class GenomicPiiDao(BaseDao):
    def __init__(self):
        super(GenomicPiiDao, self).__init__(
            GenomicSetMember, order_by_ending=['id'])

    def get_id(self, obj):
        pass

    def from_client_json(self):
        pass

    def to_client_json(self, result):
        if result['data'].consentForGenomicsROR == QuestionnaireStatus.SUBMITTED:
            if result['mode'] == 'GEM':
                return {
                    "biobank_id": result['data'].biobankId,
                    "first_name": result['data'].firstName,
                    "last_name": result['data'].lastName,
                    "sex_at_birth": result['data'].sexAtBirth,
                }

            elif result['mode'] == 'RHP':
                return {
                    "biobank_id": result['data'].biobankId,
                    "first_name": result['data'].firstName,
                    "last_name": result['data'].lastName,
                    "date_of_birth": result['data'].dateOfBirth,
                    "sex_at_birth": result['data'].sexAtBirth,
                }
            else:
                return {"message": "Only GEM and RHP modes supported."}

        else:
            return {"message": "No RoR consent."}

    def get_by_pid(self, pid):
        """
        Returns Biobank ID, First Name, and Last Name for requested PID
        :param pid:
        :return: query results for PID
        """
        with self.session() as session:
            return (
                session.query(GenomicSetMember.biobankId,
                              ParticipantSummary.firstName,
                              ParticipantSummary.lastName,
                              ParticipantSummary.consentForGenomicsROR,
                              ParticipantSummary.dateOfBirth,
                              GenomicSetMember.sexAtBirth,)
                .join(
                    ParticipantSummary,
                    GenomicSetMember.participantId == ParticipantSummary.participantId,
                ).filter(
                    GenomicSetMember.participantId == pid,
                    GenomicSetMember.gemPass == "Y",
                    ParticipantSummary.withdrawalStatus == WithdrawalStatus.NOT_WITHDRAWN,
                    ParticipantSummary.withdrawalStatus == SuspensionStatus.NOT_SUSPENDED,
                    GenomicSetMember.genomicWorkflowState != GenomicWorkflowState.IGNORE,
                ).first()
            )


class GenomicOutreachDao(BaseDao):
    def __init__(self):
        super(GenomicOutreachDao, self).__init__(
            GenomicSetMember, order_by_ending=['id'])

    def get_id(self, obj):
        pass

    def from_client_json(self, resource, participant_id=None, mode=None):
        if mode is None or mode.lower() not in config.GENOMIC_API_MODES:
            raise BadRequest(f"GenomicOutreach Mode required to be one of {config.GENOMIC_API_MODES}.")

        genome_type = config.GENOME_TYPE_ARRAY

        if mode.lower() == "rhp":
            genome_type = config.GENOME_TYPE_WGS

        # Ensure PID exists
        p = ParticipantDao().get(participant_id)
        if p is None:
            raise NotFound(f'P{participant_id} is not found.')

        try:
            report_state = self._determine_report_state(resource['status'].lower())
            modified_date = parser.parse(resource['date'])

        except KeyError:
            raise BadRequest("Resource is missing required fields: status, date")

        member = GenomicSetMember(participantId=participant_id,
                                  genomicSetId=1,
                                  genomeType=genome_type,
                                  genomicWorkflowState=report_state,
                                  genomicWorkflowStateModifiedTime=modified_date)

        return member


    def to_client_json(self, result):
        report_statuses = list()

        for participant in result['data']:
            if participant[1] == GenomicWorkflowState.GEM_RPT_READY:
                status = "ready"

            elif participant[1] == GenomicWorkflowState.GEM_RPT_PENDING_DELETE:
                status = "pending_delete"

            elif participant[1] == GenomicWorkflowState.GEM_RPT_DELETED:
                status = "deleted"

            else:
                status = "unset"

            report_statuses.append(
                {
                    "participant_id": f'P{participant[0]}',
                    "report_status": status
                 }
            )

        # handle date
        try:
            ts = pytz.utc.localize(result['date'])
        except ValueError:
            ts = result['date']

        client_json = {
            "participant_report_statuses": report_statuses,
            "timestamp": ts
        }
        return client_json

    def participant_state_lookup(self, pid):
        """
        Returns GEM report status for pid
        :param pid:
        :return:
        """
        with self.session() as session:
            return (
                session.query(GenomicSetMember.participantId,
                              GenomicSetMember.genomicWorkflowState)
                .join(
                    ParticipantSummary,
                    GenomicSetMember.participantId == ParticipantSummary.participantId,
                ).filter(
                    ParticipantSummary.withdrawalStatus == WithdrawalStatus.NOT_WITHDRAWN,
                    ParticipantSummary.suspensionStatus == SuspensionStatus.NOT_SUSPENDED,
                    GenomicSetMember.genomicWorkflowState.in_((GenomicWorkflowState.GEM_RPT_READY,
                                                               GenomicWorkflowState.GEM_RPT_PENDING_DELETE,
                                                               GenomicWorkflowState.GEM_RPT_DELETED)),
                    ParticipantSummary.participantId == pid
                ).all()
            )

    def date_lookup(self, start_date, end_date=None):
        """
        Returns list of PIDs and GEM report status
        :param start_date:
        :param end_date:
        :return: lists of PIDs and report states
        """
        as_of_ts = clock.CLOCK.now()
        if end_date is None:
            end_date = as_of_ts

        with self.session() as session:
            return (
                session.query(GenomicSetMember.participantId,
                              GenomicSetMember.genomicWorkflowState)
                .join(
                    ParticipantSummary,
                    GenomicSetMember.participantId == ParticipantSummary.participantId,
                ).filter(
                    ParticipantSummary.withdrawalStatus == WithdrawalStatus.NOT_WITHDRAWN,
                    ParticipantSummary.suspensionStatus == SuspensionStatus.NOT_SUSPENDED,
                    GenomicSetMember.genomicWorkflowState.in_((GenomicWorkflowState.GEM_RPT_READY,
                                                               GenomicWorkflowState.GEM_RPT_PENDING_DELETE,
                                                               GenomicWorkflowState.GEM_RPT_DELETED)),
                    GenomicSetMember.genomicWorkflowStateModifiedTime > start_date,
                    GenomicSetMember.genomicWorkflowStateModifiedTime < end_date,
                ).all()
            )

    @staticmethod
    def _determine_report_state(resource_status):
        """
        Reads 'resource_status' from ptsc and determines which report state value
        to set
        :param resource_status: string
        :return: GenomicWorkflowState for report state
        """
        state_mapping = {
            "ready": GenomicWorkflowState.GEM_RPT_READY,
            "pending_delete": GenomicWorkflowState.GEM_RPT_PENDING_DELETE,
            "deleted": GenomicWorkflowState.GEM_RPT_DELETED,
        }

        return state_mapping[resource_status]


class GenomicManifestFileDao(BaseDao):
    def __init__(self):
        super(GenomicManifestFileDao, self).__init__(
            GenomicManifestFile, order_by_ending=['id'])

    def get_id(self, obj):
        pass

    def from_client_json(self):
        pass

    def get_manifest_file_from_filepath(self, filepath):
        with self.session() as session:
            return session.query(GenomicManifestFile).filter(
                GenomicManifestFile.filePath == filepath,
                GenomicManifestFile.ignore_flag == 0
            ).one_or_none()

    def count_records_for_manifest_file(self, manifest_file_obj):

        with self.session() as session:
            if manifest_file_obj.manifestTypeId == GenomicManifestTypes.BIOBANK_GC:
                return session.query(
                    functions.count(GenomicSetMember.id)
                ).join(
                        GenomicFileProcessed,
                        GenomicFileProcessed.id == GenomicSetMember.aw1FileProcessedId
                ).join(
                    GenomicManifestFile,
                    GenomicManifestFile.id == GenomicFileProcessed.genomicManifestFileId
                ).filter(
                    GenomicManifestFile.id == manifest_file_obj.id
                ).one_or_none()

    def get_record_count_from_filepath(self, filepath):
        with self.session() as session:
            return session.query(
                GenomicManifestFile.recordCount
            ).filter(
                GenomicManifestFile.filePath == filepath
            ).first()

    def update_record_count(self, manifest_file_obj, new_rec_count, project_id=None):

        with self.session() as session:
            manifest_file_obj.recordCount = new_rec_count
            session.merge(manifest_file_obj)

            bq_genomic_manifest_file_update(manifest_file_obj.id, project_id=project_id)
            genomic_manifest_file_update(manifest_file_obj.id)


class GenomicManifestFeedbackDao(BaseDao):
    def __init__(self):
        super(GenomicManifestFeedbackDao, self).__init__(
            GenomicManifestFeedback, order_by_ending=['id'])

    def get_id(self, obj):
        pass

    def from_client_json(self):
        pass

    def get_feedback_record_from_manifest_id(self, manifest_id):
        """
        Returns the feedback record for a manifest ID
        :param manifest_id:
        :return: GenomicManifestFeedback object
        """
        with self.session() as session:
            return session.query(GenomicManifestFeedback).filter(
                GenomicManifestFeedback.inputManifestFileId == manifest_id,
                GenomicManifestFeedback.ignoreFlag == 0
            ).one_or_none()

    def increment_feedback_count(self, manifest_id, _project_id):
        """
        Update the manifest feedback record's count
        :param _project_id:
        :param manifest_id:
        :return:
        """
        fb = self.get_feedback_record_from_manifest_id(manifest_id)

        # Increment and update the record
        if fb is not None:
            fb.feedbackRecordCount += 1

            with self.session() as session:
                session.merge(fb)

            bq_genomic_manifest_feedback_update(fb.id, project_id=_project_id)
            genomic_manifest_feedback_update(fb.id)
        else:
            raise ValueError(f'No feedback record for manifest id {manifest_id}')

    def get_feedback_equals_record_count(self):
        """
        Retrieves feedback records where feedback count = record_count
        :return: list of feedback records
        """

        with self.session() as session:
            results = session.query(GenomicManifestFeedback).join(
                GenomicManifestFile,
                GenomicManifestFile.id == GenomicManifestFeedback.inputManifestFileId
            ).filter(
                GenomicManifestFeedback.ignoreFlag == 0,
                GenomicManifestFeedback.feedbackRecordCount == GenomicManifestFile.recordCount,
                GenomicManifestFeedback.feedbackManifestFileId == None,
            ).all()

        return list(results)

    def get_feedback_record_counts_from_filepath(self, filepath):
        with self.session() as session:
            return session.query(
                GenomicManifestFeedback.feedbackRecordCount
            ).filter(
                GenomicManifestFile.filePath == filepath
            ).first()

    def get_feedback_records_for_aw2f(self):
        pass


class GenomicAW1RawDao(BaseDao):
    def __init__(self):
        super(GenomicAW1RawDao, self).__init__(
            GenomicAW1Raw, order_by_ending=['id'])

    def get_id(self, obj):
        pass

    def from_client_json(self):
        pass

    def get_from_filepath(self, filepath):
        with self.session() as session:
            return session.query(
                GenomicAW1Raw
            ).filter(
                GenomicAW1Raw.file_path == filepath
            ).all()

    def get_record_count_from_filepath(self, filepath):
        with self.session() as session:
            return session.query(
                functions.count(GenomicAW1Raw.id)
            ).filter(
                GenomicAW1Raw.file_path == filepath
            ).one_or_none()


class GenomicAW2RawDao(BaseDao):
    def __init__(self):
        super(GenomicAW2RawDao, self).__init__(
            GenomicAW2Raw, order_by_ending=['id'])

    def get_id(self, obj):
        pass

    def from_client_json(self):
        pass

    def get_from_filepath(self, filepath):
        with self.session() as session:
            return session.query(
                GenomicAW2Raw
            ).filter(
                GenomicAW2Raw.file_path == filepath
            ).all()

    def get_record_count_from_filepath(self, filepath):
        with self.session() as session:
            return session.query(
                functions.count(GenomicAW2Raw.id)
            ).filter(
                GenomicAW2Raw.file_path == filepath
            ).one_or_none()


class GenomicIncidentDao(UpdatableDao):
    def __init__(self):
        super(GenomicIncidentDao, self).__init__(
            GenomicIncident, order_by_ending=['id'])

    def get_id(self, obj):
        pass

    def from_client_json(self):
        pass
