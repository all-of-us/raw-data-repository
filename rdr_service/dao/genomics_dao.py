import collections

import pytz
import sqlalchemy
import logging

from sqlalchemy.exc import OperationalError
from sqlalchemy.sql import functions

from rdr_service import clock
from rdr_service.dao.base_dao import UpdatableDao, BaseDao
from rdr_service.model.genomics import (
    GenomicSet,
    GenomicSetMember,
    GenomicJobRun,
    GenomicFileProcessed,
    GenomicGCValidationMetrics
)
from rdr_service.participant_enums import (
    GenomicSetStatus,
    GenomicSetMemberStatus,
    GenomicSubProcessResult,
    QuestionnaireStatus,
    WithdrawalStatus,
    SuspensionStatus,
    GenomicWorkflowState,
)
from rdr_service.model.participant import Participant
from rdr_service.model.participant_summary import ParticipantSummary
from rdr_service.query import FieldFilter, Operator, OrderBy, Query


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

    def get_members_from_set_id(self, set_id):
        """
        Retrieves all genomic set member records matching the set_id
        :param set_id
        :return: result set of GenomicSetMembers
        """
        with self.session() as session:
            return session.query(GenomicSetMember).filter(
                GenomicSetMember.genomicSetId == set_id,
                GenomicSetMember.genomicWorkflowState != GenomicWorkflowState.IGNORE,
            ).all()

    def update_member_job_run_id(self, member, job_run_id, field):
        """
        Updates the GenomicSetMember with a job_run_id for an arbitrary workflow
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
            return self.update(member)
        except OperationalError:
            logging.error(f'Error updating member id: {member.id}.')
            return GenomicSubProcessResult.ERROR

    def update_member_state(self, member, new_state):
        """
        Sets the member's state to a new state
        :param member: GenomicWorkflowState
        :param new_state:
        """

        member.genomicWorkflowState = new_state
        return self.update(member)

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

    def get_null_field_members(self, field):
        """
        Get the genomic set members with a null value for a field
        useful for reconciliation processes.
        :param field: field to lookup null
        :return: GenomicSetMember list
        """
        with self.session() as session:
            members = session.query(GenomicSetMember).filter(
                getattr(GenomicSetMember, field) == None,
                GenomicSetMember.genomicWorkflowState != GenomicWorkflowState.IGNORE
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
                        (ParticipantSummary.consentForStudyEnrollmentAuthored > _date)
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

    def get_control_sample(self, sample_id):
        """
        Returns the GenomicSetMember record for a control sample
        :param sample_id:
        :return: GenomicSetMember
        """
        with self.session() as session:
            return session.query(
                GenomicSetMember
            ).filter(
                GenomicSetMember.genomicWorkflowState == GenomicWorkflowState.CONTROL_SAMPLE,
                GenomicSetMember.sampleId == sample_id
            ).first()


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
                           file_result=None):
        """
        Inserts the file record
        :param run_id: the id of the current genomics_job_run
        :param path: the path of the current file to be inserted
        :param bucket_name: name of Google Cloud bucket being processed
        :param file_name: name of file being processed
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


class GenomicGCValidationMetricsDao(UpdatableDao):
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
            'sexConcordance': 'sexconcordance',
            'sexPloidy': 'sexploidy',
            'alignedQ30Bases': 'alignedq30bases',
            'arrayConcordance': 'arrayconcordance',
            'processingStatus': 'processingstatus',
            'notes': 'notes',
            'siteId': 'siteid',
        }
        # The mapping between the columns in the DB and the data to ingest

    def get_id(self, obj):
        return obj.id

    def insert_gc_validation_metrics_batch(self, data_to_insert):
        """
        Inserts a batch of GC validation metrics
        :param data_to_insert: list of dictionary rows to insert
        :return: result code
        """
        try:
            for row in data_to_insert:
                gc_metrics_obj = GenomicGCValidationMetrics()
                for key in self.data_mappings.keys():
                    try:
                        gc_metrics_obj.__setattr__(key, row[self.data_mappings[key]])
                    except KeyError:
                        gc_metrics_obj.__setattr__(key, None)
                self.insert(gc_metrics_obj)
            return GenomicSubProcessResult.SUCCESS
        except RuntimeError:
            return GenomicSubProcessResult.ERROR

    def get_null_set_members(self):
        """
        Retrieves all gc metrics with a null genomic_set_member_id
        :return: list of returned GenomicGCValidationMetrics objects
        """
        with self.session() as session:
            return (
                session.query(GenomicGCValidationMetrics)
                .filter(GenomicGCValidationMetrics.genomicSetMemberId == None)
                .all()
            )

    def get_with_missing_gen_files(self):
        """
        Retrieves all gc metrics with missing genotyping files
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

    def get_with_missing_seq_files(self):
        """
        Retrieves all gc metrics with missing sequencing files
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
                .filter(GenomicGCValidationMetrics.genomicSetMemberId == member_id)
                .first()
            )

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

    def from_client_json(self):
        pass

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

        client_json = {
            "participant_report_statuses": report_statuses,
            "timestamp": pytz.utc.localize(result['date'])
        }
        return client_json

    def participant_lookup(self, pid):
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
                    ParticipantSummary.consentForGenomicsRORAuthored > start_date,
                    ParticipantSummary.consentForGenomicsRORAuthored < end_date,
                ).all()
            )
