import collections
import logging
import os

import pytz
import sqlalchemy

from datetime import datetime, timedelta
from dateutil import parser

from sqlalchemy import and_, or_, func
from sqlalchemy.exc import OperationalError
from sqlalchemy.orm import aliased, Query
from sqlalchemy.sql import functions
from sqlalchemy.sql.expression import literal, distinct

from typing import List, Dict, Tuple

from sqlalchemy.sql.functions import coalesce
from werkzeug.exceptions import BadRequest, NotFound

from rdr_service import clock, code_constants, config
from rdr_service.clock import CLOCK
from rdr_service.config import GAE_PROJECT
from rdr_service.genomic_enums import GenomicJob, GenomicIncidentStatus, GenomicQcStatus, GenomicSubProcessStatus, \
    ResultsWorkflowState, ResultsModuleType
from rdr_service.dao.base_dao import UpdatableDao, BaseDao, UpsertableDao
from rdr_service.dao.participant_dao import ParticipantDao
from rdr_service.model.code import Code
from rdr_service.model.config_utils import get_biobank_id_prefix
from rdr_service.model.genomics import (
    GenomicSet,
    GenomicSetMember,
    GenomicJobRun,
    GenomicFileProcessed,
    GenomicGCValidationMetrics,
    GenomicManifestFile,
    GenomicManifestFeedback,
    GenomicAW1Raw,
    GenomicAW2Raw,
    GenomicIncident,
    GenomicCloudRequests,
    GenomicMemberReportState,
    GenomicInformingLoop,
    GenomicGcDataFile, GenomicGcDataFileMissing, GcDataFileStaging, GemToGpMigration, UserEventMetrics,
    GenomicResultViewed, GenomicAW3Raw, GenomicAW4Raw, GenomicW2SCRaw, GenomicW3SRRaw, GenomicW4WRRaw,
    GenomicCVLAnalysis, GenomicW3SCRaw, GenomicResultWorkflowState, GenomicW3NSRaw, GenomicW5NFRaw, GenomicW3SSRaw,
    GenomicCVLSecondSample, GenomicW2WRaw, GenomicW1ILRaw, GenomicCVLResultPastDue, GenomicSampleSwapMember,
    GenomicSampleSwap, GenomicAppointmentEvent, GenomicResultWithdrawals, GenomicAppointmentEventMetrics,
    GenomicAppointmentEventNotified, GenomicStorageUpdate, GenomicGCROutreachEscalationNotified)
from rdr_service.model.questionnaire import QuestionnaireConcept, QuestionnaireQuestion
from rdr_service.model.questionnaire_response import QuestionnaireResponse, QuestionnaireResponseAnswer
from rdr_service.participant_enums import (
    QuestionnaireStatus,
    WithdrawalStatus,
    SuspensionStatus, DeceasedStatus)
from rdr_service.genomic_enums import GenomicSetStatus, GenomicSetMemberStatus, GenomicWorkflowState, \
    GenomicSubProcessResult, GenomicManifestTypes, GenomicReportState, GenomicContaminationCategory
from rdr_service.model.biobank_order import BiobankOrder, BiobankOrderIdentifier
from rdr_service.model.biobank_stored_sample import BiobankStoredSample
from rdr_service.model.participant import Participant
from rdr_service.model.participant_summary import ParticipantSummary
from rdr_service.query import FieldFilter, Operator, OrderBy, Query
from rdr_service.genomic.genomic_mappings import genome_type_to_aw1_aw2_file_prefix as genome_type_map, \
    cvl_result_reconciliation_modules, message_broker_report_ready_event_state_mappings, \
    message_broker_report_viewed_event_state_mappings
from rdr_service.genomic.genomic_mappings import informing_loop_event_mappings
from rdr_service.genomic.genomic_mappings import wgs_file_types_attributes, array_file_types_attributes


class GenomicDaoMixin:

    ingestion_job_ids = [
        GenomicJob.METRICS_INGESTION,
        GenomicJob.AW1_MANIFEST,
        GenomicJob.AW1F_MANIFEST,
        GenomicJob.AW4_ARRAY_WORKFLOW,
        GenomicJob.AW4_WGS_WORKFLOW,
        GenomicJob.AW5_ARRAY_MANIFEST,
        GenomicJob.AW5_WGS_MANIFEST
    ]

    exclude_states = [GenomicWorkflowState.IGNORE]

    def get_last_updated_records(self, from_date, _ids=True) -> List:
        from_date = from_date.replace(microsecond=0)
        if not hasattr(self.model_type, 'modified'):
            return []

        with self.session() as session:
            if _ids:
                records = session.query(self.model_type.id)
            else:
                records = session.query(self.model_type)

            records = records.filter(
                self.model_type.modified >= from_date
            )
            return records.all()

    def get_from_filepath(self, filepath) -> List:
        if not hasattr(self.model_type, 'file_path'):
            return []

        with self.session() as session:
            return session.query(
                self.model_type
            ).filter(
                self.model_type.file_path == filepath,
                self.model_type.ignore_flag == 0,
            ).all()

    def insert_bulk(self, batch: List[Dict]) -> None:
        with self.session() as session:
            session.bulk_insert_mappings(self.model_type, batch)

    def bulk_update(self, model_objects: List[Dict]) -> None:
        with self.session() as session:
            session.bulk_update_mappings(self.model_type, model_objects)


class GenomicSetDao(UpdatableDao, GenomicDaoMixin):
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

    @staticmethod
    def _get_validation_data_query_for_genomic_set_id(genomic_set_id):
        """
    Build a sqlalchemy query for validation data.

    :type genomic_set_id: int
    :return: sqlalchemy query
    """
        existing_valid_query = (
            sqlalchemy.select([func.count().label("existing_count")])
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


class GenomicSetMemberDao(UpdatableDao, GenomicDaoMixin):
    """ Stub for GenomicSetMember model """

    validate_version_match = False

    def __init__(self):
        super(GenomicSetMemberDao, self).__init__(GenomicSetMember, order_by_ending=["id"])
        self.report_state_dao = GenomicMemberReportStateDao()

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

    @staticmethod
    def update_biobank_id_with_session(session, genomic_set_id):
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

    @staticmethod
    def bulk_update_validation_status_with_session(session, member_update_params_iterable):
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

    @staticmethod
    def bulk_update_package_id_with_session(session, genomic_set_id, client_id_package_id_pair_iterable):
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

    @staticmethod
    def bulk_update_genotyping_sample_manifest_data_with_session(session, genotyping_data_iterable):
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

    def get_member_from_biobank_id_and_sample_id(self, biobank_id, sample_id):
        """
        Retrieves a genomic set member record matching the biobank Id
        :param biobank_id:
        :param sample_id:
        :return: a GenomicSetMember object
        """
        with self.session() as session:
            member = session.query(GenomicSetMember).filter(
                GenomicSetMember.biobankId == biobank_id,
                GenomicSetMember.sampleId == sample_id,
                GenomicSetMember.ignoreFlag != 1,
                GenomicSetMember.genomicWorkflowState.notin_(self.exclude_states),
            ).one_or_none()
        return member

    def get_member_from_biobank_id_in_state(self, biobank_id, genome_type, states):
        """
        Retrieves a genomic set member record matching the biobank Id
        :param biobank_id:
        :param genome_type:
        :param states: list of genomic_workflow_states
        :return: a GenomicSetMember object
        """
        with self.session() as session:
            member = session.query(GenomicSetMember).filter(
                GenomicSetMember.biobankId == biobank_id,
                GenomicSetMember.genomeType == genome_type,
                GenomicSetMember.genomicWorkflowState.in_(states),
                GenomicSetMember.sampleId.is_(None),
            ).first()
        return member

    def get_member_from_sample_id(self, sample_id, genome_type=None):
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
                GenomicSetMember.genomicWorkflowState.notin_(self.exclude_states),
            )

            if genome_type:
                member = member.filter(
                    GenomicSetMember.genomeType == genome_type
                )
            return member.first()

    def get_members_from_sample_ids(self, sample_ids, genome_type=None):
        """
        Returns genomicSetMember objects for list of sample IDs.
        :param sample_ids:
        :param genome_type:
        :return:
        """
        with self.session() as session:
            members = session.query(GenomicSetMember).filter(
                GenomicSetMember.sampleId.in_(sample_ids),
                GenomicSetMember.genomicWorkflowState.notin_(self.exclude_states),
            )

            if genome_type:
                members = members.filter(
                    GenomicSetMember.genomeType == genome_type
                )
            return members.all()

    def get_members_from_member_ids(self, member_ids):
        with self.session() as session:
            return session.query(GenomicSetMember).filter(
                GenomicSetMember.id.in_(member_ids)
            ).all()

    def get_members_with_non_null_sample_ids(self):
        """
        For use by unit tests only
        :return: query results
        """
        if GAE_PROJECT == "localhost":
            with self.session() as session:
                return session.query(GenomicSetMember).filter(
                    GenomicSetMember.sampleId.isnot(None)
                ).all()

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
                GenomicSetMember.genomicWorkflowState.notin_(self.exclude_states),
                GenomicSetMember.genomicWorkflowState == state,
            ).first()
        return member

    def get_member_from_aw3_sample(self, sample_id):
        """
        Retrieves a genomic set member record matching the sample_id
        The sample_id is supplied in AW1 manifest, not biobank_stored_sample_id
        :param sample_id:
        :return: a GenomicSetMember object
        """
        with self.session() as session:
            member = session.query(GenomicSetMember).filter(
                GenomicSetMember.sampleId == sample_id,
                GenomicSetMember.genomicWorkflowState.notin_(self.exclude_states),
                GenomicSetMember.ignoreFlag == 0,
                GenomicSetMember.aw3ManifestJobRunID.isnot(None)
            ).one_or_none()
        return member

    def get_member_from_collection_tube(self, tube_id, genome_type, state=None):
        """
        Retrieves a genomic set member record matching the collection_tube_id
        Needs a genome type
        :param state:
        :param genome_type: aou_wgs, aou_array, aou_cvl
        :param tube_id:
        :return: a GenomicSetMember object
        """
        with self.session() as session:
            member = session.query(GenomicSetMember).filter(
                GenomicSetMember.collectionTubeId == tube_id,
                GenomicSetMember.genomeType == genome_type,
                GenomicSetMember.genomicWorkflowState.notin_(self.exclude_states),
            )
            if state:
                member = member.filter(
                    GenomicSetMember.genomicWorkflowState == state
                )
            member = member.first()
        return member

    def get_member_from_collection_tube_with_null_sample_id(self, tube_id, genome_type):
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
                GenomicSetMember.genomicWorkflowState.notin_(self.exclude_states),
                GenomicSetMember.sampleId.is_(None)
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
                GenomicSetMember.genomicWorkflowState.notin_(self.exclude_states)
            )
            if bids:
                members_query = members_query.filter(
                    GenomicSetMember.biobankId.in_(bids)
                )
            return members_query.all()

    def get_consent_removal_date(self, member):
        """
        Calculates the earliest removal date between GROR or Primary Consent
        :param member
        :return: datetime
        """
        # get both Primary and GROR dates and consent statuses
        with self.session() as session:
            consent_status = session.query(ParticipantSummary.consentForStudyEnrollment,
                                           ParticipantSummary.consentForStudyEnrollmentAuthored,
                                           ParticipantSummary.consentForGenomicsROR,
                                           ParticipantSummary.consentForGenomicsRORAuthored,
                                           ParticipantSummary.withdrawalStatus,
                                           ParticipantSummary.withdrawalAuthored
                                           ).filter(
                ParticipantSummary.participantId == member.participantId,
            ).first()

        # Calculate gem consent removal date
        # Earliest date between GROR or Primary if both,
        withdraw_dates = []
        if consent_status.consentForGenomicsROR != QuestionnaireStatus.SUBMITTED and \
            consent_status.consentForGenomicsRORAuthored:
            withdraw_dates.append(consent_status.consentForGenomicsRORAuthored)

        if consent_status.consentForStudyEnrollment != QuestionnaireStatus.SUBMITTED and \
            consent_status.consentForStudyEnrollmentAuthored:
            withdraw_dates.append(consent_status.consentForStudyEnrollmentAuthored)

        if consent_status.withdrawalStatus != WithdrawalStatus.NOT_WITHDRAWN and \
            consent_status.withdrawalAuthored:
            withdraw_dates.append(consent_status.withdrawalAuthored)

        if withdraw_dates:
            return min([d for d in withdraw_dates if d is not None])

        return False

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
                GenomicSetMember.genomicWorkflowState.notin_(self.exclude_states),
                GenomicManifestFile.filePath == filepath
            ).one_or_none()

    def get_aw2_missing_with_all_files(self, genome_type):
        with self.session() as session:
            members_query = session.query(
                GenomicSetMember.id
            ).join(
                GenomicGCValidationMetrics,
                GenomicSetMember.id == GenomicGCValidationMetrics.genomicSetMemberId
            ).filter(
                GenomicSetMember.genomicWorkflowState == GenomicWorkflowState.GC_DATA_FILES_MISSING,
                GenomicGCValidationMetrics.ignoreFlag != 1
            )
            if genome_type == config.GENOME_TYPE_ARRAY:
                members_query = members_query.filter(
                    GenomicSetMember.genomeType == config.GENOME_TYPE_ARRAY,
                    GenomicGCValidationMetrics.idatRedPath.isnot(None),
                    GenomicGCValidationMetrics.idatGreenPath.isnot(None),
                    GenomicGCValidationMetrics.idatRedMd5Path.isnot(None),
                    GenomicGCValidationMetrics.idatGreenMd5Path.isnot(None),
                    GenomicGCValidationMetrics.vcfPath.isnot(None),
                    GenomicGCValidationMetrics.vcfTbiPath.isnot(None),
                    GenomicGCValidationMetrics.vcfMd5Path.isnot(None),
                )
            if genome_type == config.GENOME_TYPE_WGS:
                members_query = members_query.filter(
                    GenomicSetMember.genomeType == config.GENOME_TYPE_WGS,
                    GenomicGCValidationMetrics.hfVcfPath.isnot(None),
                    GenomicGCValidationMetrics.hfVcfTbiPath.isnot(None),
                    GenomicGCValidationMetrics.hfVcfMd5Path.isnot(None),
                    GenomicGCValidationMetrics.cramPath.isnot(None),
                    GenomicGCValidationMetrics.cramMd5Path.isnot(None),
                    GenomicGCValidationMetrics.craiPath.isnot(None),
                )
            return members_query.all()

    def get_all_contamination_reextract(self):
        reextract_states = [GenomicContaminationCategory.EXTRACT_BOTH,
                            GenomicContaminationCategory.EXTRACT_WGS]
        with self.session() as session:
            replated = aliased(GenomicSetMember)

            return session.query(
                GenomicSetMember,
                GenomicGCValidationMetrics.contaminationCategory
            ).join(
                GenomicGCValidationMetrics,
                GenomicSetMember.id == GenomicGCValidationMetrics.genomicSetMemberId
            ).outerjoin(
                replated,
                replated.replatedMemberId == GenomicSetMember.id
            ).filter(
                GenomicGCValidationMetrics.contaminationCategory.in_(reextract_states),
                GenomicSetMember.genomicWorkflowState.notin_(self.exclude_states),
                GenomicGCValidationMetrics.ignoreFlag == 0,
                replated.id.is_(None)
            ).all()

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
            self.update(member)

        except OperationalError:
            logging.error(f'Error updating member id: {member.id}.')
            return GenomicSubProcessResult.ERROR

    def update_member_job_run_id(self, member_ids, job_run_id, field):
        """
        Updates the GenomicSetMember with a job_run_id for an arbitrary workflow
        :param member_ids: the GenomicSetMember object ids to update
        :param job_run_id:
        :param field: the field for the job-run workflow (i.e. reconciliation, cvl, etc.)
        :return: query result or result code of error
        """
        if not self._is_valid_set_member_job_field(job_field_name=field):
            logging.error(f'{field} is not a valid job ID field.')
            return GenomicSubProcessResult.ERROR

        try:
            logging.info(f'Updating {field} with run ID.')

            if type(member_ids) is not list:
                member_ids = [member_ids]

            for m_id in member_ids:
                member = self.get(m_id)
                setattr(member, field, job_run_id)
                self.update(member)

            return GenomicSubProcessResult.SUCCESS

        # pylint: disable=broad-except
        except Exception as e:
            logging.error(e)
            return GenomicSubProcessResult.ERROR

    def batch_update_member_field(
        self,
        member_ids,
        field,
        value,
        is_job_run=False,
    ):

        if is_job_run and not self._is_valid_set_member_job_field(job_field_name=field):
            logging.error(f'{field} is not a valid job ID field.')
            return GenomicSubProcessResult.ERROR
        try:
            if type(member_ids) is not list:
                member_ids = [member_ids]

            for m_id in member_ids:
                member = self.get(m_id)
                setattr(member, field, value)
                self.update(member)

            return GenomicSubProcessResult.SUCCESS

        # pylint: disable=broad-except
        except Exception as e:
            logging.error(e)
            return GenomicSubProcessResult.ERROR

    def set_informing_loop_ready(self, member):
        member.informingLoopReadyFlag = 1
        member.informingLoopReadyFlagModified = clock.CLOCK.now()
        self.update(member)

    def update_member_workflow_state(self, member, new_state):
        """
        Sets the member's state to a new state
        :param member: GenomicWorkflowState
        :param new_state:
        """
        member.genomicWorkflowState = new_state
        member.genomicWorkflowStateStr = new_state.name
        member.genomicWorkflowStateModifiedTime = clock.CLOCK.now()
        self.update(member)

    def update_member_results_state(self, member, new_state):
        """
        Sets the member's state to a new state
        :param member:  ResultsWorkflowState
        :param new_state:
        """
        member.resultsWorkflowState = new_state
        member.resultsWorkflowStateStr = new_state.name
        member.resultsWorkflowStateModifiedTime = clock.CLOCK.now()
        self.update(member)

    def get_blocklist_members_from_date(self, *, attributes, from_days=1):
        from_date = (clock.CLOCK.now() - timedelta(days=from_days)).replace(microsecond=0)
        attributes.add('GenomicSetMember.id')
        eval_attrs = [eval(obj) for obj in attributes]
        members = sqlalchemy.orm.Query(eval_attrs)

        with self.session() as session:
            members = members.filter(
                or_(
                    and_(
                        GenomicSetMember.created >= from_date,
                        GenomicSetMember.genomicWorkflowState == GenomicWorkflowState.AW0,
                    ),
                    GenomicSetMember.modified >= from_date
                )
            )

            return members.with_session(session).all()

    def get_members_for_cvl_reconciliation(self):
        """
        Simple select from GSM
        :return: unreconciled GenomicSetMembers with ror consent and seq. data
        """
        with self.session() as session:
            members = session.query(GenomicSetMember).filter(
                GenomicSetMember.reconcileCvlJobRunId == None,
                GenomicSetMember.sequencingFileName != None,
                GenomicSetMember.genomicWorkflowState.notin_(self.exclude_states),
            ).all()
        return members

    def get_unconsented_gror_or_primary(self, workflow_states):
        """
        Get the genomic set members with GROR updated to No Consent
        Or removed primary consent in a list of workflow_states
        :return: GenomicSetMember query results
        """
        with self.session() as session:
            members = session.query(GenomicSetMember).join(
                (ParticipantSummary,
                 GenomicSetMember.participantId == ParticipantSummary.participantId)
            ).filter(
                GenomicSetMember.genomicWorkflowState.notin_(self.exclude_states),
                GenomicSetMember.genomicWorkflowState.in_(workflow_states) &
                (
                    (ParticipantSummary.consentForGenomicsROR != QuestionnaireStatus.SUBMITTED)
                    |
                    (ParticipantSummary.consentForStudyEnrollment != QuestionnaireStatus.SUBMITTED)
                    |
                    (ParticipantSummary.withdrawalStatus != WithdrawalStatus.NOT_WITHDRAWN)
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
                GenomicSetMember.genomicWorkflowState.notin_(self.exclude_states),
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
                GenomicSetMember.genomicWorkflowState.notin_(self.exclude_states)
            ).one_or_none()

    def get_new_participants(
        self,
        genomic_workflow_state,
        biobank_prefix=None,
        genomic_set_id=None,
        is_sql=False
    ):
        with self.session() as session:
            participants = session.query(
                GenomicSetMember.collectionTubeId,
                functions.concat(biobank_prefix, GenomicSetMember.biobankId).label('biobank_id'),
                GenomicSetMember.sexAtBirth,
                GenomicSetMember.genomeType,
                func.IF(GenomicSetMember.nyFlag == 1,
                        sqlalchemy.sql.expression.literal("Y"),
                        sqlalchemy.sql.expression.literal("N")).label('ny_flag'),
                func.IF(GenomicSetMember.validationStatus == 1,
                        sqlalchemy.sql.expression.literal("Y"),
                        sqlalchemy.sql.expression.literal("N")).label('validation_passed'),
                GenomicSetMember.ai_an
            ).filter(
                GenomicSetMember.genomicWorkflowState == genomic_workflow_state
            )

            if genomic_set_id:
                participants = participants.filter(
                    GenomicSetMember.genomicSetId == genomic_set_id
                )

            participants = participants.order_by(GenomicSetMember.id)

            if is_sql:
                sql = self.literal_sql_from_query(participants)
                return sql

            return participants.all()

    def get_member_from_raw_aw1_record(self, record):
        bid = record.biobank_id

        if bid.startswith("HG"):
            # This record is a control sample
            self.handle_control_samples_from_raw_aw1(record)
            return False

        if bid[0] in [get_biobank_id_prefix(), 'T', 'A']:
            bid = bid[1:]

        with self.session() as session:
            return session.query(
                GenomicSetMember
            ).filter(
                GenomicSetMember.biobankId == bid,
                GenomicSetMember.genomeType == record.test_name,
                GenomicSetMember.sampleId.is_(None)
            ).first()

    @classmethod
    def base_informing_loop_ready(cls):
        return (
            sqlalchemy.orm.Query(
                GenomicSetMember
            ).join(
                ParticipantSummary,
                ParticipantSummary.participantId == GenomicSetMember.participantId
            ).join(
                GenomicGCValidationMetrics,
                and_(
                    GenomicGCValidationMetrics.genomicSetMemberId == GenomicSetMember.id,
                    GenomicGCValidationMetrics.ignoreFlag != 1
                )
            ).filter(
                GenomicGCValidationMetrics.processingStatus.ilike('pass'),
                GenomicSetMember.genomeType == config.GENOME_TYPE_WGS,
                ParticipantSummary.withdrawalStatus == WithdrawalStatus.NOT_WITHDRAWN,
                ParticipantSummary.suspensionStatus == SuspensionStatus.NOT_SUSPENDED,
                ParticipantSummary.deceasedStatus == DeceasedStatus.UNSET,
                GenomicGCValidationMetrics.sexConcordance.ilike('true'),
                GenomicGCValidationMetrics.drcSexConcordance.ilike('pass'),
                GenomicSetMember.qcStatus == GenomicQcStatus.PASS,
                GenomicSetMember.gcManifestSampleSource.ilike('whole blood'),
                ParticipantSummary.consentForStudyEnrollment == QuestionnaireStatus.SUBMITTED,
                ParticipantSummary.consentForGenomicsROR == QuestionnaireStatus.SUBMITTED,
                GenomicGCValidationMetrics.drcFpConcordance.ilike('pass'),
                GenomicSetMember.diversionPouchSiteFlag != 1,
                ParticipantSummary.participantOrigin != 'careevolution',
                GenomicSetMember.ignoreFlag != 1,
                GenomicSetMember.blockResults != 1,
            )
        )

    def get_members_for_informing_loop_ready(self, limit=None):
        informing_loop_ready = self.base_informing_loop_ready().filter(
            GenomicSetMember.informingLoopReadyFlag != 1,
            GenomicSetMember.informingLoopReadyFlagModified.is_(None)
        ).subquery()
        with self.session() as session:
            records = session.query(
                GenomicSetMember
            ).join(
                BiobankStoredSample,
                BiobankStoredSample.biobankStoredSampleId == GenomicSetMember.collectionTubeId
            ).join(
                BiobankOrderIdentifier,
                BiobankOrderIdentifier.value == BiobankStoredSample.biobankOrderIdentifier
            ).join(
                BiobankOrder,
                BiobankOrder.biobankOrderId == BiobankOrderIdentifier.biobankOrderId
            ).filter(
                GenomicSetMember.id == informing_loop_ready.c.id
            ).order_by(
                BiobankOrder.finalizedTime
            )
            if limit:
                return records.distinct().limit(limit).all()

            return records.all()

    def get_ready_loop_by_participant_id(self, participant_id):
        informing_loop_ready = self.base_informing_loop_ready().filter(
            GenomicSetMember.informingLoopReadyFlag == 1,
            GenomicSetMember.informingLoopReadyFlagModified.isnot(None)
        ).subquery()
        with self.session() as session:
            record = session.query(
                    distinct(GenomicSetMember.participantId).label('participant_id'),
                    literal('informing_loop_ready').label('type')
                ).join(
                    informing_loop_ready,
                    informing_loop_ready.c.participant_id == participant_id
                ).filter(
                  GenomicSetMember.participantId == participant_id
            )
            return record.first()

    def get_member_by_participant_id(self, participant_id, genome_type=config.GENOME_TYPE_ARRAY):
        with self.session() as session:
            return session.query(
                    GenomicSetMember
                ).filter(
                  GenomicSetMember.participantId == participant_id,
                  GenomicSetMember.genomeType == genome_type
            ).first()

    def get_record_from_attr(self, *, attr, value):
        with self.session() as session:
            record = session.query(GenomicSetMember) \
                .filter(getattr(GenomicSetMember, attr) == value,
                        getattr(GenomicSetMember, attr).isnot(None)
                        )
            return record.all()

    def update_loop_ready_attrs(self, member, **kwargs):
        informing_loop_ready_flag = kwargs.get('informing_loop_ready_flag', 1)
        informing_loop_ready_flag_modified = kwargs.get('informing_loop_ready_flag_modified', clock.CLOCK.now())

        member.informingLoopReadyFlag = informing_loop_ready_flag
        member.informingLoopReadyFlagModified = informing_loop_ready_flag_modified
        self.update(member)

    def handle_control_samples_from_raw_aw1(self, record):
        """ Create control samples from aw1 raw data """

        # need a genomic set
        max_set_id = self.get_collection_tube_max_set_id()[0]

        # Insert new member with biobank_id and collection tube ID from AW1
        new_member_obj = GenomicSetMember(
            genomicSetId=max_set_id,
            participantId=0,
            biobankId=record.biobank_id,
            validationStatus=GenomicSetMemberStatus.VALID,
            genomeType=record.genome_type,
            genomicWorkflowState=GenomicWorkflowState.AW1,
            genomicWorkflowStateStr=GenomicWorkflowState.AW1.name,
            gcSiteId=record.site_name,
            packageId=record.package_id,
            sampleId=record.sample_id,
            gcManifestBoxStorageUnitId=record.box_storageunit_id,
            gcManifestBoxPlateId=record.box_id_plate_id,
            gcManifestWellPosition=record.well_position,
            gcManifestParentSampleId=record.parent_sample_id,
            collectionTubeId=record.collection_tube_id,
            gcManifestMatrixId=record.matrix_id,
            gcManifestTreatments=record.treatments,
            gcManifestQuantity_ul=record.quantity,
            gcManifestTotalConcentration_ng_per_ul=record.total_concentration,
            gcManifestTotalDNA_ng=record.total_dna,
            gcManifestVisitDescription=record.visit_description,
            gcManifestSampleSource=record.sample_source,
            gcManifestStudy=record.study,
            gcManifestTrackingNumber=record.tracking_number,
            gcManifestContact=record.contact,
            gcManifestEmail=record.email,
            gcManifestStudyPI=record.study_pi,
            gcManifestTestName=record.test_name,
            gcManifestFailureMode=record.failure_mode,
            gcManifestFailureDescription=record.failure_mode_desc,
        )

        return self.insert(new_member_obj)

    def get_gem_results_for_report_state(self, obj: GenomicSetMember = None):
        with self.session() as session:
            records = session.query(
                GenomicSetMember.id.label('genomic_set_member_id'),
                GenomicSetMember.participantId.label('participant_id'),
                literal('result_ready').label('event_type'),
                literal('gem').label('module'),
                GenomicSetMember.sampleId.label('sample_id'),
                GenomicJobRun.created.label('event_authored_time'),
                GenomicSetMember.genomicWorkflowState
            ).join(
                GenomicJobRun,
                GenomicJobRun.id == GenomicSetMember.gemA2ManifestJobRunId
            ).outerjoin(
                GenomicMemberReportState,
                GenomicMemberReportState.genomic_set_member_id == GenomicSetMember.id
            ).filter(
                GenomicMemberReportState.id.is_(None),
                GenomicSetMember.gemA2ManifestJobRunId.isnot(None),
                GenomicSetMember.genomicWorkflowState.in_([
                    GenomicWorkflowState.GEM_RPT_READY,
                    GenomicWorkflowState.GEM_RPT_PENDING_DELETE,
                    GenomicWorkflowState.GEM_RPT_DELETED
                ])
            )
            if obj:
                records = records.filter(GenomicSetMember.id == obj.id)
                return records.one()

            return records.all()

    @classmethod
    def _is_valid_set_member_job_field(cls, job_field_name):
        return job_field_name is not None and hasattr(GenomicSetMember, job_field_name)

    def get_members_from_biobank_ids(self, biobank_ids, genome_type=None):
        """
        Returns genomicSetMember objects for list of Biobank IDs.
        :param exclude_states:
        :param biobank_ids:
        :param genome_type:
        :return:
        """
        with self.session() as session:
            members = session.query(GenomicSetMember).filter(
                GenomicSetMember.biobankId.in_(biobank_ids),
                GenomicSetMember.genomicWorkflowState.notin_(self.exclude_states),
                GenomicSetMember.ignoreFlag == 0
            )

            if genome_type:
                members = members.filter(
                    GenomicSetMember.genomeType == genome_type
                )
            return members.all()

    def get_array_members_files_available(self, sample_list=None):
        required_file_types = [file_type['file_type'] for file_type in array_file_types_attributes if
                               file_type['required']]
        with self.session() as session:
            subquery = session.query(
                GenomicSetMember.id,
                func.count(GenomicGcDataFile.file_type).label("file_count")
            ).join(
                GenomicGCValidationMetrics,
                GenomicGCValidationMetrics.genomicSetMemberId == GenomicSetMember.id
            ).outerjoin(
                GenomicGcDataFile,
                GenomicGcDataFile.identifier_value == GenomicGCValidationMetrics.chipwellbarcode
            ).filter(
                GenomicSetMember.genomicWorkflowState == GenomicWorkflowState.GC_DATA_FILES_MISSING,
                GenomicSetMember.genomeType == 'aou_array',
                GenomicSetMember.ignoreFlag != 1,
                GenomicGCValidationMetrics.ignoreFlag != 1,
                GenomicGcDataFile.ignore_flag != 1,
                GenomicGcDataFile.file_type.in_(required_file_types)
            ).group_by(GenomicSetMember.id).subquery()

            members = session.query(
                GenomicSetMember
            ).select_from(
                GenomicSetMember
            ).join(
                subquery,
                subquery.c.id == GenomicSetMember.id,
            ).filter(
                subquery.c.file_count == len(required_file_types)
            )

            if sample_list:
                members = members.filter(
                    GenomicSetMember.sampleId.in_(sample_list)
                )
            return members.all()

    def get_wgs_members_files_available(self, sample_list=None):
        required_file_types = [file_type['file_type'] for file_type in wgs_file_types_attributes
                     if file_type['required']]
        with self.session() as session:
            subquery = session.query(
                GenomicSetMember.id,
                func.count(GenomicGcDataFile.file_type).label("file_count")
            ).join(
                GenomicGCValidationMetrics,
                GenomicGCValidationMetrics.genomicSetMemberId == GenomicSetMember.id
            ).outerjoin(
                GenomicGcDataFile,
                GenomicGcDataFile.identifier_value == GenomicSetMember.sampleId
            ).filter(
                GenomicSetMember.genomicWorkflowState == GenomicWorkflowState.GC_DATA_FILES_MISSING,
                GenomicSetMember.genomeType == 'aou_wgs',
                GenomicSetMember.ignoreFlag != 1,
                GenomicGCValidationMetrics.ignoreFlag != 1,
                GenomicGcDataFile.ignore_flag != 1,
                GenomicGcDataFile.file_type.in_(required_file_types)
            ).group_by(GenomicSetMember.id).subquery()

            members = session.query(
                GenomicSetMember
            ).select_from(
                GenomicSetMember
            ).join(
                subquery,
                subquery.c.id == GenomicSetMember.id,
            ).filter(
                subquery.c.file_count == len(required_file_types)
            )
            if sample_list:
                members = members.filter(
                    GenomicSetMember.sampleId.in_(sample_list)
                )
            return members.all()


class GenomicJobRunDao(UpdatableDao, GenomicDaoMixin):
    """ Stub for GenomicJobRun model """
    validate_version_match = False

    def __init__(self):
        super(GenomicJobRunDao, self).__init__(GenomicJobRun, order_by_ending=['id'])
        self.member_dao = GenomicSetMemberDao()

    def get_id(self, obj):
        return obj.id

    def from_client_json(self):
        pass

    def get_last_successful_runtime(self, job_id):
        with self.session() as session:
            return session.query(
                functions.max(GenomicJobRun.startTime)
            ).filter(
                GenomicJobRun.jobId == job_id,
                GenomicJobRun.runStatus == GenomicSubProcessStatus.COMPLETED,
                GenomicJobRun.runResult.in_([
                    GenomicSubProcessResult.SUCCESS,
                    GenomicSubProcessResult.NO_FILES
                ])).one()[0]

    def insert_run_record(self, job_id):
        """
        Inserts the job_run record.
        :param job_id: the ID of the current genomic job
        :return: the object inserted
        """
        job_run = GenomicJobRun()
        job_run.jobId = job_id
        job_run.jobIdStr = job_id.name
        job_run.startTime = clock.CLOCK.now()

        return self.insert(job_run)

    def update_run_record(self, run_id, result, status):
        with self.session() as session:
            return self._update_run_record_with_session(session, run_id, result, status)

    @staticmethod
    def _update_run_record_with_session(session, run_id, result, status):
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
                    GenomicJobRun.runResultStr: sqlalchemy.bindparam("run_result_str_param"),
                    GenomicJobRun.runStatus: sqlalchemy.bindparam("run_status_param"),
                    GenomicJobRun.endTime: sqlalchemy.bindparam("end_time_param"),
                }
            )
        )
        query_params = {
            "run_id_param": run_id,
            "run_result_param": result,
            "run_result_str_param": result.name,
            "run_status_param": status,
            "end_time_param": clock.CLOCK.now()
        }
        return session.execute(query, query_params)


class GenomicFileProcessedDao(UpdatableDao, GenomicDaoMixin):
    """ Stub for GenomicFileProcessed model """

    validate_version_match = False

    def __init__(self):
        super(GenomicFileProcessedDao, self).__init__(
            GenomicFileProcessed, order_by_ending=['id'])
        self.member_dao = GenomicSetMemberDao()

    def get_id(self, obj):
        return obj.id

    def from_client_json(self):
        pass

    def get_record_from_filename(self, file_name):
        with self.session() as session:
            return session.query(GenomicFileProcessed).filter(
                GenomicFileProcessed.fileName == file_name
            ).order_by(GenomicFileProcessed.id.desc()).first()

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

    def get_ingestion_deltas_from_date(self, *, from_date, ingestion_type):
        results = []

        if ingestion_type not in [GenomicJob.AW1_MANIFEST,
                                  GenomicJob.METRICS_INGESTION]:
            return results

        with self.session() as session:
            if ingestion_type == GenomicJob.AW1_MANIFEST:
                results = session.query(
                    func.count(GenomicAW1Raw.id).label("raw_record_count"),
                    func.count(GenomicSetMember.id).label("ingested_count"),
                    GenomicAW1Raw.file_path,
                    literal('AW1').label('file_type'),
                ).outerjoin(
                    GenomicManifestFile,
                    GenomicManifestFile.filePath == GenomicAW1Raw.file_path
                ).outerjoin(
                    GenomicFileProcessed,
                    GenomicFileProcessed.genomicManifestFileId == GenomicManifestFile.id
                ).outerjoin(
                    GenomicSetMember,
                    GenomicSetMember.aw1FileProcessedId == GenomicFileProcessed.id
                ).filter(
                    GenomicAW1Raw.created >= from_date,
                    GenomicAW1Raw.ignore_flag != 1,
                    GenomicAW1Raw.biobank_id != "",
                    GenomicAW1Raw.biobank_id.isnot(None)
                ).group_by(
                    GenomicAW1Raw.file_path
                ).distinct()
            elif ingestion_type == GenomicJob.METRICS_INGESTION:
                results = session.query(
                    func.count(GenomicAW2Raw.id).label("raw_record_count"),
                    func.count(GenomicGCValidationMetrics.id).label("ingested_count"),
                    GenomicAW2Raw.file_path,
                    literal('AW2').label('file_type'),
                ).outerjoin(
                    GenomicManifestFile,
                    GenomicManifestFile.filePath == GenomicAW2Raw.file_path
                ).outerjoin(
                    GenomicFileProcessed,
                    GenomicFileProcessed.genomicManifestFileId == GenomicManifestFile.id
                ).outerjoin(
                    GenomicGCValidationMetrics,
                    GenomicGCValidationMetrics.genomicFileProcessedId == GenomicFileProcessed.id
                ).filter(
                    GenomicAW2Raw.created >= from_date,
                    GenomicAW2Raw.ignore_flag != 1,
                    GenomicAW2Raw.biobank_id != "",
                    GenomicAW2Raw.biobank_id.isnot(None)
                ).group_by(
                    GenomicAW2Raw.file_path
                ).distinct()

            return results.all()

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

    @staticmethod
    def _update_file_record_with_session(session, file_id,
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


class GenomicGCValidationMetricsDao(UpsertableDao, GenomicDaoMixin):
    """ Stub for GenomicGCValidationMetrics model """
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
            'mappedReadsPct': 'mappedreadspct',
            'contaminationCategory': 'contamination_category',
            'contaminationCategoryStr': 'contamination_category_str',
            'sexConcordance': 'sexconcordance',
            'sexPloidy': 'sexploidy',
            'alignedQ30Bases': 'alignedq30bases',
            'arrayConcordance': 'arrayconcordance',
            'processingStatus': 'processingstatus',
            'notes': 'notes',
            'siteId': 'siteid',
            'pipelineId': 'pipelineid',
            'cramPath': 'cramPath',
            'craiPath': 'craiPath',
            'cramMd5Path': 'cramMd5Path',
            'hfVcfPath': 'hfVcfPath',
            'hfVcfMd5Path': 'hfVcfMd5Path',
            'hfVcfTbiPath': 'hfVcfTbiPath',
            'idatRedPath': 'idatRedPath',
            'idatGreenPath': 'idatGreenPath',
            'idatRedMd5Path': 'idatRedMd5Path',
            'idatGreenMd5Path': 'idatGreenMd5Path',
            'vcfPath': 'vcfPath',
            'vcfTbiPath': 'vcfTbiPath',
            'vcfMd5Path': 'vcfMd5Path',
            'gvcfPath': 'gvcfPath',
            'gvcfMd5Path': 'gvcfMd5Path'
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

    def from_client_json(self):
        pass

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

    def get_metrics_by_member_id(self, member_id, pipeline_id=None):
        """
        Retrieves gc metric record with the member_id
        :param: member_id
        :param: pipeline_id
        :return: GenomicGCValidationMetrics object
        """
        with self.session() as session:
            record = session.query(
                GenomicGCValidationMetrics
            ).filter(
                GenomicGCValidationMetrics.genomicSetMemberId == member_id,
                GenomicGCValidationMetrics.ignoreFlag != 1
            )
            if pipeline_id:
                record = record.filter(
                    GenomicGCValidationMetrics.pipelineId == pipeline_id
                )

            return record.one_or_none()

    def get_metric_record_counts_from_filepath(self, filepath):
        with self.session() as session:
            return session.query(
                functions.count(GenomicGCValidationMetrics.id)
            ).join(
                GenomicFileProcessed,
                GenomicFileProcessed.id == GenomicGCValidationMetrics.genomicFileProcessedId
            ).filter(
                GenomicFileProcessed.filePath == filepath,
                GenomicGCValidationMetrics.ignoreFlag != 1
            ).one_or_none()

    def get_fully_processed_metrics(self, genome_type, limit=None):
        with self.session() as session:
            records = session.query(
                GenomicGCValidationMetrics
            ).join(
                GenomicSetMember,
                GenomicSetMember.id == GenomicGCValidationMetrics.genomicSetMemberId
            ).outerjoin(
                GenomicStorageUpdate,
                and_(
                    GenomicStorageUpdate.metrics_id == GenomicGCValidationMetrics.id,
                    GenomicStorageUpdate.genome_type == genome_type,
                    GenomicStorageUpdate.ignore_flag != 1
                )
            ).filter(
                GenomicSetMember.aw4ManifestJobRunID.isnot(None),
                GenomicStorageUpdate.id.is_(None)
            )

            if genome_type != config.GENOME_TYPE_ARRAY:
                return records.limit(limit).all() if limit else records.all()

            records = records.filter(
                GenomicSetMember.gemA2ManifestJobRunId.isnot(None),
            )
            return records.limit(limit).all() if limit else records.all()

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

        self.exclude_states = [GenomicWorkflowState.IGNORE]

    def get_id(self, obj):
        pass

    def from_client_json(self):
        pass

    def to_client_json(self, result):
        participant_data = result.get('data')
        participant_dict = participant_data._asdict()
        return {self.camel_to_snake(k): v for k, v in participant_dict.items()}

    def get_pii(self, mode, participant_id=None, biobank_id=None):
        """
        :param mode:
        :param participant_id:
        :param biobank_id:
        :return: query results for PID
        """
        mode = mode.lower()
        informing_loop_ready = GenomicSetMemberDao.base_informing_loop_ready().filter(
            GenomicSetMember.informingLoopReadyFlag == 1,
            GenomicSetMember.informingLoopReadyFlagModified.isnot(None)
        ).subquery()

        with self.session() as session:
            if mode == 'gp' and participant_id:
                record = session.query(
                    GenomicSetMember.biobankId,
                    ParticipantSummary.firstName,
                    ParticipantSummary.lastName,
                    ParticipantSummary.dateOfBirth,
                    GenomicSetMember.sexAtBirth,
                    sqlalchemy.case(
                        [
                            (informing_loop_ready.c.id.isnot(None), True)
                        ],
                        else_=False
                    ).label('hgmInformingLoop')
                ).join(
                    ParticipantSummary,
                    GenomicSetMember.participantId == ParticipantSummary.participantId,
                ).outerjoin(
                    informing_loop_ready,
                    informing_loop_ready.c.participant_id == GenomicSetMember.participantId
                ).filter(
                    GenomicSetMember.participantId == participant_id
                )
            elif mode == 'rhp' and biobank_id:
                record = session.query(
                    func.concat('P', GenomicSetMember.participantId).label('participantId'),
                    ParticipantSummary.firstName,
                    ParticipantSummary.lastName,
                    ParticipantSummary.dateOfBirth,
                    GenomicSetMember.gcManifestSampleSource.label('sampleSource'),
                    BiobankStoredSample.confirmed.label('collectionDate')
                ).join(
                    ParticipantSummary,
                    GenomicSetMember.participantId == ParticipantSummary.participantId,
                ).join(
                    BiobankStoredSample,
                    BiobankStoredSample.biobankStoredSampleId == GenomicSetMember.collectionTubeId
                ).filter(
                    or_(
                        GenomicSetMember.cvlW4wrHdrManifestJobRunID.isnot(None),
                        GenomicSetMember.cvlW4wrPgxManifestJobRunID.isnot(None)
                    ),
                    GenomicSetMember.biobankId == biobank_id
                )

            record = record.filter(
                ParticipantSummary.consentForGenomicsROR == QuestionnaireStatus.SUBMITTED,
                ParticipantSummary.withdrawalStatus == WithdrawalStatus.NOT_WITHDRAWN,
                ParticipantSummary.suspensionStatus == SuspensionStatus.NOT_SUSPENDED,
                GenomicSetMember.genomicWorkflowState.notin_(self.exclude_states)
            )

            return record.first()


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
                                  biobankId=p.biobankId,
                                  genomicSetId=1,
                                  genomeType=genome_type,
                                  genomicWorkflowState=report_state,
                                  genomicWorkflowStateStr=report_state.name,
                                  gemA2ManifestJobRunId=1,
                                  gemPass='Y',
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
                session.query(
                    GenomicSetMember.participantId,
                    GenomicSetMember.genomicWorkflowState
                )
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
                session.query(
                    GenomicSetMember.participantId,
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


class GenomicOutreachDaoV2(BaseDao):
    def __init__(self):
        super(GenomicOutreachDaoV2, self).__init__(
            GenomicSetMember, order_by_ending=['id'])

        self.allowed_modules = ['gem', 'hdr', 'pgx']
        self.module = self.allowed_modules

        self.req_allowed_types = ['result', 'informingLoop']
        self.req_type = self.req_allowed_types

        self.report_state_map = {
            'gem': [GenomicReportState.GEM_RPT_READY,
                    GenomicReportState.GEM_RPT_PENDING_DELETE,
                    GenomicReportState.GEM_RPT_DELETED],
            'pgx': [GenomicReportState.PGX_RPT_READY,
                    GenomicReportState.CVL_RPT_PENDING_DELETE,
                    GenomicReportState.CVL_RPT_DELETED],
            'hdr': [GenomicReportState.HDR_RPT_UNINFORMATIVE,
                    GenomicReportState.HDR_RPT_POSITIVE,
                    GenomicReportState.CVL_RPT_PENDING_DELETE,
                    GenomicReportState.CVL_RPT_DELETED]
        }
        self.sample_swaps = []
        self.report_query_state = self.get_report_state_query_config()

    def get_id(self, obj):
        pass

    def from_client_json(self):
        pass

    def to_client_json(self, _dict):

        def _get_sample_swap_module(sample_swap):
            if not sample_swap:
                return ''
            if sample_swap:
                swap = list(filter(lambda x: x.id == sample_swap.genomic_sample_swap, self.sample_swaps))[0]
                return f'_{swap.name}_{sample_swap.category.name}'.lower()

        def _get_sample_swaps(data):
            if any(hasattr(obj, 'GenomicSampleSwapMember') for obj in data):
                sample_swap_dao = GenomicSampleSwapDao()
                return sample_swap_dao.get_all()

        self.sample_swaps = _get_sample_swaps(data=_dict.get('data'))
        timestamp = pytz.utc.localize(_dict.get('date'))
        report_statuses = []

        if not _dict.get('data'):
            return {
                "data": report_statuses,
                "timestamp": timestamp
            }

        for participant_data in _dict.get('data'):
            pid, cvl_modules = participant_data.participant_id, ['hdr', 'pgx']

            if 'result' in participant_data.type:
                report_status, report_module = self._determine_report_state(participant_data.genomic_report_state)

                report_obj = {
                    "type": 'result',
                    "participant_id": f'P{pid}',
                    "module": report_module,
                    "status": report_status
                }

                if hasattr(participant_data, 'GenomicSampleSwapMember'):
                    genomic_swap_module = _get_sample_swap_module(
                        sample_swap=participant_data.GenomicSampleSwapMember
                    )
                    report_obj['module'] = f'{report_module}{genomic_swap_module}'

                if 'viewed' in participant_data.type:
                    report_obj['status'] = 'viewed'

                if participant_data.report_revision_number is not None:
                    report_obj['report_revision_number'] = participant_data.report_revision_number

                if participant_data.genomic_report_state in self.report_state_map.get('hdr'):
                    report_obj['hdr_result_status'] = participant_data.genomic_report_state.name.split('_', 2)[
                        -1].lower()

                report_statuses.append(report_obj)

            elif 'informing_loop' in participant_data.type:
                if 'ready' in participant_data.type:
                    for module in cvl_modules:
                        if module in self.module:
                            report_statuses.append({
                                "module": module,
                                "type": 'informingLoop',
                                "status": 'ready',
                                "participant_id": f'P{pid}',
                            })
                if 'decision' in participant_data.type:
                    report_statuses.append({
                        "module": participant_data.module_type.lower(),
                        "type": 'informingLoop',
                        "status": 'completed',
                        "decision": participant_data.decision_value,
                        "participant_id": f'P{pid}'
                    })

        return {
            "data": report_statuses,
            "timestamp": timestamp
        }

    def get_outreach_data(self, participant_id=None, start_date=None, end_date=None):
        informing_loops, results = [], []
        end_date = clock.CLOCK.now() if not end_date else end_date
        informing_loop_ready = GenomicSetMemberDao.base_informing_loop_ready().subquery()

        def _set_genome_types():
            genome_types = []
            if 'gem' in self.module:
                genome_types.append(config.GENOME_TYPE_ARRAY)
            if 'hdr' or 'pgx' in self.module:
                genome_types.append(config.GENOME_TYPE_WGS)
            return genome_types

        query_genome_types = _set_genome_types()

        def _get_max_decision_loops(decision_loops):
            if not decision_loops:
                return []

            max_loops, participant_ids = [], {obj.participant_id for obj in decision_loops}

            for pid in participant_ids:
                participant_loops = list(filter(lambda x: x.participant_id == pid, decision_loops))

                modules = {obj.module_type for obj in participant_loops}
                for module in modules:
                    max_loops.append(
                        max([loop for loop in participant_loops if loop.module_type.lower() == module.lower()],
                            key=lambda x: x.id))
            return max_loops

        with self.session() as session:
            if 'informingLoop' in self.req_type:
                decision_loop = (
                    session.query(
                        distinct(GenomicInformingLoop.participant_id).label('participant_id'),
                        GenomicInformingLoop.module_type,
                        GenomicInformingLoop.decision_value,
                        GenomicInformingLoop.id,
                        literal('informing_loop_decision').label('type')
                    )
                    .join(
                        ParticipantSummary,
                        ParticipantSummary.participantId == GenomicInformingLoop.participant_id
                    )
                    .join(
                        GenomicSetMember,
                        and_(
                            GenomicSetMember.participantId == GenomicInformingLoop.participant_id,
                            GenomicSetMember.genomeType.in_(query_genome_types)
                        )
                    ).filter(
                        ParticipantSummary.withdrawalStatus == WithdrawalStatus.NOT_WITHDRAWN,
                        ParticipantSummary.suspensionStatus == SuspensionStatus.NOT_SUSPENDED,
                        GenomicInformingLoop.decision_value.isnot(None),
                        GenomicInformingLoop.module_type.in_(self.module),
                        GenomicInformingLoop.event_authored_time.isnot(None),
                        GenomicSetMember.ignoreFlag != 1
                    )
                )
                ready_loop = (
                    session.query(
                        distinct(GenomicSetMember.participantId).label('participant_id'),
                        literal('informing_loop_ready').label('type')
                    )
                    .join(
                        informing_loop_ready,
                        informing_loop_ready.c.participant_id == GenomicSetMember.participantId
                    ).filter(
                        GenomicSetMember.informingLoopReadyFlag == 1,
                        GenomicSetMember.informingLoopReadyFlagModified.isnot(None)
                    )
                )
                if participant_id:
                    decision_loop = decision_loop.filter(
                        GenomicSetMember.participantId == participant_id
                    )
                    ready_loop = ready_loop.filter(
                        GenomicSetMember.participantId == participant_id
                    )
                if start_date:
                    decision_loop = decision_loop.filter(
                        or_(
                            GenomicInformingLoop.event_authored_time > start_date,
                            GenomicInformingLoop.created > start_date
                        ),
                        GenomicInformingLoop.event_authored_time < end_date
                    )
                    ready_loop = ready_loop.filter(
                        GenomicSetMember.informingLoopReadyFlagModified > start_date,
                        GenomicSetMember.informingLoopReadyFlagModified < end_date
                    )

                informing_loops = _get_max_decision_loops(decision_loop.all()) + ready_loop.all()

            if 'result' in self.req_type:
                result_ready_query = (
                    session.query(
                        distinct(GenomicMemberReportState.participant_id).label('participant_id'),
                        GenomicMemberReportState.genomic_report_state,
                        GenomicMemberReportState.report_revision_number,
                        GenomicSampleSwapMember,
                        literal('result_ready').label('type')
                    )
                    .join(
                        ParticipantSummary,
                        ParticipantSummary.participantId == GenomicMemberReportState.participant_id
                    )
                    .join(
                        GenomicSetMember,
                        and_(
                            GenomicSetMember.id == GenomicMemberReportState.genomic_set_member_id,
                            GenomicSetMember.genomeType.in_(query_genome_types)
                        )
                    ).outerjoin(
                        GenomicSampleSwapMember,
                        GenomicSampleSwapMember.genomic_set_member_id == GenomicSetMember.id
                    ).filter(
                        ParticipantSummary.withdrawalStatus == WithdrawalStatus.NOT_WITHDRAWN,
                        ParticipantSummary.suspensionStatus == SuspensionStatus.NOT_SUSPENDED,
                        GenomicMemberReportState.genomic_report_state.in_(self.report_query_state),
                        GenomicMemberReportState.event_authored_time.isnot(None),
                        GenomicSetMember.ignoreFlag != 1
                    )
                )

                result_viewed_query = (
                    session.query(
                        distinct(GenomicResultViewed.participant_id).label('participant_id'),
                        GenomicMemberReportState.genomic_report_state,
                        GenomicMemberReportState.report_revision_number,
                        literal('result_viewed').label('type')
                    ).join(
                        ParticipantSummary,
                        ParticipantSummary.participantId == GenomicResultViewed.participant_id
                    ).join(
                        GenomicMemberReportState,
                        and_(
                            GenomicMemberReportState.sample_id == GenomicResultViewed.sample_id,
                            GenomicMemberReportState.module == GenomicResultViewed.module_type
                        )
                    ).join(
                        GenomicSetMember,
                        GenomicSetMember.id == GenomicMemberReportState.genomic_set_member_id,
                    ).filter(
                        ParticipantSummary.withdrawalStatus == WithdrawalStatus.NOT_WITHDRAWN,
                        ParticipantSummary.suspensionStatus == SuspensionStatus.NOT_SUSPENDED,
                        GenomicMemberReportState.genomic_report_state.in_(self.report_query_state),
                        GenomicResultViewed.event_authored_time.isnot(None),
                        GenomicSetMember.ignoreFlag != 1
                    )
                )

                if participant_id:
                    result_ready_query = result_ready_query.filter(
                        ParticipantSummary.participantId == participant_id
                    )
                    result_viewed_query = result_viewed_query.filter(
                        ParticipantSummary.participantId == participant_id
                    )

                if start_date:
                    result_ready_query = result_ready_query.filter(
                        or_(
                            GenomicMemberReportState.event_authored_time > start_date,
                            GenomicMemberReportState.created > start_date
                        ),
                        GenomicMemberReportState.event_authored_time < end_date,
                    )
                    result_viewed_query = result_viewed_query.filter(
                        or_(
                            GenomicResultViewed.event_authored_time > start_date,
                            GenomicResultViewed.created > start_date
                        ),
                        GenomicResultViewed.event_authored_time < end_date,
                    )

                results = result_ready_query.all() + result_viewed_query.all()

            return informing_loops + results

    def _determine_report_state(self, status):
        p_status, p_module = None, None
        for key, report_values in self.report_state_map.items():
            if status in report_values:
                p_module = key
                p_status = status.name.split('_', 2)[-1].lower() if 'hdr' not in key else 'ready'
                break
        return p_status, p_module

    def get_report_state_query_config(self):
        mappings = []
        if not self.module:
            for value in self.report_state_map.values():
                mappings.extend(value)
            return mappings

        for mod in self.module:
            for key, value in self.report_state_map.items():
                if mod == key:
                    mappings.extend(value)
        return mappings


class GenomicSchedulingDao(BaseDao):
    def __init__(self):
        super().__init__(GenomicSetMember, order_by_ending=['id'])

    def get_id(self, obj):
        pass

    def from_client_json(self):
        pass

    def to_client_json(self, payload_dict):
        timestamp = pytz.utc.localize(payload_dict.get('date'))
        appointments = []

        def format_datetime_objs(obj: Dict) -> Dict:
            for key, value in obj.items():
                if isinstance(value, datetime):
                    obj[key] = pytz.utc.localize(value)
            return obj

        if not payload_dict.get('data'):
            return {
                "data": appointments,
                "timestamp": timestamp
            }

        for appointment in payload_dict.get('data'):
            status = appointment.status.split('_')[-1]
            participant_id = f'P{appointment.participant_id}'
            appointment = {k: v for k, v in appointment._asdict().items() if v is not None}
            appointment['participant_id'] = participant_id
            appointment['status'] = status
            appointment['type'] = 'appointment'
            if any(isinstance(value, datetime) for value in appointment.values()):
                appointment = format_datetime_objs(appointment)

            appointments.append(appointment)

        return {
            "data": appointments,
            "timestamp": timestamp
        }

    def get_latest_scheduling_data(self, participant_id=None, start_date=None, end_date=None, module=None):
        max_appointment_id_subquery = sqlalchemy.orm.Query(
            functions.max(GenomicAppointmentEvent.appointment_id).label(
                'max_appointment_id'
            )
        ).filter(
            GenomicAppointmentEvent.event_type.notlike('%note_available')
        ).group_by(
            GenomicAppointmentEvent.participant_id,
            GenomicAppointmentEvent.module_type
        ).subquery()

        max_event_authored_time_subquery = sqlalchemy.orm.Query(
            functions.max(GenomicAppointmentEvent.event_authored_time).label(
                'max_event_authored_time'
            )
        ).filter(
            GenomicAppointmentEvent.event_type.notlike('%note_available')
        ).group_by(
            GenomicAppointmentEvent.participant_id,
            GenomicAppointmentEvent.module_type
        ).subquery()

        note_alias = aliased(GenomicAppointmentEvent)

        with self.session() as session:
            records = session.query(
                GenomicAppointmentEvent.appointment_id,
                GenomicAppointmentEvent.participant_id,
                GenomicAppointmentEvent.module_type.label('module'),
                GenomicAppointmentEvent.event_type.label('status'),
                GenomicAppointmentEvent.appointment_timestamp,
                GenomicAppointmentEvent.appointment_timezone,
                GenomicAppointmentEvent.source,
                GenomicAppointmentEvent.location,
                GenomicAppointmentEvent.contact_number,
                GenomicAppointmentEvent.language,
                GenomicAppointmentEvent.cancellation_reason,
                sqlalchemy.case(
                    [
                        (note_alias.id.isnot(None), True)
                    ],
                    else_=False
                ).label('note_available'),
            ).outerjoin(
                note_alias,
                and_(
                    note_alias.appointment_id == GenomicAppointmentEvent.appointment_id,
                    note_alias.event_type.like('%note_available')
                )
            ).filter(
                and_(
                    GenomicAppointmentEvent.appointment_id == max_appointment_id_subquery.c.max_appointment_id,
                    GenomicAppointmentEvent.event_authored_time ==
                    max_event_authored_time_subquery.c.max_event_authored_time
                )
            )

            if module:
                records = records.filter(
                    GenomicAppointmentEvent.module_type.ilike(module)
                )
            if participant_id:
                records = records.filter(
                    GenomicAppointmentEvent.participant_id == participant_id
                )
            if start_date:
                records = records.filter(
                    GenomicAppointmentEvent.event_authored_time > start_date,
                    GenomicAppointmentEvent.event_authored_time < end_date
                )

            return records.distinct().all()


class GenomicManifestFileDao(BaseDao, GenomicDaoMixin):
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
            if manifest_file_obj.manifestTypeId == GenomicManifestTypes.AW1:
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
                GenomicManifestFile.filePath == filepath,
                GenomicManifestFile.ignore_flag != 1
            ).first()

    def update_record_count(self, manifest_file_obj, new_rec_count):
        with self.session() as session:
            manifest_file_obj.recordCount = new_rec_count
            session.merge(manifest_file_obj)


class GenomicManifestFeedbackDao(UpdatableDao, GenomicDaoMixin):
    validate_version_match = False

    def __init__(self):
        super(GenomicManifestFeedbackDao, self).__init__(GenomicManifestFeedback, order_by_ending=['id'])

    def get_id(self, obj):
        return obj.id

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

    def increment_feedback_count(self, manifest_id):
        """
        Update the manifest feedback record's count
        :param manifest_id:
        :return:
        """
        fb = self.get_feedback_record_from_manifest_id(manifest_id)

        # Increment and update the record
        if fb is not None:
            fb.feedbackRecordCount += 1

            with self.session() as session:
                session.merge(fb)
        else:
            raise ValueError(f'No feedback record for manifest id {manifest_id}')

    def get_feedback_records_past_date_cutoff(self, num_days):
        with self.session() as session:
            results = session.query(GenomicManifestFeedback).join(
                GenomicManifestFile,
                GenomicManifestFile.id == GenomicManifestFeedback.inputManifestFileId
            ).filter(
                GenomicManifestFeedback.ignoreFlag == 0,
                GenomicManifestFeedback.feedbackComplete == 0,
                GenomicManifestFile.uploadDate <= CLOCK.now() - timedelta(days=num_days),
                GenomicManifestFeedback.feedbackManifestFileId.is_(None),
            ).all()
        return results

    def get_contamination_remainder_feedback_ids(self):
        with self.session() as session:
            results = session.query(
                distinct(GenomicManifestFeedback.id)
            ).join(
                GenomicFileProcessed,
                GenomicManifestFeedback.inputManifestFileId == GenomicFileProcessed.genomicManifestFileId
            ).join(
                GenomicSetMember,
                GenomicFileProcessed.id == GenomicSetMember.aw1FileProcessedId
            ).join(
                GenomicGCValidationMetrics,
                GenomicSetMember.id == GenomicGCValidationMetrics.genomicSetMemberId
            ).filter(
                GenomicSetMember.aw2fManifestJobRunID.is_(None),
                GenomicSetMember.genomicWorkflowState.notin_(self.exclude_states),
                GenomicManifestFeedback.feedbackManifestFileId.isnot(None),
            ).all()
        return results

    def get_feedback_records_from_ids(self, ids: list):
        with self.session() as session:
            return session.query(
                self.model_type
            ).filter(
                self.model_type.id.in_(ids)
            ).all()

    def get_feedback_record_counts_from_filepath(self, filepath):
        with self.session() as session:
            return session.query(GenomicManifestFeedback.feedbackRecordCount).join(
                GenomicManifestFile,
                GenomicManifestFile.id == GenomicManifestFeedback.inputManifestFileId
            ).filter(
                GenomicManifestFeedback.ignoreFlag != 1,
                GenomicManifestFile.filePath == filepath
            ).first()

    def get_feedback_reconcile_records(self, filepath=None):
        with self.session() as session:
            feedback_records = session.query(
                GenomicManifestFeedback.id.label('feedback_id'),
                functions.count(GenomicAW1Raw.sample_id).label('raw_feedback_count'),
                GenomicManifestFeedback.feedbackRecordCount,
                GenomicManifestFile.filePath,
            ).join(
                GenomicManifestFile,
                GenomicManifestFile.id == GenomicManifestFeedback.inputManifestFileId
            ).join(
                GenomicAW1Raw,
                GenomicAW1Raw.file_path == GenomicManifestFile.filePath
            ).join(
                GenomicSetMember,
                GenomicSetMember.sampleId == GenomicAW1Raw.sample_id
            ).join(
                GenomicGCValidationMetrics,
                GenomicGCValidationMetrics.genomicSetMemberId == GenomicSetMember.id
            ).filter(
                GenomicAW1Raw.sample_id.isnot(None),
                GenomicGCValidationMetrics.contamination.isnot(None),
                GenomicGCValidationMetrics.contamination != '',
                GenomicSetMember.genomeType.notin_(["saliva_array", "saliva_wgs"]),
                GenomicSetMember.genomicWorkflowState.notin_(self.exclude_states),
                GenomicGCValidationMetrics.ignoreFlag == 0,
            ).group_by(
                GenomicManifestFeedback.id,
                GenomicManifestFeedback.feedbackRecordCount,
                GenomicManifestFile.filePath,
            )

            if filepath:
                feedback_records = feedback_records.filter(
                    GenomicManifestFile.filePath == filepath
                )

            return feedback_records.all()


class GenomicAW1RawDao(BaseDao, GenomicDaoMixin):
    def __init__(self):
        super(GenomicAW1RawDao, self).__init__(
            GenomicAW1Raw, order_by_ending=['id'])

    def get_id(self, obj):
        pass

    def from_client_json(self):
        pass

    def get_record_count_from_filepath(self, filepath):
        with self.session() as session:
            return session.query(
                functions.count(GenomicAW1Raw.id)
            ).filter(
                GenomicAW1Raw.file_path == filepath,
                GenomicAW1Raw.biobank_id != ""
            ).one_or_none()

    def get_raw_record_from_identifier_genome_type(self, *, identifier, genome_type):
        with self.session() as session:
            record = session.query(GenomicAW1Raw).filter(
                GenomicAW1Raw.biobank_id == identifier,
                GenomicAW1Raw.file_path.like(f"%$_{genome_type_map[genome_type]}$_%", escape="$"),
                GenomicAW1Raw.ignore_flag == 0
            ).order_by(
                GenomicAW1Raw.biobank_id.desc(),
                GenomicAW1Raw.created.desc()
            ).first()

            return record

    def get_set_member_deltas(self):
        with self.session() as session:
            return session.query(
                GenomicAW1Raw
            ).outerjoin(
                GenomicSetMember,
                GenomicSetMember.sampleId == GenomicAW1Raw.sample_id
            ).filter(
                GenomicSetMember.id.is_(None),
                GenomicAW1Raw.ignore_flag == 0,
                GenomicAW1Raw.biobank_id != "",
                GenomicAW1Raw.sample_id != "",
                GenomicAW1Raw.collection_tube_id != "",
                GenomicAW1Raw.test_name != "",
            ).order_by(GenomicAW1Raw.id).all()

    def truncate(self):
        if GAE_PROJECT == 'localhost' and os.environ["UNITTEST_FLAG"] == "1":
            with self.session() as session:
                session.execute("DELETE FROM genomic_aw1_raw WHERE TRUE")

    def delete_from_filepath(self, filepath):
        with self.session() as session:
            session.query(GenomicAW1Raw).filter(
                GenomicAW1Raw.file_path == filepath
            ).delete()


class GenomicAW2RawDao(BaseDao, GenomicDaoMixin):
    def __init__(self):
        super(GenomicAW2RawDao, self).__init__(
            GenomicAW2Raw, order_by_ending=['id'])

    def get_id(self, obj):
        pass

    def from_client_json(self):
        pass

    def get_record_count_from_filepath(self, filepath):
        with self.session() as session:
            return session.query(
                functions.count(GenomicAW2Raw.id)
            ).filter(
                GenomicAW2Raw.file_path == filepath
            ).one_or_none()

    def get_raw_record_from_identifier_genome_type(self, *, identifier, genome_type):
        with self.session() as session:
            record = session.query(GenomicAW2Raw).filter(
                GenomicAW2Raw.sample_id == identifier,
                GenomicAW2Raw.file_path.like(f"%$_{genome_type_map[genome_type]}$_%", escape="$"),
                GenomicAW2Raw.ignore_flag == 0
            ).order_by(
                GenomicAW2Raw.biobank_id.desc(),
                GenomicAW2Raw.created.desc()
            ).first()
            return record

    def get_aw2_ingestion_deltas(self):
        with self.session() as session:
            return session.query(
                GenomicAW2Raw
            ).join(
                GenomicSetMember,
                GenomicSetMember.sampleId == GenomicAW2Raw.sample_id
            ).outerjoin(
                GenomicGCValidationMetrics,
                GenomicGCValidationMetrics.genomicSetMemberId == GenomicSetMember.id
            ).filter(
                GenomicGCValidationMetrics.id.is_(None),
                GenomicAW2Raw.ignore_flag == 0,
                GenomicAW2Raw.biobank_id != "",
                GenomicAW2Raw.sample_id != "",
                GenomicSetMember.replatedMemberId.is_(None),
            ).order_by(GenomicAW2Raw.id).all()

    def delete_from_filepath(self, filepath):
        with self.session() as session:
            session.query(GenomicAW2Raw).filter(
                GenomicAW2Raw.file_path == filepath
            ).delete()

    def truncate(self):
        if GAE_PROJECT == 'localhost' and os.environ["UNITTEST_FLAG"] == "1":
            with self.session() as session:
                session.execute("DELETE FROM genomic_aw2_raw WHERE TRUE")


class GenomicAW3RawDao(BaseDao, GenomicDaoMixin):
    def __init__(self):
        super(GenomicAW3RawDao, self).__init__(
            GenomicAW3Raw, order_by_ending=['id'])

    def get_id(self, obj):
        pass

    def from_client_json(self):
        pass


class GenomicAW4RawDao(BaseDao, GenomicDaoMixin):
    def __init__(self):
        super(GenomicAW4RawDao, self).__init__(
            GenomicAW4Raw, order_by_ending=['id'])

    def get_id(self, obj):
        pass

    def from_client_json(self):
        pass


class GenomicW1ILRawDao(BaseDao, GenomicDaoMixin):
    def __init__(self):
        super(GenomicW1ILRawDao, self).__init__(
            GenomicW1ILRaw, order_by_ending=['id'])

    def get_id(self, obj):
        pass

    def from_client_json(self):
        pass


class GenomicW2WRawDao(BaseDao, GenomicDaoMixin):
    def __init__(self):
        super(GenomicW2WRawDao, self).__init__(
            GenomicW2WRaw, order_by_ending=['id'])

    def get_id(self, obj):
        pass

    def from_client_json(self):
        pass


class GenomicW2SCRawDao(BaseDao, GenomicDaoMixin):
    def __init__(self):
        super(GenomicW2SCRawDao, self).__init__(
            GenomicW2SCRaw, order_by_ending=['id'])

    def get_id(self, obj):
        pass

    def from_client_json(self):
        pass


class GenomicW3NSRawDao(BaseDao, GenomicDaoMixin):
    def __init__(self):
        super(GenomicW3NSRawDao, self).__init__(
            GenomicW3NSRaw, order_by_ending=['id'])

    def get_id(self, obj):
        pass

    def from_client_json(self):
        pass


class GenomicW3SCRawDao(BaseDao, GenomicDaoMixin):
    def __init__(self):
        super(GenomicW3SCRawDao, self).__init__(
            GenomicW3SCRaw, order_by_ending=['id'])

    def get_id(self, obj):
        pass

    def from_client_json(self):
        pass


class GenomicW3SRRawDao(BaseDao, GenomicDaoMixin):
    def __init__(self):
        super(GenomicW3SRRawDao, self).__init__(
            GenomicW3SRRaw, order_by_ending=['id'])

    def get_id(self, obj):
        pass

    def from_client_json(self):
        pass


class GenomicW3SSRawDao(BaseDao, GenomicDaoMixin):
    def __init__(self):
        super(GenomicW3SSRawDao, self).__init__(
            GenomicW3SSRaw, order_by_ending=['id'])

    def get_id(self, obj):
        pass

    def from_client_json(self):
        pass


class GenomicW4WRRawDao(BaseDao, GenomicDaoMixin):
    def __init__(self):
        super(GenomicW4WRRawDao, self).__init__(
            GenomicW4WRRaw, order_by_ending=['id'])

    def get_id(self, obj):
        pass

    def from_client_json(self):
        pass


class GenomicW5NFRawDao(BaseDao, GenomicDaoMixin):
    def __init__(self):
        super(GenomicW5NFRawDao, self).__init__(
            GenomicW5NFRaw, order_by_ending=['id'])

    def get_id(self, obj):
        pass

    def from_client_json(self):
        pass


class GenomicIncidentDao(UpdatableDao, GenomicDaoMixin):
    validate_version_match = False

    def __init__(self):
        super(GenomicIncidentDao, self).__init__(
            GenomicIncident, order_by_ending=['id'])

    def get_id(self, obj):
        return obj.id

    def from_client_json(self):
        pass

    def get_by_message(self, message):
        maximum_message_length = GenomicIncident.message.type.length
        _, truncated_value = self.truncate_value(
            message,
            maximum_message_length,
        )
        with self.session() as session:
            return session.query(
                GenomicIncident
            ).filter(
                GenomicIncident.message == truncated_value
            ).first()

    def get_by_source_file_id(self, file_id):
        with self.session() as session:
            return session.query(
                GenomicIncident
            ).filter(
                GenomicIncident.source_file_processed_id == file_id
            ).all()

    def insert(self, incident: GenomicIncident) -> GenomicIncident:
        maximum_message_length = GenomicIncident.message.type.length
        is_truncated, truncated_value = self.truncate_value(
            incident.message,
            maximum_message_length,
        )
        if is_truncated:
            logging.warning('Truncating incident message when storing (too many characters for database column)')
        incident.message = truncated_value

        return super(GenomicIncidentDao, self).insert(incident)

    @staticmethod
    def truncate_value(value, max_length):
        is_truncated = False
        if len(value) > max_length:
            is_truncated = True
            value = value[:max_length]

        return is_truncated, value

    def get_new_ingestion_incidents(self, from_days=1):
        with self.session() as session:
            incidents = session.query(
                GenomicIncident.id,
                GenomicIncident.code,
                GenomicIncident.submitted_gc_site_id,
                GenomicIncident.message,
                GenomicJobRun.jobId,
                GenomicFileProcessed.filePath,
                GenomicFileProcessed.fileName
            ).join(
                GenomicJobRun,
                GenomicJobRun.id == GenomicIncident.source_job_run_id
            ).join(
                GenomicFileProcessed,
                GenomicFileProcessed.id == GenomicIncident.source_file_processed_id
            ).filter(
                GenomicIncident.email_notification_sent == 0,
                GenomicJobRun.jobId.in_(self.ingestion_job_ids),
                GenomicIncident.source_file_processed_id.isnot(None),
                GenomicIncident.source_job_run_id.isnot(None),
                GenomicIncident.submitted_gc_site_id.isnot(None)
            )

            if from_days:
                from_date = clock.CLOCK.now() - timedelta(days=from_days)
                from_date = from_date.replace(microsecond=0)

                incidents = incidents.filter(
                    GenomicIncident.created >= from_date
                )
            return incidents.all()

    def batch_update_incident_fields(self, ids, _type='email'):
        value_update_map = {
            'email': {
                'email_notification_sent': 1,
                'email_notification_sent_date': datetime.utcnow()
            },
            'resolved': {
                'status': GenomicIncidentStatus.RESOLVED.name
            }
        }

        value_dict = value_update_map[_type]

        if not type(ids) is list:
            ids = [ids]

        for _id in ids:
            current_incident = self.get(_id)
            for key, value in value_dict.items():
                setattr(current_incident, key, value)

            self.update(current_incident)

    def get_open_incident_by_file_name(self, filename):
        with self.session() as session:
            return session.query(
                GenomicIncident
            ).filter(
                GenomicIncident.manifest_file_name == filename,
                GenomicIncident.status == GenomicIncidentStatus.OPEN.name
            ).all()

    def get_daily_report_resolved_manifests(self, from_date):
        with self.session() as session:
            incidents = session.query(
                GenomicJobRun.jobId.label('job_id'),
                GenomicFileProcessed.filePath.label('file_path'),
                GenomicIncident.status
            ).join(
                GenomicJobRun,
                GenomicJobRun.id == GenomicIncident.source_job_run_id
            ).join(
                GenomicFileProcessed,
                GenomicFileProcessed.id == GenomicIncident.source_file_processed_id
            ).filter(
                GenomicIncident.status == GenomicIncidentStatus.RESOLVED.name,
                GenomicJobRun.jobId.in_(self.ingestion_job_ids),
                GenomicIncident.source_file_processed_id.isnot(None),
                GenomicIncident.source_job_run_id.isnot(None),
                or_(GenomicIncident.created >= from_date.replace(microsecond=0),
                    GenomicIncident.modified >= from_date.replace(microsecond=0))
            )

            return incidents.all()

    def get_daily_report_incidents(self, from_date):
        with self.session() as session:
            incidents = session.query(
                GenomicIncident.code,
                GenomicIncident.created,
                GenomicIncident.biobank_id,
                GenomicIncident.genomic_set_member_id,
                GenomicIncident.source_job_run_id,
                GenomicIncident.source_file_processed_id
            ).filter(
                GenomicIncident.created >= from_date.replace(microsecond=0)
            ).order_by(GenomicIncident.created.desc())

            return incidents.all()


class GenomicCloudRequestsDao(UpdatableDao):
    validate_version_match = False

    def __init__(self):
        super(GenomicCloudRequestsDao, self).__init__(
            GenomicCloudRequests, order_by_ending=['id'])

    def get_id(self, obj):
        pass

    def from_client_json(self):
        pass


class GenomicMemberReportStateDao(UpdatableDao, GenomicDaoMixin):
    validate_version_match = False

    def __init__(self):
        super(GenomicMemberReportStateDao, self).__init__(
            GenomicMemberReportState, order_by_ending=['id'])

    def get_id(self, obj):
        return obj.id

    def from_client_json(self):
        pass

    def get_from_member_id(self, obj_id):
        with self.session() as session:
            return session.query(
                GenomicMemberReportState
            ).filter(
                GenomicMemberReportState.genomic_set_member_id == obj_id
            ).first()

    def process_gem_result_to_report(
        self,
        obj: GenomicSetMember
    ):
        report_state = self.get_report_state_from_wf_state(obj.genomicWorkflowState)
        result = obj._asdict() or obj.asdict()
        del result['genomicWorkflowState']
        result['genomic_report_state'] = report_state
        result['genomic_report_state_str'] = report_state.name
        result['created'] = clock.CLOCK.now()
        result['modified'] = clock.CLOCK.now()
        return result

    @staticmethod
    def get_report_state_from_wf_state(wf_state):
        for value in GenomicReportState:
            if value.name.lower() == wf_state.name.lower():
                return value
        return None

    def get_hdr_result_positive_no_appointment(self, days=14) -> List[Tuple[int, None]]:
        """Returns participants with hdr positive result over 14 days ago and no appointment scheduled/completed"""
        result_before = clock.CLOCK.now() - timedelta(days=days)

        max_event_authored_time_subquery = sqlalchemy.orm.Query(
            functions.max(GenomicAppointmentEvent.event_authored_time).label(
                'max_event_authored_time'
            )
        ).filter(
            GenomicAppointmentEvent.event_type.notlike('%note_available'),
            GenomicAppointmentEvent.module_type == 'hdr'
        ).group_by(
            GenomicAppointmentEvent.participant_id,
        ).subquery()

        with self.session() as session:
            return session.query(
                GenomicMemberReportState.participant_id
            ).outerjoin(
                GenomicAppointmentEvent,
                and_(
                    GenomicAppointmentEvent.participant_id == GenomicMemberReportState.participant_id,
                    GenomicAppointmentEvent.module_type == 'hdr'
                )
            ).outerjoin(
                GenomicGCROutreachEscalationNotified,
                GenomicMemberReportState.participant_id == GenomicGCROutreachEscalationNotified.participant_id
            ).filter(
                GenomicGCROutreachEscalationNotified.participant_id.is_(None),
                GenomicMemberReportState.genomic_report_state == GenomicReportState.HDR_RPT_POSITIVE,
                GenomicMemberReportState.event_authored_time < result_before,
                or_(GenomicAppointmentEvent.event_type.notin_(('appointment_completed', 'appointment_scheduled',
                                                               'appointment_updated')),
                    GenomicAppointmentEvent.event_type.is_(None)),
                or_(GenomicAppointmentEvent.event_authored_time ==
                    max_event_authored_time_subquery.c.max_event_authored_time,
                    GenomicAppointmentEvent.event_authored_time.is_(None))
            ).distinct().all()


class GenomicInformingLoopDao(UpdatableDao, GenomicDaoMixin):
    validate_version_match = False

    def __init__(self):
        super(GenomicInformingLoopDao, self).__init__(
            GenomicInformingLoop, order_by_ending=['id'])

    def get_id(self, obj):
        return obj.id

    def from_client_json(self):
        pass

    @classmethod
    def build_latest_decision_query(cls, module: str) -> sqlalchemy.orm.Query:
        later_informing_loop_decision = aliased(GenomicInformingLoop)
        return (
            sqlalchemy.orm.Query(GenomicInformingLoop)
            .outerjoin(
                later_informing_loop_decision,
                and_(
                    later_informing_loop_decision.participant_id == GenomicInformingLoop.participant_id,
                    later_informing_loop_decision.module_type == module,
                    GenomicInformingLoop.event_authored_time < later_informing_loop_decision.event_authored_time,
                    later_informing_loop_decision.event_type == 'informing_loop_decision'
                )
            ).filter(
                GenomicInformingLoop.module_type == module,
                later_informing_loop_decision.event_authored_time.is_(None),
                GenomicInformingLoop.event_type == 'informing_loop_decision'
            )
        )

    def get_latest_il_for_pids(self, pid_list, module="gem", decision_values_only=True):
        """
        Returns latest event_type and decision_value
        genomic_informing_loop record for a set of participants
        :param decision_values_only: default true; don't want "started" events
        :param pid_list: list of participant_id
        :param module: gem (default), hdr, or pgx
        :return: query result
        """
        with self.session() as session:
            query = self.build_latest_decision_query(module).with_entities(
                GenomicInformingLoop.event_type,
                GenomicInformingLoop.decision_value,
                GenomicInformingLoop.created_from_metric_id
            ).filter(
                GenomicInformingLoop.participant_id.in_(pid_list)
            )
            if decision_values_only:
                query.filter(GenomicInformingLoop.event_type == 'informing_loop_decision')
            return query.with_session(session).all()

    def prepare_gem_migration_obj(self, row):
        decision_mappings = {
            "ConsentAncestryTraits_No": "no",
            "ConsentAncestryTraits_Yes": "yes",
            "ConsentAncestryTraits_NotSure": "maybe_later"
        }
        return self.to_dict(GenomicInformingLoop(
            created=clock.CLOCK.now(),
            modified=clock.CLOCK.now(),
            participant_id=row.participantId,
            event_type='informing_loop_decision',
            event_authored_time=row.authored,
            module_type='gem',
            decision_value=decision_mappings.get(row.value)
        ))


class GenomicResultViewedDao(UpdatableDao, GenomicDaoMixin):
    validate_version_match = False

    def __init__(self):
        super(GenomicResultViewedDao, self).__init__(
            GenomicResultViewed, order_by_ending=['id'])

    def get_id(self, obj):
        return obj.id

    def from_client_json(self):
        pass

    def get_result_record_by_pid_module(self, pid, module='gem'):
        with self.session() as session:
            return session.query(
                GenomicResultViewed
            ).filter(
                GenomicResultViewed.participant_id == pid,
                GenomicResultViewed.module_type == module
            ).one_or_none()


class GenomicAppointmentEventDao(BaseDao, GenomicDaoMixin):

    def __init__(self):
        super(GenomicAppointmentEventDao, self).__init__(
            GenomicAppointmentEvent, order_by_ending=['id'])

    def get_id(self, obj):
        pass

    def from_client_json(self):
        pass

    def get_appointments_gror_changed(self):
        with self.session() as session:
            return session.query(
                GenomicAppointmentEvent
            ).join(
                ParticipantSummary,
                GenomicAppointmentEvent.participant_id == ParticipantSummary.participantId
            ).outerjoin(
                GenomicAppointmentEventNotified,
                GenomicAppointmentEvent.participant_id == GenomicAppointmentEventNotified.participant_id
            ).filter(
                GenomicAppointmentEvent.event_type == 'appointment_scheduled',
                GenomicAppointmentEventNotified.id.is_(None),
                ParticipantSummary.consentForGenomicsROR.in_((QuestionnaireStatus.SUBMITTED_NO_CONSENT,
                                                              QuestionnaireStatus.SUBMITTED_NOT_SURE))
            ).all()


class GenomicAppointmentEventNotifiedDao(BaseDao, GenomicDaoMixin):

    def __init__(self):
        super(GenomicAppointmentEventNotifiedDao, self).__init__(
            GenomicAppointmentEventNotified, order_by_ending=['id'])

    def get_id(self, obj):
        pass

    def from_client_json(self):
        pass


class GenomicGcDataFileDao(BaseDao):
    def __init__(self):
        super(GenomicGcDataFileDao, self).__init__(
            GenomicGcDataFile, order_by_ending=['id'])

    def from_client_json(self):
        pass

    def get_id(self, obj):
        pass

    def get_with_sample_id(self, sample_id):
        with self.session() as session:
            return session.query(
                GenomicGcDataFile
            ).filter(
                GenomicGcDataFile.identifier_type == 'sample_id',
                GenomicGcDataFile.identifier_value == sample_id,
                GenomicGcDataFile.ignore_flag == 0
            ).all()

    def get_with_chipwellbarcode(self, chipwellbarcode):
        with self.session() as session:
            return session.query(
                GenomicGcDataFile
            ).filter(
                GenomicGcDataFile.identifier_type == 'chipwellbarcode',
                GenomicGcDataFile.identifier_value == chipwellbarcode,
                GenomicGcDataFile.ignore_flag == 0
            ).all()

    def get_with_file_path(self, file_path):
        with self.session() as session:
            return session.query(
                GenomicGcDataFile
            ).filter(
                GenomicGcDataFile.file_path == file_path,
                GenomicGcDataFile.ignore_flag == 0
            ).all()


class GcDataFileStagingDao(BaseDao, GenomicDaoMixin):
    def __init__(self):
        super(GcDataFileStagingDao, self).__init__(
            GcDataFileStaging, order_by_ending=['id'])

    def truncate(self):
        with self.session() as session:
            session.execute("DELETE FROM gc_data_file_staging WHERE TRUE")

    def get_missing_gc_data_file_records(self, sample_ids=None):
        with self.session() as session:
            query = session.query(
                GcDataFileStaging
            ).outerjoin(
                GenomicGcDataFile,
                GcDataFileStaging.file_path == GenomicGcDataFile.file_path
            ).filter(
                GenomicGcDataFile.id.is_(None)
            )
            if sample_ids:
                query = query.filter(
                    GenomicGcDataFile.identifier_type == 'sample_id',
                    GenomicGcDataFile.identifier_value.in_(sample_ids)
                )
            return query.all()


class GenomicGcDataFileMissingDao(UpdatableDao):
    validate_version_match = False

    def __init__(self):
        super(GenomicGcDataFileMissingDao, self).__init__(
            GenomicGcDataFileMissing, order_by_ending=['id'])

    def from_client_json(self):
        pass

    def get_id(self, obj):
        return obj.id

    def get_with_metrics_id(self, metric_id):
        with self.session() as session:
            return session.query(
                GenomicGcDataFileMissing
            ).filter(
                GenomicGcDataFileMissing.gc_validation_metric_id == metric_id,
                GenomicGcDataFileMissing.ignore_flag == 0
            ).all()

    def get_with_run_id(self, run_id):
        with self.session() as session:
            return session.query(
                GenomicGcDataFileMissing
            ).filter(
                GenomicGcDataFileMissing.run_id == run_id,
                GenomicGcDataFileMissing.ignore_flag == 0
            ).all()

    def remove_resolved_from_days(self, *, num_days=90):
        delete_date = datetime.utcnow() - timedelta(days=num_days)
        with self.session() as session:
            return session.query(
                GenomicGcDataFileMissing
            ).filter(
                GenomicGcDataFileMissing.resolved == 1,
                GenomicGcDataFileMissing.ignore_flag == 0,
                GenomicGcDataFileMissing.resolved_date.isnot(None),
                GenomicGcDataFileMissing.resolved_date < delete_date
            ).delete()

    def get_files_to_resolve(self, limit=None):
        with self.session() as session:
            subquery = session.query(
                GenomicGcDataFileMissing.id,
                GenomicGcDataFileMissing.gc_validation_metric_id,
                GenomicGcDataFileMissing.file_type,
                sqlalchemy.case(
                    [
                        (GenomicSetMember.genomeType == 'aou_array', GenomicGCValidationMetrics.chipwellbarcode),
                        (GenomicSetMember.genomeType == 'aou_wgs',
                         GenomicSetMember.sampleId)
                    ], ).label('identifier_value'),
                sqlalchemy.case(
                    [
                        (GenomicSetMember.genomeType == 'aou_array', 'chipwellbarcode'),
                        (GenomicSetMember.genomeType == 'aou_wgs', 'sample_id')
                    ], ).label('identifier_type')
            ).join(
                GenomicGCValidationMetrics,
                GenomicGCValidationMetrics.id == GenomicGcDataFileMissing.gc_validation_metric_id
            ).join(
                GenomicSetMember,
                GenomicSetMember.id == GenomicGCValidationMetrics.genomicSetMemberId
            ).filter(
                GenomicGcDataFileMissing.resolved == 0,
                GenomicGcDataFileMissing.ignore_flag == 0,
                GenomicGcDataFileMissing.resolved_date.is_(None)
            ).subquery()

            results = session.query(
                subquery,
            ).join(
                GenomicGcDataFile,
                and_(GenomicGcDataFile.identifier_type == subquery.c.identifier_type,
                     GenomicGcDataFile.identifier_value == subquery.c.identifier_value,
                     GenomicGcDataFile.file_type == subquery.c.file_type, )
            )

            if limit:
                results = results.limit(limit)

            return results.all()

    def batch_update_resolved_file(self, unresolved_files):
        file_dao = GenomicGcDataFileDao()
        method_map = {
            'chipwellbarcode': file_dao.get_with_chipwellbarcode,
            'sample_id': file_dao.get_with_sample_id
        }
        get_method = method_map[unresolved_files[0].identifier_type]

        for unresolved_file in unresolved_files:
            existing_data_files = get_method(unresolved_file.identifier_value)
            has_file_type_record = any([obj.file_type == unresolved_file.file_type for obj in existing_data_files])
            if has_file_type_record:
                update_record = self.get(unresolved_file.id)
                update_record.resolved = 1
                update_record.resolved_date = datetime.utcnow()
                self.update(update_record)


class GemToGpMigrationDao(BaseDao):
    def __init__(self):
        super(GemToGpMigrationDao, self).__init__(
            GemToGpMigration, order_by_ending=['id'])

    def prepare_obj(self, row, job_run, file_path):
        return self.to_dict(GemToGpMigration(
            file_path=file_path,
            run_id=job_run,
            created=clock.CLOCK.now(),
            modified=clock.CLOCK.now(),
            participant_id=row.participantId,
            informing_loop_status='success',
            informing_loop_authored=row.authored,
            ancestry_traits_response=row.value,
        ))

    def get_data_for_export(self, run_id, limit=None, pids=None):
        with self.session() as session:
            results = session.query(
                QuestionnaireResponse.participantId,
                QuestionnaireResponse.authored,
                Code.value
            ).join(
                QuestionnaireResponseAnswer,
                QuestionnaireResponseAnswer.questionnaireResponseId == QuestionnaireResponse.questionnaireResponseId
            ).join(
                Code,
                Code.codeId == QuestionnaireResponseAnswer.valueCodeId
            ).outerjoin(
                GemToGpMigration,
                and_(GemToGpMigration.participant_id == QuestionnaireResponse.participantId,
                     GemToGpMigration.run_id == run_id)
            ).filter(
                Code.value.in_(["ConsentAncestryTraits_Yes",
                                "ConsentAncestryTraits_No",
                                "ConsentAncestryTraits_NotSure"]),
            )
            if pids:
                results = results.filter(
                    QuestionnaireResponse.participantId.in_(pids)
                )

            if limit:
                results = results.limit(limit)

            return results.all()


class UserEventMetricsDao(BaseDao, GenomicDaoMixin):
    def __init__(self):
        super(UserEventMetricsDao, self).__init__(
            UserEventMetrics, order_by_ending=['id'])

    def from_client_json(self):
        pass

    def truncate(self):
        if GAE_PROJECT == 'localhost' and os.environ["UNITTEST_FLAG"] == "1":
            with self.session() as session:
                session.execute("DELETE FROM user_event_metrics WHERE TRUE")

    def get_id(self, obj):
        pass

    def build_set_member_sample_id_subquery(self, genome_type):

        set_member_alias = aliased(GenomicSetMember)

        sample_id_query = sqlalchemy.orm.Query(
            [GenomicSetMember.sampleId.label("sample_id"),
             GenomicSetMember.participantId.label("participant_id"),
             GenomicSetMember.id.label("member_id")]
        ).outerjoin(
            set_member_alias,
            and_(
                set_member_alias.participantId == GenomicSetMember.participantId,
                set_member_alias.ignoreFlag == 0,
                set_member_alias.genomeType == GenomicSetMember.genomeType,
                GenomicSetMember.id < set_member_alias.id,
            )
        ).filter(
            GenomicSetMember.genomeType == genome_type,
            GenomicSetMember.ignoreFlag == 0,
            GenomicSetMember.sampleId.isnot(None),
            set_member_alias.id.is_(None)
        ).subquery()

        return sample_id_query

    def get_event_message_informing_loop_mismatches(self, module="gem"):
        """
        Returns message data for records in user_event_metrics
        but not in genomic_informing_loop
        :param module: gem (default), hdr, or pgx
        :return: query result
        """
        event_type_str = "informing_loop_decision"
        replace_string = f"{module}.{event_type_str}."

        genome_type = "aou_wgs" if module in ['hdr', 'pgx'] else 'aou_array'

        event_mappings = {il.replace(replace_string, ""): event for il, event
                          in informing_loop_event_mappings.items() if il.startswith(module)}

        with self.session() as session:
            event_metrics_alias = aliased(UserEventMetrics)
            informing_loop_alias = aliased(GenomicInformingLoop)

            records_subquery = session.query(
                UserEventMetrics.participant_id,
                UserEventMetrics.id,
                UserEventMetrics.event_name,
                coalesce(GenomicInformingLoop.decision_value, "missing").label("decision_value"),
                UserEventMetrics.created_at,
                GenomicInformingLoop.sample_id,
                coalesce(GenomicInformingLoop.event_authored_time, "0").label("event_authored_time"),
                sqlalchemy.case(
                    [
                        (UserEventMetrics.event_name == event_mappings['yes'], 'yes'),
                        (UserEventMetrics.event_name == event_mappings['no'], 'no'),
                        (UserEventMetrics.event_name == event_mappings['maybe_later'], 'maybe_later')
                    ],
                    else_="missing"
                ).label('event_value')
            ).select_from(
                UserEventMetrics
            ).outerjoin(
                event_metrics_alias,
                and_(
                    event_metrics_alias.participant_id == UserEventMetrics.participant_id,
                    event_metrics_alias.event_name.in_(event_mappings.values()),
                    UserEventMetrics.created_at < event_metrics_alias.created_at,
                )
            ).outerjoin(
                GenomicInformingLoop,
                and_(
                    UserEventMetrics.participant_id == GenomicInformingLoop.participant_id,
                    GenomicInformingLoop.event_type == event_type_str,
                    GenomicInformingLoop.module_type == module,
                )
            ).outerjoin(
                informing_loop_alias,
                and_(
                    informing_loop_alias.participant_id == GenomicInformingLoop.participant_id,
                    informing_loop_alias.module_type == GenomicInformingLoop.module_type,
                    GenomicInformingLoop.event_authored_time < informing_loop_alias.event_authored_time,
                )
            ).filter(
                UserEventMetrics.ignore_flag == 0,
                UserEventMetrics.event_name.in_(event_mappings.values()),
                UserEventMetrics.reconcile_job_run_id.is_(None),
                event_metrics_alias.created_at.is_(None)
            ).subquery()

            sample_ids_subquery = self.build_set_member_sample_id_subquery(genome_type)

            records = session.query(
                records_subquery.c.participant_id,
                sqlalchemy.func.max(records_subquery.c.id).label("event_id"),
                records_subquery.c.decision_value,
                records_subquery.c.event_value,
                records_subquery.c.created_at,
                coalesce(records_subquery.c.sample_id, sample_ids_subquery.c.sample_id).label("sample_id"),
            ).join(
                sample_ids_subquery,
                sample_ids_subquery.c.participant_id == records_subquery.c.participant_id
            ).filter(
                records_subquery.c.decision_value != records_subquery.c.event_value,
                records_subquery.c.event_authored_time < records_subquery.c.created_at
            ).group_by(
                records_subquery.c.participant_id,
                records_subquery.c.decision_value,
                records_subquery.c.event_value,
                records_subquery.c.created_at,
                coalesce(records_subquery.c.sample_id, sample_ids_subquery.c.sample_id)
            )

            return records.all()

    def get_event_message_results_ready_mismatches(self, module="pgx"):
        """
        Returns message data for records in user_event_metrics
        but not in genomic_report_state
        :param module: hdr, or pgx
        :return: query result
        """
        module_mappings = cvl_result_reconciliation_modules
        event_type = "result_ready"

        event_names = [name for name in message_broker_report_ready_event_state_mappings.keys()
                       if name.startswith(module)]

        with self.session() as session:
            sample_ids_subquery = self.build_set_member_sample_id_subquery("aou_wgs")

            records = session.query(
                sample_ids_subquery.c.sample_id,
                sample_ids_subquery.c.member_id,
                UserEventMetrics.participant_id,
                UserEventMetrics.event_name,
                UserEventMetrics.created_at,
                UserEventMetrics.id.label("event_id")
            ).outerjoin(
                GenomicMemberReportState,
                and_(
                    GenomicMemberReportState.participant_id == UserEventMetrics.participant_id,
                    GenomicMemberReportState.module == module_mappings[module],
                    GenomicMemberReportState.event_type == event_type
                )
            ).join(
                sample_ids_subquery,
                sample_ids_subquery.c.participant_id == UserEventMetrics.participant_id
            ).filter(
                GenomicMemberReportState.id.is_(None),
                UserEventMetrics.event_name.in_(event_names),
                UserEventMetrics.ignore_flag != 1
            )

            return records.all()

    def get_event_message_results_viewed_mismatches(self, module="pgx"):
        """
        Returns message data for records in user_event_metrics
        but not in genomic_report_viewed
        :param module: hdr, or pgx
        :return: query result
        """
        module_mappings = cvl_result_reconciliation_modules
        event_type = "result_viewed"

        event_names = [name for name in message_broker_report_viewed_event_state_mappings
                       if name.startswith(module)]

        with self.session() as session:
            sample_ids_subquery = self.build_set_member_sample_id_subquery("aou_wgs")

            records = session.query(
                sample_ids_subquery.c.sample_id,
                sample_ids_subquery.c.member_id,
                UserEventMetrics.participant_id,
                UserEventMetrics.event_name,
                UserEventMetrics.created_at,
                UserEventMetrics.id.label("event_id")
            ).outerjoin(
                GenomicResultViewed,
                and_(
                    GenomicResultViewed.participant_id == UserEventMetrics.participant_id,
                    GenomicResultViewed.module_type == module_mappings[module],
                    GenomicResultViewed.event_type == event_type
                )
            ).join(
                sample_ids_subquery,
                sample_ids_subquery.c.participant_id == UserEventMetrics.participant_id
            ).filter(
                GenomicResultViewed.id.is_(None),
                UserEventMetrics.event_name.in_(event_names),
                UserEventMetrics.ignore_flag != 1
            )

            return records.all()


class GenomicCVLSecondSampleDao(BaseDao):

    def __init__(self):
        super(GenomicCVLSecondSampleDao, self).__init__(
            GenomicCVLSecondSample, order_by_ending=['id'])

    def from_client_json(self):
        pass

    def get_id(self, obj):
        pass


class GenomicCVLAnalysisDao(UpdatableDao):

    validate_version_match = False

    def __init__(self):
        super(GenomicCVLAnalysisDao, self).__init__(
            GenomicCVLAnalysis, order_by_ending=['id'])

    def from_client_json(self):
        pass

    def get_id(self, obj):
        return obj.id

    def get_passed_analysis_member_module(self, member_id, module):
        with self.session() as session:
            return session.query(
                GenomicCVLAnalysis
            ).filter(
                GenomicCVLAnalysis.genomic_set_member_id == member_id,
                GenomicCVLAnalysis.clinical_analysis_type == module,
                GenomicCVLAnalysis.ignore_flag != 1,
                GenomicCVLAnalysis.failed == 0,
            ).one_or_none()


class GenomicResultWorkflowStateDao(BaseDao):

    def __init__(self):
        super(GenomicResultWorkflowStateDao, self).__init__(
            GenomicResultWorkflowState, order_by_ending=['id'])

    def from_client_json(self):
        pass

    def get_id(self, obj):
        pass

    def get_by_member_id(self, member_id, module_type=None):
        with self.session() as session:
            records = session.query(
                GenomicResultWorkflowState
            ).filter(
                GenomicResultWorkflowState.genomic_set_member_id == member_id
            )
            if not module_type:
                return records.all()

            records = records.filter(
                GenomicResultWorkflowState.results_module == module_type
            ).one_or_none()

            return records

    def insert_new_result_record(self, *, member_id, module_type, state=None):
        inserted_state = ResultsWorkflowState.CVL_W1IL if not state else state
        self.insert(GenomicResultWorkflowState(
            genomic_set_member_id=member_id,
            results_workflow_state=inserted_state,
            results_workflow_state_str=inserted_state.name,
            results_module=module_type,
            results_module_str=module_type.name
        ))


class GenomicQueriesDao(BaseDao):
    def __init__(self):
        super(GenomicQueriesDao, self).__init__(
            GenomicSetMember, order_by_ending=['id'])

    def from_client_json(self):
        pass

    def get_id(self, obj):
        pass

    @classmethod
    def transform_cvl_site_id(cls, site_id=None):
        # co => bi => cvl workflow
        site_id_map = {
            'co': 'bi'
        }
        if not site_id or site_id \
                not in site_id_map.keys():
            return site_id

        return site_id_map[site_id]

    def get_missing_data_files_for_aw3(self, genome_type):
        missing_files_map = {
            config.GENOME_TYPE_ARRAY: array_file_types_attributes,
            config.GENOME_TYPE_WGS: wgs_file_types_attributes
        }[genome_type]
        required_file_types = [file_type['file_type'] for file_type in missing_files_map
                               if file_type['required']]

        with self.session() as session:
            if genome_type == config.GENOME_TYPE_ARRAY:
                subquery = session.query(
                    GenomicSetMember.id,
                    func.count(GenomicGcDataFile.file_type).label("file_count")
                ).join(
                    GenomicGCValidationMetrics,
                    GenomicGCValidationMetrics.genomicSetMemberId == GenomicSetMember.id
                ).outerjoin(
                    GenomicGcDataFile,
                    GenomicGcDataFile.identifier_value == GenomicGCValidationMetrics.chipwellbarcode
                ).filter(
                    GenomicSetMember.genomeType == genome_type,
                    GenomicGcDataFile.ignore_flag != 1,
                    GenomicGcDataFile.file_type.in_(required_file_types)
                ).group_by(GenomicSetMember.id).subquery()

                records = session.query(
                    GenomicSetMember.sampleId
                ).join(
                    subquery,
                    and_(
                        subquery.c.id == GenomicSetMember.id
                    )
                ).filter(
                    subquery.c.file_count !=
                    len(required_file_types)
                )
                return records.distinct().all()

            elif genome_type == config.GENOME_TYPE_WGS:
                subquery = session.query(
                    GenomicSetMember.id,
                    func.count(GenomicGcDataFile.file_type).label("file_count")
                ).outerjoin(
                    GenomicGcDataFile,
                    GenomicGcDataFile.identifier_value == GenomicSetMember.sampleId
                ).filter(
                    GenomicSetMember.genomeType == genome_type,
                    GenomicGcDataFile.ignore_flag != 1,
                    GenomicGcDataFile.file_type.in_(required_file_types)
                ).group_by(GenomicSetMember.id).subquery()

                records = session.query(
                    GenomicSetMember.sampleId
                ).join(
                    subquery,
                    and_(
                        subquery.c.id == GenomicSetMember.id
                    )
                ).filter(
                    subquery.c.file_count !=
                    len(required_file_types)
                )
                return records.distinct().all()

    def get_aw3_array_records(self, **kwargs):
        # should be only array genome but query also
        # used for array investigation workflow
        genome_type = kwargs.get('genome_type', config.GENOME_TYPE_ARRAY)

        idat_red_path = aliased(GenomicGcDataFile)
        idat_green_path = aliased(GenomicGcDataFile)
        idat_red_md5_path = aliased(GenomicGcDataFile)
        idat_green_md5_path = aliased(GenomicGcDataFile)
        vcf_path = aliased(GenomicGcDataFile)
        vcf_tbi_path = aliased(GenomicGcDataFile)
        vcf_md5_path = aliased(GenomicGcDataFile)

        with self.session() as session:
            aw3_rows = session.query(
                GenomicGCValidationMetrics.chipwellbarcode,
                func.concat(get_biobank_id_prefix(), GenomicSetMember.biobankId),
                GenomicSetMember.sampleId,
                func.concat(get_biobank_id_prefix(),
                            GenomicSetMember.biobankId, '_',
                            GenomicSetMember.sampleId),
                GenomicSetMember.sexAtBirth,
                GenomicSetMember.gcSiteId,
                sqlalchemy.func.concat('gs://', idat_red_path.file_path).label('idatRedPath'),
                sqlalchemy.func.concat('gs://', idat_green_path.file_path).label('idatRedMd5Path'),
                sqlalchemy.func.concat('gs://', idat_red_md5_path.file_path).label('idatGreenPath'),
                sqlalchemy.func.concat('gs://', idat_green_md5_path.file_path).label('idatGreenMd5Path'),
                sqlalchemy.func.concat('gs://', vcf_path.file_path).label('vcfPath'),
                sqlalchemy.func.concat('gs://', vcf_tbi_path.file_path).label('vcfTbiPath'),
                sqlalchemy.func.concat('gs://', vcf_md5_path.file_path).label('vcfMd5Path'),
                GenomicGCValidationMetrics.callRate,
                GenomicGCValidationMetrics.sexConcordance,
                GenomicGCValidationMetrics.contamination,
                GenomicGCValidationMetrics.processingStatus,
                Participant.researchId,
                GenomicSetMember.gcManifestSampleSource,
                GenomicGCValidationMetrics.pipelineId,
                func.IF(
                    GenomicSetMember.ai_an == 'Y',
                    sqlalchemy.sql.expression.literal("True"),
                    sqlalchemy.sql.expression.literal("False")),
                func.IF(
                    GenomicSetMember.blockResearch == 1,
                    sqlalchemy.sql.expression.literal("True"),
                    sqlalchemy.sql.expression.literal("False")),
                GenomicSetMember.blockResearchReason
            ).join(
                ParticipantSummary,
                ParticipantSummary.participantId == GenomicSetMember.participantId
            ).join(
                GenomicGCValidationMetrics,
                GenomicGCValidationMetrics.genomicSetMemberId == GenomicSetMember.id
            ).join(
                Participant,
                Participant.participantId == ParticipantSummary.participantId
            ).join(
                idat_red_path,
                and_(
                    idat_red_path.file_type == 'Red.idat',
                    idat_red_path.identifier_value == GenomicGCValidationMetrics.chipwellbarcode
                )
            ).join(
                idat_green_path,
                and_(
                    idat_green_path.file_type == 'Grn.idat',
                    idat_green_path.identifier_value == GenomicGCValidationMetrics.chipwellbarcode
                )
            ).join(
                idat_red_md5_path,
                and_(
                    idat_red_md5_path.file_type == 'Red.idat.md5sum',
                    idat_red_md5_path.identifier_value == GenomicGCValidationMetrics.chipwellbarcode
                )
            ).join(
                idat_green_md5_path,
                and_(
                    idat_green_md5_path.file_type == 'Grn.idat.md5sum',
                    idat_green_md5_path.identifier_value == GenomicGCValidationMetrics.chipwellbarcode
                )
            ).join(
                vcf_path,
                and_(
                    vcf_path.file_type == 'vcf.gz',
                    vcf_path.identifier_value == GenomicGCValidationMetrics.chipwellbarcode
                )
            ).join(
                vcf_tbi_path,
                and_(
                    vcf_tbi_path.file_type == 'vcf.gz.tbi',
                    vcf_tbi_path.identifier_value == GenomicGCValidationMetrics.chipwellbarcode
                )
            ).join(
                vcf_md5_path,
                and_(
                    vcf_md5_path.file_type == 'vcf.gz.md5sum',
                    vcf_md5_path.identifier_value == GenomicGCValidationMetrics.chipwellbarcode
                )
            ).outerjoin(
                GenomicAW3Raw,
                and_(
                    GenomicAW3Raw.sample_id == GenomicSetMember.sampleId,
                    GenomicAW3Raw.genome_type == genome_type,
                    GenomicAW3Raw.ignore_flag != 1,
                )
            ).filter(
                GenomicSetMember.genomicWorkflowState != GenomicWorkflowState.IGNORE,
                GenomicSetMember.genomeType == genome_type,
                GenomicSetMember.aw3ManifestJobRunID.is_(None),
                GenomicSetMember.ignoreFlag != 1,
                GenomicGCValidationMetrics.processingStatus.ilike('pass'),
                GenomicGCValidationMetrics.ignoreFlag != 1,
                ParticipantSummary.withdrawalStatus == WithdrawalStatus.NOT_WITHDRAWN,
                ParticipantSummary.suspensionStatus == SuspensionStatus.NOT_SUSPENDED,
                GenomicAW3Raw.id.is_(None)
            )
            return aw3_rows.distinct().all()

    def get_aw3_wgs_records(self, **kwargs):
        # should be only wgs genome but query also
        # used for wgs investigation workflow
        genome_type = kwargs.get('genome_type', config.GENOME_TYPE_WGS)
        pipeline_id = kwargs.get('pipeline_id')

        if not pipeline_id:
            return []

        with self.session() as session:

            hard_filtered_vcf_gz = aliased(GenomicGcDataFile)
            hard_filtered_vcf_gz_tbi = aliased(GenomicGcDataFile)
            hard_filtered_vcf_gz_md5_sum = aliased(GenomicGcDataFile)
            cram = aliased(GenomicGcDataFile)
            cram_md5_sum = aliased(GenomicGcDataFile)
            cram_crai = aliased(GenomicGcDataFile)
            hard_filtered_gvcf_gz = aliased(GenomicGcDataFile)
            hard_filtered_gvcf_gz_md5_sum = aliased(GenomicGcDataFile)
            array_check = aliased(GenomicSetMember)

            aw3_rows = session.query(
                func.concat(get_biobank_id_prefix(), GenomicSetMember.biobankId),
                GenomicSetMember.sampleId,
                sqlalchemy.func.concat(get_biobank_id_prefix(),
                                       GenomicSetMember.biobankId, '_',
                                       GenomicSetMember.sampleId),
                GenomicSetMember.sexAtBirth,
                GenomicSetMember.gcSiteId,
                sqlalchemy.func.concat('gs://', hard_filtered_vcf_gz.file_path).label('hfVcfPath'),
                sqlalchemy.func.concat('gs://', hard_filtered_vcf_gz_tbi.file_path).label('hfVcfTbiPath'),
                sqlalchemy.func.concat('gs://', hard_filtered_vcf_gz_md5_sum.file_path).label('hfVcfMd5Path'),
                sqlalchemy.func.concat('gs://', cram.file_path).label('cramPath'),
                sqlalchemy.func.concat('gs://', cram_md5_sum.file_path).label('cramMd5Path'),
                sqlalchemy.func.concat('gs://', cram_crai.file_path).label('craiPath'),
                sqlalchemy.func.concat('gs://', hard_filtered_gvcf_gz.file_path).label('gvcfPath'),
                sqlalchemy.func.concat('gs://', hard_filtered_gvcf_gz_md5_sum.file_path).label('gvcfMd5Path'),
                GenomicGCValidationMetrics.contamination,
                GenomicGCValidationMetrics.sexConcordance,
                GenomicGCValidationMetrics.processingStatus,
                GenomicGCValidationMetrics.meanCoverage,
                Participant.researchId,
                GenomicSetMember.gcManifestSampleSource,
                GenomicGCValidationMetrics.mappedReadsPct,
                GenomicGCValidationMetrics.sexPloidy,
                sqlalchemy.func.IF(
                    GenomicSetMember.ai_an == 'Y',
                    sqlalchemy.sql.expression.literal("True"),
                    sqlalchemy.sql.expression.literal("False")),
                sqlalchemy.func.IF(
                    GenomicSetMember.blockResearch == 1,
                    sqlalchemy.sql.expression.literal("True"),
                    sqlalchemy.sql.expression.literal("False")),
                GenomicSetMember.blockResearchReason,
                GenomicGCValidationMetrics.pipelineId,
                GenomicGCValidationMetrics.processingCount
            ).join(
                ParticipantSummary,
                ParticipantSummary.participantId == GenomicSetMember.participantId
            ).join(
                GenomicGCValidationMetrics,
                GenomicGCValidationMetrics.genomicSetMemberId == GenomicSetMember.id
            ).join(
                Participant,
                Participant.participantId == ParticipantSummary.participantId
            ).join(
                array_check,
                and_(
                    array_check.biobankId == GenomicSetMember.biobankId,
                    array_check.aw3ManifestJobRunID.isnot(None),
                    array_check.genomeType == config.GENOME_TYPE_ARRAY
                )
            ).join(
                hard_filtered_vcf_gz,
                and_(
                    hard_filtered_vcf_gz.file_type == 'hard-filtered.vcf.gz',
                    hard_filtered_vcf_gz.identifier_value == GenomicSetMember.sampleId,
                    hard_filtered_vcf_gz.file_path.contains(pipeline_id)
                )
            ).join(
                hard_filtered_vcf_gz_tbi,
                and_(
                    hard_filtered_vcf_gz_tbi.file_type == 'hard-filtered.vcf.gz.tbi',
                    hard_filtered_vcf_gz_tbi.identifier_value == GenomicSetMember.sampleId,
                    hard_filtered_vcf_gz_tbi.file_path.contains(pipeline_id)
                )
            ).join(
                hard_filtered_vcf_gz_md5_sum,
                and_(
                    hard_filtered_vcf_gz_md5_sum.file_type == 'hard-filtered.vcf.gz.md5sum',
                    hard_filtered_vcf_gz_md5_sum.identifier_value == GenomicSetMember.sampleId,
                    hard_filtered_vcf_gz_md5_sum.file_path.contains(pipeline_id)
                )
            ).join(
                cram,
                and_(
                    cram.file_type == 'cram',
                    cram.identifier_value == GenomicSetMember.sampleId,
                    cram.file_path.contains(pipeline_id)
                )
            ).join(
                cram_md5_sum,
                and_(
                    cram_md5_sum.file_type == 'cram.md5sum',
                    cram_md5_sum.identifier_value == GenomicSetMember.sampleId,
                    cram_md5_sum.file_path.contains(pipeline_id)
                )
            ).join(
                cram_crai,
                and_(
                    cram_crai.file_type == 'cram.crai',
                    cram_crai.identifier_value == GenomicSetMember.sampleId,
                    cram_crai.file_path.contains(pipeline_id)
                )
            ).join(
                hard_filtered_gvcf_gz,
                and_(
                    hard_filtered_gvcf_gz.file_type == 'hard-filtered.gvcf.gz',
                    hard_filtered_gvcf_gz.identifier_value == GenomicSetMember.sampleId,
                    hard_filtered_gvcf_gz.file_path.contains(pipeline_id)
                )
            ).join(
                hard_filtered_gvcf_gz_md5_sum,
                and_(
                    hard_filtered_gvcf_gz_md5_sum.file_type == 'hard-filtered.gvcf.gz.md5sum',
                    hard_filtered_gvcf_gz_md5_sum.identifier_value == GenomicSetMember.sampleId,
                    hard_filtered_gvcf_gz_md5_sum.file_path.contains(pipeline_id)
                )
            ).outerjoin(
                GenomicAW3Raw,
                and_(
                    GenomicAW3Raw.sample_id == GenomicSetMember.sampleId,
                    GenomicAW3Raw.genome_type == genome_type,
                    GenomicAW3Raw.ignore_flag != 1,
                )
            ).filter(
                or_(
                    and_(
                        GenomicSetMember.genomicWorkflowState != GenomicWorkflowState.IGNORE,
                        GenomicSetMember.genomeType == genome_type,
                        GenomicSetMember.aw3ManifestJobRunID.is_(None),
                        GenomicSetMember.ignoreFlag != 1,
                        GenomicGCValidationMetrics.processingStatus.ilike('pass'),
                        GenomicGCValidationMetrics.ignoreFlag != 1,
                        ParticipantSummary.withdrawalStatus == WithdrawalStatus.NOT_WITHDRAWN,
                        ParticipantSummary.suspensionStatus == SuspensionStatus.NOT_SUSPENDED,
                        GenomicAW3Raw.id.is_(None),
                        GenomicGCValidationMetrics.pipelineId == pipeline_id
                    ),
                    GenomicGCValidationMetrics.aw3ReadyFlag == 1
                )
            )
            return aw3_rows.distinct().all()

    # CVL pipeline start
    def get_w3sr_records(self, **kwargs):
        gc_site_id = self.transform_cvl_site_id(kwargs.get('site_id'))
        sample_ids = kwargs.get('sample_ids')

        with self.session() as session:
            records = session.query(
                func.concat(
                    get_biobank_id_prefix(),
                    GenomicSetMember.biobankId
                ),
                GenomicSetMember.sampleId,
                GenomicSetMember.gcManifestParentSampleId,
                GenomicSetMember.collectionTubeId,
                GenomicSetMember.sexAtBirth,
                func.IF(
                    GenomicSetMember.nyFlag == 1,
                    sqlalchemy.sql.expression.literal("Y"),
                    sqlalchemy.sql.expression.literal("N")
                ).label('nyFlag'),
                sqlalchemy.sql.expression.literal("aou_cvl").label('genomeType'),
                func.IF(
                    GenomicSetMember.gcSiteId == 'bi',
                    sqlalchemy.sql.expression.literal("co"),
                    GenomicSetMember.gcSiteId
                ).label('gcSiteId'),
                GenomicSetMember.ai_an
            ).join(
                ParticipantSummary,
                ParticipantSummary.participantId == GenomicSetMember.participantId
            ).filter(
                ParticipantSummary.withdrawalStatus == WithdrawalStatus.NOT_WITHDRAWN,
                ParticipantSummary.suspensionStatus == SuspensionStatus.NOT_SUSPENDED,
                ParticipantSummary.deceasedStatus == DeceasedStatus.UNSET,
                ParticipantSummary.consentForGenomicsROR == QuestionnaireStatus.SUBMITTED,
                ParticipantSummary.consentForStudyEnrollment == QuestionnaireStatus.SUBMITTED,
                GenomicSetMember.cvlW2scManifestJobRunID.isnot(None),
                GenomicSetMember.cvlW3srManifestJobRunID.is_(None),
                GenomicSetMember.gcSiteId.isnot(None),
                GenomicSetMember.genomeType == config.GENOME_TYPE_WGS
            )

            if gc_site_id:
                records = records.filter(
                    GenomicSetMember.gcSiteId == gc_site_id.lower()
                )

            if sample_ids:
                records = records.filter(
                    GenomicSetMember.sampleId.in_(sample_ids)
                )

            return records.distinct().all()

    def get_data_ready_for_w1il_manifest(self, module: str, cvl_id: str, sample_ids=None):
        """
        Returns the genomic set member and other data needed for a W1IL manifest.
        :param module: Module to retrieve genomic set members for, either 'pgx' or 'hdr'
        :param cvl_id: CVL id that the data will go to ('co', 'uw', 'bcm')
        :param sample_ids: Sample IDs that the data will filter on if defined
        """

        gc_site_id = self.transform_cvl_site_id(cvl_id)
        previous_w1il_job_field = {
            'pgx': GenomicSetMember.cvlW1ilPgxJobRunId,
            'hdr': GenomicSetMember.cvlW1ilHdrJobRunId
        }[module]

        informing_loop_decision_query = GenomicInformingLoopDao.build_latest_decision_query(
            module=module
        ).with_entities(
            GenomicInformingLoop.participant_id,
            GenomicInformingLoop.decision_value
        ).filter(
            GenomicInformingLoop.decision_value.ilike('yes')
        )
        informing_loop_subquery = aliased(GenomicInformingLoop, informing_loop_decision_query.subquery())

        with self.session() as session:
            query = session.query(
                func.concat(get_biobank_id_prefix(), GenomicSetMember.biobankId),
                GenomicSetMember.sampleId,
                GenomicGCValidationMetrics.hfVcfPath.label('vcf_raw_path'),
                GenomicGCValidationMetrics.hfVcfTbiPath.label('vcf_raw_index_path'),
                GenomicGCValidationMetrics.hfVcfMd5Path.label('vcf_raw_md5_path'),
                GenomicGCValidationMetrics.gvcfPath.label('gvcf_path'),
                GenomicGCValidationMetrics.gvcfMd5Path.label('gvcf_md5_path'),
                GenomicGCValidationMetrics.cramPath.label('cram_name'),
                GenomicSetMember.sexAtBirth.label('sex_at_birth'),
                func.IF(
                    GenomicSetMember.nyFlag == 1,
                    sqlalchemy.sql.expression.literal("Y"),
                    sqlalchemy.sql.expression.literal("N")
                ).label('ny_flag'),
                sqlalchemy.func.upper(GenomicSetMember.gcSiteId).label('genome_center'),
                sqlalchemy.case(
                    [(ParticipantSummary.consentForGenomicsROR == QuestionnaireStatus.SUBMITTED, 'Y')],
                    else_='N'
                ).label('consent_for_gror'),
                sqlalchemy.literal('aou_cvl').label('genome_type'),
                sqlalchemy.case(
                    [(informing_loop_subquery.decision_value.ilike('yes'), 'Y')],
                    else_='N'
                ).label(f'informing_loop_{module}'),
                GenomicGCValidationMetrics.aouHdrCoverage.label('aou_hdr_coverage'),
                GenomicGCValidationMetrics.contamination,
                GenomicGCValidationMetrics.sexPloidy.label('sex_ploidy')
            ).join(
                ParticipantSummary,
                ParticipantSummary.participantId == GenomicSetMember.participantId
            ).join(
                GenomicGCValidationMetrics,
                and_(
                    GenomicGCValidationMetrics.genomicSetMemberId == GenomicSetMember.id,
                    GenomicGCValidationMetrics.ignoreFlag != 1
                )
            ).join(
                informing_loop_subquery,
                informing_loop_subquery.participant_id == GenomicSetMember.participantId
            ).filter(
                GenomicGCValidationMetrics.processingStatus.ilike('pass'),
                GenomicSetMember.genomeType == config.GENOME_TYPE_WGS,
                ParticipantSummary.withdrawalStatus == WithdrawalStatus.NOT_WITHDRAWN,
                ParticipantSummary.suspensionStatus == SuspensionStatus.NOT_SUSPENDED,
                ParticipantSummary.deceasedStatus == DeceasedStatus.UNSET,
                GenomicGCValidationMetrics.sexConcordance.ilike('true'),     # check AW2 gives sex concordance as true
                GenomicGCValidationMetrics.drcSexConcordance.ilike('pass'),  # check AW4 gives sex concordance as true
                GenomicSetMember.qcStatus == GenomicQcStatus.PASS,
                GenomicSetMember.gcManifestSampleSource.ilike('whole blood'),
                ParticipantSummary.consentForStudyEnrollment == QuestionnaireStatus.SUBMITTED,
                ParticipantSummary.consentForGenomicsROR == QuestionnaireStatus.SUBMITTED,
                GenomicGCValidationMetrics.drcFpConcordance.ilike('pass'),
                GenomicSetMember.diversionPouchSiteFlag != 1,
                GenomicSetMember.gcSiteId.ilike(gc_site_id),
                ParticipantSummary.participantOrigin != 'careevolution',
                GenomicSetMember.ignoreFlag != 1,
                GenomicSetMember.genomicWorkflowState == GenomicWorkflowState.CVL_READY,
                previous_w1il_job_field.is_(None)
            )

            if sample_ids:
                query = query.filter(
                    GenomicSetMember.sampleId.in_(sample_ids)
                )

            return query.all()

    def get_data_ready_for_w2w_manifest(self, cvl_id: str, sample_ids=None):
        gc_site_id = self.transform_cvl_site_id(cvl_id)

        with self.session() as session:
            query = (
                session.query(
                    func.concat(get_biobank_id_prefix(), GenomicSetMember.biobankId).label('biobank_id'),
                    GenomicSetMember.sampleId.label('sample_id'),
                    func.date_format(
                        ParticipantSummary.withdrawalAuthored,
                        '%Y-%m-%dT%H:%i:%S+00:00'
                    ).label('date_of_consent_removal')
                ).join(
                    ParticipantSummary,
                    GenomicSetMember.participantId == ParticipantSummary.participantId
                ).filter(
                    GenomicSetMember.genomeType == config.GENOME_TYPE_WGS,
                    or_(
                        GenomicSetMember.cvlW1ilPgxJobRunId.isnot(None),
                        GenomicSetMember.cvlW1ilHdrJobRunId.isnot(None)
                    ),
                    GenomicSetMember.cvlW2scManifestJobRunID.isnot(None),
                    GenomicSetMember.cvlW2wJobRunId.is_(None),
                    ParticipantSummary.withdrawalStatus != WithdrawalStatus.NOT_WITHDRAWN,
                    GenomicSetMember.gcSiteId.ilike(gc_site_id),
                    GenomicSetMember.ignoreFlag != 1
                )
            )

            if sample_ids:
                query = query.filter(
                    GenomicSetMember.sampleId.in_(sample_ids)
                )

            return query.all()

    @staticmethod
    def _join_gror_answer(query, answer_code_str_list, start_datetime):
        questionnaire_response = aliased(QuestionnaireResponse)
        questionnaire_concept = aliased(QuestionnaireConcept)
        answer = aliased(QuestionnaireResponseAnswer)
        question = aliased(QuestionnaireQuestion)

        survey_code = aliased(Code)
        question_code = aliased(Code)
        answer_code = aliased(Code)

        new_query = query.join(
            questionnaire_response,
            and_(
                questionnaire_response.participantId == ParticipantSummary.participantId,
                questionnaire_response.authored > start_datetime
            )
        ).join(
            questionnaire_concept,
            and_(
                questionnaire_concept.questionnaireId == questionnaire_response.questionnaireId,
                questionnaire_concept.questionnaireVersion == questionnaire_response.questionnaireVersion
            )
        ).join(
            survey_code,
            and_(
                survey_code.codeId == questionnaire_concept.codeId,
                survey_code.value.ilike(code_constants.CONSENT_FOR_GENOMICS_ROR_MODULE)
            )
        ).join(
            answer,
            answer.questionnaireResponseId == questionnaire_response.questionnaireResponseId
        ).join(
            question,
            question.questionnaireQuestionId == answer.questionId
        ).join(
            question_code,
            and_(
                question_code.codeId == question.codeId,
                question_code.value.ilike(code_constants.GROR_CONSENT_QUESTION_CODE)
            )
        ).join(
            answer_code,
            and_(
                answer_code.codeId == answer.valueCodeId,
                answer_code.value.in_(answer_code_str_list)
            )
        )

        return new_query

    def get_w1il_yes_no_yes_participants(self, start_datetime):
        # Find W1IL participants that have re-submitted a Yes response to GROR after being included in a W1IL.
        with self.session() as session:
            query = session.query(
                ParticipantSummary.participantId
            ).join(
                GenomicSetMember,
                GenomicSetMember.participantId == ParticipantSummary.participantId
            ).join(
                GenomicJobRun,
                GenomicJobRun.id.in_([
                    GenomicSetMember.cvlW1ilHdrJobRunId,
                    GenomicSetMember.cvlW1ilPgxJobRunId
                ])
            ).filter(
                ParticipantSummary.consentForGenomicsROR == QuestionnaireStatus.SUBMITTED
            )
            query = self._join_gror_answer(
                query=query,
                answer_code_str_list=[code_constants.CONSENT_GROR_YES_CODE],
                start_datetime=func.greatest(GenomicJobRun.startTime, start_datetime)
            )

            # For each of the participants found, check to see if they have a No response to the GROR after the W1IL.
            # (Just to filter out any strange double-Yes's we got, ie. participants that had a Yes before the W1IL
            #  and then submitted another Yes afterwards with no revocation in between)
            query = self._join_gror_answer(
                query=query,
                answer_code_str_list=[
                    code_constants.CONSENT_GROR_NO_CODE,
                    code_constants.CONSENT_GROR_NOT_SURE
                ],
                start_datetime=GenomicJobRun.startTime
            )

            return query.distinct().all()

    def get_results_withdrawn_participants(self):
        with self.session() as session:
            records = session.query(
                GenomicSetMember.participantId.label('participant_id'),
                func.max(sqlalchemy.case(
                    [
                        (GenomicSetMember.gemA1ManifestJobRunId.isnot(None), True)
                    ],
                    else_=False
                )).label('array_results'),
                func.max(sqlalchemy.case(
                    [
                        (
                            or_(
                                GenomicSetMember.cvlW1ilHdrJobRunId.isnot(None),
                                GenomicSetMember.cvlW1ilPgxJobRunId.isnot(None)
                            ), True)
                    ],
                    else_=False
                )).label('cvl_results')
            ).join(
                ParticipantSummary,
                ParticipantSummary.participantId == GenomicSetMember.participantId
            ).outerjoin(
                GenomicResultWithdrawals,
                GenomicResultWithdrawals.participant_id == GenomicSetMember.participantId
            ).filter(
                ParticipantSummary.withdrawalStatus != WithdrawalStatus.NOT_WITHDRAWN,
                GenomicResultWithdrawals.id.is_(None),
                and_(
                    or_(
                        GenomicSetMember.gemA1ManifestJobRunId.isnot(None),
                        GenomicSetMember.cvlW1ilHdrJobRunId.isnot(None),
                        GenomicSetMember.cvlW1ilPgxJobRunId.isnot(None)
                    )
                )
            ).group_by(
                GenomicSetMember.participantId
            )
            return records.all()


class GenomicCVLResultPastDueDao(UpdatableDao, GenomicDaoMixin):

    validate_version_match = False

    def __init__(self):
        super(GenomicCVLResultPastDueDao, self).__init__(
            GenomicCVLResultPastDue, order_by_ending=['id'])

    def from_client_json(self):
        pass

    def get_id(self, obj):
        return obj.id

    def get_past_due_samples(self, result_type):
        now = clock.CLOCK.now()
        cvl_limits = config.getSettingJson(config.GENOMIC_CVL_RECONCILE_LIMITS, {})
        if not cvl_limits:
            return []

        genomic_past_due_alias = aliased(GenomicCVLResultPastDue)
        result_attributes = {
            GenomicJob.RECONCILE_CVL_HDR_RESULTS: {
                'w1il_run_id': GenomicSetMember.cvlW1ilHdrJobRunId,
                'analysis_type': ResultsModuleType.HDRV1
            },
            GenomicJob.RECONCILE_CVL_PGX_RESULTS: {
                'w1il_run_id': GenomicSetMember.cvlW1ilPgxJobRunId,
                'analysis_type': ResultsModuleType.PGXV1
            }
        }[result_type]

        with self.session() as session:
            records = session.query(
                GenomicSetMember.id.label('genomicSetMemberId'),
                GenomicSetMember.sampleId,
                GenomicSetMember.gcSiteId.label('cvlSiteId'),
                literal(result_attributes.get('analysis_type').name).label('resultsType')
            ).outerjoin(
                genomic_past_due_alias,
                genomic_past_due_alias.sample_id == GenomicSetMember.sampleId
            ).outerjoin(
                GenomicW4WRRaw,
                and_(
                    GenomicW4WRRaw.sample_id == GenomicSetMember.sampleId,
                    GenomicW4WRRaw.clinical_analysis_type == result_attributes.get('analysis_type')
                )
            ).join(
                GenomicJobRun,
                GenomicJobRun.id == result_attributes.get('w1il_run_id')
            )
            if result_type == GenomicJob.RECONCILE_CVL_PGX_RESULTS:
                pgx_time_limit = cvl_limits.get('pgx_time_limit')
                records = records.filter(
                    GenomicJobRun.created < (now - timedelta(days=pgx_time_limit))
                )
            elif result_type == GenomicJob.RECONCILE_CVL_HDR_RESULTS:
                hdr_time_limit = cvl_limits.get('hdr_time_limit')
                w3sc_extension = cvl_limits.get('w3sc_extension')

                no_w3sc = and_(
                    GenomicW3SCRaw.id.is_(None),
                    GenomicJobRun.created < (now - timedelta(days=hdr_time_limit))
                ).self_group()

                w3sc_with_ext = and_(
                    GenomicW3SCRaw.id.isnot(None),
                    GenomicJobRun.created < (now - timedelta(days=hdr_time_limit) - timedelta(
                        days=w3sc_extension))
                ).self_group()

                records = records.outerjoin(
                    GenomicW2WRaw,
                    and_(
                        GenomicW2WRaw.sample_id == GenomicSetMember.sampleId
                    )
                ).outerjoin(
                    GenomicW3SCRaw,
                    and_(
                        GenomicW3SCRaw.sample_id == GenomicSetMember.sampleId
                    )
                ).filter(
                    GenomicW2WRaw.id.is_(None),
                    or_(*[no_w3sc, w3sc_with_ext])
                )

            records = records.filter(
                result_attributes.get('w1il_run_id').isnot(None),
                GenomicSetMember.genomeType == config.GENOME_TYPE_WGS,
                genomic_past_due_alias.id.is_(None),
                GenomicW4WRRaw.id.is_(None)
            )
            return records.all()

    def get_samples_for_notifications(self):
        with self.session() as session:
            return session.query(
                GenomicCVLResultPastDue.id,
                GenomicCVLResultPastDue.cvl_site_id,
                GenomicCVLResultPastDue.sample_id,
                GenomicCVLResultPastDue.results_type
            ).filter(
                GenomicCVLResultPastDue.email_notification_sent != 1,
                GenomicCVLResultPastDue.resolved != 1,
            ).all()

    def get_samples_to_resolve(self):
        with self.session() as session:
            records = session.query(
                GenomicCVLResultPastDue.id
            ).join(
                GenomicSetMember,
                and_(
                    GenomicSetMember.id == GenomicCVLResultPastDue.genomic_set_member_id,
                    GenomicSetMember.sampleId == GenomicCVLResultPastDue.sample_id,
                )
            ).outerjoin(
                GenomicW4WRRaw,
                and_(
                    GenomicW4WRRaw.sample_id == GenomicSetMember.sampleId,
                    GenomicW4WRRaw.clinical_analysis_type == GenomicCVLResultPastDue.results_type
                )
            ).filter(
                GenomicCVLResultPastDue.resolved != 1,
                GenomicW4WRRaw.id.isnot(None)
            )
            return records.all()

    def batch_update_samples(self, update_type, _ids):
        update_map = {
            GenomicJob.RECONCILE_CVL_RESOLVE: {
                'resolved': 1,
                'resolved_date': datetime.utcnow()
            },
            GenomicJob.RECONCILE_CVL_ALERTS: {
                'email_notification_sent': 1,
                'email_notification_sent_date': datetime.utcnow()
            }
        }[update_type]

        _ids = [_ids] if not type(_ids) is list else _ids

        for _id in _ids:
            current_record = self.get(_id)
            for key, value in update_map.items():
                setattr(current_record, key, value)

            self.update(current_record)


class GenomicSampleSwapDao(BaseDao):
    def __init__(self):
        super(GenomicSampleSwapDao, self).__init__(
            GenomicSampleSwap, order_by_ending=['id'])

    def from_client_json(self):
        pass

    def get_id(self, obj):
        pass


class GenomicResultWithdrawalsDao(BaseDao, GenomicDaoMixin):
    def __init__(self):
        super(GenomicResultWithdrawalsDao, self).__init__(
            GenomicResultWithdrawals, order_by_ending=['id']
        )

    def from_client_json(self):
        pass

    def get_id(self, obj):
        pass


class GenomicAppointmentEventMetricsDao(UpdatableDao, GenomicDaoMixin):
    def __init__(self):
        super(GenomicAppointmentEventMetricsDao, self).__init__(
            GenomicAppointmentEventMetrics, order_by_ending=['id']
        )

    def from_client_json(self):
        pass

    def get_id(self, obj):
        return obj.id

    def get_missing_appointments(self):
        with self.session() as session:
            return session.query(
                GenomicAppointmentEventMetrics.id,
                GenomicAppointmentEventMetrics.event_type,
                GenomicAppointmentEventMetrics.event_authored_time,
                GenomicAppointmentEventMetrics.participant_id,
                GenomicAppointmentEventMetrics.appointment_event,
                GenomicAppointmentEventMetrics.reconcile_job_run_id,
                GenomicAppointmentEvent.id.label('appointment_event_id')
            ).outerjoin(
                GenomicAppointmentEvent,
                and_(
                    GenomicAppointmentEvent.participant_id == GenomicAppointmentEventMetrics.participant_id,
                    GenomicAppointmentEvent.module_type == GenomicAppointmentEventMetrics.module_type,
                    GenomicAppointmentEvent.event_type == GenomicAppointmentEventMetrics.event_type,
                    GenomicAppointmentEvent.event_authored_time == GenomicAppointmentEventMetrics.event_authored_time
                )
            ).filter(
                GenomicAppointmentEventMetrics.reconcile_job_run_id.is_(None)
            ).all()


class GenomicDefaultBaseDao(BaseDao, GenomicDaoMixin):
    def __init__(self, model_type):
        super(GenomicDefaultBaseDao, self).__init__(
            model_type, order_by_ending=['id']
        )

    def from_client_json(self):
        pass

    def get_id(self, obj):
        pass
