import datetime
import logging

from dateutil.parser import parse
import faker
import re
import threading

import sqlalchemy
import sqlalchemy.orm

from sqlalchemy import or_, and_
from sqlalchemy.orm import Query, joinedload
from sqlalchemy.sql import expression
from typing import Collection, List

# Note: leaving for future use if we go back to using a relationship to PatientStatus table.
# from sqlalchemy.orm import selectinload
from werkzeug.exceptions import BadRequest, NotFound

from rdr_service import clock, config
from rdr_service.api_util import (
    format_json_code,
    format_json_date,
    format_json_enum,
    format_json_hpo,
    format_json_org,
    format_json_site,
    parse_json_enum
)
from rdr_service.app_util import is_care_evo_and_not_prod
from rdr_service.cloud_utils.gcp_google_pubsub import submit_pipeline_pubsub_msg_from_model
from rdr_service.code_constants import BIOBANK_TESTS, COHORT_1_REVIEW_CONSENT_YES_CODE, ORIGINATING_SOURCES,\
    PMI_SKIP_CODE, PPI_SYSTEM, PRIMARY_CONSENT_UPDATE_MODULE, PRIMARY_CONSENT_UPDATE_QUESTION_CODE, UNSET
from rdr_service.dao.base_dao import UpdatableDao
from rdr_service.dao.biobank_stored_sample_dao import BiobankStoredSampleDao
from rdr_service.dao.code_dao import CodeDao
from rdr_service.dao.database_utils import get_sql_and_params_for_array, replace_null_safe_equals
from rdr_service.dao.genomics_dao import GenomicSetMemberDao
from rdr_service.dao.hpo_dao import HPODao
from rdr_service.dao.organization_dao import OrganizationDao
from rdr_service.dao.participant_dao import ParticipantDao
from rdr_service.dao.participant_incentives_dao import ParticipantIncentivesDao
from rdr_service.dao.patient_status_dao import PatientStatusDao
from rdr_service.dao.site_dao import SiteDao
from rdr_service.domain_model.ehr import ParticipantEhrFile
from rdr_service.logic.enrollment_info import EnrollmentCalculation, EnrollmentDependencies
from rdr_service.model.account_link import AccountLink
from rdr_service.model.config_utils import from_client_biobank_id, to_client_biobank_id
from rdr_service.model.consent_file import ConsentType
from rdr_service.model.enrollment_status_history import EnrollmentStatusHistory
from rdr_service.model.participant_summary import (
    ParticipantGenderAnswers,
    ParticipantRaceAnswers,
    ParticipantSummary,
    WITHDRAWN_PARTICIPANT_FIELDS,
    WITHDRAWN_PARTICIPANT_VISIBILITY_TIME
)
from rdr_service.model.patient_status import PatientStatus
from rdr_service.model.participant import Participant
from rdr_service.model.pediatric_data_log import PediatricDataLog, PediatricDataType
from rdr_service.model.retention_eligible_metrics import RetentionEligibleMetrics
from rdr_service.model.utils import get_property_type, to_client_participant_id
from rdr_service.participant_enums import (
    BiobankOrderStatus,
    EhrStatus,
    EnrollmentStatus,
    EnrollmentStatusV30,
    EnrollmentStatusV32,
    DeceasedStatus,
    ConsentExpireStatus,
    GenderIdentity,
    ParticipantCohort,
    PatientStatusFlag,
    PhysicalMeasurementsStatus,
    QuestionnaireStatus,
    Race,
    SampleCollectionMethod,
    SampleStatus,
    SuspensionStatus,
    WithdrawalStatus,
    get_bucketed_age,
    SelfReportedPhysicalMeasurementsStatus,
    PhysicalMeasurementsCollectType
)
from rdr_service.model.code import Code
from rdr_service.query import FieldFilter, FieldJsonContainsFilter, Operator, OrderBy, PropertyType
from rdr_service.repository.obfuscation_repository import ObfuscationRepository
from rdr_service.repository.questionnaire_response_repository import QuestionnaireResponseRepository
from rdr_service.services.system_utils import min_or_none


# By default / secondarily order by last name, first name, DOB, and participant ID
_ORDER_BY_ENDING = ("lastName", "firstName", "dateOfBirth", "participantId")
# The default ordering of results for queries for withdrawn participants.
_WITHDRAWN_ORDER_BY_ENDING = ("withdrawalTime", "participantId")
_CODE_FILTER_FIELDS = ("organization", "site", "awardee")
_SITE_FIELDS = (
    "clinicPhysicalMeasurementsCreatedSite",
    "clinicPhysicalMeasurementsFinalizedSite",
    "biospecimenSourceSite",
    "biospecimenCollectedSite",
    "biospecimenProcessedSite",
    "biospecimenFinalizedSite",
    "site",
    "enrollmentSite",
)

# Lazy caches of property names for client JSON conversion.
_DATE_FIELDS = set()
_ENUM_FIELDS = set()
_CODE_FIELDS = set()
_fields_lock = threading.RLock()

# Query used to update the enrollment status for all participant summaries after
# a Biobank samples import.
# TODO(DA-631): This should likely be a conditional update (e.g. see
# baseline/dna updates) which updates last modified.

_ENROLLMENT_STATUS_CASE_SQL = """
        CASE WHEN (consent_for_study_enrollment = :submitted
                   AND consent_for_electronic_health_records = :submitted
                   AND (consent_cohort != :cohort_3 OR
                        (consent_for_genomics_ror BETWEEN :submitted AND :submitted_not_sure)
                       )
                   AND num_completed_baseline_ppi_modules = :num_baseline_ppi_modules
                   AND (clinic_physical_measurements_status = :completed OR
                   self_reported_physical_measurements_status = :completed)
                   AND samples_to_isolate_dna = :received) OR
                  (consent_for_study_enrollment = :submitted
                   AND consent_for_electronic_health_records = :unset
                   AND consent_for_dv_electronic_health_records_sharing = :submitted
                   AND (consent_cohort != :cohort_3 OR
                        (consent_for_genomics_ror BETWEEN :submitted AND :submitted_not_sure)
                       )
                   AND num_completed_baseline_ppi_modules = :num_baseline_ppi_modules
                   AND (clinic_physical_measurements_status = :completed OR
                   self_reported_physical_measurements_status = :completed)
                   AND samples_to_isolate_dna = :received)
             THEN :full_participant
             WHEN (consent_for_study_enrollment = :submitted
                   AND consent_for_electronic_health_records = :submitted
                   AND (consent_cohort != :cohort_3 OR
                        (consent_for_genomics_ror BETWEEN :submitted AND :submitted_not_sure)
                       )
                   AND num_completed_baseline_ppi_modules = :num_baseline_ppi_modules
                   AND clinic_physical_measurements_status != :completed
                   AND self_reported_physical_measurements_status != :completed
                   AND samples_to_isolate_dna = :received) OR
                  (consent_for_study_enrollment = :submitted
                   AND consent_for_electronic_health_records = :unset
                   AND consent_for_dv_electronic_health_records_sharing = :submitted
                   AND (consent_cohort != :cohort_3 OR
                        (consent_for_genomics_ror BETWEEN :submitted AND :submitted_not_sure)
                       )
                   AND num_completed_baseline_ppi_modules = :num_baseline_ppi_modules
                   AND clinic_physical_measurements_status != :completed
                   AND self_reported_physical_measurements_status != :completed
                   AND samples_to_isolate_dna = :received)
             THEN :core_minus_pm
             WHEN (consent_for_study_enrollment = :submitted
                   AND consent_for_electronic_health_records = :submitted) OR
                  (consent_for_study_enrollment = :submitted
                   AND consent_for_electronic_health_records = :unset
                   AND consent_for_dv_electronic_health_records_sharing = :submitted
                  )
             THEN :member
             ELSE :interested
        END
"""

# DA-614 - Notes: Because there can be multiple distinct samples with the same test for a
# participant and we can't show them all in the participant summary.  The HealthPro team
# wants to see status and timestamp of received records over disposed records. Currently
# this sql sets a generic disposed status instead of the specific disposal status. The
# HealthPro team wants a new API to query biobank_stored_samples and get the specific
# disposed status there instead from the participant summary.
_SAMPLE_SQL = """,
      sample_status_%(test)s =
        CASE WHEN EXISTS(SELECT * FROM biobank_stored_sample bss
                         WHERE bss.biobank_id = ps.biobank_id
                         AND bss.test = %(sample_param_ref)s)
          THEN
              # DA-614 - Only set disposed status when ALL samples for this test are disposed of.
              CASE WHEN (SELECT MIN(bss.status) FROM biobank_stored_sample bss
                       WHERE bss.biobank_id = ps.biobank_id
                       AND bss.test = %(sample_param_ref)s) >= :disposed_bad
                   THEN :disposed
              ELSE :received END
          ELSE :unset END,
      sample_status_%(test)s_time =
        CASE WHEN EXISTS(SELECT * FROM biobank_stored_sample bss
              WHERE bss.biobank_id = ps.biobank_id AND bss.test = %(sample_param_ref)s)
          THEN
              # DA-614 - Only use disposed datetime when ALL samples for this test are disposed of.
              CASE WHEN (SELECT MIN(bss.status) FROM biobank_stored_sample bss
                       WHERE bss.biobank_id = ps.biobank_id
                       AND bss.test = %(sample_param_ref)s) >= :disposed_bad
                   THEN (SELECT MAX(disposed) from biobank_stored_sample bss
                     WHERE bss.biobank_id = ps.biobank_id
                       AND bss.test = %(sample_param_ref)s)
              ELSE (SELECT MAX(confirmed) from biobank_stored_sample bss
                     WHERE bss.biobank_id = ps.biobank_id and (bss.status < :disposed_bad or bss.status is null)
                       AND bss.test = %(sample_param_ref)s)
              END
          ELSE NULL END
   """

_COLLECTION_METHOD_CASE_SQL = f"""
    # Results in NULL if an order wasn't found (since we're unsure how the order was made)
    CASE
        WHEN bmko.id IS NOT NULL
            # there's a mail-kit order tied to the sample
            THEN {int(SampleCollectionMethod.MAIL_KIT)}
        WHEN bo.biobank_order_id IS NOT NULL
            # there's an order created for the sample, but no mail-kit order tied to it
            THEN {int(SampleCollectionMethod.ON_SITE)}
        ELSE
            # there's no order for the sample
            {int(SampleCollectionMethod.UNSET)}
    END
"""
_SAMPLE_COLLECTION_METHOD_SQL = f""",
    sample_%(test)s_collection_method =
    CASE WHEN EXISTS(SELECT * FROM biobank_stored_sample bss
          WHERE bss.biobank_id = ps.biobank_id AND bss.test = %(sample_param_ref)s)
      THEN
          # Use the same sample that is used to set the status and time fields
          CASE WHEN (SELECT MIN(bss.status) FROM biobank_stored_sample bss
                   WHERE bss.biobank_id = ps.biobank_id
                   AND bss.test = %(sample_param_ref)s) >= :disposed_bad
               THEN (
                   SELECT {_COLLECTION_METHOD_CASE_SQL}
                     FROM biobank_stored_sample bss
                     LEFT JOIN biobank_order_identifier boi on boi.value = bss.biobank_order_identifier
                     LEFT JOIN biobank_order bo on bo.biobank_order_id = boi.biobank_order_id
                     LEFT JOIN biobank_mail_kit_order bmko on bmko.biobank_order_id = bo.biobank_order_id
                     WHERE bss.biobank_id = ps.biobank_id AND bss.test = %(sample_param_ref)s
                     ORDER BY disposed DESC
                     LIMIT 1)
          ELSE (
                    SELECT {_COLLECTION_METHOD_CASE_SQL}
                    FROM biobank_stored_sample bss
                    LEFT JOIN biobank_order_identifier boi on boi.value = bss.biobank_order_identifier
                    LEFT JOIN biobank_order bo on bo.biobank_order_id = boi.biobank_order_id
                    LEFT JOIN biobank_mail_kit_order bmko on bmko.biobank_order_id = bo.biobank_order_id
                    WHERE bss.biobank_id = ps.biobank_id and (bss.status < :disposed_bad or bss.status is null)
                        AND bss.test = %(sample_param_ref)s
                    ORDER BY confirmed DESC
                    LIMIT 1)
          END
      ELSE NULL END
"""

_WHERE_SQL = """
not ps.sample_status_%(test)s_time <=>
(SELECT MAX(bss.confirmed) FROM biobank_stored_sample bss
WHERE bss.biobank_id = ps.biobank_id
AND bss.test = %(sample_param_ref)s)

"""


def _get_sample_sql_and_params(now):
    """Gets SQL and params needed to update status and time fields on the participant summary for
  each biobank sample.
  """
    sql = """
  UPDATE
    participant_summary ps
  SET
    ps.last_modified = :now
  """
    params = {
        "received": int(SampleStatus.RECEIVED),
        "unset": int(SampleStatus.UNSET),
        "disposed": int(SampleStatus.DISPOSED),
        # DA-871: use first bad disposed reason code value.
        "disposed_bad": int(SampleStatus.SAMPLE_NOT_RECEIVED),
        "now": now,
    }
    where_sql = ""
    for i in range(0, len(BIOBANK_TESTS)):
        sample_param = "sample%d" % i
        sample_param_ref = ":%s" % sample_param
        lower_test = BIOBANK_TESTS[i].lower()
        sql += _SAMPLE_SQL % {"test": lower_test, "sample_param_ref": sample_param_ref}
        if lower_test == '1sal2':
            sql += _SAMPLE_COLLECTION_METHOD_SQL % {"test": lower_test, "sample_param_ref": sample_param_ref}
        params[sample_param] = BIOBANK_TESTS[i]
        if where_sql != "":
            where_sql += " or "
        where_sql += _WHERE_SQL % {"test": lower_test, "sample_param_ref": sample_param_ref}

    sql += f" WHERE ({where_sql})"

    return sql, params


def _get_baseline_sql_and_params():
    tests_sql, params = get_sql_and_params_for_array(
        config.getSettingList(config.BASELINE_SAMPLE_TEST_CODES), "baseline"
    )
    return (
        """
      (
        SELECT
          COUNT(*)
        FROM
          biobank_stored_sample
        WHERE
          biobank_stored_sample.biobank_id = participant_summary.biobank_id
          AND biobank_stored_sample.confirmed IS NOT NULL
          AND biobank_stored_sample.test IN %s
      )
      """
        % (tests_sql),
        params,
    )


def _get_dna_isolates_sql_and_params():
    tests_sql, params = get_sql_and_params_for_array(config.getSettingList(config.DNA_SAMPLE_TEST_CODES), "dna")
    params.update({"received": int(SampleStatus.RECEIVED), "unset": int(SampleStatus.UNSET)})
    return (
        """
      (
        CASE WHEN EXISTS(SELECT * FROM biobank_stored_sample
                         WHERE biobank_stored_sample.biobank_id = participant_summary.biobank_id
                         AND biobank_stored_sample.confirmed IS NOT NULL
                         AND biobank_stored_sample.test IN %s)
        THEN :received ELSE :unset END
      )
      """
        % (tests_sql),
        params,
    )


def _get_baseline_ppi_module_sql():
    baseline_ppi_module_fields = config.getSettingList(config.BASELINE_PPI_QUESTIONNAIRE_FIELDS, [])

    baseline_ppi_module_sql = "%s" % ",".join(
        ["""%s_time""" % re.sub("(?<!^)(?=[A-Z])", "_", item).lower() for item in baseline_ppi_module_fields]
    )
    return baseline_ppi_module_sql


class ParticipantSummaryDao(UpdatableDao):
    def __init__(self):
        super(ParticipantSummaryDao, self).__init__(ParticipantSummary, order_by_ending=_ORDER_BY_ENDING)
        self.hpo_dao = HPODao()
        self.code_dao = CodeDao()
        self.site_dao = SiteDao()
        self.organization_dao = OrganizationDao()
        self.patient_status_dao = PatientStatusDao()
        self.participant_dao = ParticipantDao()
        self.incentive_dao = ParticipantIncentivesDao()
        self.faker = faker.Faker()

        self.hpro_consents = []
        self.participant_incentives = []

    # pylint: disable=unused-argument
    def from_client_json(self, resource, participant_id, client_id):
        column_names = self.to_dict(self.model_type)
        participant = self.participant_dao.get(participant_id)
        static_keys = ["participantId", "biobankId"]

        payload_attrs = {key: value for key, value in resource.items()
                         if key in column_names and key not in
                         static_keys}

        default_attrs = {
            "participantId": participant.participantId,
            "biobankId": participant.biobankId,
            "hpoId": participant.hpoId,
            "firstName": self.faker.first_name(),
            "lastName": self.faker.first_name(),
            "withdrawalStatus": WithdrawalStatus.NOT_WITHDRAWN,
            "suspensionStatus": SuspensionStatus.NOT_SUSPENDED,
            "participantOrigin": participant.participantOrigin,
            "isEhrDataAvailable": False,
            "hasCoreData": False
        }
        default_attrs.update(payload_attrs)

        self.parse_resource_enums(default_attrs)

        return self.model_type(**default_attrs)

    def get_id(self, obj):
        return obj.participantId

    def get_with_children(self, obj_id):
        with self.session() as session:
            # Note: leaving for future use if we go back to using a relationship to PatientStatus table.
            # return self.get_with_session(session, obj_id,
            #                              options=self.get_eager_child_loading_query_options())
            return self.get_with_session(session, obj_id, options=self._default_api_query_options())

    @classmethod
    def get_by_ids_with_session(cls, session: sqlalchemy.orm.Session,
                                obj_ids: Collection) -> Collection[ParticipantSummary]:
        return session.query(
            ParticipantSummary
        ).filter(
            ParticipantSummary.participantId.in_(obj_ids)
        ).all()

    def get_by_participant_id(self, participant_id):
        with self.session() as session:
            return session.query(
                ParticipantSummary
            ).filter(
                ParticipantSummary.participantId == participant_id
            ).options(self._default_api_query_options()).one_or_none()

    @classmethod
    def get_for_update_with_linked_data(cls, participant_id, session) -> ParticipantSummary:
        dao = ParticipantSummaryDao()
        return dao.get_for_update(
            session=session,
            obj_id=participant_id,
            options=[
                joinedload(ParticipantSummary.relatedParticipants).load_only(),
                # NOTE: only loading existence of account linkage for now, since that's all
                # that's needed for enrollment status calculations

                joinedload(ParticipantSummary.pediatricData)
            ]
        )

    @classmethod
    def _default_api_query_options(cls):
        return [
            joinedload(ParticipantSummary.relatedParticipants).load_only()
            .joinedload(AccountLink.related).load_only()
            .joinedload(Participant.participantSummary).load_only(
                ParticipantSummary.participantId,
                ParticipantSummary.firstName,
                ParticipantSummary.lastName
            ),
            joinedload(ParticipantSummary.pediatricData)
        ]

    def get_by_hpo(self, hpo, session, yield_batch_size=1000):
        """ Returns participants for HPO except test and ghost participants"""
        return session.query(
            ParticipantSummary
        ).join(
            Participant, Participant.participantId == ParticipantSummary.participantId
        ).filter(
            ParticipantSummary.hpoId == hpo.hpoId,
            Participant.isTestParticipant != 1,
            # Just filtering on isGhostId != 1 will return no results
            or_(Participant.isGhostId != 1, Participant.isGhostId == None)
        ).yield_per(yield_batch_size)

    def _validate_update(self, session, obj, existing_obj):  # pylint: disable=unused-argument
        """Participant summaries don't have a version value; drop it from validation logic."""
        if not existing_obj:
            raise NotFound(f"{self.model_type.__name__} with id {id} does not exist")

    def parse_resource_enums(self, resource):
        for key in resource.keys():
            if key in self.to_dict(self.model_type):
                _type = getattr(self.model_type, key)
                if _type.expression.type.__class__.__name__.lower() == 'enum':
                    _cls = _type.expression.type.enum_type
                    parse_json_enum(resource, key, _cls)
        return resource

    @staticmethod
    def _has_withdrawn_filter(query):
        for field_filter in query.field_filters:
            if field_filter.field_name == "withdrawalStatus" and field_filter.value == WithdrawalStatus.NO_USE:
                return True
            if field_filter.field_name == "withdrawalTime" and field_filter.value is not None:
                return True
        return False

    @staticmethod
    def _get_non_withdrawn_filter_field(query):
        """Returns the first field referenced in query filters which isn't in
    WITHDRAWN_PARTICIPANT_FIELDS."""
        for field_filter in query.field_filters:
            if not field_filter.field_name in WITHDRAWN_PARTICIPANT_FIELDS:
                return field_filter.field_name
        return None

    def _initialize_query(self, session, query_def):
        filter_client = False
        non_withdrawn_field = self._get_non_withdrawn_filter_field(query_def)
        client_id = self.get_client_id()
        # Care evolution can GET participants from PTSC if env < prod.
        if client_id in ORIGINATING_SOURCES and not is_care_evo_and_not_prod():
            filter_client = True
        if self._has_withdrawn_filter(query_def):
            if non_withdrawn_field:
                raise BadRequest(f"Can't query on {non_withdrawn_field} for withdrawn participants")
            # When querying for withdrawn participants, ensure that the only fields being filtered on or
            # ordered by are in WITHDRAWN_PARTICIPANT_FIELDS.
            return super(ParticipantSummaryDao, self)._initialize_query(session, query_def)
        else:
            if query_def.attributes:
                eval_attrs = [eval(f'{self.model_type.__name__}.{attribute}') for attribute in query_def.attributes]
                query = Query(eval_attrs)
            else:
                query = super(ParticipantSummaryDao, self)._initialize_query(session, query_def)

            withdrawn_visible_start = clock.CLOCK.now() - WITHDRAWN_PARTICIPANT_VISIBILITY_TIME
            if filter_client and non_withdrawn_field:
                return query.filter(ParticipantSummary.participantOrigin == client_id,
                    or_(
                        ParticipantSummary.withdrawalStatus != WithdrawalStatus.NO_USE,
                        ParticipantSummary.withdrawalTime >= withdrawn_visible_start,
                        )
                )
            elif filter_client:
                return query.filter(
                        ParticipantSummary.participantOrigin == client_id
                        )
            elif non_withdrawn_field:
                # When querying on fields that aren't available for withdrawn participants,
                # ensure that we only return participants
                # who have not withdrawn or withdrew in the past 48 hours.
                return query.filter(
                    or_(
                        ParticipantSummary.withdrawalStatus != WithdrawalStatus.NO_USE,
                        ParticipantSummary.withdrawalTime >= withdrawn_visible_start,
                    )
                )
            else:
                # When querying on fields that are available for withdrawn participants, return everybody;
                # withdrawn participants will have all but WITHDRAWN_PARTICIPANT_FIELDS cleared out 48
                # hours after withdrawing.
                return query

    def _get_order_by_ending(self, query):
        if self._has_withdrawn_filter(query):
            return _WITHDRAWN_ORDER_BY_ENDING
        return self.order_by_ending

    def _add_order_by(self, query, order_by, field_names, fields):
        if order_by.field_name in _CODE_FILTER_FIELDS:
            return super(ParticipantSummaryDao, self)._add_order_by(
                query, OrderBy(order_by.field_name + "Id", order_by.ascending), field_names, fields
            )
        elif order_by.field_name == 'state':
            return self._add_order_by_state(order_by, query)
        elif order_by.field_name == 'questionnaireOnEnvironmentalExposures':
            return self._add_env_exposures_order(query, order_by, PediatricDataLog.id.isnot(None))
        elif order_by.field_name == 'questionnaireOnEnvironmentalExposuresTime':
            return self._add_env_exposures_order(query, order_by, PediatricDataLog.created)
        elif order_by.field_name == 'questionnaireOnEnvironmentalExposuresAuthored':
            return self._add_env_exposures_order(query, order_by, PediatricDataLog.value)
        return super(ParticipantSummaryDao, self)._add_order_by(query, order_by, field_names, fields)

    @staticmethod
    def _add_order_by_state(order_by, query):
        query = query.outerjoin(Code, ParticipantSummary.stateId == Code.codeId)
        if order_by.ascending:
            return query.order_by(Code.display)
        else:
            return query.order_by(Code.display.desc())

    @classmethod
    def _add_env_exposures_order(cls, query, order_by, field):
        if not order_by.ascending:
            field = field.desc()

        return query.outerjoin(
            PediatricDataLog,
            and_(
                PediatricDataLog.participant_id == ParticipantSummary.participantId,
                PediatricDataLog.data_type == PediatricDataType.ENVIRONMENTAL_EXPOSURES
            )
        ).order_by(field)

    def _make_query(self, session, query_definition):
        query, order_by_field_names = super(ParticipantSummaryDao, self)._make_query(session, query_definition)
        # Note: leaving for future use if we go back to using a relationship to PatientStatus table.
        # query.options(selectinload(ParticipantSummary.patientStatus))
        # sql = self.query_to_text(query)

        if not query_definition.attributes:  # temporarily skip any joinloads if using result field filters
            query = query.options(*self._default_api_query_options())
        return query, order_by_field_names

    def make_query_filter(self, field_name, value):
        """Handle HPO and code values when parsing filter values."""
        if field_name == "biobankId":
            value = from_client_biobank_id(value, log_exception=True)
        if field_name == "hpoId" or field_name == "awardee":
            hpo = self.hpo_dao.get_by_name(value)
            if not hpo:
                raise BadRequest(f"No HPO found with name {value}")
            if field_name == "awardee":
                field_name = "hpoId"
            return super(ParticipantSummaryDao, self).make_query_filter(field_name, hpo.hpoId)
        if field_name == "organization":
            if value == UNSET:
                return super(ParticipantSummaryDao, self).make_query_filter(field_name + "Id", None)
            organization = self.organization_dao.get_by_external_id(value)
            if not organization:
                raise BadRequest(f"No organization found with name {value}")
            return super(ParticipantSummaryDao, self).make_query_filter(field_name + "Id", organization.organizationId)
        if field_name in _SITE_FIELDS:
            if value == UNSET:
                return super(ParticipantSummaryDao, self).make_query_filter(field_name + "Id", None)
            site = self.site_dao.get_by_google_group(value)
            if not site:
                raise BadRequest(f"No site found with google group {value}")
            return super(ParticipantSummaryDao, self).make_query_filter(field_name + "Id", site.siteId)
        if field_name in _CODE_FILTER_FIELDS:
            if value == UNSET:
                return super(ParticipantSummaryDao, self).make_query_filter(field_name + "Id", None)
            # Note: we do not at present support querying for UNMAPPED code values.
            code = self.code_dao.get_code(PPI_SYSTEM, value)
            if not code:
                raise BadRequest(f"No code found: {value}")
            return super(ParticipantSummaryDao, self).make_query_filter(field_name + "Id", code.codeId)
        if field_name == "patientStatus":
            return self._make_patient_status_field_filter(field_name, value)
        if field_name == "participantOrigin":
            if value not in ORIGINATING_SOURCES:
                raise BadRequest(f"No origin source found for {value}")
            return super(ParticipantSummaryDao, self).make_query_filter(field_name, value)
        if field_name == 'updatedSince':
            return self._make_updated_since_filter(value)

        return super(ParticipantSummaryDao, self).make_query_filter(field_name, value)

    def _make_patient_status_field_filter(self, field_name, value):
        try:
            organization_external_id, status_text = value.split(":")
        except ValueError:
            raise BadRequest(
                ("Invalid patientStatus parameter: `{}`. It must be in the format `ORGANIZATION:VALUE`").format(value)
            )
        try:
            status = PatientStatusFlag(status_text)
        except (KeyError, TypeError):
            raise BadRequest(
                ("Invalid patientStatus parameter: `{}`. `VALUE` must be one of {}").format(
                    value, list(PatientStatusFlag.to_dict().keys())
                )
            )
        organization = self.organization_dao.get_by_external_id(organization_external_id)
        if not organization:
            raise BadRequest(f"No organization found with name {organization_external_id}")
        # Note: leaving for future use if we go back to using a relationship to PatientStatus table.
        # return PatientStatusFieldFilter(field_name, Operator.EQUALS, value,
        #                                 organization=organization,
        #                                 status=status)

        if status == PatientStatusFlag.UNSET:
            filter_value = '{{"organization": "{0}"}}'.format(organization.externalId)
            filter_obj = FieldJsonContainsFilter(field_name, Operator.NOT_EQUALS, filter_value)
        else:
            filter_value = '{{"organization": "{0}", "status": "{1}"}}'.format(organization.externalId, str(status))
            filter_obj = FieldJsonContainsFilter(field_name, Operator.EQUALS, filter_value)

        return filter_obj

    def _make_updated_since_filter(self, updated_since_value):
        return FieldFilter('lastModified', Operator.GREATER_THAN_OR_EQUALS, updated_since_value)

    def update_from_biobank_stored_samples(self, participant_id=None, biobank_ids=None, session=None):
        """Rewrites sample-related summary data. Call this after updating BiobankStoredSamples.
    If participant_id is provided, only that participant will have their summary updated."""
        now = clock.CLOCK.now()
        sample_sql, sample_params = _get_sample_sql_and_params(now)

        baseline_tests_sql, baseline_tests_params = _get_baseline_sql_and_params()
        dna_tests_sql, dna_tests_params = _get_dna_isolates_sql_and_params()

        counts_sql = """
    UPDATE
      participant_summary
    SET
      num_baseline_samples_arrived = {baseline_tests_sql},
      samples_to_isolate_dna = {dna_tests_sql},
      last_modified = :now
    WHERE
      (
        num_baseline_samples_arrived != {baseline_tests_sql}
        OR samples_to_isolate_dna != {dna_tests_sql}
      )
    """.format(
            baseline_tests_sql=baseline_tests_sql, dna_tests_sql=dna_tests_sql
        )
        counts_params = {"now": now}
        counts_params.update(baseline_tests_params)
        counts_params.update(dna_tests_params)

        # If participant_id is provided, add the participant ID filter to all update statements.
        if participant_id:
            sample_sql += " AND participant_id = :participant_id"
            sample_params["participant_id"] = participant_id
            counts_sql += " AND participant_id = :participant_id"
            counts_params["participant_id"] = participant_id

        sample_sql = replace_null_safe_equals(sample_sql)
        counts_sql = replace_null_safe_equals(counts_sql)

        if not session:
            with self.session() as session:
                self._run_sql_updates(sample_sql, sample_params, counts_sql, counts_params, biobank_ids, session)
        else:
            self._run_sql_updates(sample_sql, sample_params, counts_sql, counts_params, biobank_ids, session)

    def _run_sql_updates(self, sample_sql, sample_params, counts_sql, counts_params, biobank_ids, session):
        session.execute(sample_sql, sample_params)
        session.execute(counts_sql, counts_params)
        session.commit()

        if biobank_ids:
            query = session.query(ParticipantSummary.participantId).filter(
                ParticipantSummary.biobankId.in_(biobank_ids)
            )
            participant_id_list = [summary.participantId for summary in query.all()]
            for participant_id in participant_id_list:
                summary = ParticipantSummaryDao.get_for_update_with_linked_data(
                    session=session,
                    participant_id=participant_id
                )
                self.update_enrollment_status(summary=summary, session=session)
                session.commit()

    def _get_num_baseline_ppi_modules(self):
        return len(config.getSettingList(config.BASELINE_PPI_QUESTIONNAIRE_FIELDS))

    @classmethod
    def _update_timestamp_value(cls, summary, field_name, new_value):
        existing_value = getattr(summary, field_name)
        if new_value and existing_value != new_value and existing_value is None:
            setattr(summary, field_name, new_value)

    def _clear_timestamp_if_set(cls, summary, field_name):
        if getattr(summary, field_name) is not None:
            setattr(summary, field_name, None)

    def update_enrollment_status(self, summary: ParticipantSummary, session,
                                 allow_downgrade=False, pdr_pubsub=True):
        """
        Updates the enrollment status field on the provided participant summary to the correct value.
        If allow_downgrade flag is set (e.g., when called by backfill tool), V3.* statuses will be recalculated
        from scratch and may revert from a higher enrollment status (such as BASELINE_PARTICIPANT) to a lower status
        """
        from rdr_service.dao.physical_measurements_dao import PhysicalMeasurementsDao

        earliest_physical_measurements_time = min_or_none([
            summary.clinicPhysicalMeasurementsFinalizedTime,
            summary.selfReportedPhysicalMeasurementsAuthored
        ])

        core_measurements = PhysicalMeasurementsDao.get_core_measurements_for_participant(
            session=session,
            participant_id=summary.participantId
        )

        earliest_biobank_received_dna_time = None
        if summary.samplesToIsolateDNA == SampleStatus.RECEIVED:
            earliest_biobank_received_dna_time = BiobankStoredSampleDao.get_earliest_confirmed_dna_sample_timestamp(
                session=session,
                biobank_id=summary.biobankId
            )

        # See ROC-1572/PDR-1699.  Provide a default date to get_interest_in_sharing_ehr_ranges() if participant
        # has SUBMITTED status for their EHR consent.  Remediates data issues w/older consent validations
        default_ehr_date = summary.consentForElectronicHealthRecordsFirstYesAuthored \
            if summary.consentForElectronicHealthRecords == QuestionnaireStatus.SUBMITTED else None

        ehr_consent_ranges = QuestionnaireResponseRepository.get_interest_in_sharing_ehr_ranges(
            participant_id=summary.participantId,
            session=session,
            default_authored_datetime=default_ehr_date
        )

        revised_consent_time_list = []
        response_collection = QuestionnaireResponseRepository.get_responses_to_surveys(
            session=session,
            survey_codes=[PRIMARY_CONSENT_UPDATE_MODULE],
            participant_ids=[summary.participantId]
        )
        if summary.participantId in response_collection:
            program_update_response_list = response_collection[summary.participantId].responses.values()
            for response in program_update_response_list:
                reconsent_answer = response.get_single_answer_for(PRIMARY_CONSENT_UPDATE_QUESTION_CODE).value.lower()
                if reconsent_answer == COHORT_1_REVIEW_CONSENT_YES_CODE.lower():
                    revised_consent_time_list.append(response.authored_datetime)
        revised_consent_time = min_or_none(revised_consent_time_list)

        wgs_sequencing_time = GenomicSetMemberDao.get_wgs_pass_date(
            session=session,
            biobank_id=summary.biobankId
        )
        first_exposures_response_time = None
        for data in (summary.pediatricData or []):
            if data.data_type == PediatricDataType.ENVIRONMENTAL_EXPOSURES:
                timestamp = parse(data.value)
                if first_exposures_response_time is None or timestamp < first_exposures_response_time:
                    first_exposures_response_time = timestamp

        enrl_dependencies = EnrollmentDependencies(
            consent_cohort=summary.consentCohort,
            primary_consent_authored_time=summary.consentForStudyEnrollmentFirstYesAuthored,
            gror_authored_time=summary.consentForGenomicsRORAuthored,
            basics_authored_time=summary.questionnaireOnTheBasicsAuthored,
            overall_health_authored_time=summary.questionnaireOnOverallHealthAuthored,
            lifestyle_authored_time=summary.questionnaireOnLifestyleAuthored,
            earliest_ehr_file_received_time=min_or_none(
                [summary.ehrReceiptTime, summary.firstParticipantMediatedEhrReceiptTime]
            ),
            earliest_mediated_ehr_receipt_time=summary.firstParticipantMediatedEhrReceiptTime,
            earliest_physical_measurements_time=earliest_physical_measurements_time,
            earliest_biobank_received_dna_time=earliest_biobank_received_dna_time,
            ehr_consent_date_range_list=ehr_consent_ranges,
            dna_update_time=revised_consent_time,
            earliest_height_measurement_time=min_or_none(
                meas.finalized for meas in core_measurements if meas.satisfiesHeightRequirements
            ),
            earliest_weight_measurement_time=min_or_none(
                meas.finalized for meas in core_measurements if meas.satisfiesWeightRequirements
            ),
            wgs_sequencing_time=wgs_sequencing_time,
            exposures_authored_time=first_exposures_response_time,
            is_pediatric_participant=summary.isPediatric,
            has_linked_guardian_accounts=(summary.relatedParticipants and len(summary.relatedParticipants) > 0)
        )
        enrollment_info = EnrollmentCalculation.get_enrollment_info(enrl_dependencies)

        # Update enrollment status if it is upgrading
        legacy_dates = enrollment_info.version_legacy_dates
        version_3_0_dates = enrollment_info.version_3_0_dates
        version_3_2_dates = enrollment_info.version_3_2_dates
        status_rank_map = {  # Re-orders the status values so we can quickly see if the current status is higher
            EnrollmentStatus.INTERESTED: 0,
            EnrollmentStatus.MEMBER: 1,
            EnrollmentStatus.CORE_MINUS_PM: 2,
            EnrollmentStatus.FULL_PARTICIPANT: 3
        }
        # TODO: for now, assume allow_downgrade should not be honored for legacy status fields (allow for V3.* only)
        if status_rank_map[summary.enrollmentStatus] < status_rank_map[enrollment_info.version_legacy_status]:
            summary.enrollmentStatus = enrollment_info.version_legacy_status
            summary.lastModified = clock.CLOCK.now()

            # Record the new status in the history table
            session.add(
                EnrollmentStatusHistory(
                    participant_id=summary.participantId,
                    version='legacy',
                    status=str(summary.enrollmentStatus),
                    timestamp=legacy_dates[summary.enrollmentStatus],
                    dependencies_snapshot=enrl_dependencies.to_json_dict()
                )
            )

        if allow_downgrade or summary.enrollmentStatusV3_0 < enrollment_info.version_3_0_status:
            summary.enrollmentStatusV3_0 = enrollment_info.version_3_0_status
            summary.lastModified = clock.CLOCK.now()
            session.add(
                EnrollmentStatusHistory(
                    participant_id=summary.participantId,
                    version='3.0',
                    status=str(summary.enrollmentStatusV3_0),
                    timestamp=version_3_0_dates[summary.enrollmentStatusV3_0],
                    dependencies_snapshot=enrl_dependencies.to_json_dict()
                )
            )

        status_rank_map_v32 = {  # Re-orders the status values so we can quickly see if the current status is higher
            EnrollmentStatusV32.PARTICIPANT: 0,
            EnrollmentStatusV32.PARTICIPANT_PLUS_EHR: 1,
            EnrollmentStatusV32.ENROLLED_PARTICIPANT: 2,
            EnrollmentStatusV32.PMB_ELIGIBLE: 3,
            EnrollmentStatusV32.CORE_MINUS_PM: 4,
            EnrollmentStatusV32.CORE_PARTICIPANT: 5
        }
        if (
            allow_downgrade
            or status_rank_map_v32[summary.enrollmentStatusV3_2] <
                status_rank_map_v32[enrollment_info.version_3_2_status]
        ):
            summary.enrollmentStatusV3_2 = enrollment_info.version_3_2_status
            summary.lastModified = clock.CLOCK.now()
            session.add(
                EnrollmentStatusHistory(
                    participant_id=summary.participantId,
                    version='3.2',
                    status=str(summary.enrollmentStatusV3_2),
                    timestamp=version_3_2_dates[summary.enrollmentStatusV3_2],
                    dependencies_snapshot=enrl_dependencies.to_json_dict()
                )
            )

        if summary.hasCoreData != enrollment_info.has_core_data and (enrollment_info.has_core_data or allow_downgrade):
            # Set hasCoreData to True if they have it now,
            # or remove it if they no longer do and we're allowing downgrades
            summary.hasCoreData = enrollment_info.has_core_data
            session.add(
                EnrollmentStatusHistory(
                    participant_id=summary.participantId,
                    version='core_data',
                    status="True",
                    timestamp=enrollment_info.core_data_time,
                    dependencies_snapshot=enrl_dependencies.to_json_dict()
                )
            )

        # DA-3777: Surface for HPRO whether pid has valid height and weight measurement data.
        # This is not spelled out in the Goal 1 definitions as an official enrollment status flag and is not
        # tracked independently in the enrollment status history; but the has_core_data definition is a superset of
        # conditions which includes the requirement that has_height_and_weight is true
        if enrl_dependencies.earliest_height_measurement_time and enrl_dependencies.earliest_weight_measurement_time:
            satisfied_hw_time = max(enrl_dependencies.earliest_height_measurement_time,
                                    enrl_dependencies.earliest_weight_measurement_time)
            if not summary.hasHeightAndWeight or summary.hasHeightAndWeightTime != satisfied_hw_time:
                summary.hasHeightAndWeight = True
                summary.hasHeightAndWeightTime = satisfied_hw_time
                summary.lastModified = clock.CLOCK.now()
        elif summary.hasHeightAndWeight:
            # PM may have been cancelled, so there are no longer valid core measurements / measurement times
            summary.hasHeightAndWeight = False
            summary.hasHeightAndWeightTime = None
            summary.lastModified = clock.CLOCK.now()

        # Set enrollment status date fields
        if EnrollmentStatus.MEMBER in legacy_dates:
            self._update_timestamp_value(summary, 'enrollmentStatusMemberTime', legacy_dates[EnrollmentStatus.MEMBER])
        if EnrollmentStatus.CORE_MINUS_PM in legacy_dates:
            self._update_timestamp_value(summary, 'enrollmentStatusCoreMinusPMTime',
                                         legacy_dates[EnrollmentStatus.CORE_MINUS_PM])
        if EnrollmentStatus.FULL_PARTICIPANT in legacy_dates:
            self._update_timestamp_value(
                summary,
                'enrollmentStatusCoreStoredSampleTime',
                legacy_dates[EnrollmentStatus.FULL_PARTICIPANT]
            )

        # For V3.* status timestamps, they can be cleared if a status has reverted (on allow_downgrade operation)
        if EnrollmentStatusV30.PARTICIPANT in version_3_0_dates:
            self._update_timestamp_value(
                summary,
                'enrollmentStatusParticipantV3_0Time',
                version_3_0_dates[EnrollmentStatusV30.PARTICIPANT]
            )
        elif allow_downgrade:
            self._clear_timestamp_if_set(summary, 'enrollmentStatusParticipantV3_0Time')

        if EnrollmentStatusV30.PARTICIPANT_PLUS_EHR in version_3_0_dates:
            self._update_timestamp_value(
                summary,
                'enrollmentStatusParticipantPlusEhrV3_0Time',
                version_3_0_dates[EnrollmentStatusV30.PARTICIPANT_PLUS_EHR]
            )
        elif allow_downgrade:
            self._clear_timestamp_if_set(summary, 'enrollmentStatusParticipantPlusEhrV3_0Time')

        if EnrollmentStatusV30.PARTICIPANT_PMB_ELIGIBLE in version_3_0_dates:
            self._update_timestamp_value(
                summary,
                'enrollmentStatusPmbEligibleV3_0Time',
                version_3_0_dates[EnrollmentStatusV30.PARTICIPANT_PMB_ELIGIBLE]
            )
        elif allow_downgrade:
            self._clear_timestamp_if_set(summary, 'enrollmentStatusPmbEligibleV3_0Time')

        if EnrollmentStatusV30.CORE_MINUS_PM in version_3_0_dates:
            self._update_timestamp_value(
                summary,
                'enrollmentStatusCoreMinusPmV3_0Time',
                version_3_0_dates[EnrollmentStatusV30.CORE_MINUS_PM]
            )
        elif allow_downgrade:
            self._clear_timestamp_if_set(summary, 'enrollmentStatusCoreMinusPmV3_0Time')

        if EnrollmentStatusV30.CORE_PARTICIPANT in version_3_0_dates:
            self._update_timestamp_value(
                summary,
                'enrollmentStatusCoreV3_0Time',
                version_3_0_dates[EnrollmentStatusV30.CORE_PARTICIPANT]
            )
        elif allow_downgrade:
            self._clear_timestamp_if_set(summary, 'enrollmentStatusCoreV3_0Time')

        if EnrollmentStatusV32.PARTICIPANT in version_3_2_dates:
            self._update_timestamp_value(
                summary,
                'enrollmentStatusParticipantV3_2Time',
                version_3_2_dates[EnrollmentStatusV32.PARTICIPANT]
            )
        elif allow_downgrade:
            self._clear_timestamp_if_set(summary, 'enrollmentStatusParticipantV3_2Time')

        if EnrollmentStatusV32.PARTICIPANT_PLUS_EHR in version_3_2_dates:
            self._update_timestamp_value(
                summary,
                'enrollmentStatusParticipantPlusEhrV3_2Time',
                version_3_2_dates[EnrollmentStatusV32.PARTICIPANT_PLUS_EHR]
            )
        elif allow_downgrade:
            self._clear_timestamp_if_set(summary, 'enrollmentStatusParticipantPlusEhrV3_2Time')

        if EnrollmentStatusV32.ENROLLED_PARTICIPANT in version_3_2_dates:
            self._update_timestamp_value(
                summary,
                'enrollmentStatusEnrolledParticipantV3_2Time',
                version_3_2_dates[EnrollmentStatusV32.ENROLLED_PARTICIPANT]
            )
        elif allow_downgrade:
            self._clear_timestamp_if_set(summary, 'enrollmentStatusEnrolledParticipantV3_2Time')

        if EnrollmentStatusV32.PMB_ELIGIBLE in version_3_2_dates:
            self._update_timestamp_value(
                summary,
                'enrollmentStatusPmbEligibleV3_2Time',
                version_3_2_dates[EnrollmentStatusV32.PMB_ELIGIBLE]
            )
        elif allow_downgrade:
            self._clear_timestamp_if_set(summary, 'enrollmentStatusPmbEligibleV3_2Time')

        if EnrollmentStatusV32.CORE_MINUS_PM in version_3_2_dates:
            self._update_timestamp_value(
                summary,
                'enrollmentStatusCoreMinusPmV3_2Time',
                version_3_2_dates[EnrollmentStatusV32.CORE_MINUS_PM]
            )
        elif allow_downgrade:
            self._clear_timestamp_if_set(summary, 'enrollmentStatusCoreMinusPmV3_2Time')

        if EnrollmentStatusV32.CORE_PARTICIPANT in version_3_2_dates:
            self._update_timestamp_value(
                summary,
                'enrollmentStatusCoreV3_2Time',
                version_3_2_dates[EnrollmentStatusV32.CORE_PARTICIPANT]
            )
        elif allow_downgrade:
            self._clear_timestamp_if_set(summary, 'enrollmentStatusCoreV3_2Time')

        if enrollment_info.core_data_time is not None:
            self._update_timestamp_value(
                summary,
                'hasCoreDataTime',
                enrollment_info.core_data_time
            )
        elif allow_downgrade:
            self._clear_timestamp_if_set(summary, 'hasCoreDataTime')

        # Legacy code for setting CoreOrdered date field
        consent = (
                      summary.consentForStudyEnrollment == QuestionnaireStatus.SUBMITTED
                      and summary.consentForElectronicHealthRecords == QuestionnaireStatus.SUBMITTED
                  ) or (
                      summary.consentForStudyEnrollment == QuestionnaireStatus.SUBMITTED
                      and summary.consentForElectronicHealthRecords is None
                      and summary.consentForDvElectronicHealthRecordsSharing == QuestionnaireStatus.SUBMITTED
        )
        summary.enrollmentStatusCoreOrderedSampleTime = self.calculate_core_ordered_sample_time(consent, summary)

        if pdr_pubsub:
            submit_pipeline_pubsub_msg_from_model(summary, self.get_connection_database_name())

    def calculate_enrollment_status(
        self, consent, num_completed_baseline_ppi_modules, physical_measurements_status,
        self_reported_physical_measurements_status, samples_to_isolate_dna, consent_cohort, gror_consent,
        consent_expire_status=ConsentExpireStatus.NOT_EXPIRED
    ):
        """
          2021-07 Note on enrollment status calculations and GROR:
          Per NIH Analytics Data Glossary and confirmation on requirements for Core participants:
          Cohort 3 participants need any GROR response (yes/no/not sure) to elevate to Core or Core Minus PM status
        """
        if consent:
            if (
                num_completed_baseline_ppi_modules == self._get_num_baseline_ppi_modules()
                and (physical_measurements_status == PhysicalMeasurementsStatus.COMPLETED or
                     self_reported_physical_measurements_status == SelfReportedPhysicalMeasurementsStatus.COMPLETED)
                and samples_to_isolate_dna == SampleStatus.RECEIVED
                and (consent_cohort != ParticipantCohort.COHORT_3 or
                     # All response status enum values other than UNSET or SUBMITTED_INVALID meet the GROR requirement
                     (gror_consent and gror_consent != QuestionnaireStatus.UNSET
                      and gror_consent != QuestionnaireStatus.SUBMITTED_INVALID))
            ):
                return EnrollmentStatus.FULL_PARTICIPANT
            elif (
                num_completed_baseline_ppi_modules == self._get_num_baseline_ppi_modules()
                and physical_measurements_status != PhysicalMeasurementsStatus.COMPLETED
                and self_reported_physical_measurements_status != SelfReportedPhysicalMeasurementsStatus.COMPLETED
                and samples_to_isolate_dna == SampleStatus.RECEIVED
                and (consent_cohort != ParticipantCohort.COHORT_3 or
                     (gror_consent and gror_consent != QuestionnaireStatus.UNSET
                      and gror_consent != QuestionnaireStatus.SUBMITTED_INVALID))
            ):
                return EnrollmentStatus.CORE_MINUS_PM
            elif consent_expire_status != ConsentExpireStatus.EXPIRED:
                return EnrollmentStatus.MEMBER
        return EnrollmentStatus.INTERESTED

    @staticmethod
    def calculate_member_time(consent, participant_summary):
        if consent and participant_summary.enrollmentStatusMemberTime is not None:
            return participant_summary.enrollmentStatusMemberTime
        elif consent:
            if (
                participant_summary.consentForElectronicHealthRecords is None
                and participant_summary.consentForDvElectronicHealthRecordsSharing == QuestionnaireStatus.SUBMITTED
            ):
                return participant_summary.consentForDvElectronicHealthRecordsSharingAuthored
            return participant_summary.consentForElectronicHealthRecordsAuthored
        else:
            return None

    def calculate_core_minus_pm_time(self, consent, participant_summary, enrollment_status):
        if (
            consent
            and participant_summary.numCompletedBaselinePPIModules == self._get_num_baseline_ppi_modules()
            and participant_summary.clinicPhysicalMeasurementsStatus != PhysicalMeasurementsStatus.COMPLETED
            and participant_summary.selfReportedPhysicalMeasurementsStatus !=
            SelfReportedPhysicalMeasurementsStatus.COMPLETED
            and participant_summary.samplesToIsolateDNA == SampleStatus.RECEIVED
            and (participant_summary.consentForGenomicsROR == QuestionnaireStatus.SUBMITTED
                 or participant_summary.consentCohort != ParticipantCohort.COHORT_3)
        ) or enrollment_status == EnrollmentStatus.CORE_MINUS_PM:

            max_core_sample_time = self.calculate_max_core_sample_time(
                participant_summary, field_name_prefix="sampleStatus"
            )

            if max_core_sample_time and participant_summary.enrollmentStatusCoreStoredSampleTime:
                return participant_summary.enrollmentStatusCoreStoredSampleTime
            else:
                return max_core_sample_time
        elif participant_summary.enrollmentStatusCoreMinusPMTime is not None:
            return participant_summary.enrollmentStatusCoreMinusPMTime
        else:
            return None

    def calculate_core_stored_sample_time(self, consent, participant_summary):
        if (
            consent
            and participant_summary.numCompletedBaselinePPIModules == self._get_num_baseline_ppi_modules()
            and (participant_summary.clinicPhysicalMeasurementsStatus == PhysicalMeasurementsStatus.COMPLETED or
                 participant_summary.selfReportedPhysicalMeasurementsStatus ==
                 SelfReportedPhysicalMeasurementsStatus.COMPLETED)
            and participant_summary.samplesToIsolateDNA == SampleStatus.RECEIVED
        ) or participant_summary.enrollmentStatus == EnrollmentStatus.FULL_PARTICIPANT:

            max_core_sample_time = self.calculate_max_core_sample_time(
                participant_summary, field_name_prefix="sampleStatus"
            )

            if max_core_sample_time and participant_summary.enrollmentStatusCoreStoredSampleTime:
                return participant_summary.enrollmentStatusCoreStoredSampleTime
            else:
                return max_core_sample_time
        else:
            return None

    def calculate_core_ordered_sample_time(self, consent, participant_summary):
        if (
            consent
            and participant_summary.numCompletedBaselinePPIModules == self._get_num_baseline_ppi_modules()
            and (
                participant_summary.clinicPhysicalMeasurementsStatus == PhysicalMeasurementsStatus.COMPLETED
                or participant_summary.selfReportedPhysicalMeasurementsStatus ==
                SelfReportedPhysicalMeasurementsStatus.COMPLETED
            )
        ) or participant_summary.enrollmentStatus == EnrollmentStatus.FULL_PARTICIPANT:

            max_core_sample_time = self.calculate_max_core_sample_time(
                participant_summary, field_name_prefix="sampleOrderStatus"
            )

            if max_core_sample_time and participant_summary.enrollmentStatusCoreOrderedSampleTime:
                return participant_summary.enrollmentStatusCoreOrderedSampleTime
            else:
                return max_core_sample_time
        else:
            return None

    def calculate_max_core_sample_time(self, participant_summary, field_name_prefix="sampleStatus"):

        keys = [field_name_prefix + "%sTime" % test for test in config.getSettingList(config.DNA_SAMPLE_TEST_CODES)]
        sample_time_list = [v for k, v in participant_summary if k in keys and v is not None]

        sample_time = min(sample_time_list) if sample_time_list else None

        if sample_time is not None:
            return max([time for time in
                        [
                            sample_time,
                            participant_summary.enrollmentStatusMemberTime,
                            participant_summary.questionnaireOnTheBasicsTime,
                            participant_summary.questionnaireOnLifestyleTime,
                            participant_summary.questionnaireOnOverallHealthTime,
                            participant_summary.clinicPhysicalMeasurementsFinalizedTime,
                            participant_summary.selfReportedPhysicalMeasurementsAuthored
                        ] if time is not None]
            )
        else:
            return None

    def calculate_distinct_visits(self, pid, finalized_time, id_, amendment=False):
        """ Participants may get PM or biobank samples on same day. This should be considered as
    a single visit in terms of program payment to participant.
    return Boolean: true if there has not been an order on same date."""
        from rdr_service.dao.biobank_order_dao import BiobankOrderDao
        from rdr_service.dao.physical_measurements_dao import PhysicalMeasurementsDao

        day_has_order, day_has_measurement = False, False
        existing_orders = BiobankOrderDao().get_biobank_orders_for_participant(pid)
        ordered_samples = BiobankOrderDao().get_ordered_samples_for_participant(pid)
        existing_measurements = PhysicalMeasurementsDao().get_measuremnets_for_participant(pid)

        order_id_to_finalized_date = {
            sample.biobankOrderId: sample.finalized.date() for sample in ordered_samples if sample.finalized
        }

        if existing_orders and finalized_time:
            for order in existing_orders:
                order_finalized_date = order_id_to_finalized_date.get(order.biobankOrderId)
                if (
                    order_finalized_date == finalized_time.date()
                    and order.biobankOrderId != id_
                    and order.orderStatus != BiobankOrderStatus.CANCELLED
                ):
                    day_has_order = True
                elif order.biobankOrderId == id_ and amendment:
                    day_has_order = True
        elif not finalized_time and amendment:
            day_has_order = True

        if existing_measurements and finalized_time:
            for measurement in existing_measurements:
                if not measurement.finalized:
                    continue
                if measurement.finalized.date() == finalized_time.date() and measurement.physicalMeasurementsId != id_:
                    day_has_measurement = True

        is_distinct_visit = not (day_has_order or day_has_measurement)
        return is_distinct_visit

    @staticmethod
    def get_client_id():
        from rdr_service import app_util, api_util
        email = app_util.get_oauth_id()
        user_info = app_util.lookup_user_info(email)
        client_id = user_info.get('clientId')
        if email == api_util.DEV_MAIL and client_id is None:
            client_id = 'example'  # account for temp configs that dont create the key
        return client_id

    def get_record_from_attr(self, *, attr, value):
        with self.session() as session:
            record = session.query(ParticipantSummary)\
                .filter(ParticipantSummary.withdrawalStatus == WithdrawalStatus.NOT_WITHDRAWN,
                    getattr(ParticipantSummary, attr) == value,
                    getattr(ParticipantSummary, attr).isnot(None))
            return record.all()

    def get_hpro_consent_paths(self, result):
        type_to_field_name_map = {  # Maps types to the prefix of the field name they populate in the resulting JSON
            ConsentType.PRIMARY: 'consentForStudyEnrollment',
            ConsentType.PEDIATRIC_PRIMARY: 'consentForStudyEnrollment',
            ConsentType.CABOR: 'consentForCABoR',
            ConsentType.EHR: 'consentForElectronicHealthRecords',
            ConsentType.PEDIATRIC_EHR: 'consentForElectronicHealthRecords',
            ConsentType.GROR: 'consentForGenomicsROR',
            ConsentType.PRIMARY_RECONSENT: 'reconsentForStudyEnrollment',
            ConsentType.EHR_RECONSENT: 'reconsentForElectronicHealthRecords'
        }
        participant_id = result['participantId']
        records = list(filter(lambda obj: obj.participant_id == participant_id, self.hpro_consents))

        for consent_type, field_name_prefix in type_to_field_name_map.items():
            field_name = f'{field_name_prefix}FilePath'
            matching_consent_list = [obj for obj in records if consent_type == obj.consent_type]

            if matching_consent_list:
                result[field_name] = matching_consent_list[0].file_path

        return result

    def get_participant_incentives(self, result):
        participant_id = result['participantId']

        records = list(filter(lambda obj: obj.participantId == participant_id, self.participant_incentives))

        records = [self.incentive_dao.convert_json_obj(obj) for obj in records]
        return records

    def to_client_json(self, obj: ParticipantSummary, payload_attributes=None) -> dict:
        if payload_attributes:
            return self.build_filtered_obj_response(obj, payload_attributes)
        return self.build_default_obj_response(obj)

    @classmethod
    def build_filtered_obj_response(
        cls,
        obj: ParticipantSummary,
        payload_attributes: List['str']
    ) -> dict:
        if hasattr(obj, '_asdict'):
            obj = obj._asdict()
        elif hasattr(obj, 'asdict'):
            obj = obj.asdict()
        return {k: v for k, v in list(obj.items()) if k in payload_attributes}

    def build_default_obj_response(self, obj: ParticipantSummary):
        result = obj.asdict()
        clinic_pm_time = result.get("clinicPhysicalMeasurementsFinalizedTime")
        self_reported_pm_time = result.get("selfReportedPhysicalMeasurementsAuthored")
        if self.hpro_consents:
            result = self.get_hpro_consent_paths(result)
        if self.participant_incentives:
            result['participantIncentives'] = self.get_participant_incentives(result)

        is_the_basics_complete = obj.questionnaireOnTheBasics == QuestionnaireStatus.SUBMITTED

        # Participants that withdrew more than 48 hours ago should have fields other than
        # WITHDRAWN_PARTICIPANT_FIELDS cleared.
        should_clear_fields_for_withdrawal = obj.withdrawalStatus == WithdrawalStatus.NO_USE and (
            obj.withdrawalTime is None
            or obj.withdrawalTime < clock.CLOCK.now() - WITHDRAWN_PARTICIPANT_VISIBILITY_TIME
        )
        if should_clear_fields_for_withdrawal:
            result = {k: result.get(k) for k in WITHDRAWN_PARTICIPANT_FIELDS}

        result["participantId"] = to_client_participant_id(obj.participantId)
        biobank_id = result.get("biobankId")
        if biobank_id:
            result["biobankId"] = to_client_biobank_id(biobank_id)

        date_of_birth = result.get("dateOfBirth")
        if date_of_birth:
            result["ageRange"] = get_bucketed_age(date_of_birth, clock.CLOCK.now())
        else:
            result["ageRange"] = UNSET

        if not result.get("primaryLanguage"):
            result["primaryLanguage"] = UNSET

        if "organizationId" in result:
            result["organization"] = result["organizationId"]
            del result["organizationId"]
            format_json_org(result, self.organization_dao, "organization")

        if result.get("genderIdentityId"):
            del result["genderIdentityId"]  # deprecated in favor of genderIdentity

        # Format True False None Responses to default to UNSET when none
        field_names = [
            'remoteIdVerificationStatus',
            'everIdVerified'
        ]
        for field_name in field_names:
            if result.get(field_name) is None:
                result[field_name] = UNSET
            elif result[field_name]:
                result[field_name] = 'True'
            else:
                result[field_name] = 'False'

        # Map demographic Enums if TheBasics was submitted and Skip wasn't in use
        if is_the_basics_complete and not should_clear_fields_for_withdrawal:
            if obj.genderIdentity is None or obj.genderIdentity == GenderIdentity.UNSET:
                result['genderIdentity'] = GenderIdentity.PMI_Skip

            if obj.race is None or obj.race == Race.UNSET:
                result['race'] = Race.PMI_Skip

        result["patientStatus"] = obj.patientStatus

        format_json_hpo(result, self.hpo_dao, "hpoId")
        result["awardee"] = result["hpoId"]
        _initialize_field_type_sets()

        for new_field_name, existing_field_name in self.get_aliased_field_map().items():
            result[new_field_name] = getattr(obj, existing_field_name)

            # register new field as date if field is date
            if type(result[new_field_name]) is datetime.datetime:
                _DATE_FIELDS.add(new_field_name)

        for fieldname in _DATE_FIELDS:
            format_json_date(result, fieldname)
        for fieldname in _CODE_FIELDS:
            is_demographic_field = fieldname in ['educationId', 'incomeId', 'sexualOrientationId', 'sexId']
            should_map_unset_to_skip = (
                is_the_basics_complete and is_demographic_field and not should_clear_fields_for_withdrawal
            )
            format_json_code(
                result, self.code_dao, fieldname,
                unset_value=PMI_SKIP_CODE if should_map_unset_to_skip else UNSET
            )
        for fieldname in _ENUM_FIELDS:
            format_json_enum(result, fieldname)
        for fieldname in _SITE_FIELDS:
            format_json_site(result, self.site_dao, fieldname)
        if obj.withdrawalStatus == WithdrawalStatus.NO_USE\
                or obj.suspensionStatus == SuspensionStatus.NO_CONTACT\
                or obj.deceasedStatus == DeceasedStatus.APPROVED:
            result["recontactMethod"] = "NO_CONTACT"

        # fill in deprecated fields
        if not clinic_pm_time and not self_reported_pm_time:
            result["physicalMeasurementsStatus"] = "UNSET"
            result["physicalMeasurementsCreatedSite"] = "UNSET"
            result["physicalMeasurementsFinalizedSite"] = "UNSET"
            result["physicalMeasurementsCollectType"] = str(PhysicalMeasurementsCollectType.UNSET)
        elif (clinic_pm_time and not self_reported_pm_time) or (clinic_pm_time and (
              clinic_pm_time >= self_reported_pm_time)):
            result["physicalMeasurementsStatus"] = result.get("clinicPhysicalMeasurementsStatus")
            result["physicalMeasurementsTime"] = result.get("clinicPhysicalMeasurementsTime")
            result["physicalMeasurementsFinalizedTime"] = result.get("clinicPhysicalMeasurementsFinalizedTime")
            result["physicalMeasurementsCreatedSite"] = result.get("clinicPhysicalMeasurementsCreatedSite")
            result["physicalMeasurementsFinalizedSite"] = result.get("clinicPhysicalMeasurementsFinalizedSite")
            result["physicalMeasurementsCollectType"] = str(PhysicalMeasurementsCollectType.SITE)
        else:
            result["physicalMeasurementsStatus"] = result.get("selfReportedPhysicalMeasurementsStatus")
            result["physicalMeasurementsFinalizedTime"] = result.get("selfReportedPhysicalMeasurementsAuthored")
            result["physicalMeasurementsCreatedSite"] = "UNSET"
            result["physicalMeasurementsFinalizedSite"] = "UNSET"
            result["physicalMeasurementsCollectType"] = str(PhysicalMeasurementsCollectType.SELF_REPORTED)

        # Check to see if we should hide 3.0 and 3.2 fields
        if not config.getSettingJson(config.ENABLE_ENROLLMENT_STATUS_3, default=False):
            del result['enrollmentStatusV3_2']
            for field_name in [
                'enrollmentStatusParticipantV3_2Time',
                'enrollmentStatusParticipantPlusEhrV3_2Time',
                'enrollmentStatusEnrolledParticipantV3_2Time',
                'enrollmentStatusPmbEligibleV3_2Time',
                'enrollmentStatusCoreMinusPmV3_2Time',
                'enrollmentStatusCoreV3_2Time',
                'hasCoreData',
                'hasCoreDataTime'
            ]:
                if field_name in result:
                    del result[field_name]

        # Check to see if we should hide digital health sharing fields
        if not config.getSettingJson(config.ENABLE_HEALTH_SHARING_STATUS_3, default=False):
            del result['healthDataStreamSharingStatus']
            if 'healthDataStreamSharingStatusTime' in result:
                del result['healthDataStreamSharingStatusTime']

        # Check if we should hide the participant mediated EHR status fields
        if not config.getSettingJson(config.ENABLE_PARTICIPANT_MEDIATED_EHR, default=False):
            for field_name in [
                'isParticipantMediatedEhrDataAvailable',
                'wasParticipantMediatedEhrAvailable',
                'firstParticipantMediatedEhrReceiptTime',
                'latestParticipantMediatedEhrReceiptTime'
            ]:
                if field_name in result:
                    del result[field_name]

        # Find any linked accounts to display
        result['relatedParticipants'] = UNSET
        if obj.isPediatric:
            if not obj.relatedParticipants:
                logging.error('Pediatric participant does not have a guardian account linked')
                return None

            related_summary_list = [link.related.participantSummary for link in obj.relatedParticipants]
            if any(summary is None for summary in related_summary_list):
                # If any of the guardians of a pediatric participant are not yet consented,
                # don't return the pediatric participant's data
                logging.error('Pediatric participant has unconsented guardian')
                return None

            result['relatedParticipants'] = [
                {
                    'participantId': to_client_participant_id(related_summary.participantId),
                    'firstName': related_summary.firstName,
                    'lastName': related_summary.lastName
                }
                for related_summary in related_summary_list
            ]

        # set the pediatric data flag
        result['isPediatric'] = True if obj.isPediatric else UNSET

        # EnvironmentalExposures module fields
        result['questionnaireOnEnvironmentalExposures'] = UNSET
        for env_exposures_data in [
            data for data in obj.pediatricData if data.data_type == PediatricDataType.ENVIRONMENTAL_EXPOSURES
        ]:
            result['questionnaireOnEnvironmentalExposures'] = str(QuestionnaireStatus.SUBMITTED)
            result['questionnaireOnEnvironmentalExposuresTime'] = env_exposures_data.created
            result['questionnaireOnEnvironmentalExposuresAuthored'] = env_exposures_data.value

        # Format other responses to default to UNSET when none
        field_names = [
            'remoteIdVerifiedOn',
            'firstIdVerifiedOn',
            'remoteIdVerificationOrigin',
            'idVerificationOrigin'
        ]
        for field_name in field_names:
            if not result.get(field_name):
                result[field_name] = UNSET

        return {k: v for k, v in list(result.items()) if v is not None}

    @staticmethod
    def get_aliased_field_map():
        return {
            'firstEhrReceiptTime': 'ehrReceiptTime',
            'latestEhrReceiptTime': 'ehrUpdateTime'
        }

    @staticmethod
    def _make_pagination_token(item_dict, field_names):
        pagination_value_list = [str(item_dict.get(field_name)) for field_name in field_names]
        repo = ObfuscationRepository()
        expire_time = clock.CLOCK.now() + datetime.timedelta(days=1)
        with ParticipantSummaryDao().session() as session:
            lookup_key = repo.store(
                {'field_list': pagination_value_list},
                expiration=expire_time,
                session=session
            )
        return super(ParticipantSummaryDao, ParticipantSummaryDao)._make_pagination_token(
            item_dict={
                'name': 'opaque_token',
                'expires': expire_time.isoformat(),
                'key': lookup_key
            },
            field_names=['name', 'expires', 'key']
        )

    def _decode_token(self, query_def, fields):
        """ If token exists in participant_summary api, decode and use lastModified to add a buffer
    of 60 seconds. This ensures when a _sync link is used no one is missed. This will return
    at a minimum, the last participant and any more that have been modified in the previous 60
    seconds. Duplicate participants returned should be handled on the client side."""
        page_data = self._unpack_page_token(query_def.pagination_token)

        if page_data[0] == 'opaque_token':
            repo = ObfuscationRepository()
            with self.session() as session:
                obfuscation_object = repo.get(page_data[2], session=session)
                if obfuscation_object is None:
                    return NotFound('Unable to find pagination data for token.')
                pagination_data = obfuscation_object['field_list']
        else:
            pagination_data = page_data
        decoded_vals = self._parse_pagination_data(pagination_data, fields)

        if query_def.order_by and (
            query_def.order_by.field_name == "lastModified"
            and query_def.always_return_token is True
            and query_def.backfill_sync is True
        ):
            decoded_vals[0] = decoded_vals[0] - datetime.timedelta(seconds=config.LAST_MODIFIED_BUFFER_SECONDS)

        return decoded_vals

    @staticmethod
    def update_ehr_status(summary, update_time):
        summary.ehrStatus = EhrStatus.PRESENT
        if not summary.ehrReceiptTime:
            summary.ehrReceiptTime = update_time
        summary.ehrUpdateTime = update_time
        return summary

    @classmethod
    def get_all_consented_participant_ids(cls, session):
        db_results = session.query(
            ParticipantSummary.participantId
        ).join(
            Participant,
            Participant.participantId == ParticipantSummary.participantId
        ).filter(
            Participant.isTestParticipant.is_(False)
        ).all()
        return [obj.participantId for obj in db_results]

    def get_participant_ids_with_hpo_ehr_data_available(self):
        with self.session() as session:
            result = session.query(ParticipantSummary.participantId).filter(
                ParticipantSummary.isEhrDataAvailable == expression.true()
            ).all()
            return {row.participantId for row in result}

    def get_participant_ids_with_mediated_ehr_data_available(self):
        with self.session() as session:
            result = session.query(ParticipantSummary.participantId).filter(
                ParticipantSummary.isParticipantMediatedEhrDataAvailable == expression.true()
            ).all()
            return {row.participantId for row in result}

    def prepare_for_ehr_status_update(self):
        with self.session() as session:
            query = (
                sqlalchemy.update(ParticipantSummary).values({
                    ParticipantSummary.isEhrDataAvailable: False,
                    ParticipantSummary.isParticipantMediatedEhrDataAvailable: False
                })
            )
            return session.execute(query)

    @classmethod
    def _bulk_update_ehr_fields(cls, session, record_list: List[ParticipantEhrFile], values_to_update) -> int:
        if not record_list:
            return 0

        query = (
            sqlalchemy.update(ParticipantSummary)
            .where(ParticipantSummary.participantId == sqlalchemy.bindparam('pid'))
            .values(values_to_update)
        )
        query_result = session.execute(query, [
            {
                'pid': record.participant_id,
                'receipt_time': record.receipt_time
            }
            for record in record_list
        ])
        return query_result.rowcount

    @classmethod
    def bulk_update_hpo_ehr_status_with_session(cls, session, record_list: List[ParticipantEhrFile]):
        return cls._bulk_update_ehr_fields(
            session=session,
            record_list=record_list,
            values_to_update={
                ParticipantSummary.ehrStatus.name: EhrStatus.PRESENT,
                ParticipantSummary.isEhrDataAvailable: True,
                ParticipantSummary.ehrUpdateTime: sqlalchemy.bindparam("receipt_time"),
                ParticipantSummary.ehrReceiptTime: sqlalchemy.case(
                    [
                        (ParticipantSummary.ehrReceiptTime.is_(None), sqlalchemy.bindparam("receipt_time"))
                    ],
                    else_=ParticipantSummary.ehrReceiptTime,
                )
            }
        )

    @classmethod
    def bulk_update_mediated_ehr_status_with_session(cls, session, record_list: List[ParticipantEhrFile]):
        return cls._bulk_update_ehr_fields(
            session=session,
            record_list=record_list,
            values_to_update={
                ParticipantSummary.wasParticipantMediatedEhrAvailable: True,
                ParticipantSummary.isParticipantMediatedEhrDataAvailable: True,
                ParticipantSummary.latestParticipantMediatedEhrReceiptTime: sqlalchemy.bindparam("receipt_time"),
                ParticipantSummary.firstParticipantMediatedEhrReceiptTime: sqlalchemy.case(
                    [
                        (
                            ParticipantSummary.firstParticipantMediatedEhrReceiptTime.is_(None),
                            sqlalchemy.bindparam("receipt_time")
                        )
                    ],
                    else_=ParticipantSummary.firstParticipantMediatedEhrReceiptTime,
                )
            }
        )

    def bulk_update_retention_eligible_flags(self, upload_date):
        with self.session() as session:
            query = (
                sqlalchemy.update(
                    ParticipantSummary
                ).where(and_(
                    ParticipantSummary.participantId == RetentionEligibleMetrics.participantId,
                    RetentionEligibleMetrics.fileUploadDate == sqlalchemy.bindparam("file_upload_date")
                ))
            ).values(
                {
                    ParticipantSummary.retentionEligibleStatus: RetentionEligibleMetrics.retentionEligibleStatus,
                    ParticipantSummary.retentionEligibleTime: RetentionEligibleMetrics.retentionEligibleTime,
                    ParticipantSummary.retentionType: RetentionEligibleMetrics.retentionType,
                    ParticipantSummary.lastActiveRetentionActivityTime:
                        RetentionEligibleMetrics.lastActiveRetentionActivityTime
                }
            )
            session.execute(query, {'file_upload_date': upload_date})

    @classmethod
    def update_profile_data(cls, participant_id: int, **kwargs):
        instance = ParticipantSummaryDao()

        with instance.session() as session:
            summary: ParticipantSummary = instance.get_with_session(
                session=session,
                obj_id=participant_id,
                for_update=True
            )

            field_map = {
                'first_name': 'firstName',
                'middle_name': 'middleName',
                'last_name': 'lastName',
                'phone_number': 'phoneNumber',
                'login_phone_number': 'loginPhoneNumber',
                'email': 'email',
                'birthdate': 'dateOfBirth',
                'address_line1': 'streetAddress',
                'address_line2': 'streetAddress2',
                'address_city': 'city',
                'address_zip_code': 'zipCode',
                'preferred_language': 'primaryLanguage'
            }
            for param_name, model_name in field_map.items():
                if param_name in kwargs:
                    setattr(summary, model_name, kwargs[param_name])

            if 'address_state' in kwargs:
                state_str = kwargs['address_state']
                state_code: Code = session.query(Code).filter(
                    Code.value == f'PIIState_{state_str}'
                ).one_or_none()
                summary.stateId = state_code.codeId if state_code else None


def _initialize_field_type_sets():
    """Using reflection, populate _DATE_FIELDS, _ENUM_FIELDS, and _CODE_FIELDS, which are
  used when formatting JSON from participant summaries.

  We call this lazily to avoid having issues with the code getting executed while SQLAlchemy
  is still initializing itself. Locking ensures we only run throught the code once.
  """
    with _fields_lock:
        # Return if this is already initialized.
        if _DATE_FIELDS:
            return
        for prop_name in dir(ParticipantSummary):
            if prop_name.startswith("_"):
                continue
            if prop_name == "genderIdentityId":  # deprecated
                continue
            prop = getattr(ParticipantSummary, prop_name)
            if callable(prop):
                continue
            property_type = get_property_type(prop)
            if property_type:
                if property_type == PropertyType.DATE or property_type == PropertyType.DATETIME:
                    _DATE_FIELDS.add(prop_name)
                elif property_type == PropertyType.ENUM:
                    _ENUM_FIELDS.add(prop_name)
                elif property_type == PropertyType.INTEGER:
                    fks = prop.property.columns[0].foreign_keys
                    if fks:
                        for fk in fks:
                            if fk._get_colspec() == "code.code_id":
                                _CODE_FIELDS.add(prop_name)
                                break


class PatientStatusFieldFilter(FieldFilter):
    """
  FieldFilter class for patientStatus relationship field
  """

    def __init__(self, field_name, operator, value, organization, status):
        super(PatientStatusFieldFilter, self).__init__(field_name, operator, value)
        self.organization = organization
        self.status = status

    def add_to_sqlalchemy_query(self, query, field):
        if self.operator == Operator.EQUALS:
            if self.status == PatientStatusFlag.UNSET:
                criterion = sqlalchemy.not_(
                    field.any(PatientStatus.organizationId == self.organization.organizationId)
                )
            else:
                criterion = field.any(
                    sqlalchemy.and_(
                        PatientStatus.organizationId == self.organization.organizationId,
                        PatientStatus.patientStatus == self.status,
                    )
                )
            return query.filter(criterion)
        else:
            raise ValueError(f"Invalid operator: {self.operator}.")


class ParticipantGenderAnswersDao(UpdatableDao):
    def __init__(self):
        super(ParticipantGenderAnswersDao, self).__init__(ParticipantGenderAnswers, order_by_ending=["id"])

    def update_gender_answers_with_session(self, session, participant_id, gender_code_ids):
        # remove old answers
        self.delete_answers_with_session(session, participant_id)
        # insert new answers
        now = clock.CLOCK.now()
        records = [
            ParticipantGenderAnswers(**dict(participantId=participant_id, created=now, modified=now, codeId=code_id))
            for code_id in gender_code_ids
        ]

        for record in records:
            session.merge(record)

    def delete_answers_with_session(self, session, participant_id):
        session.query(ParticipantGenderAnswers).filter(
            ParticipantGenderAnswers.participantId == participant_id
        ).delete()


class ParticipantRaceAnswersDao(UpdatableDao):
    def __init__(self):
        super(ParticipantRaceAnswersDao, self).__init__(ParticipantRaceAnswers, order_by_ending=["id"])

    def update_race_answers_with_session(self, session, participant_id, race_code_ids):
        # remove old answers
        self.delete_answers_with_session(session, participant_id)
        # insert new answers
        now = clock.CLOCK.now()
        records = [
            ParticipantRaceAnswers(**dict(participantId=participant_id, created=now, modified=now, codeId=code_id))
            for code_id in race_code_ids
        ]

        for record in records:
            session.merge(record)

    def delete_answers_with_session(self, session, participant_id):
        session.query(ParticipantRaceAnswers).filter(ParticipantRaceAnswers.participantId == participant_id).delete()
