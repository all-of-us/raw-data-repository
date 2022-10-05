import datetime
import faker
import re
import threading

import sqlalchemy
import sqlalchemy.orm

from sqlalchemy import or_, and_
from sqlalchemy.sql import expression
from typing import Collection

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
from rdr_service.code_constants import BIOBANK_TESTS, ORIGINATING_SOURCES, PMI_SKIP_CODE, PPI_SYSTEM, UNSET
from rdr_service.dao.base_dao import UpdatableDao
from rdr_service.dao.code_dao import CodeDao
from rdr_service.dao.database_utils import get_sql_and_params_for_array, replace_null_safe_equals
from rdr_service.dao.hpo_dao import HPODao
from rdr_service.dao.organization_dao import OrganizationDao
from rdr_service.dao.participant_dao import ParticipantDao
from rdr_service.dao.participant_incentives_dao import ParticipantIncentivesDao
from rdr_service.dao.patient_status_dao import PatientStatusDao
from rdr_service.dao.site_dao import SiteDao
from rdr_service.logic.enrollment_info import EnrollmentCalculation, EnrollmentDependencies
from rdr_service.model.config_utils import from_client_biobank_id, to_client_biobank_id
from rdr_service.model.consent_file import ConsentType
from rdr_service.model.enrollment_status_history import EnrollmentStatusHistory
from rdr_service.model.retention_eligible_metrics import RetentionEligibleMetrics
from rdr_service.model.participant_summary import (
    ParticipantGenderAnswers,
    ParticipantRaceAnswers,
    ParticipantSummary,
    WITHDRAWN_PARTICIPANT_FIELDS,
    WITHDRAWN_PARTICIPANT_VISIBILITY_TIME
)
from rdr_service.model.patient_status import PatientStatus
from rdr_service.model.participant import Participant
from rdr_service.model.utils import get_property_type, to_client_participant_id
from rdr_service.participant_enums import (
    BiobankOrderStatus,
    EhrStatus,
    EnrollmentStatus,
    EnrollmentStatusV30,
    EnrollmentStatusV31,
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

    sql += " WHERE " + where_sql

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
            return self.get_with_session(session, obj_id)

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
            ).one_or_none()

    def get_by_hpo(self, hpo):
        """ Returns participants for HPO except test and ghost participants"""
        with self.session() as session:
            return session.query(
                ParticipantSummary
            ).join(
                Participant, Participant.participantId == ParticipantSummary.participantId
            ).filter(
                ParticipantSummary.hpoId == hpo.hpoId,
                Participant.isTestParticipant != 1,
                # Just filtering on isGhostId != 1 will return no results
                or_(Participant.isGhostId != 1, Participant.isGhostId == None)
            ).all()

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

    def _has_withdrawn_filter(self, query):
        for field_filter in query.field_filters:
            if field_filter.field_name == "withdrawalStatus" and field_filter.value == WithdrawalStatus.NO_USE:
                return True
            if field_filter.field_name == "withdrawalTime" and field_filter.value is not None:
                return True
        return False

    def _get_non_withdrawn_filter_field(self, query):
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
        return super(ParticipantSummaryDao, self)._add_order_by(query, order_by, field_names, fields)

    @staticmethod
    def _add_order_by_state(order_by, query):
        query = query.outerjoin(Code, ParticipantSummary.stateId == Code.codeId)
        if order_by.ascending:
            return query.order_by(Code.display)
        else:
            return query.order_by(Code.display.desc())

    def _make_query(self, session, query_def):
        query, order_by_field_names = super(ParticipantSummaryDao, self)._make_query(session, query_def)
        # Note: leaving for future use if we go back to using a relationship to PatientStatus table.
        # query.options(selectinload(ParticipantSummary.patientStatus))
        # sql = self.query_to_text(query)
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

    def update_from_biobank_stored_samples(self, participant_id=None, biobank_ids=None):
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
      num_baseline_samples_arrived != {baseline_tests_sql} OR
      samples_to_isolate_dna != {dna_tests_sql}
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

        with self.session() as session:
            session.execute(sample_sql, sample_params)
            session.execute(counts_sql, counts_params)
            session.commit()

            if biobank_ids:
                summary_list = session.query(ParticipantSummary).filter(
                    ParticipantSummary.biobankId.in_(biobank_ids)
                ).all()
                for summary in summary_list:
                    self.update_enrollment_status(
                        summary=summary,
                        session=session
                    )
                    session.commit()

    def _get_num_baseline_ppi_modules(self):
        return len(config.getSettingList(config.BASELINE_PPI_QUESTIONNAIRE_FIELDS))

    @classmethod
    def _set_if_empty(cls, summary, field_name, new_value):
        if getattr(summary, field_name) is None and new_value is not None:
            setattr(summary, field_name, new_value)

    def update_enrollment_status(self, summary: ParticipantSummary, session):
        """
        Updates the enrollment status field on the provided participant summary to the correct value.
        """

        earliest_physical_measurements_time = min_or_none([
            summary.clinicPhysicalMeasurementsFinalizedTime,
            summary.selfReportedPhysicalMeasurementsAuthored
        ])
        earliest_biobank_received_dna_time = min_or_none([
            summary.sampleStatus1ED10Time,
            summary.sampleStatus2ED10Time,
            summary.sampleStatus1ED04Time,
            summary.sampleStatus1SALTime,
            summary.sampleStatus1SAL2Time
        ])

        ehr_consent_ranges = QuestionnaireResponseRepository.get_interest_in_sharing_ehr_ranges(
            participant_id=summary.participantId,
            session=session
        )

        dna_update_time_list = [summary.questionnaireOnDnaProgramAuthored]
        if summary.consentForStudyEnrollmentAuthored != summary.consentForStudyEnrollmentFirstYesAuthored:
            dna_update_time_list.append(summary.consentForStudyEnrollmentAuthored)
        dna_update_time = min_or_none(dna_update_time_list)

        enrollment_info = EnrollmentCalculation.get_enrollment_info(
            EnrollmentDependencies(
                consent_cohort=summary.consentCohort,
                primary_consent_authored_time=summary.consentForStudyEnrollmentAuthored,
                gror_authored_time=summary.consentForGenomicsRORAuthored,
                basics_authored_time=summary.questionnaireOnTheBasicsAuthored,
                overall_health_authored_time=summary.questionnaireOnOverallHealthAuthored,
                lifestyle_authored_time=summary.questionnaireOnLifestyleAuthored,
                earliest_ehr_file_received_time=summary.firstEhrReceiptTime,
                earliest_physical_measurements_time=earliest_physical_measurements_time,
                earliest_biobank_received_dna_time=earliest_biobank_received_dna_time,
                ehr_consent_date_range_list=ehr_consent_ranges,
                dna_update_time=dna_update_time
            )
        )

        # Update enrollment status if it is upgrading
        legacy_dates = enrollment_info.version_legacy_dates
        version_3_0_dates = enrollment_info.version_3_0_dates
        version_3_1_dates = enrollment_info.version_3_1_dates
        status_rank_map = {  # Re-orders the status values so we can quickly see if the current status is higher
            EnrollmentStatus.INTERESTED: 0,
            EnrollmentStatus.MEMBER: 1,
            EnrollmentStatus.CORE_MINUS_PM: 2,
            EnrollmentStatus.FULL_PARTICIPANT: 3
        }
        if status_rank_map[summary.enrollmentStatus] < status_rank_map[enrollment_info.version_legacy_status]:
            summary.enrollmentStatus = enrollment_info.version_legacy_status
            summary.lastModified = clock.CLOCK.now()

            # Record the new status in the history table
            session.add(
                EnrollmentStatusHistory(
                    participant_id=summary.participantId,
                    version='legacy',
                    status=str(summary.enrollmentStatus),
                    timestamp=legacy_dates[summary.enrollmentStatus]
                )
            )
        if summary.enrollmentStatusV3_0 < enrollment_info.version_3_0_status:
            summary.enrollmentStatusV3_0 = enrollment_info.version_3_0_status
            summary.lastModified = clock.CLOCK.now()
            session.add(
                EnrollmentStatusHistory(
                    participant_id=summary.participantId,
                    version='3.0',
                    status=str(summary.enrollmentStatusV3_0),
                    timestamp=version_3_0_dates[summary.enrollmentStatusV3_0]
                )
            )
        if summary.enrollmentStatusV3_1 < enrollment_info.version_3_1_status:
            summary.enrollmentStatusV3_1 = enrollment_info.version_3_1_status
            summary.lastModified = clock.CLOCK.now()
            session.add(
                EnrollmentStatusHistory(
                    participant_id=summary.participantId,
                    version='3.1',
                    status=str(summary.enrollmentStatusV3_1),
                    timestamp=version_3_1_dates[summary.enrollmentStatusV3_1]
                )
            )

        # Set enrollment status date fields
        if EnrollmentStatus.MEMBER in legacy_dates:
            self._set_if_empty(summary, 'enrollmentStatusMemberTime', legacy_dates[EnrollmentStatus.MEMBER])
        if EnrollmentStatus.CORE_MINUS_PM in legacy_dates:
            self._set_if_empty(summary, 'enrollmentStatusCoreMinusPMTime', legacy_dates[EnrollmentStatus.CORE_MINUS_PM])
        if EnrollmentStatus.FULL_PARTICIPANT in legacy_dates:
            self._set_if_empty(
                summary,
                'enrollmentStatusCoreStoredSampleTime',
                legacy_dates[EnrollmentStatus.FULL_PARTICIPANT]
            )

        if EnrollmentStatusV30.PARTICIPANT in version_3_0_dates:
            self._set_if_empty(
                summary,
                'enrollmentStatusParticipantV3_0Time',
                version_3_0_dates[EnrollmentStatusV30.PARTICIPANT]
            )
        if EnrollmentStatusV30.PARTICIPANT_PLUS_EHR in version_3_0_dates:
            self._set_if_empty(
                summary,
                'enrollmentStatusParticipantPlusEhrV3_0Time',
                version_3_0_dates[EnrollmentStatusV30.PARTICIPANT_PLUS_EHR]
            )
        if EnrollmentStatusV30.PARTICIPANT_PMB_ELIGIBLE in version_3_0_dates:
            self._set_if_empty(
                summary,
                'enrollmentStatusPmbEligibleV3_0Time',
                version_3_0_dates[EnrollmentStatusV30.PARTICIPANT_PMB_ELIGIBLE]
            )
        if EnrollmentStatusV30.CORE_MINUS_PM in version_3_0_dates:
            self._set_if_empty(
                summary,
                'enrollmentStatusCoreMinusPmV3_0Time',
                version_3_0_dates[EnrollmentStatusV30.CORE_MINUS_PM]
            )
        if EnrollmentStatusV30.CORE_PARTICIPANT in version_3_0_dates:
            self._set_if_empty(
                summary,
                'enrollmentStatusCoreV3_0Time',
                version_3_0_dates[EnrollmentStatusV30.CORE_PARTICIPANT]
            )

        if EnrollmentStatusV31.PARTICIPANT in version_3_1_dates:
            self._set_if_empty(
                summary,
                'enrollmentStatusParticipantV3_1Time',
                version_3_1_dates[EnrollmentStatusV31.PARTICIPANT]
            )
        if EnrollmentStatusV31.PARTICIPANT_PLUS_EHR in version_3_1_dates:
            self._set_if_empty(
                summary,
                'enrollmentStatusParticipantPlusEhrV3_1Time',
                version_3_1_dates[EnrollmentStatusV31.PARTICIPANT_PLUS_EHR]
            )
        if EnrollmentStatusV31.PARTICIPANT_PLUS_BASICS in version_3_1_dates:
            self._set_if_empty(
                summary,
                'enrollmentStatusParticipantPlusBasicsV3_1Time',
                version_3_1_dates[EnrollmentStatusV31.PARTICIPANT_PLUS_BASICS]
            )
        if EnrollmentStatusV31.CORE_MINUS_PM in version_3_1_dates:
            self._set_if_empty(
                summary,
                'enrollmentStatusCoreMinusPmV3_1Time',
                version_3_1_dates[EnrollmentStatusV31.CORE_MINUS_PM]
            )
        if EnrollmentStatusV31.CORE_PARTICIPANT in version_3_1_dates:
            self._set_if_empty(
                summary,
                'enrollmentStatusCoreV3_1Time',
                version_3_1_dates[EnrollmentStatusV31.CORE_PARTICIPANT]
            )
        if EnrollmentStatusV31.BASELINE_PARTICIPANT in version_3_1_dates:
            self._set_if_empty(
                summary,
                'enrollmentStatusParticipantPlusBaselineV3_1Time',
                version_3_1_dates[EnrollmentStatusV31.BASELINE_PARTICIPANT]
            )

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
        consents_map = {
            ConsentType.PRIMARY: 'consentForStudyEnrollment',
            ConsentType.CABOR: 'consentForCABoR',
            ConsentType.EHR: 'consentForElectronicHealthRecords',
            ConsentType.GROR: 'consentForGenomicsROR',
            ConsentType.PRIMARY_RECONSENT: 'reconsentForStudyEnrollment',
            ConsentType.EHR_RECONSENT: 'reconsentForElectronicHealthRecords'
        }
        participant_id = result['participantId']
        records = list(filter(lambda obj: obj.participant_id == participant_id, self.hpro_consents))

        for consent_type, consent_name in consents_map.items():
            value_path_key = f'{consent_name}FilePath'
            has_consent_path = [obj for obj in records if consent_type == obj.consent_type]

            if has_consent_path:
                result[value_path_key] = has_consent_path[0].file_path

        # DA-2895: Copy reconsentForStudyEnrollmentFilePath value to incorrect field name.
        # This can be removed after HealthPro updates.
        if 'reconsentForStudyEnrollmentFilePath' in result:
            result['reconsentForStudyEnrollementFilePath'] = result['reconsentForStudyEnrollmentFilePath']

        return result

    def get_participant_incentives(self, result):
        participant_id = result['participantId']

        records = list(filter(lambda obj: obj.participantId == participant_id, self.participant_incentives))

        records = [self.incentive_dao.convert_json_obj(obj) for obj in records]
        return records

    def to_client_json(self, model: ParticipantSummary, strip_none_values=True):
        result = model.asdict()
        clinic_pm_time = result.get("clinicPhysicalMeasurementsFinalizedTime")
        self_reported_pm_time = result.get("selfReportedPhysicalMeasurementsAuthored")
        if self.hpro_consents:
            result = self.get_hpro_consent_paths(result)

        if self.participant_incentives:
            result['participantIncentives'] = self.get_participant_incentives(result)

        is_the_basics_complete = model.questionnaireOnTheBasics == QuestionnaireStatus.SUBMITTED

        # Participants that withdrew more than 48 hours ago should have fields other than
        # WITHDRAWN_PARTICIPANT_FIELDS cleared.
        should_clear_fields_for_withdrawal = model.withdrawalStatus == WithdrawalStatus.NO_USE and (
            model.withdrawalTime is None
            or model.withdrawalTime < clock.CLOCK.now() - WITHDRAWN_PARTICIPANT_VISIBILITY_TIME
        )
        if should_clear_fields_for_withdrawal:
            result = {k: result.get(k) for k in WITHDRAWN_PARTICIPANT_FIELDS}

        result["participantId"] = to_client_participant_id(model.participantId)
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

        # Map demographic Enums if TheBasics was submitted and Skip wasn't in use
        if is_the_basics_complete and not should_clear_fields_for_withdrawal:
            if model.genderIdentity is None or model.genderIdentity == GenderIdentity.UNSET:
                result['genderIdentity'] = GenderIdentity.PMI_Skip

            if model.race is None or model.race == Race.UNSET:
                result['race'] = Race.PMI_Skip

        result["patientStatus"] = model.patientStatus

        format_json_hpo(result, self.hpo_dao, "hpoId")
        result["awardee"] = result["hpoId"]
        _initialize_field_type_sets()

        for new_field_name, existing_field_name in self.get_aliased_field_map().items():
            result[new_field_name] = getattr(model, existing_field_name)

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
        if model.withdrawalStatus == WithdrawalStatus.NO_USE\
                or model.suspensionStatus == SuspensionStatus.NO_CONTACT\
                or model.deceasedStatus == DeceasedStatus.APPROVED:
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

        # Check to see if we should hide 3.0 and 3.1 fields
        if not config.getSettingJson(config.ENABLE_ENROLLMENT_STATUS_3, default=False):
            del result['enrollmentStatusV3_0']
            del result['enrollmentStatusV3_1']
            for field_name in [
                'enrollmentStatusParticipantV3_0Time'
                'enrollmentStatusParticipantPlusEhrV3_0Time'
                'enrollmentStatusPmbEligibleV3_0Time'
                'enrollmentStatusCoreMinusPmV3_0Time'
                'enrollmentStatusCoreV3_0Time'
                'enrollmentStatusParticipantV3_1Time'
                'enrollmentStatusParticipantPlusEhrV3_1Time'
                'enrollmentStatusParticipantPlusBasicsV3_1Time'
                'enrollmentStatusCoreMinusPmV3_1Time'
                'enrollmentStatusCoreV3_1Time'
                'enrollmentStatusParticipantPlusBaselineV3_1Time'
            ]:
                if field_name in result:
                    del result[field_name]

        # Check to see if we should hide digital health sharing fields
        if not config.getSettingJson(config.ENABLE_HEALTH_SHARING_STATUS_3, default=False):
            del result['healthDataStreamSharingStatusV3_1']
            if 'healthDataStreamSharingStatusV3_1Time' in result:
                del result['healthDataStreamSharingStatusV3_1Time']

        # Strip None values.
        if strip_none_values is True:
            result = {k: v for k, v in list(result.items()) if v is not None}

        return result

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
                pagination_data = ['field_list']
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

    def get_participant_ids_with_ehr_data_available(self):
        with self.session() as session:
            result = session.query(ParticipantSummary.participantId).filter(
                ParticipantSummary.isEhrDataAvailable == expression.true()
            ).all()
            return {row.participantId for row in result}

    def prepare_for_ehr_status_update(self):
        with self.session() as session:
            query = (
                sqlalchemy.update(ParticipantSummary).values({
                    ParticipantSummary.isEhrDataAvailable: False
                })
            )
            return session.execute(query)

    @staticmethod
    def bulk_update_ehr_status_with_session(session, parameter_sets):
        query = (
            sqlalchemy.update(ParticipantSummary)
            .where(ParticipantSummary.participantId == sqlalchemy.bindparam("pid"))
            .values(
                {
                    ParticipantSummary.ehrStatus.name: EhrStatus.PRESENT,
                    ParticipantSummary.isEhrDataAvailable: True,
                    ParticipantSummary.ehrUpdateTime: sqlalchemy.bindparam("receipt_time"),
                    ParticipantSummary.ehrReceiptTime: sqlalchemy.case(
                        [(ParticipantSummary.ehrReceiptTime.is_(None), sqlalchemy.bindparam("receipt_time"))],
                        else_=ParticipantSummary.ehrReceiptTime,
                    ),
                }
            )
        )
        return session.execute(query, parameter_sets)

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
