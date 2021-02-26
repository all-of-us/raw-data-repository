import datetime
import re
import threading

import sqlalchemy
import sqlalchemy.orm
from sqlalchemy import or_, and_
from sqlalchemy.sql import expression

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
)
from rdr_service.app_util import is_care_evo_and_not_prod
from rdr_service.code_constants import BIOBANK_TESTS, PPI_SYSTEM, UNSET, ORIGINATING_SOURCES
from rdr_service.dao.base_dao import UpdatableDao
from rdr_service.dao.code_dao import CodeDao
from rdr_service.dao.database_utils import get_sql_and_params_for_array, replace_null_safe_equals
from rdr_service.dao.hpo_dao import HPODao
from rdr_service.dao.organization_dao import OrganizationDao
from rdr_service.dao.patient_status_dao import PatientStatusDao
from rdr_service.dao.site_dao import SiteDao
from rdr_service.model.config_utils import from_client_biobank_id, to_client_biobank_id
from rdr_service.model.participant_summary import (
    ParticipantGenderAnswers,
    ParticipantRaceAnswers,
    ParticipantSummary,
    WITHDRAWN_PARTICIPANT_FIELDS,
    WITHDRAWN_PARTICIPANT_VISIBILITY_TIME,
    RETENTION_WINDOW
)
from rdr_service.model.patient_status import PatientStatus
from rdr_service.model.utils import get_property_type, to_client_participant_id
from rdr_service.participant_enums import (
    BiobankOrderStatus,
    EhrStatus,
    EnrollmentStatus,
    DeceasedStatus,
    ConsentExpireStatus,
    ParticipantCohort,
    PatientStatusFlag,
    PhysicalMeasurementsStatus,
    QuestionnaireStatus,
    SampleCollectionMethod,
    SampleStatus,
    SuspensionStatus,
    WithdrawalStatus,
    get_bucketed_age,
    RetentionStatus,
    RetentionType
)
from rdr_service.query import FieldFilter, FieldJsonContainsFilter, Operator, OrderBy, PropertyType

# By default / secondarily order by last name, first name, DOB, and participant ID
_ORDER_BY_ENDING = ("lastName", "firstName", "dateOfBirth", "participantId")
# The default ordering of results for queries for withdrawn participants.
_WITHDRAWN_ORDER_BY_ENDING = ("withdrawalTime", "participantId")
_CODE_FILTER_FIELDS = ("organization", "site", "awardee")
_SITE_FIELDS = (
    "physicalMeasurementsCreatedSite",
    "physicalMeasurementsFinalizedSite",
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
                   AND num_completed_baseline_ppi_modules = :num_baseline_ppi_modules
                   AND physical_measurements_status = :completed
                   AND samples_to_isolate_dna = :received) OR
                  (consent_for_study_enrollment = :submitted
                   AND consent_for_electronic_health_records = :unset
                   AND consent_for_dv_electronic_health_records_sharing = :submitted
                   AND num_completed_baseline_ppi_modules = :num_baseline_ppi_modules
                   AND physical_measurements_status = :completed
                   AND samples_to_isolate_dna = :received)
             THEN :full_participant
             WHEN (consent_for_study_enrollment = :submitted
                   AND consent_for_electronic_health_records = :submitted
                   AND num_completed_baseline_ppi_modules = :num_baseline_ppi_modules
                   AND physical_measurements_status != :completed
                   AND samples_to_isolate_dna = :received) OR
                  (consent_for_study_enrollment = :submitted
                   AND consent_for_electronic_health_records = :unset
                   AND consent_for_dv_electronic_health_records_sharing = :submitted
                   AND num_completed_baseline_ppi_modules = :num_baseline_ppi_modules
                   AND physical_measurements_status != :completed
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

_ENROLLMENT_STATUS_SQL = """
    UPDATE
      participant_summary
    SET
      enrollment_status = {enrollment_status_case_sql},
      last_modified = :now
    WHERE
      (
        (enrollment_status != :full_participant and enrollment_status != :core_minus_pm)
        OR
        (enrollment_status = :core_minus_pm AND :full_participant = {enrollment_status_case_sql})
      )
      AND enrollment_status != {enrollment_status_case_sql}
   """.format(
    enrollment_status_case_sql=_ENROLLMENT_STATUS_CASE_SQL
)

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


def _get_sample_status_time_sql_and_params():
    """Gets SQL that to update enrollmentStatusCoreStoredSampleTime field
  on the participant summary.
  """

    dns_test_list = config.getSettingList(config.DNA_SAMPLE_TEST_CODES)

    status_time_sql = "%s" % ",".join(
        ["""COALESCE(sample_status_%s_time, '3000-01-01')""" % item for item in dns_test_list]
    )
    baseline_ppi_module_fields = config.getSettingList(config.BASELINE_PPI_QUESTIONNAIRE_FIELDS, [])

    baseline_ppi_module_sql = "%s" % ",".join(
        ["""%s_time""" % re.sub("(?<!^)(?=[A-Z])", "_", item).lower() for item in baseline_ppi_module_fields]
    )

    sub_sql = """
    SELECT
      participant_id,
      GREATEST(
        CASE WHEN enrollment_status_member_time IS NOT NULL THEN enrollment_status_member_time
             ELSE consent_for_electronic_health_records_time
        END,
        physical_measurements_finalized_time,
        {baseline_ppi_module_sql},
        CASE WHEN
            LEAST(
                {status_time_sql}
                ) = '3000-01-01' THEN NULL
            ELSE LEAST(
                {status_time_sql}
                )
        END
      ) AS new_core_stored_sample_time
    FROM
      participant_summary
  """.format(
        status_time_sql=status_time_sql, baseline_ppi_module_sql=baseline_ppi_module_sql
    )

    sql = """
    UPDATE
      participant_summary AS a
      INNER JOIN ({sub_sql}) AS b ON a.participant_id = b.participant_id
    SET
      a.enrollment_status_core_stored_sample_time = b.new_core_stored_sample_time
    WHERE a.enrollment_status = 3
    AND a.enrollment_status_core_stored_sample_time IS NULL
    """.format(
        sub_sql=sub_sql
    )

    return sql


class ParticipantSummaryDao(UpdatableDao):
    def __init__(self):
        super(ParticipantSummaryDao, self).__init__(ParticipantSummary, order_by_ending=_ORDER_BY_ENDING)
        self.hpo_dao = HPODao()
        self.code_dao = CodeDao()
        self.site_dao = SiteDao()
        self.organization_dao = OrganizationDao()
        self.patient_status_dao = PatientStatusDao()

    def get_id(self, obj):
        return obj.participantId

    # Note: leaving for future use if we go back to using a relationship to PatientStatus table.
    # def get_eager_child_loading_query_options(self):
    #   return [
    #     sqlalchemy.orm.subqueryload(self.model_type.patientStatus)
    #   ]

    def get_with_children(self, obj_id):
        with self.session() as session:
            # Note: leaving for future use if we go back to using a relationship to PatientStatus table.
            # return self.get_with_session(session, obj_id,
            #                              options=self.get_eager_child_loading_query_options())
            return self.get_with_session(session, obj_id)

    def _validate_update(self, session, obj, existing_obj):  # pylint: disable=unused-argument
        """Participant summaries don't have a version value; drop it from validation logic."""
        if not existing_obj:
            raise NotFound(f"{self.model_type.__name__} with id {id} does not exist")

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
        return super(ParticipantSummaryDao, self)._add_order_by(query, order_by, field_names, fields)

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
        if field_name == 'retentionType':
            return self._make_retention_type_filter('retentionEligibleStatus', value)

        return super(ParticipantSummaryDao, self).make_query_filter(field_name, value)

    def _make_retention_type_filter(self, field_name, value):
        return RetentionTypeFieldFilter(field_name, Operator.EQUALS, value)

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

    def update_from_biobank_stored_samples(self, participant_id=None):
        """Rewrites sample-related summary data. Call this after updating BiobankStoredSamples.
    If participant_id is provided, only that participant will have their summary updated."""
        now = clock.CLOCK.now()
        sample_sql, sample_params = _get_sample_sql_and_params(now)

        baseline_tests_sql, baseline_tests_params = _get_baseline_sql_and_params()
        dna_tests_sql, dna_tests_params = _get_dna_isolates_sql_and_params()

        sample_status_time_sql = _get_sample_status_time_sql_and_params()
        sample_status_time_params = {}

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

        enrollment_status_sql = _ENROLLMENT_STATUS_SQL
        enrollment_status_params = {
            "submitted": int(QuestionnaireStatus.SUBMITTED),
            "unset": int(QuestionnaireStatus.UNSET),
            "num_baseline_ppi_modules": self._get_num_baseline_ppi_modules(),
            "completed": int(PhysicalMeasurementsStatus.COMPLETED),
            "received": int(SampleStatus.RECEIVED),
            "full_participant": int(EnrollmentStatus.FULL_PARTICIPANT),
            "core_minus_pm": int(EnrollmentStatus.CORE_MINUS_PM),
            "member": int(EnrollmentStatus.MEMBER),
            "interested": int(EnrollmentStatus.INTERESTED),
            "now": now,
        }

        # If participant_id is provided, add the participant ID filter to all update statements.
        if participant_id:
            sample_sql += " AND participant_id = :participant_id"
            sample_params["participant_id"] = participant_id
            counts_sql += " AND participant_id = :participant_id"
            counts_params["participant_id"] = participant_id
            enrollment_status_sql += " AND participant_id = :participant_id"
            enrollment_status_params["participant_id"] = participant_id
            sample_status_time_sql += " AND a.participant_id = :participant_id"
            sample_status_time_params["participant_id"] = participant_id

        sample_sql = replace_null_safe_equals(sample_sql)
        counts_sql = replace_null_safe_equals(counts_sql)

        with self.session() as session:
            session.execute(sample_sql, sample_params)
            session.execute(counts_sql, counts_params)
            session.execute(enrollment_status_sql, enrollment_status_params)
            # TODO: Change this to the optimized sql in _update_dv_stored_samples()
            session.execute(sample_status_time_sql, sample_status_time_params)

    def _get_num_baseline_ppi_modules(self):
        return len(config.getSettingList(config.BASELINE_PPI_QUESTIONNAIRE_FIELDS))

    def update_enrollment_status(self, summary):
        """Updates the enrollment status field on the provided participant summary to
    the correct value based on the other fields on it. Called after
    a questionnaire response or physical measurements are submitted."""
        consent = (
            summary.consentForStudyEnrollment == QuestionnaireStatus.SUBMITTED
            and summary.consentForElectronicHealthRecords == QuestionnaireStatus.SUBMITTED
        ) or (
            summary.consentForStudyEnrollment == QuestionnaireStatus.SUBMITTED
            and summary.consentForElectronicHealthRecords is None
            and summary.consentForDvElectronicHealthRecordsSharing == QuestionnaireStatus.SUBMITTED
        )

        enrollment_status = self.calculate_enrollment_status(
            consent,
            summary.numCompletedBaselinePPIModules,
            summary.physicalMeasurementsStatus,
            summary.samplesToIsolateDNA,
            summary.consentCohort,
            summary.consentForGenomicsROR,
            summary.ehrConsentExpireStatus
        )
        summary.enrollmentStatusCoreOrderedSampleTime = self.calculate_core_ordered_sample_time(consent, summary)
        summary.enrollmentStatusCoreStoredSampleTime = self.calculate_core_stored_sample_time(consent, summary)
        summary.enrollmentStatusCoreMinusPMTime = self.calculate_core_minus_pm_time(consent, summary)

        # [DA-1623] Participants that have 'Core' status should never lose it
        # CORE_MINUS_PM status can not downgrade, but can upgrade to FULL_PARTICIPANT
        if summary.enrollmentStatus not in (EnrollmentStatus.FULL_PARTICIPANT, EnrollmentStatus.CORE_MINUS_PM) \
            or (summary.enrollmentStatus == EnrollmentStatus.CORE_MINUS_PM
                and enrollment_status == EnrollmentStatus.FULL_PARTICIPANT):
            # Update last modified date if status changes
            if summary.enrollmentStatus != enrollment_status:
                summary.lastModified = clock.CLOCK.now()

            summary.enrollmentStatus = enrollment_status
            summary.enrollmentStatusMemberTime = self.calculate_member_time(consent, summary)

    def calculate_enrollment_status(
        self, consent, num_completed_baseline_ppi_modules, physical_measurements_status, samples_to_isolate_dna,
        consent_cohort, gror_consent, consent_expire_status=ConsentExpireStatus.NOT_EXPIRED
    ):
        if consent:
            if (
                num_completed_baseline_ppi_modules == self._get_num_baseline_ppi_modules()
                and physical_measurements_status == PhysicalMeasurementsStatus.COMPLETED
                and samples_to_isolate_dna == SampleStatus.RECEIVED
                and (gror_consent == QuestionnaireStatus.SUBMITTED or consent_cohort != ParticipantCohort.COHORT_3)
            ):
                return EnrollmentStatus.FULL_PARTICIPANT
            elif (
                num_completed_baseline_ppi_modules == self._get_num_baseline_ppi_modules()
                and physical_measurements_status != PhysicalMeasurementsStatus.COMPLETED
                and samples_to_isolate_dna == SampleStatus.RECEIVED
                and (gror_consent == QuestionnaireStatus.SUBMITTED or consent_cohort != ParticipantCohort.COHORT_3)
            ):
                return EnrollmentStatus.CORE_MINUS_PM
            elif consent_expire_status != ConsentExpireStatus.EXPIRED:
                return EnrollmentStatus.MEMBER
        return EnrollmentStatus.INTERESTED

    def calculate_member_time(self, consent, participant_summary):
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

    def calculate_core_minus_pm_time(self, consent, participant_summary):
        if (
            consent
            and participant_summary.numCompletedBaselinePPIModules == self._get_num_baseline_ppi_modules()
            and participant_summary.physicalMeasurementsStatus != PhysicalMeasurementsStatus.COMPLETED
            and participant_summary.samplesToIsolateDNA == SampleStatus.RECEIVED
            and (participant_summary.consentForGenomicsROR == QuestionnaireStatus.SUBMITTED
                 or participant_summary.consentCohort != ParticipantCohort.COHORT_3)
        ) or participant_summary.enrollmentStatus == EnrollmentStatus.CORE_MINUS_PM:

            max_core_sample_time = self.calculate_max_core_sample_time(
                participant_summary, field_name_prefix="sampleStatus"
            )

            if max_core_sample_time and participant_summary.enrollmentStatusCoreStoredSampleTime:
                return participant_summary.enrollmentStatusCoreStoredSampleTime
            else:
                return max_core_sample_time
        else:
            return None

    def calculate_core_stored_sample_time(self, consent, participant_summary):
        if (
            consent
            and participant_summary.numCompletedBaselinePPIModules == self._get_num_baseline_ppi_modules()
            and participant_summary.physicalMeasurementsStatus == PhysicalMeasurementsStatus.COMPLETED
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
            and participant_summary.physicalMeasurementsStatus == PhysicalMeasurementsStatus.COMPLETED
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
                            participant_summary.physicalMeasurementsFinalizedTime,
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

    def get_client_id(self):
        from rdr_service import app_util, api_util
        email = app_util.get_oauth_id()
        user_info = app_util.lookup_user_info(email)
        client_id = user_info.get('clientId')
        if email == api_util.DEV_MAIL and client_id is None:
            client_id = 'example'  # account for temp configs that dont create the key
        return client_id

    def to_client_json(self, model):
        result = model.asdict()
        # Participants that withdrew more than 48 hours ago should have fields other than
        # WITHDRAWN_PARTICIPANT_FIELDS cleared.
        if model.withdrawalStatus == WithdrawalStatus.NO_USE and (
            model.withdrawalTime is None
            or model.withdrawalTime < clock.CLOCK.now() - WITHDRAWN_PARTICIPANT_VISIBILITY_TIME
        ):
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

        if result.get("primaryLanguage") is None:
            result["primaryLanguage"] = UNSET

        if "organizationId" in result:
            result["organization"] = result["organizationId"]
            del result["organizationId"]
            format_json_org(result, self.organization_dao, "organization")

        if result.get("genderIdentityId"):
            del result["genderIdentityId"]  # deprecated in favor of genderIdentity

        result["retentionType"] = str(RetentionType.UNSET)
        if model.retentionEligibleStatus == RetentionStatus.ELIGIBLE:
            eighteen_month_ago = clock.CLOCK.now() - RETENTION_WINDOW
            if (model.questionnaireOnHealthcareAccessAuthored and
                model.questionnaireOnHealthcareAccessAuthored > eighteen_month_ago) or \
                (model.questionnaireOnFamilyHealthAuthored and
                 model.questionnaireOnFamilyHealthAuthored > eighteen_month_ago) or \
                (model.questionnaireOnMedicalHistoryAuthored and
                 model.questionnaireOnMedicalHistoryAuthored > eighteen_month_ago) or \
                (model.questionnaireOnCopeNovAuthored and
                 model.questionnaireOnCopeNovAuthored > eighteen_month_ago) or \
                (model.questionnaireOnCopeJulyAuthored and
                 model.questionnaireOnCopeJulyAuthored > eighteen_month_ago) or \
                (model.questionnaireOnCopeJuneAuthored and
                 model.questionnaireOnCopeJuneAuthored > eighteen_month_ago) or \
                (model.questionnaireOnCopeMayAuthored and
                 model.questionnaireOnCopeMayAuthored > eighteen_month_ago) or \
                (model.questionnaireOnCopeDecAuthored and
                 model.questionnaireOnCopeDecAuthored > eighteen_month_ago) or \
                (model.questionnaireOnCopeFebAuthored and
                 model.questionnaireOnCopeFebAuthored > eighteen_month_ago) or \
                (model.consentCohort == ParticipantCohort.COHORT_1 and
                 model.consentForStudyEnrollmentAuthored != model.consentForStudyEnrollmentFirstYesAuthored and
                 model.consentForStudyEnrollmentAuthored > eighteen_month_ago) or \
                (model.consentCohort == ParticipantCohort.COHORT_1 and model.consentForGenomicsRORAuthored and
                 model.consentForGenomicsRORAuthored > eighteen_month_ago) or \
                (model.consentCohort == ParticipantCohort.COHORT_2 and model.consentForGenomicsRORAuthored and
                 model.consentForGenomicsRORAuthored > eighteen_month_ago):
                result["retentionType"] = str(RetentionType.ACTIVE)
            if model.ehrUpdateTime and model.ehrUpdateTime > eighteen_month_ago:
                if result["retentionType"] == str(RetentionType.ACTIVE):
                    result["retentionType"] = str(RetentionType.ACTIVE_AND_PASSIVE)
                else:
                    result["retentionType"] = str(RetentionType.PASSIVE)

        # Note: leaving for future use if we go back to using a relationship to PatientStatus table.
        # def format_patient_status_record(status_obj):
        #   status_dict = self.patient_status_dao.to_client_json(status_obj)
        #   return {
        #     'organization': status_dict['organization'],
        #     'status': status_dict['patient_status'],
        #   }
        # result['patientStatus'] = map(format_patient_status_record, model.patientStatus)
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
            format_json_code(result, self.code_dao, fieldname)
        for fieldname in _ENUM_FIELDS:
            format_json_enum(result, fieldname)
        for fieldname in _SITE_FIELDS:
            format_json_site(result, self.site_dao, fieldname)
        if model.withdrawalStatus == WithdrawalStatus.NO_USE\
                or model.suspensionStatus == SuspensionStatus.NO_CONTACT\
                or model.deceasedStatus == DeceasedStatus.APPROVED:
            result["recontactMethod"] = "NO_CONTACT"

        # Strip None values.
        result = {k: v for k, v in list(result.items()) if v is not None}

        return result

    @staticmethod
    def get_aliased_field_map():
        return {
            'firstEhrReceiptTime': 'ehrReceiptTime',
            'latestEhrReceiptTime': 'ehrUpdateTime'
        }

    def _decode_token(self, query_def, fields):
        """ If token exists in participant_summary api, decode and use lastModified to add a buffer
    of 60 seconds. This ensures when a _sync link is used no one is missed. This will return
    at a minimum, the last participant and any more that have been modified in the previous 60
    seconds. Duplicate participants returned should be handled on the client side."""
        decoded_vals = super(ParticipantSummaryDao, self)._decode_token(query_def, fields)
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


class RetentionTypeFieldFilter(FieldFilter):
    def __init__(self, field_name, operator, value):
        super(RetentionTypeFieldFilter, self).__init__(field_name, operator, value)

    def add_to_sqlalchemy_query(self, query, field):
        if self.value in [str(RetentionType(item)) for item in RetentionType]:
            eighteen_month_ago = clock.CLOCK.now() - RETENTION_WINDOW
            active_criterion = or_(
                ParticipantSummary.questionnaireOnHealthcareAccessAuthored > eighteen_month_ago,
                ParticipantSummary.questionnaireOnFamilyHealthAuthored > eighteen_month_ago,
                ParticipantSummary.questionnaireOnMedicalHistoryAuthored > eighteen_month_ago,
                ParticipantSummary.questionnaireOnCopeNovAuthored > eighteen_month_ago,
                ParticipantSummary.questionnaireOnCopeJulyAuthored > eighteen_month_ago,
                ParticipantSummary.questionnaireOnCopeJuneAuthored > eighteen_month_ago,
                ParticipantSummary.questionnaireOnCopeMayAuthored > eighteen_month_ago,
                ParticipantSummary.questionnaireOnCopeDecAuthored > eighteen_month_ago,
                ParticipantSummary.questionnaireOnCopeFebAuthored > eighteen_month_ago,
                and_(
                    ParticipantSummary.consentCohort == ParticipantCohort.COHORT_1,
                    ParticipantSummary.consentForStudyEnrollmentAuthored !=
                    ParticipantSummary.consentForStudyEnrollmentFirstYesAuthored,
                    ParticipantSummary.consentForStudyEnrollmentAuthored > eighteen_month_ago
                ),
                and_(
                    ParticipantSummary.consentCohort == ParticipantCohort.COHORT_1,
                    ParticipantSummary.consentForGenomicsRORAuthored > eighteen_month_ago
                ),
                and_(
                    ParticipantSummary.consentCohort == ParticipantCohort.COHORT_2,
                    ParticipantSummary.consentForGenomicsRORAuthored > eighteen_month_ago
                )
            )
            not_active_criterion = and_(
                or_(
                    ParticipantSummary.questionnaireOnHealthcareAccessAuthored == None,
                    ParticipantSummary.questionnaireOnHealthcareAccessAuthored <= eighteen_month_ago
                ),
                or_(
                    ParticipantSummary.questionnaireOnFamilyHealthAuthored == None,
                    ParticipantSummary.questionnaireOnFamilyHealthAuthored <= eighteen_month_ago
                ),
                or_(
                    ParticipantSummary.questionnaireOnMedicalHistoryAuthored == None,
                    ParticipantSummary.questionnaireOnMedicalHistoryAuthored <= eighteen_month_ago
                ),
                or_(
                    ParticipantSummary.questionnaireOnCopeNovAuthored == None,
                    ParticipantSummary.questionnaireOnCopeNovAuthored <= eighteen_month_ago
                ),
                or_(
                    ParticipantSummary.questionnaireOnCopeJulyAuthored == None,
                    ParticipantSummary.questionnaireOnCopeJulyAuthored <= eighteen_month_ago
                ),
                or_(
                    ParticipantSummary.questionnaireOnCopeJuneAuthored == None,
                    ParticipantSummary.questionnaireOnCopeJuneAuthored <= eighteen_month_ago
                ),
                or_(
                    ParticipantSummary.questionnaireOnCopeMayAuthored == None,
                    ParticipantSummary.questionnaireOnCopeMayAuthored <= eighteen_month_ago
                ),
                or_(
                    ParticipantSummary.questionnaireOnCopeDecAuthored == None,
                    ParticipantSummary.questionnaireOnCopeDecAuthored <= eighteen_month_ago
                ),
                or_(
                    ParticipantSummary.questionnaireOnCopeFebAuthored == None,
                    ParticipantSummary.questionnaireOnCopeFebAuthored <= eighteen_month_ago
                ),
                or_(
                    ParticipantSummary.consentCohort != ParticipantCohort.COHORT_1,
                    ParticipantSummary.consentForStudyEnrollmentAuthored ==
                    ParticipantSummary.consentForStudyEnrollmentFirstYesAuthored,
                    ParticipantSummary.consentForStudyEnrollmentAuthored <= eighteen_month_ago
                ),
                or_(
                    ParticipantSummary.consentCohort != ParticipantCohort.COHORT_1,
                    ParticipantSummary.consentForGenomicsRORAuthored == None,
                    ParticipantSummary.consentForGenomicsRORAuthored <= eighteen_month_ago
                ),
                or_(
                    ParticipantSummary.consentCohort != ParticipantCohort.COHORT_2,
                    ParticipantSummary.consentForGenomicsRORAuthored == None,
                    ParticipantSummary.consentForGenomicsRORAuthored <= eighteen_month_ago
                )
            )
            if self.value == str(RetentionType.ACTIVE):
                query = query.filter(
                    field == RetentionStatus.ELIGIBLE,
                    active_criterion,
                    or_(
                        ParticipantSummary.ehrUpdateTime == None,
                        ParticipantSummary.ehrUpdateTime <= eighteen_month_ago
                    )

                )
            elif self.value == str(RetentionType.PASSIVE):
                query = query.filter(
                    field == RetentionStatus.ELIGIBLE,
                    not_active_criterion,
                    ParticipantSummary.ehrUpdateTime > eighteen_month_ago
                )
            elif self.value == str(RetentionType.ACTIVE_AND_PASSIVE):
                query = query.filter(
                    field == RetentionStatus.ELIGIBLE,
                    active_criterion,
                    ParticipantSummary.ehrUpdateTime > eighteen_month_ago
                )
            elif self.value == str(RetentionType.UNSET):
                query = query.filter(
                    or_(
                        field == RetentionStatus.NOT_ELIGIBLE,
                        and_(
                            not_active_criterion,
                            or_(
                                ParticipantSummary.ehrUpdateTime == None,
                                ParticipantSummary.ehrUpdateTime <= eighteen_month_ago
                            )
                        )
                    )
                )
            return query
        else:
            raise ValueError(f"Invalid parameter: {self.value}.")


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
