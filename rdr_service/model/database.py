from contextlib import contextmanager
from typing import List

from alembic.operations import ops as alembic_ops
from alembic.script import write_hooks
import backoff
from sqlalchemy import create_engine
from sqlalchemy.exc import DBAPIError
from sqlalchemy.orm import Session, sessionmaker

from rdr_service.model.base import Base, MetricsBase, RexBase

# All tables in the schema should be imported below here.
# pylint: disable=unused-import
from rdr_service.model.api_user import ApiUser
from rdr_service.model.etm import EtmQuestionnaire, EtmQuestionnaireResponse, EtmQuestionnaireResponseAnswer, \
    EtmQuestionnaireResponseMetadata
from rdr_service.model.participant import Participant, ParticipantHistory
from rdr_service.model.participant_incentives import ParticipantIncentives
from rdr_service.model.participant_summary import ParticipantSummary
from rdr_service.model.participant_cohort_pilot import ParticipantCohortPilot
from rdr_service.model.biobank_stored_sample import BiobankStoredSample
from rdr_service.model.biobank_order import BiobankOrder, BiobankOrderIdentifier, BiobankOrderedSample, \
    BiobankSpecimen, BiobankAliquot, BiobankAliquotDataset, BiobankAliquotDatasetItem, BiobankSpecimenAttribute, \
    BiobankQuestOrderSiteAddress
from rdr_service.model.biobank_mail_kit_order import BiobankMailKitOrder
from rdr_service.model.code import CodeBook, Code, CodeHistory
from rdr_service.model.calendar import Calendar
from rdr_service.model.deceased_report import DeceasedReport
from rdr_service.model.deceased_report_import_record import DeceasedReportImportRecord
from rdr_service.model.ehr import EhrReceipt, ParticipantEhrReceipt
from rdr_service.model.enrollment_status_history import EnrollmentStatusHistory
from rdr_service.model.ghost_api_check import GhostApiCheck
from rdr_service.model.hpo import HPO
from rdr_service.model.hpro_consent_files import HealthProConsentFile
from rdr_service.model.log_position import LogPosition
from rdr_service.model.measurements import PhysicalMeasurements, Measurement
from rdr_service.model.metric_set import AggregateMetrics, MetricSet
from rdr_service.model.metrics import MetricsVersion, MetricsBucket
from rdr_service.model.metrics_cache import MetricsEnrollmentStatusCache, MetricsAgeCache, MetricsRaceCache, \
  MetricsRegionCache, MetricsGenderCache, MetricsLanguageCache, MetricsLifecycleCache
from rdr_service.model.obfuscation import Obfuscation
from rdr_service.model.organization import Organization
from rdr_service.model.questionnaire import Questionnaire, QuestionnaireHistory, QuestionnaireQuestion
from rdr_service.model.questionnaire import QuestionnaireConcept
from rdr_service.model.questionnaire_response import QuestionnaireResponse, QuestionnaireResponseAnswer
from rdr_service.model.site import Site
from rdr_service.model.genomics import GenomicSet, GenomicSetMember
from rdr_service.model.genomic_datagen import GenomicDataGenRun
from rdr_service.model.patient_status import PatientStatus
from rdr_service.model.bigquery_sync import BigQuerySync
from rdr_service.model.requests_log import RequestsLog
from rdr_service.model.survey import Survey, SurveyQuestion, SurveyQuestionOption
from rdr_service.model.workbench_workspace import WorkbenchWorkspaceApproved, WorkbenchWorkspaceSnapshot, \
    WorkbenchWorkspaceUser, WorkbenchWorkspaceUserHistory
from rdr_service.model.workbench_researcher import WorkbenchResearcher, WorkbenchResearcherHistory, \
    WorkbenchInstitutionalAffiliations, WorkbenchInstitutionalAffiliationsHistory
from rdr_service.model.metadata import Metadata
from rdr_service.model.resource_type import ResourceType
from rdr_service.model.resource_schema import ResourceSchema
from rdr_service.model.resource_data import ResourceData
from rdr_service.model.resource_search_results import ResourceSearchResults
from rdr_service.model.consent_file import ConsentFile, ConsentErrorReport
from rdr_service.model.consent_response import ConsentResponse
from rdr_service.model.covid_antibody_study import BiobankCovidAntibodySample, QuestCovidAntibodyTestResult,\
    QuestCovidAntibodyTest
from rdr_service.model.hpo_lite_pairing_import_record import HpoLitePairingImportRecord
from rdr_service.model.message_broker import MessageBrokerRecord, MessageBrokerMetadata, MessageBrokerDestAuthInfo
from rdr_service.model.retention_eligible_metrics import RetentionEligibleMetrics
from rdr_service.model.ce_health_reconciliation import CeHealthReconciliation
from rdr_service.model.onsite_id_verification import OnsiteIdVerification
from rdr_service.model.curation_etl import CdrEtlRunHistory, CdrEtlSurveyHistory, CdrExcludedCode
from rdr_service.model.profile_update import ProfileUpdate

from rdr_service.model.rex import TestModel

RETRY_CONNECTION_LIMIT = 10


class Database(object):
    """Maintains state for accessing the database."""

    def __init__(self, url, **kwargs):
        # Add echo=True here to spit out SQL statements.
        # Set pool_recycle to 3600 -- one hour in seconds -- which is lower than the MySQL wait_timeout
        # parameter (which defaults to 8 hours) to ensure that we don't attempt to use idle database
        # connections after this period. (See DA-237.) To change the db wait_timeout (seconds), run:
        # gcloud --project <proj> sql instances patch rdrmaindb --database-flags wait_timeout=28800
        self._engine = create_engine(
            url, pool_pre_ping=True, pool_recycle=3600, pool_size=30, max_overflow=20, **kwargs
            )
        self.db_type = url.drivername
        if self.db_type == "sqlite":
            self._engine.execute("PRAGMA foreign_keys = ON;")
        # expire_on_commit = False allows us to access model objects outside of a transaction.
        # It also means that after a commit, a model object won't read from the database for its
        # properties. (Which should be fine.)
        self._Session = sessionmaker(bind=self._engine, expire_on_commit=False)

    def get_engine(self):
        return self._engine

    def raw_connection(self):
        return self._engine.raw_connection()

    def create_schema(self):
        Base.metadata.create_all(self._engine)

    def create_metrics_schema(self):
        MetricsBase.metadata.create_all(self._engine)

    def create_rex_schema(self):
        RexBase.metadata.create_all(self._engine)

    def make_session(self) -> Session:
        return self._Session()

    @contextmanager
    def session(self):
        sess = self.make_session()
        try:
            yield sess
            sess.commit()
        except Exception:
            sess.rollback()
            # TODO: rolling back the session here might be preventing us from recording request_logs
            #  when within the context (such as when inserting with a session in the DAOs)
            raise
        finally:
            sess.close()

    # TODO: at the time of writing, a PR went in to backoff that adds a `max_time` parameter.  This
    # will be part of backoff 1.5 (we are on backoff 1.4).  When it releases, use it to prevent
    # ludicriously long retry periods.
    @backoff.on_exception(
        backoff.expo,
        DBAPIError,
        max_tries=RETRY_CONNECTION_LIMIT,
        giveup=lambda err: not getattr(err, "connection_invalidated", False),
    )
    def autoretry(self, func):
        """Runs a function of the db session and attempts to commit.  If we encounter a dropped
    connection, we retry the operation.  The retries are spaced out using exponential backoff with
    full jitter.  All other errors are propagated.
    """
        with self.session() as session:
            return func(session)


def get_class_for_table_name(table_name):
    for model in Base._decl_class_registry.values():
        if getattr(model, '__tablename__', '') == table_name:
            return model


class AutoHistoryRevisionGenerator:
    """Used to create alembic operations for managing history tables"""
    first_pass = True

    @classmethod
    def process_revision_directives(cls, _, __, directives):
        # RDR upgrades get processed first, and then the directives are processed again for Metrics.
        # Just want to process them once for RDR.
        if cls.first_pass:
            cls.first_pass = False
        else:
            return

        migration_script: alembic_ops.MigrationScript = directives[0]
        cls._process_op_list(migration_script.upgrade_ops_list)
        cls._process_op_list(migration_script.downgrade_ops_list)

    @classmethod
    def _process_op_list(cls, op_container_list: List[alembic_ops.OpContainer]):
        """Check the given operation list and add corresponding history table operations if needed"""

        for container in op_container_list:
            for op in container.ops:
                table_name = op.table_name if hasattr(op, 'table_name') else None
                schema_class = get_class_for_table_name(table_name)
                should_have_history_table = getattr(schema_class, 'history_table', False)

                if should_have_history_table:
                    if isinstance(op, alembic_ops.CreateTableOp):
                        container.ops.extend([
                            cls._get_create_history_table_op(schema_class=schema_class),
                            cls._get_create_history_table_trigger_op(schema_class=schema_class)
                        ])
                    elif isinstance(op, alembic_ops.DropTableOp):
                        container.ops.extend([
                            cls._get_drop_history_triggers_op(table_name=table_name),
                            alembic_ops.DropTableOp(table_name=f'{table_name}_history')
                        ])
                    elif isinstance(op, alembic_ops.ModifyTableOps):
                        for child_op in op.ops:
                            if (
                                isinstance(child_op, alembic_ops.DropColumnOp)
                                and child_op.column_name not in cls._get_excluded_column_names(
                                    schema_class=schema_class
                                )
                            ):
                                container.ops.append(alembic_ops.DropColumnOp(
                                    table_name=f'{table_name}_history',
                                    column_name=child_op.column_name
                                ))
                                cls._append_trigger_reset_ops(op_container=container, schema_class=schema_class)
                            elif (
                                isinstance(child_op, alembic_ops.AddColumnOp)
                                and child_op.column.expression.name not in cls._get_excluded_column_names(
                                    schema_class=schema_class
                                )
                            ):
                                container.ops.append(alembic_ops.AddColumnOp(
                                    table_name=f'{table_name}_history',
                                    column=child_op.column
                                ))
                                cls._append_trigger_reset_ops(op_container=container, schema_class=schema_class)


    @classmethod
    def _get_create_history_table_op(cls, schema_class):
        table_name = schema_class.__tablename__
        result = f"""
            CREATE TABLE {table_name}_history LIKE {table_name};

            ALTER TABLE {table_name}_history
            CHANGE COLUMN `id` `id` INTEGER NOT NULL,
            DROP PRIMARY KEY,
            ADD revision_action VARCHAR(8) DEFAULT 'insert' FIRST,
            ADD revision_id INT(6) NOT NULL AFTER revision_action,
            ADD revision_dt DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6) AFTER revision_id;

            ALTER TABLE {table_name}_history
            ADD INDEX idx_revision (revision_id),
            CHANGE COLUMN `revision_id` `revision_id` INT(6) NOT NULL AUTO_INCREMENT,
            ADD PRIMARY KEY (`id`, revision_id);
        """

        excluded_columns = cls._get_excluded_column_names(schema_class=schema_class)
        drop_column_statements = """,\n""".join(
            f'DROP COLUMN {excluded_column_name}' for excluded_column_name in excluded_columns
        )
        if excluded_columns:
            result += f"""
                ALTER TABLE {table_name}_history
                {drop_column_statements};
            """

        return alembic_ops.ExecuteSQLOp(result)

    @classmethod
    def _get_history_tracked_columns(cls, schema_class):
        result = []
        exclude_list = cls._get_excluded_column_names(schema_class=schema_class, default=[])
        for attribute in schema_class.__dict__.values():
            # If attribute is a sqlalchemy column, add it to the result if it should be tracked in the history
            if hasattr(attribute, 'expression'):
                expression = attribute.expression
                if expression.name not in exclude_list:
                    result.append(expression.name)
        return result

    @classmethod
    def _get_excluded_column_names(cls, schema_class, default=None):
        return getattr(schema_class, 'exclude_column_names_from_history', default)

    @classmethod
    def _append_trigger_reset_ops(cls, op_container, schema_class):
        op_container.ops.extend([
            cls._get_drop_history_triggers_op(table_name=schema_class.__tablename__),
            cls._get_create_history_table_trigger_op(schema_class=schema_class)
        ])

    @classmethod
    def _get_create_history_table_trigger_op(cls, schema_class):
        table_name = schema_class.__tablename__
        history_column_names = cls._get_history_tracked_columns(schema_class)

        updated_tracked_column_check = ' OR\n'.join([
            f'NEW.{column_name} <=> OLD.{column_name}'
            for column_name in history_column_names
        ])

        return alembic_ops.ExecuteSQLOp(f"""
            CREATE TRIGGER {table_name}__ai AFTER INSERT ON {table_name} FOR EACH ROW
                INSERT INTO {table_name}_history (revision_action, revision_dt, {', '.join(history_column_names)})
                SELECT 'insert', NOW(6), {', '.join(history_column_names)}
                FROM {table_name} AS d WHERE d.id = NEW.id;

            CREATE TRIGGER {table_name}__au AFTER UPDATE ON {table_name} FOR EACH ROW
                INSERT INTO {table_name}_history (revision_action, revision_dt, {', '.join(history_column_names)})
                SELECT 'update', NOW(6), {', '.join(history_column_names)}
                FROM {table_name} AS d
                WHERE d.id = NEW.id
                    AND (
                        {updated_tracked_column_check}
                    );

            CREATE TRIGGER {table_name}__bd BEFORE DELETE ON {table_name} FOR EACH ROW
                INSERT INTO {table_name}_history (revision_action, revision_dt, {', '.join(history_column_names)})
                SELECT 'delete', NOW(6), {', '.join(history_column_names)}
                FROM {table_name} AS d WHERE d.id = OLD.id;
        """)

    @classmethod
    def _get_drop_history_triggers_op(cls, table_name):
        return alembic_ops.ExecuteSQLOp(f"""
            DROP TRIGGER IF EXISTS {table_name}__ai;
            DROP TRIGGER IF EXISTS {table_name}__au;
            DROP TRIGGER IF EXISTS {table_name}__bd;
        """)


@write_hooks.register('custom_sql_newlines')
def format_sql_newlines(file_name, _):
    """
    Generating the SQL strings for the history tables will result in escaped newlines in the revision file output.
    This hook is set up to un-escape the newlines in the output so that it's easier to read the sql for the history
    table management.
    """
    lines = []
    with open(file_name) as file:
        for line in file:
            if '"' in line and '\\n' in line:
                line = line.replace('"', '"""').replace('\\n', '\n')
            elif "'" in line and '\\n' in line:
                line = line.replace("'", '"""').replace('\\n', '\n')
            lines.append(line)
    with open(file_name, 'w') as file:
        file.write("".join(lines))
