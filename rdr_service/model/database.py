from contextlib import contextmanager

import backoff
from sqlalchemy import create_engine
from sqlalchemy.exc import DBAPIError
from sqlalchemy.orm import Session, sessionmaker

from rdr_service.model.base import Base, MetricsBase

# All tables in the schema should be imported below here.
# pylint: disable=unused-import
from rdr_service.model.api_user import ApiUser
from rdr_service.model.participant import Participant, ParticipantHistory
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
from rdr_service.model.hpo import HPO
from rdr_service.model.log_position import LogPosition
from rdr_service.model.measurements import PhysicalMeasurements, Measurement
from rdr_service.model.metric_set import AggregateMetrics, MetricSet
from rdr_service.model.metrics import MetricsVersion, MetricsBucket
from rdr_service.model.metrics_cache import MetricsEnrollmentStatusCache, MetricsAgeCache, MetricsRaceCache, \
  MetricsRegionCache, MetricsGenderCache, MetricsLanguageCache, MetricsLifecycleCache
from rdr_service.model.organization import Organization
from rdr_service.model.questionnaire import Questionnaire, QuestionnaireHistory, QuestionnaireQuestion
from rdr_service.model.questionnaire import QuestionnaireConcept
from rdr_service.model.questionnaire_response import QuestionnaireResponse, QuestionnaireResponseAnswer
from rdr_service.model.site import Site
from rdr_service.model.genomics import GenomicSet, GenomicSetMember
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
from rdr_service.model.covid_antibody_study import BiobankCovidAntibodySample, QuestCovidAntibodyTestResult,\
    QuestCovidAntibodyTest
from rdr_service.model.hpo_lite_pairing_import_record import HpoLitePairingImportRecord
from rdr_service.model.retention_eligible_metrics import RetentionEligibleMetrics

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
