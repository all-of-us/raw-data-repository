import logging
from typing import Optional, Union

import google.cloud.bigquery
from google.cloud import bigquery
from google.cloud.exceptions import NotFound

from rdr_service.tools.tool_libs.tool_base import cli_run, ToolBase
from rdr_service.etl.bq import queries

_logger = logging.getLogger("rdr_logger")

tool_cmd = "curation-bq"
tool_desc = "Run Curation ETL process in BigQuery using replicated MySQL tables"

# Ghost-participant eligibility was only enforced after this date.
_GHOST_DATE_CUTOFF = "2022-03-18"


class CurationBQ(ToolBase):
    """Orchestrates the full Curation CDM ETL pipeline in BigQuery.

    Data sources are BigQuery datasets that are replicated from MySQL (Cloud SQL)
    rather than being queried via EXTERNAL_QUERY or a direct Cloud SQL connection.

    Usage workflow:
        1. Ensure ``--rdr-dataset`` and ``--voc-dataset`` replicated datasets are
           up-to-date and contain all required tables.
        2. Run with ``--load-data`` to copy vocabulary / RDR lookup tables from the
           replicated datasets into the working ETL dataset.
        3. Run with ``--run-etl`` to execute the full CDM transformation pipeline.
        4. Run with ``--export`` to push final tables to GCS or another BQ dataset.
    """

    # Tables imported from the vocabulary replicated dataset into the working dataset.
    # (src_clean and src_meas are now generated directly by ETL queries.)
    _VOC_IMPORT_TABLES: list[str] = ["concept", "concept_relationship"]

    # Tables imported from the RDR replicated dataset into the working dataset.
    _RDR_IMPORT_TABLES: list[str] = [
        "rdr_site",
        "rdr_measurement_to_qualifier",
        "rdr_deceased_report",
    ]

    _MEASUREMENT_STEPS = {"src_meas", "tmp_cv_concept_lk", "tmp_vcv_concept_lk",
                          "src_meas_mapped", "tmp_visits_src", "visit_occurrence",
                          "care_site", "measurement", "note", "tmp_fact_rel_sd",
                          "fact_relationship"}
    _SURVEY_STEPS = {"observation", "qrai_author", "qrai_language", "qrai_code"}

    # ETL pipeline step order.  Each key maps to an entry in queries.queries.
    etl_process_steps: list[str] = [
        # ── Phase 1: source data generation from replicated MySQL tables ──────
        "participant_filter",
        "questionnaire_answers_by_module",
        "src_clean",
        "filter_surveys",
        "filter_questions",
        "src_meas",
        # ── Phase 2: CDM table population (unchanged from before) ─────────────
        "src_participant",
        "src_mapped",
        "src_gender",
        "src_race",
        "src_race_2",
        "src_ethnicity",
        "src_ethnicity_2",
        "src_person_location",
        "location",
        "update_location_id",
        "person",
        "tmp_cv_concept_lk",
        "tmp_vcv_concept_lk",
        "src_meas_mapped",
        "tmp_visits_src",
        "visit_occurrence",
        "observation",
        "care_site",
        "measurement",
        "note",
        "tmp_fact_rel_sd",
        "fact_relationship",
        "procedure_occurrence",
        "death",
        "ehr_consent_temp_table",
        "ehr_consent",
        "wear_consent",
        "participant_id_mapping",
        "qrai_author",
        "qrai_language",
        "qrai_code",
        "tmp_survey_conduct",
        "survey_conduct",
        "create_empty_tables",
        "pid_rid_mapping",
        "cope_survey_semantic_version_map",
        "temp_obs_target",
        "temp_obs",
        "observation_period",
        "finalize",
    ]

    export_tables: list[str] = [
        "care_site",
        "condition_era",
        "condition_occurrence",
        "consent",
        "cost",
        "death",
        "device_exposure",
        "dose_era",
        "drug_era",
        "drug_exposure",
        "fact_relationship",
        "location",
        "measurement",
        "metadata",
        "note_nlp",
        "observation",
        "observation_period",
        "payer_plan_period",
        "person",
        "pid_rid_mapping",
        "procedure_occurrence",
        "provider",
        "questionnaire_response_additional_info",
        "visit_detail",
        "visit_occurrence",
        "wear_consent",
        "survey_conduct",
        "cope_survey_semantic_version_map",
        "note",
        "specimen",
    ]

    def __init__(self, args, gcp_env=None, tool_name=None, replica=False) -> None:
        super().__init__(args, gcp_env, tool_name, replica)
        self.client = bigquery.Client()
        self.dataset_id = f"{args.project}.{args.dataset}"
        self.rdr_dataset = args.rdr_dataset
        self.voc_dataset = getattr(args, "voc_dataset", None) or args.rdr_dataset
        self.etl_filters = getattr(args, "etl_filters", "etl_filters")

    # ------------------------------------------------------------------
    # Public entry-point
    # ------------------------------------------------------------------

    def run(self) -> Optional[int]:
        """Dispatch to the requested sub-command."""
        super().run()
        if not self.args.dataset:
            _logger.error("No dataset specified")
            return 1

        if self.args.load_data:
            self.import_tables_to_bq()
        elif self.args.run_etl:
            self.run_etl()
        elif self.args.export:
            self.export()
        else:
            _logger.error("One of --load-data, --run-etl, or --export must be set")
        return None

    # ------------------------------------------------------------------
    # Query execution helpers
    # ------------------------------------------------------------------

    def run_query(
        self,
        sql: str,
        job_config: Union[None, google.cloud.bigquery.QueryJobConfig],
    ) -> None:
        """Execute a BigQuery SQL statement, optionally writing to a destination table.

        Args:
            sql: The BigQuery SQL string to execute.
            job_config: Optional job configuration (includes destination table).
        """
        if self.args.dry_run:
            _logger.info(sql)
            return
        query_job = self.client.query(sql, job_config=job_config)
        result = query_job.result()
        _logger.debug("Rows affected: %s", result.total_rows)

    # ------------------------------------------------------------------
    # Data loading
    # ------------------------------------------------------------------

    def import_table(self, table_name: str, source_dataset: str) -> None:
        """Copy a single table from a replicated BQ dataset into the working dataset.

        Args:
            table_name: Unqualified table name.
            source_dataset: Fully-qualified BQ dataset (``project.dataset``).
        """
        job_config = bigquery.QueryJobConfig(
            destination=f"{self.dataset_id}.{table_name}",
            write_disposition=bigquery.job.WriteDisposition.WRITE_TRUNCATE,
        )
        sql = f"SELECT * FROM `{source_dataset}.{table_name}`"
        _logger.debug("Importing %s from %s", table_name, source_dataset)
        self.run_query(sql, job_config)

    def import_tables_to_bq(self) -> None:
        """Load vocabulary and RDR lookup tables into the working ETL dataset.

        The tables imported here are those referenced by downstream ETL queries
        as ``{dataset_id}.<table>`` (e.g. concept, site).  The source data comes
        from the BigQuery-replicated datasets (``--rdr-dataset`` / ``--voc-dataset``)
        rather than from Cloud SQL via EXTERNAL_QUERY.

        ``src_clean`` and ``src_meas`` are **not** imported here; they are
        generated directly by the Phase 1 ETL queries during ``--run-etl``.
        """
        for table in self._VOC_IMPORT_TABLES:
            self.import_table(table, self.voc_dataset)
        for table in self._RDR_IMPORT_TABLES:
            self.import_table(table, self.rdr_dataset)

    # ------------------------------------------------------------------
    # ETL execution
    # ------------------------------------------------------------------

    def _build_format_args(self) -> dict[str, str]:
        """Compute all SQL template variables from CLI args.

        Returns:
            Mapping of placeholder name → SQL fragment string.
        """
        args = self.args

        # ── age filter ──────────────────────────────────────────────────
        if getattr(args, "include_participants_under_18", False):
            age_filter = ""
        else:
            age_filter = (
                "AND DATE_DIFF("
                "DATE(ps.consent_for_study_enrollment_first_yes_authored), "
                "DATE(ps.date_of_birth), YEAR) >= 18"
            )

        # ── withdrawal + cutoff filter ──────────────────────────────────
        cutoff: Optional[str] = getattr(args, "cutoff", None)
        if cutoff:
            withdrawal_filter = (
                f"AND SAFE_CAST(ps.consent_for_study_enrollment_first_yes_authored AS TIMESTAMP)"
                f" < TIMESTAMP('{cutoff}')\n"
                "AND (\n"
                "    ps.withdrawal_status != 2\n"  # NOT NO_USE
                "    OR (\n"
                "        ps.withdrawal_status = 2\n"
                f"       AND SAFE_CAST(ps.withdrawal_authored AS TIMESTAMP) >= TIMESTAMP('{cutoff}')\n"
                "    )\n"
                ")"
            )
            cutoff_authored_filter = (
                f"AND SAFE_CAST(COALESCE(qr.authored, qr.created) AS TIMESTAMP) < TIMESTAMP('{cutoff}')"
            )
            cutoff_finalized_filter = f"AND SAFE_CAST(pm.finalized AS TIMESTAMP) < TIMESTAMP('{cutoff}')"
            cutoff_death_filter = f"AND SAFE_CAST(dr.authored AS TIMESTAMP) < TIMESTAMP('{cutoff}')"
        else:
            withdrawal_filter = "AND ps.withdrawal_status != 2"  # NOT NO_USE
            cutoff_authored_filter = ""
            cutoff_finalized_filter = ""
            cutoff_death_filter = ""

        # ── participant origin filter ───────────────────────────────────
        origin: Optional[str] = getattr(args, "origin", None)
        if origin and origin != "all":
            origin_filter = f"AND p.participant_origin = '{origin}'"
        else:
            origin_filter = ""

        # ── excluded PID list ───────────────────────────────────────────
        exclude_pids: list[int] = getattr(args, "exclude_pid_list", []) or []
        if exclude_pids:
            pid_csv = ", ".join(str(p) for p in exclude_pids)
            exclude_pid_filter = f"AND p.participant_id NOT IN ({pid_csv})"
        else:
            exclude_pid_filter = ""

        # ── survey include / exclude filter ────────────────────────────
        include_surveys: str = getattr(args, "include_surveys", "") or ""
        exclude_surveys: str = getattr(args, "exclude_surveys", "") or ""
        if include_surveys:
            quoted = ", ".join(f"'{s}'" for s in include_surveys.split(","))
            survey_filter = f"AND c.value IN ({quoted})"
        elif exclude_surveys:
            quoted = ", ".join(f"'{s}'" for s in exclude_surveys.split(","))
            survey_filter = f"AND c.value NOT IN ({quoted})"
        else:
            survey_filter = ""

        # ── physical measurement collect-type filter ────────────────────
        include_in_person: bool = not getattr(args, "exclude_in_person_pm", False)
        include_remote: bool = not getattr(args, "exclude_remote_pm", False)
        if include_in_person and include_remote:
            pm_collect_type_filter = ""
        elif include_in_person:
            pm_collect_type_filter = "AND (pm.collect_type != 2 OR pm.collect_type IS NULL)"
        else:
            pm_collect_type_filter = "AND pm.collect_type = 2"

        return dict(
            dataset_id=self.dataset_id,
            rdr_dataset=self.rdr_dataset,
            etl_filters=self.etl_filters,
            cutoff=cutoff or "",
            age_filter=age_filter,
            withdrawal_filter=withdrawal_filter,
            origin_filter=origin_filter,
            exclude_pid_filter=exclude_pid_filter,
            cutoff_authored_filter=cutoff_authored_filter,
            cutoff_finalized_filter=cutoff_finalized_filter,
            cutoff_death_filter=cutoff_death_filter,
            survey_filter=survey_filter,
            pm_collect_type_filter=pm_collect_type_filter,
        )

    def run_etl(self) -> None:
        """Execute the full ETL pipeline against BigQuery.

        Phase 1 steps generate ``participant_filter``, ``questionnaire_answers_by_module``,
        ``src_clean``, and ``src_meas`` directly from the replicated RDR dataset.
        Phase 2 steps transform the CDM tables exactly as before.
        """
        fmt = self._build_format_args()
        omit_measurements: bool = getattr(self.args, "omit_measurements", False)
        omit_surveys: bool = getattr(self.args, "omit_surveys", False)

        for step in self.etl_process_steps:
            if omit_measurements and step in self._MEASUREMENT_STEPS:
                _logger.debug("Skipping measurement step: %s", step)
                continue
            if omit_surveys and step in self._SURVEY_STEPS:
                _logger.debug("Skipping survey step: %s", step)
                continue

            entry = queries.queries[step]
            table_name: Optional[str] = entry["destination"]
            query: str = entry["query"]
            append_to_table: bool = entry.get("append", False)

            _logger.debug("Running ETL step: %s", step)

            if not table_name:
                job_config = None
            else:
                write_disp = (
                    bigquery.job.WriteDisposition.WRITE_APPEND
                    if append_to_table
                    else bigquery.job.WriteDisposition.WRITE_TRUNCATE
                )
                job_config = bigquery.QueryJobConfig(
                    destination=f"{self.dataset_id}.{table_name}",
                    write_disposition=write_disp,
                )

            self.run_query(query.format(**fmt), job_config)

    # ------------------------------------------------------------------
    # Export
    # ------------------------------------------------------------------

    def export(self) -> None:
        """Export finalised CDM tables to GCS or another BigQuery dataset.

        Args are consumed from ``self.args``:
            destination: ``project.dataset`` string.
        """
        client = bigquery.Client()
        export_dataset_id = f"{self.args.project}.{self.args.destination}"
        try:
            dataset = client.get_dataset(export_dataset_id)
        except NotFound:
            _logger.error("Export dataset does not exist: %s", export_dataset_id)
            return
        if dataset.location != "us-central1":
            _logger.error("Export dataset must be in us-central1 region")
            return

        for table in self.export_tables:
            _logger.info("Exporting table: %s", table)
            source = f"{self.args.project}.{self.args.dataset}.{table}"
            destination = f"{self.args.destination}.{table}"
            client.copy_table(source, destination)


# ---------------------------------------------------------------------------
# CLI argument registration
# ---------------------------------------------------------------------------

def add_additional_arguments(parser) -> None:
    """Register all CLI arguments for the curation-bq tool."""
    parser.add_argument("--debug", help="enable debug output",
                        default=False, action="store_true")
    parser.add_argument("--log-file", help="write output to a log file",
                        default=False, action="store_true")

    # ── required ────────────────────────────────────────────────────────
    parser.add_argument(
        "--dataset",
        help="Working BigQuery dataset for the ETL run (e.g. my_project.etl_20260101)",
        required=True,
    )
    parser.add_argument(
        "--rdr-dataset",
        help=(
            "Fully-qualified BigQuery dataset containing MySQL-replicated RDR tables "
            "(e.g. my_project.rdr_replica).  Used as the source for participant, "
            "questionnaire_response, physical_measurements, etc."
        ),
        required=True,
    )

    # ── data sources ────────────────────────────────────────────────────
    parser.add_argument(
        "--voc-dataset",
        help=(
            "Fully-qualified BigQuery dataset containing vocabulary tables "
            "(concept, concept_relationship).  Defaults to --rdr-dataset if omitted."
        ),
        default=None,
    )
    parser.add_argument(
        "--etl-filters",
        help=(
            "BigQuery dataset containing ETL filter tables "
            "(combined_survey_filter, combined_question_filter, source_to_concept_map). "
            "Defaults to 'etl_filters'."
        ),
        default="etl_filters",
    )

    # ── operations ──────────────────────────────────────────────────────
    parser.add_argument(
        "--load-data",
        help="Copy vocabulary and RDR lookup tables into the working dataset",
        default=False, action="store_true",
    )
    parser.add_argument(
        "--run-etl",
        help="Execute the full ETL pipeline against BigQuery",
        default=False, action="store_true",
    )
    parser.add_argument(
        "--export",
        help="Export finalised CDM tables to GCS or another BQ dataset",
        default=False, action="store_true",
    )

    # ── ETL filters ─────────────────────────────────────────────────────
    parser.add_argument(
        "--cutoff",
        help="Data cut-off date (YYYY-MM-DD).  Only data authored before this date "
             "is included.  Also controls withdrawal eligibility logic.",
        default=None,
    )
    parser.add_argument(
        "--origin",
        help=(
            "Participant origin to process: vibrent | careevolution | all.  "
            "Defaults to all (no origin filter)."
        ),
        default="all",
    )
    parser.add_argument(
        "--include-surveys",
        help="Comma-separated list of survey names to include (mutually exclusive with "
             "--exclude-surveys).",
        default=None,
    )
    parser.add_argument(
        "--exclude-surveys",
        help="Comma-separated list of survey names to exclude (mutually exclusive with "
             "--include-surveys).",
        default=None,
    )
    parser.add_argument(
        "--omit-surveys",
        help="Skip observation table population from survey data",
        default=False, action="store_true",
    )
    parser.add_argument(
        "--omit-measurements",
        help="Skip physical measurements pipeline",
        default=False, action="store_true",
    )
    parser.add_argument(
        "--exclude-in-person-pm",
        help="Exclude in-person physical measurements",
        default=False, action="store_true",
    )
    parser.add_argument(
        "--exclude-remote-pm",
        help="Exclude remote physical measurements",
        default=False, action="store_true",
    )
    parser.add_argument(
        "--include-participants-under-18",
        help="Include participants who were under 18 at consent",
        default=False, action="store_true",
    )

    # ── export options ──────────────────────────────────────────────────
    parser.add_argument(
        "--destination",
        help="BQ dataset (project.dataset) for export",
    )

    # ── development helpers ─────────────────────────────────────────────
    parser.add_argument(
        "--dry-run",
        help="Print generated SQL instead of executing it",
        default=False, action="store_true",
    )


def run() -> None:
    cli_run(tool_cmd, tool_desc, CurationBQ, add_additional_arguments)
