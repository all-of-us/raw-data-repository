from types import SimpleNamespace
import unittest

import mock
from google.cloud import bigquery

from rdr_service.etl.bq import queries
from rdr_service.tools.tool_libs.curation_bq import CurationBQ


def _build_args(**overrides):
    defaults = {
        "project": "test-project",
        "dataset": "etl_dataset",
        "rdr_dataset": "test-project.rdr_replica",
        "voc_dataset": None,
        "etl_filters": "etl_filters",
        "cutoff": None,
        "origin": "all",
        "include_surveys": None,
        "exclude_surveys": None,
        "exclude_in_person_pm": False,
        "exclude_remote_pm": False,
        "include_participants_under_18": False,
        "exclude_pid_list": [],
        "participant_list_file": None,
        "exclude_participants": None,
        "omit_measurements": False,
        "omit_surveys": False,
        "dry_run": True,
        "load_data": False,
        "run_etl": False,
        "export": False,
        "snapshot_audit": False,
        "audit_dataset": None,
        "audit_run_id": None,
        "snapshot_label": None,
    }
    defaults.update(overrides)
    return SimpleNamespace(**defaults)


class CurationBQTest(unittest.TestCase):

    @staticmethod
    def _create_tool(args: SimpleNamespace) -> CurationBQ:
        with mock.patch("rdr_service.tools.tool_libs.curation_bq.bigquery.Client"):
            return CurationBQ(args)

    def test_ehr_consent_cutoff_filter_disabled_without_cutoff(self):
        tool = self._create_tool(_build_args(cutoff=None))

        fmt = tool._build_format_args()
        self.assertEqual("AND FALSE", fmt["ehr_consent_cutoff_not_validated_filter"])

        consent_query = queries.queries["ehr_consent"]["query"].format(**fmt)
        self.assertIn("AND FALSE", consent_query)
        self.assertNotIn("TIMESTAMP('')", consent_query)

    def test_ehr_consent_cutoff_filter_enabled_with_cutoff(self):
        tool = self._create_tool(_build_args(cutoff="2023-05-01"))

        fmt = tool._build_format_args()
        self.assertIn("TIMESTAMP('2023-05-01')", fmt["ehr_consent_cutoff_not_validated_filter"])

    def test_conflicting_pm_exclusion_flags_raise_value_error(self):
        tool = self._create_tool(
            _build_args(exclude_in_person_pm=True, exclude_remote_pm=True)
        )

        with self.assertRaisesRegex(ValueError, "cannot both be specified"):
            tool._build_format_args()

    def test_participant_list_file_adds_include_filter(self):
        tool = self._create_tool(_build_args(participant_list_file="participants.txt"))

        with mock.patch.object(
            tool,
            "_read_participant_ids_file",
            return_value=[123, 456],
        ) as read_mock:
            fmt = tool._build_format_args()

        self.assertEqual(
            "AND p.participant_id IN (123, 456)",
            fmt["participant_selection_filter"],
        )
        read_mock.assert_called_once_with("participants.txt")

    def test_empty_participant_list_file_disables_participant_filter_selection(self):
        tool = self._create_tool(_build_args(participant_list_file="participants.txt"))

        with mock.patch.object(
            tool,
            "_read_participant_ids_file",
            return_value=[],
        ):
            fmt = tool._build_format_args()

        self.assertEqual("AND FALSE", fmt["participant_selection_filter"])

    def test_exclude_participants_file_extends_exclude_filter(self):
        tool = self._create_tool(
            _build_args(exclude_pid_list=[111], exclude_participants="exclude.txt")
        )

        with mock.patch.object(
            tool,
            "_read_participant_ids_file",
            return_value=[222, 333],
        ) as read_mock:
            fmt = tool._build_format_args()

        self.assertEqual(
            "AND p.participant_id NOT IN (111, 222, 333)",
            fmt["exclude_pid_filter"],
        )
        read_mock.assert_called_once_with("exclude.txt")

    def test_participant_filter_query_includes_file_filters(self):
        tool = self._create_tool(
            _build_args(
                participant_list_file="participants.txt",
                exclude_participants="exclude.txt",
            )
        )

        with mock.patch.object(
            tool,
            "_read_participant_ids_file",
            side_effect=[[123, 456], [222]],
        ):
            fmt = tool._build_format_args()

        participant_filter_query = queries.queries["participant_filter"]["query"].format(**fmt)
        self.assertIn("AND p.participant_id IN (123, 456)", participant_filter_query)
        self.assertIn("AND p.participant_id NOT IN (222)", participant_filter_query)

    def test_omit_measurements_uses_empty_src_meas_query_and_truncates_table(self):
        tool = self._create_tool(_build_args(omit_measurements=True))
        tool.etl_process_steps = ["src_meas", "measurement"]

        with mock.patch.object(tool, "run_query") as run_query_mock:
            tool.run_etl()

        self.assertEqual(1, run_query_mock.call_count)
        query, job_config = run_query_mock.call_args.args

        self.assertIn("WHERE 1 = 0", query)
        self.assertEqual("test-project", job_config.destination.project)
        self.assertEqual("etl_dataset", job_config.destination.dataset_id)
        self.assertEqual("src_meas", job_config.destination.table_id)
        self.assertEqual(
            bigquery.job.WriteDisposition.WRITE_TRUNCATE,
            job_config.write_disposition,
        )

    def test_snapshot_audit_mode_dispatch(self):
        tool = self._create_tool(
            _build_args(snapshot_audit=True, audit_dataset="audit_dataset")
        )

        with mock.patch("rdr_service.tools.tool_libs.curation_bq.ToolBase.run"):
            with mock.patch.object(tool, "snapshot_audit_tables") as snapshot_mock:
                result = tool.run()

        self.assertIsNone(result)
        snapshot_mock.assert_called_once_with()

    def test_snapshot_audit_requires_audit_dataset(self):
        tool = self._create_tool(_build_args(snapshot_audit=True, audit_dataset=None))

        with mock.patch("rdr_service.tools.tool_libs.curation_bq.ToolBase.run"):
            result = tool.run()

        self.assertEqual(1, result)

    def test_snapshot_audit_conflicts_with_other_modes(self):
        tool = self._create_tool(
            _build_args(snapshot_audit=True, audit_dataset="audit_dataset", run_etl=True)
        )

        with mock.patch("rdr_service.tools.tool_libs.curation_bq.ToolBase.run"):
            result = tool.run()

        self.assertEqual(1, result)

    def test_snapshot_audit_generates_expected_sql(self):
        tool = self._create_tool(
            _build_args(
                snapshot_audit=True,
                audit_dataset="audit_dataset",
                audit_run_id="run-123",
                snapshot_label="manual",
                cutoff="2026-01-01",
            )
        )

        with mock.patch.object(tool, "run_query", return_value=10) as run_query_mock:
            tool.snapshot_audit_tables()

        self.assertEqual(6, run_query_mock.call_count)
        sql_calls = [call.args[0] for call in run_query_mock.call_args_list]

        self.assertTrue(any("participant_filter_snapshot" in sql for sql in sql_calls))
        self.assertTrue(any("observation_snapshot" in sql for sql in sql_calls))
        self.assertTrue(any("measurement_snapshot" in sql for sql in sql_calls))
        self.assertTrue(any("'run-123' AS audit_run_id" in sql for sql in sql_calls))
        self.assertTrue(any("'manual' AS snapshot_label" in sql for sql in sql_calls))
        self.assertTrue(any("'2026-01-01' AS etl_cutoff" in sql for sql in sql_calls))
        self.assertTrue(any("`test-project.audit_dataset.participant_filter_snapshot`" in sql for sql in sql_calls))

    def test_ppsc_consent_step_runs_after_src_clean_and_before_mapping(self):
        steps = CurationBQ.etl_process_steps
        ppsc_index = steps.index("ppsc_consent_src_clean")

        self.assertEqual(steps.index("src_clean") + 1, ppsc_index)
        self.assertLess(ppsc_index, steps.index("filter_surveys"))
        self.assertLess(ppsc_index, steps.index("src_mapped"))

    def test_ppsc_consent_step_appends_to_src_clean(self):
        tool = self._create_tool(_build_args())
        tool.etl_process_steps = ["ppsc_consent_src_clean"]

        with mock.patch.object(tool, "run_query") as run_query_mock:
            tool.run_etl()

        query, job_config = run_query_mock.call_args.args
        self.assertEqual("src_clean", job_config.destination.table_id)
        self.assertEqual(
            bigquery.job.WriteDisposition.WRITE_APPEND,
            job_config.write_disposition,
        )
        self.assertIn("`test-project.rdr_replica.ppsc_consent_event`", query)
        self.assertIn("'PPSC_Consent'", query)

    def test_ppsc_consent_query_maps_each_consent_type(self):
        fmt = self._create_tool(_build_args())._build_format_args()
        query = queries.queries["ppsc_consent_src_clean"]["query"].format(**fmt)

        for event_type, question_code, answer_type in (
            ("Primary Consent", "ExtraConsent_AgreeToConsent", "bool"),
            ("Pediatric Permission", "ExtraConsent_AgreeToConsent", "bool"),
            ("EHR Authorization", "EHRConsentPII_ConsentPermission", "code"),
            ("Adult EHR Authorization", "PPSC_AdultEHRAuthorization", "code"),
            ("Pediatric EHR Authorization", "PPSC_PediatricEHRAuthorization", "code"),
        ):
            self.assertRegex(
                query, rf"'{event_type}'(?: AS event_type_name)?,\s*'{question_code}'"
                       rf"(?: AS question_ppi_code)?,\s*'{answer_type}'"
            )
        for code in (
            "ConsentPermission_Yes", "ConsentPermission_No",
            "PPSC_AdultEHRAuthorization_Yes", "PPSC_AdultEHRAuthorization_No",
            "PPSC_PediatricEHRAuthorization_Yes", "PPSC_PediatricEHRAuthorization_No",
            "PPSC_ConsentOther",
        ):
            self.assertIn(f"'{code}'", query)
        self.assertNotIn("PPSC_PrimaryConsent_Yes", query)
        self.assertNotIn("PPSC_PediatricPermission_Yes", query)

    def test_ppsc_consent_query_encodes_bool_and_code_answers(self):
        fmt = self._create_tool(_build_args())._build_format_args()
        query = queries.queries["ppsc_consent_src_clean"]["query"].format(**fmt)

        self.assertIn("CONCAT('cln.', cc.answer_type)      AS unit_id", query)
        self.assertRegex(
            query,
            r"WHEN cc\.answer_type != 'bool'\s+THEN CAST\(NULL AS INT64\)\s+"
            r"WHEN cc\.consent_status = 'yes'\s+THEN 1\s+ELSE 0\s+END\s+AS value_boolean",
        )
        self.assertRegex(
            query,
            r"WHEN cc\.answer_type = 'bool'\s+THEN CAST\(NULL AS STRING\)\s+"
            r"WHEN cc\.consent_status = 'yes'\s+THEN cc\.yes_ppi_code\s+"
            r"WHEN cc\.consent_status = 'no'\s+THEN cc\.no_ppi_code",
        )

    def test_ppsc_consent_query_without_optional_filters(self):
        fmt = self._create_tool(_build_args())._build_format_args()
        query = queries.queries["ppsc_consent_src_clean"]["query"].format(**fmt)

        self.assertNotIn("ce.event_authored_time AS TIMESTAMP", query)
        self.assertNotIn("'PPSC_Consent' IN", query)

    def test_ppsc_consent_query_applies_cutoff_and_min_date(self):
        args = _build_args(cutoff="2025-06-01", min_survey_date="2020-01-01")
        fmt = self._create_tool(args)._build_format_args()
        query = queries.queries["ppsc_consent_src_clean"]["query"].format(**fmt)

        self.assertIn(
            "AND SAFE_CAST(ce.event_authored_time AS TIMESTAMP) < TIMESTAMP('2025-06-01')",
            query,
        )
        self.assertIn(
            "AND SAFE_CAST(ce.event_authored_time AS TIMESTAMP) >= TIMESTAMP('2020-01-01')",
            query,
        )

    def test_ppsc_consent_query_respects_survey_filters(self):
        include_fmt = self._create_tool(
            _build_args(include_surveys="TheBasics")
        )._build_format_args()
        exclude_fmt = self._create_tool(
            _build_args(exclude_surveys="PPSC_Consent")
        )._build_format_args()

        self.assertEqual(
            "AND 'PPSC_Consent' IN ('TheBasics')",
            include_fmt["ppsc_consent_survey_filter"],
        )
        self.assertEqual(
            "AND 'PPSC_Consent' NOT IN ('PPSC_Consent')",
            exclude_fmt["ppsc_consent_survey_filter"],
        )


if __name__ == "__main__":
    unittest.main()



