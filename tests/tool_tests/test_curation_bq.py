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


if __name__ == "__main__":
    unittest.main()



