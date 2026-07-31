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
        "omit_measurements": False,
        "omit_surveys": False,
        "dry_run": True,
        "load_data": False,
        "run_etl": False,
        "export": False,
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


if __name__ == "__main__":
    unittest.main()


