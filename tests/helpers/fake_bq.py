# This file is for faking a BigQuery client for unit tests

from datetime import date
from typing import Dict, List, Any


class Row(dict):
    __getattr__ = dict.get


class FakeBQJob:
    def __init__(self, rows): self._rows = rows

    def result(self): return self._rows


class FakeBQClient:
    """
    This is currently only used for the Biospecimen API.
    Future iterations should generalize this if usage become widespread
    """

    def __init__(self, partitions: Dict[date, List[Dict[str, Any]]]):
        self.partitions = partitions  # {date: [rows...]}

    def _max_run_date(self):
        return max(self.partitions) if self.partitions else None

    def query(self, sql: str, job_config=None):
        # Parameters used in the query to fake
        params = {}
        if job_config and getattr(job_config, "query_parameters", None):
            params = {p.name: p.value for p in job_config.query_parameters}

        # Handle SELECT MAX(run_date) AS d
        if "SELECT MAX(run_date) AS d" in sql:
            return FakeBQJob([Row({"d": self._max_run_date()})])

        # Determine run_date
        run_date = params.get("run_date", None)
        if run_date is None:
            run_date = self._max_run_date()

        # Base rows from chosen partition
        rows = [Row(r) for r in sorted(
            self.partitions.get(run_date, []),
            key=lambda r: r["nph_participant_id"]
        )]

        # Single participant path
        if "AND nph_participant_id = @pid" in sql:
            pid = params["pid"]
            rows = [r for r in rows if r["nph_participant_id"] == pid]
            return FakeBQJob(rows)

        # List path (cursor + limit)
        cursor = params.get("cursor", None)
        if cursor is not None:
            rows = [r for r in rows if r["nph_participant_id"] > cursor]

        limit_and_1 = params.get("limit_and_1") or params.get("limit_plus1") or 1000
        rows = rows[: int(limit_and_1)]
        return FakeBQJob(rows)
