import collections
import datetime
import dateutil
import re

import pytz
from rdr_service.config import GAE_PROJECT
from googleapiclient.discovery import build


class BigQueryJob(object):
    """
  Executes a BigQuery Job and handles iterating over result pages.

  DESIGNED FOR READ-ONLY USE AS OF 2019-05-28 (remove this line when updated for writes)
  """

    JobReference = collections.namedtuple("JobReference", ["projectId", "jobId", "location"])

    def __init__(
        self,
        query,
        project_id=None,
        default_dataset_id=None,
        use_legacy_sql=False,
        retry_count=10,
        socket_timeout=10 * 60 * 1000,  # 10 minutes
        page_size=1000,
    ):
        self.query = query
        self.project_id = project_id or GAE_PROJECT
        self.default_dataset_id = default_dataset_id
        self.use_legacy_sql = use_legacy_sql
        self.retry_count = retry_count
        self.socket_timeout = socket_timeout
        self.page_size = page_size
        # https://github.com/pior/appsecrets/issues/7
        # https://github.com/googleapis/google-api-python-client/issues/299
        self._service = build("bigquery", "v2", cache_discovery=False)
        self._job_ref = None
        self._page_token = None

    def __iter__(self):
        self._page_token = None
        return self

    def __next__(self):
        if self._job_ref is None:
            return self.get_rows_from_response(self.start_job())
        elif self._page_token is None:
            raise StopIteration()
        else:
            return self.get_rows_from_response(self.get_job_results(page_token=self._page_token))

    def next(self):
        return self.__next__()

    def _make_job_body(self):
        return {
            "defaultDataset": {"projectId": self.project_id, "datasetId": self.default_dataset_id},
            "query": self.query,
            "timeoutMs": self.socket_timeout,
            "useLegacySql": self.use_legacy_sql,
            "maxResults": self.page_size,
        }

    def start_job(self):
        job_body = self._make_job_body()
        result = (
            self._service.jobs().query(projectId=self.project_id, body=job_body).execute(num_retries=self.retry_count)
        )
        self._job_ref = self.JobReference(**result["jobReference"])

        if not result['jobComplete'] and 'errors' not in result:
            result = self._retrieve_query_results()

        page_token = result.get("pageToken")
        if page_token:
            self._page_token = page_token
        return result

    def get_job_results(self, page_token=None):
        result = self._retrieve_query_results(page_token)
        page_token = result.get("pageToken")
        self._page_token = page_token
        return result

    def _retrieve_query_results(self, page_token=None):
        kwargs = dict(projectId=self._job_ref.projectId, jobId=self._job_ref.jobId, maxResults=self.page_size)
        if page_token is not None:
            kwargs["pageToken"] = page_token
        result = self._service.jobs().getQueryResults(**kwargs).execute()

        max_checks = 100
        check_count = 0
        while not result['jobComplete'] and 'errors' not in result and check_count < max_checks:
            check_count += 1
            result = self._service.jobs().getQueryResults(**kwargs).execute()

        return result

    @classmethod
    def _merge_schema_data(cls, schema, row):
        """ Recursive function to merge schema and values together """
        data = collections.OrderedDict()
        tmp_row = list(zip(schema, row["f"]))

        for field, value in tmp_row:
            if field["mode"] == "REPEATED":
                sub_rows = value["v"]
                data[field["name"]] = list()
                for sub_row in sub_rows:
                    data[field["name"]].append(cls._merge_schema_data(field["fields"], sub_row["v"]))
            else:
                data[field["name"]] = value["v"]

        return data

    @classmethod
    def get_rows(cls, response):
        """ an alternate method to get rows from a response object """
        if not response:
            raise ValueError("invalid parameter value for response")

        rows = list()
        schema = response["schema"]["fields"]
        for row in response["rows"]:
            rows.append(cls._merge_schema_data(schema, row))

        return rows

    @classmethod
    def get_rows_from_response(cls, response):
        """
    Make a `list` of `Row` (namedtuple) objects representing the bigquery response dict structure.
    This list allows for cleaner usage of common result access patterns.

    :param response: Response `dict` structure from a call to `bigquery()`
    :rtype: list


    # NOTE: This structure will need to be made recursive to allow for nested structures in results
    """
        field_names = [f["name"] for f in response["schema"]["fields"]]
        field_types = [f["type"] for f in response["schema"]["fields"]]
        Row = collections.namedtuple("Row", field_names)
        return [
            Row(*[cls.parse_value(typename, field["v"]) for field, typename in zip(row["f"], field_types)])
            for row in response["rows"]
        ]

    @classmethod
    def parse_value(cls, typename, value):
        """
    from https://cloud.google.com/bigquery/docs/reference/standard-sql/data-types

    > STRING, BYTES, INTEGER, INT64 (same as INTEGER), FLOAT, FLOAT64 (same as FLOAT),
    > BOOLEAN, BOOL (same as BOOLEAN), TIMESTAMP, DATE, TIME, DATETIME,
    > RECORD (where RECORD indicates that the field contains a nested schema)
    > or STRUCT (same as RECORD).
    """
        if value is None:
            return None
        mapping = cls.get_parsers_by_typename_map()
        parser = mapping.get(typename)
        if not parser:
            raise NotImplemented("No parser implemented for type: {}".format(typename))
        return parser(value)

    @staticmethod
    def _parse_passthrough(value):
        return value

    @staticmethod
    def _parse_int(value):
        return int(value.replace(",", ""))

    @staticmethod
    def _parse_float(value):
        return float(value.replace(",", ""))

    @staticmethod
    def _parse_bool(value):
        return value.lower() == "true"

    @staticmethod
    def _parse_timestamp(value):
        try:
            if not value:
                return None
            date = datetime.datetime.strptime(re.sub(r"\bT\b", "", str(value)), "%Y-%m-%d %H:%M:%S.%f %Z")
            if date.tzinfo is None:
                date = date.replace(tzinfo=pytz.UTC)
            return date
        except ValueError:
            try:
                return datetime.datetime.utcfromtimestamp(float(value)).replace(tzinfo=pytz.UTC)
            except (ValueError, TypeError):
                raise ValueError("Could not parse {} as TIMESTAMP".format(value))

    @classmethod
    def get_parsers_by_typename_map(cls):
        return {
            "STRING": cls._parse_passthrough,
            "BYTES": cls._parse_passthrough,
            "INTEGER": cls._parse_int,
            "INT64": cls._parse_int,
            "FLOAT": cls._parse_float,
            "FLOAT64": cls._parse_float,
            "BOOLEAN": cls._parse_bool,
            "BOOL": cls._parse_bool,
            "TIMESTAMP": cls._parse_timestamp,
            "DATE": lambda v: dateutil.parser.parse(v).date(),
            "TIME": lambda v: dateutil.parser.parse(v).time(),
            "DATETIME": lambda v: dateutil.parser.parse(v),
        }
