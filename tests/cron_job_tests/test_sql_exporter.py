import csv

from rdr_service.offline.sql_exporter import SqlExporter
from rdr_service.participant_enums import UNSET_HPO_ID
from tests.helpers.mysql_helper_data import AZ_HPO_ID, PITT_HPO_ID
from tests.helpers.unittest_base import BaseTestCase
from rdr_service.api_util import open_cloud_file

_BUCKET_NAME = "pmi-drc-biobank-test.appspot.com"
_FILE_NAME = "hpo_ids.csv"


class SqlExporterTest(BaseTestCase):

    mock_bucket_paths = [_BUCKET_NAME]

    def testHpoExport_withoutRows(self):
        self.clear_default_storage()
        self.create_mock_buckets(self.mock_bucket_paths)

        SqlExporter(_BUCKET_NAME).run_export(_FILE_NAME, "SELECT hpo_id id, name name FROM hpo LIMIT 0")

        assert_csv_contents(self, _BUCKET_NAME, _FILE_NAME, [["id", "name"]])

    def testHpoExport_withRows(self):
        self.clear_default_storage()
        self.create_mock_buckets(self.mock_bucket_paths)

        SqlExporter(_BUCKET_NAME).run_export(_FILE_NAME, "SELECT hpo_id id, name name FROM hpo ORDER BY hpo_id")
        assert_csv_contents(
            self,
            _BUCKET_NAME,
            _FILE_NAME,
            [["id", "name"], [str(UNSET_HPO_ID), "UNSET"], [str(AZ_HPO_ID), "AZ_TUCSON"], [str(PITT_HPO_ID), "PITT"]],
        )


def assert_csv_contents(test, bucket_name, file_name, contents):
    with open_cloud_file("/%s/%s" % (bucket_name, file_name)) as f:
        reader = csv.reader(f)
        rows = sorted(reader)

    test.assertEqual(sorted(contents), rows)
