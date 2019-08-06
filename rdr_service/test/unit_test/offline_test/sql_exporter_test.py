from rdr_service.offline.sql_exporter import SqlExporter
from rdr_service.participant_enums import UNSET_HPO_ID
from rdr_service.test.unit_test.offline_test.gcs_utils import assertCsvContents
from rdr_service.test.unit_test.unit_test_util import AZ_HPO_ID, CloudStorageSqlTestBase, PITT_HPO_ID

_BUCKET_NAME = "pmi-drc-biobank-test.appspot.com"
_FILE_NAME = "hpo_ids.csv"


class SqlExporterTest(CloudStorageSqlTestBase):
    def testHpoExport_withoutRows(self):
        SqlExporter(_BUCKET_NAME).run_export(_FILE_NAME, "SELECT hpo_id id, name name FROM hpo LIMIT 0")

        assertCsvContents(self, _BUCKET_NAME, _FILE_NAME, [["id", "name"]])

    def testHpoExport_withRows(self):
        SqlExporter(_BUCKET_NAME).run_export(_FILE_NAME, "SELECT hpo_id id, name name FROM hpo ORDER BY hpo_id")
        assertCsvContents(
            self,
            _BUCKET_NAME,
            _FILE_NAME,
            [["id", "name"], [str(UNSET_HPO_ID), "UNSET"], [str(AZ_HPO_ID), "AZ_TUCSON"], [str(PITT_HPO_ID), "PITT"]],
        )
