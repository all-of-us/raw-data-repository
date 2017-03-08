import csv

from dao.hpo_dao import HPODao
from model.hpo import HPO
from offline.sql_exporter import SqlExporter
from participant_enums import UNSET_HPO_ID
from offline_test.gcs_utils import assertCsvContents
from testlib import testutil
from unit_test_util import SqlTestBase, PITT_HPO_ID

BUCKET_NAME = 'pmi-drc-biobank-test.appspot.com'
FILE_NAME = "hpo_ids.csv"

class SqlExporterTest(testutil.CloudStorageTestBase):

  def setUp(self):
    super(SqlExporterTest, self).setUp()
    testutil.HandlerTestBase.setUp(self)
    SqlTestBase.setup_database()

  def tearDown(self):
    super(SqlExporterTest, self).tearDown()
    SqlTestBase.teardown_database()

  def testHpoExport_withoutRows(self):
    SqlExporter(BUCKET_NAME).run_export(FILE_NAME,
                                        "SELECT hpo_id id, name name FROM hpo ORDER BY hpo_id")

    assertCsvContents(self, BUCKET_NAME, FILE_NAME, [['id', 'name']])

  def testHpoExport_withRows(self):
    SqlTestBase.setup_hpos()
    SqlExporter(BUCKET_NAME).run_export(FILE_NAME,
                                        "SELECT hpo_id id, name name FROM hpo ORDER BY hpo_id")
    assertCsvContents(self, BUCKET_NAME, FILE_NAME, [['id', 'name'],
                                                     [str(UNSET_HPO_ID), 'UNSET'],
                                                     [str(PITT_HPO_ID), 'PITT']])
