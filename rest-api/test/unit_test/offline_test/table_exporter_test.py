import csv
import os

from cloudstorage import cloudstorage_api
from dao.participant_dao import ParticipantDao, make_primary_provider_link_for_name
from offline.table_exporter import TableExporter
from offline_test.gcs_utils import assertCsvContents
from unit_test_util import CloudStorageSqlTestBase, FlaskTestBase
from google.appengine.ext import deferred

class TableExporterTest(CloudStorageSqlTestBase, FlaskTestBase):
  def setUp(self):
    super(TableExporterTest, self).setUp(with_views=True)

  def testDeidentifiedExport_empty(self):
    TableExporter.export_tables('rdr', ['ppi_participant_view'], 'dir', deidentify=True)

    tasks = self.taskqueue_stub.get_filtered_tasks()
    self.assertEqual(len(tasks), 1)
    csv_path = deferred.run(tasks[0].payload)

    assertCsvContents(self, os.path.dirname(csv_path), os.path.basename(csv_path),
                      [['participant_id', 'hpo', 'enrollment_status']])

  def testDeidentifiedExport_participantIds(self):
    TableExporter.export_tables('rdr', ['ppi_participant_view'], 'dir', deidentify=True)

    p1 = self._participant_with_defaults(
        participantId=1,
        version=2,
        biobankId=2,
        providerLink=make_primary_provider_link_for_name('PITT'))
    ParticipantDao().insert(p1)
    p2 = self._participant_with_defaults(
        participantId=2,
        version=3,
        biobankId=3,
        providerLink=make_primary_provider_link_for_name('PITT'))
    ParticipantDao().insert(p2)

    tasks = self.taskqueue_stub.get_filtered_tasks()
    self.assertEqual(len(tasks), 1)
    csv_path = deferred.run(tasks[0].payload)

    with cloudstorage_api.open('/' + csv_path, mode='r') as output:
      reader = csv.reader(output)
      rows = list(reader)[1:]
    self.assertEqual(2, len(rows))

    pmi_ids = set([p1.participantId, p2.participantId])
    obf_ids = set([row[0] for row in rows])
    self.assertFalse(pmi_ids.intersection(obf_ids),
                     'should be no overlap between pmi_ids and obfuscated IDs')
    self.assertEquals(2, len(obf_ids))
