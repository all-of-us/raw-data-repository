import csv
import os

from rdr_service.config import LOCALHOST_DEFAULT_BUCKET_NAME
from rdr_service.api_util import open_cloud_file, list_blobs
from rdr_service.dao.participant_dao import ParticipantDao
from rdr_service.offline.table_exporter import TableExporter
from rdr_service.participant_enums import make_primary_provider_link_for_name
from tests.helpers.unittest_base import BaseTestCase


class TableExporterTest(BaseTestCase):
    def setUp(self):
        super(TableExporterTest, self).setUp()

    def testDeidentifiedExport_empty(self):
        mock_export_sub_folder = 'dir'
        mock_bucket = [LOCALHOST_DEFAULT_BUCKET_NAME, LOCALHOST_DEFAULT_BUCKET_NAME + os.sep + mock_export_sub_folder]
        self.clear_default_storage()
        self.create_mock_buckets(mock_bucket)

        TableExporter.export_tables("rdr", ["ppi_participant_view"], mock_export_sub_folder, deidentify=True)

        blobs = list(list_blobs(LOCALHOST_DEFAULT_BUCKET_NAME))
        self.assertEqual(len(blobs), 1)
        blob = blobs[0]
        self.assertEqual(blob.name, 'dir/ppi_participant_view.csv')

    def testDeidentifiedExport_participantIds(self):
        mock_export_sub_folder = 'dir'
        mock_bucket = [LOCALHOST_DEFAULT_BUCKET_NAME, LOCALHOST_DEFAULT_BUCKET_NAME + os.sep + mock_export_sub_folder]
        self.clear_default_storage()
        self.create_mock_buckets(mock_bucket)

        p1 = self._participant_with_defaults(
            participantId=1, version=2, biobankId=2, providerLink=make_primary_provider_link_for_name("PITT")
        )
        ParticipantDao().insert(p1)
        p2 = self._participant_with_defaults(
            participantId=2, version=3, biobankId=3, providerLink=make_primary_provider_link_for_name("PITT")
        )
        ParticipantDao().insert(p2)

        TableExporter.export_tables("rdr", ["ppi_participant_view"], mock_export_sub_folder, deidentify=True)

        csv_path = 'dir/ppi_participant_view.csv'

        with open_cloud_file("/%s/%s" % (LOCALHOST_DEFAULT_BUCKET_NAME, csv_path)) as f:
            reader = csv.reader(f)
            rows = list(reader)[1:]
            self.assertEqual(2, len(rows))

            pmi_ids = set([p1.participantId, p2.participantId])
            obf_ids = set([row[0] for row in rows])
            self.assertFalse(pmi_ids.intersection(obf_ids), "should be no overlap between pmi_ids and obfuscated IDs")
            self.assertEqual(2, len(obf_ids))
