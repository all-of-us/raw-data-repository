import os

from rdr_service.api_util import open_cloud_file
from rdr_service.dao.study_nph_dao import NphStoredSampleDao
from rdr_service.data_gen.generators.nph import NphDataGenerator
from rdr_service.offline.study_nph_biobank_import_inventory_file import import_biobank_inventory_into_stored_samples
from tests.helpers.unittest_base import BaseTestCase
from tests.test_data import data_path


class NphBiobankWorkflowsTest(BaseTestCase):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.test_bucket = "test-bucket"

    def setUp(self, *args, **kwargs) -> None:
        super().setUp(*args, **kwargs)
        self.nph_datagen = NphDataGenerator()

    def tearDown(self):
        super().tearDown()
        self.clear_table_after_test("nph.stored_sample")

    def create_cloud_csv(self, test_data_filename, file_name=None, bucket=None, prefix=None):

        test_file_path = data_path(test_data_filename, os.path.dirname(__file__), "test_data")

        with open(test_file_path) as f:
            lines = f.readlines()
            csv_str = ""
            for line in lines:
                csv_str += line

        bucket = self.test_bucket if bucket is None else bucket
        output_filename = test_data_filename if file_name is None else file_name
        if prefix is None:
            path = f"/{bucket}/{output_filename}"
        else:
            path = f"/{bucket}/{prefix}/{output_filename}"

        with open_cloud_file(path, mode='wb') as cloud_file:
            cloud_file.write(csv_str.encode("utf-8"))

        return cloud_file

    def test_import_biobank_inventory_into_stored_samples(self):
        self.nph_datagen.create_database_participant(
            id=101,
            biobank_id=11110000101
        )
        self.create_cloud_csv("test_nph_biobank_nightly_import_file_001.csv")

        import_biobank_inventory_into_stored_samples("test-bucket/test_nph_biobank_nightly_import_file_001.csv")

        ss_dao = NphStoredSampleDao()
        stored_samples = ss_dao.get_all()
        self.assertEqual(stored_samples[0].sample_id, "00005")
        self.assertEqual(stored_samples[1].sample_id, "00006")
