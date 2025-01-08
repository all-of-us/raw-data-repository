import mock
import os

from rdr_service import clock, config
from rdr_service.api_util import open_cloud_file
from rdr_service.dao.rex_dao import RexStudyDao
from rdr_service.dao.study_nph_dao import NphStoredSampleDao, NphSampleUpdateDao
from rdr_service.data_gen.generators.nph import NphDataGenerator, NphSmsDataGenerator
from rdr_service.model.study_nph import SampleUpdate
from rdr_service.offline.study_nph_biobank_file_export import (get_processing_timestamp,
                                                               main as study_nph_biobank_file_export_job)
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
        self.sms_datagen = NphSmsDataGenerator()
        self.sample_update_dao = NphSampleUpdateDao()
        self.created_date = clock.CLOCK.now()
        self.temporarily_override_config_setting(
            key=config.NPH_SAMPLE_DATA_BIOBANK_NIGHTLY_FILE_DROP,
            value=[self.test_bucket]
        )

    def setup_for_biobank_file_export(self, num_orders, num_ordered_samples, nph_p1, nph_p2):
        study_dao = RexStudyDao()
        aou = study_dao.model_type(schema_name='rdr')
        nph = study_dao.model_type(schema_name='nph')
        study_dao.insert(aou)
        study_dao.insert(nph)

        pa_state_code = self.data_generator.create_database_code(value="PIIState_PA")
        ny_state_code = self.data_generator.create_database_code(value="PIIState_NY")
        male_sex_code = self.data_generator.create_database_code(value="SexAtBirth_Male")
        female_sex_code = self.data_generator.create_database_code(value="SexAtBirth_Female")

        aou_participant_1 = self.data_generator.create_database_participant()
        aou_participant_2 = self.data_generator.create_database_participant()

        p1 = self.data_generator.create_database_participant_summary(
            participant=aou_participant_1,
            aian=0,
            sexId=male_sex_code.codeId,
            stateId=ny_state_code.codeId
        )
        p2 = self.data_generator.create_database_participant_summary(
            participant=aou_participant_2,
            aian=1,
            sexId=female_sex_code.codeId,
            stateId=pa_state_code.codeId
        )

        self.nph_datagen.create_database_rex_participant_mapping(
            primary_participant_id=aou_participant_1.participantId,
            ancillary_participant_id=nph_p1.id,
        )
        self.nph_datagen.create_database_rex_participant_mapping(
            primary_participant_id=aou_participant_2.participantId,
            ancillary_participant_id=nph_p2.id,
        )
        self.sms_datagen.create_database_study_category(
            name="3",
            type_label="module"
        )
        self.sms_datagen.create_database_study_category(
            name="Diet",
            type_label="visitType",
            parent_id=1
        )
        self.sms_datagen.create_database_study_category(
            name="Day 0",
            type_label="timepoint",
            parent_id=2
        )

        for i in range(num_orders):
            orders_p1 = self.sms_datagen.create_database_order(
                nph_order_id=f"10{nph_p1.id + (i + 1) + p1.biobankId}",
                participant_id=nph_p1.id,
                client_id=f"111{i}",
                notes={"collected": "test", "finalized": "test"},
                category_id=3,
                created=self.created_date,
                modified=self.created_date
            )

            orders_p2 = self.sms_datagen.create_database_order(
                nph_order_id=f"10{nph_p2.id + (i + 1) + p2.biobankId}",
                participant_id=nph_p2.id,
                client_id=f"222{i}",
                notes={"collected": "test", "finalized": "test"},
                category_id=3,
                created=self.created_date,
                modified=self.created_date
            )

            for _ in range(num_ordered_samples):
                sample_1 = self.sms_datagen.create_database_ordered_sample(
                    order_id=orders_p1.id,
                    nph_sample_id=f'11{p1.participantId}',
                    test='Test'
                )
                sample_2 = self.sms_datagen.create_database_ordered_sample(
                    order_id=orders_p2.id,
                    nph_sample_id=f'11{p2.participantId}',
                    test='Test'
                )
                update_entry_1 = SampleUpdate(created=self.created_date, rdr_ordered_sample_id=sample_1.id)
                update_entry_2 = SampleUpdate(created=self.created_date, rdr_ordered_sample_id=sample_2.id)
                self.sample_update_dao.insert(update_entry_1)
                self.sample_update_dao.insert(update_entry_2)

    def tearDown(self):
        super().tearDown()
        self.clear_table_after_test("nph.ordered_sample")
        self.clear_table_after_test("nph.order")
        self.clear_table_after_test("nph.study_category")
        self.clear_table_after_test("nph.participant")
        self.clear_table_after_test("nph.sample_update")
        self.clear_table_after_test("rex.participant_mapping")
        self.clear_table_after_test("rex.study")
        self.clear_table_after_test("rdr.participant_summary")

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
        self.assertEqual(stored_samples[0].specimen_volume_ul, None)
        self.assertEqual(stored_samples[0].freeze_thaw_count, 2)
        self.assertEqual(stored_samples[1].sample_id, "00006")
        self.assertEqual(stored_samples[1].specimen_volume_ul, 1000)
        self.assertEqual(stored_samples[1].freeze_thaw_count, 0)

    def test_no_error_on_timestamp(self):
        sample = self.sms_datagen.create_database_ordered_sample(
            nph_sample_id=10006,
            test="ST01",
            supplemental_fields={
                "bowelMovement": "I had normal formed stool, and my stool looks like Type 3 and/or 4",
                "bowelMovementQuality": "I tend to have normal formed stool - Type 3 and 4"
            },
            collected="2024-04-08T15:11:00",
            finalized="2024-05-09T15:11:00",
        )
        timestamp = get_processing_timestamp(sample)
        self.assertEqual(timestamp, sample.finalized)

    @mock.patch('rdr_service.storage.GoogleCloudStorageProvider.open')
    @mock.patch('rdr_service.offline.study_nph_biobank_file_export._get_crc32c_checksum_from_gcs_blob')
    def test_nph_biobank_file_export(self, blob_mock, gcp_mock):
        nph_p1 = self.nph_datagen.create_database_participant()
        nph_p2 = self.nph_datagen.create_database_participant()
        self.setup_for_biobank_file_export(2, 3, nph_p1, nph_p2)
        blob_mock.return_value = 'ABCDE=='
        response = study_nph_biobank_file_export_job()

        p1_order1, p1_order2 = response[0], response[2]
        p2_order1, p2_order2 = response[1], response[3]

        # Check that the file was uploaded
        self.assertEqual(gcp_mock.call_count, 1)

        self.assertEqual(p1_order1.get('participantID'), f"T{nph_p1.biobank_id}")
        self.assertEqual(p1_order1.get('gender'), 'Male')
        self.assertEqual(p1_order1.get('ai_an_flag'), 'N')
        self.assertEqual(p1_order1.get('collections')[0].get('nyFlag'), 'Y')

        self.assertEqual(p1_order2.get('participantID'), p1_order1.get('participantID'))
        self.assertEqual(p1_order2.get('gender'), 'Male')
        self.assertEqual(p1_order2.get('ai_an_flag'), 'N')
        self.assertEqual(p1_order2.get('collections')[0].get('nyFlag'), 'Y')

        self.assertEqual(p2_order1.get('participantID'), f"T{nph_p2.biobank_id}")
        self.assertEqual(p2_order1.get('gender'), 'Female')
        self.assertEqual(p2_order1.get('ai_an_flag'), 'Y')
        self.assertEqual(p2_order1.get('collections')[0].get('nyFlag'), 'N')

        self.assertEqual(p2_order2.get('participantID'), p2_order1.get('participantID'))
        self.assertEqual(p2_order2.get('gender'), 'Female')
        self.assertEqual(p2_order2.get('ai_an_flag'), 'Y')
        self.assertEqual(p2_order2.get('collections')[0].get('nyFlag'), 'N')
