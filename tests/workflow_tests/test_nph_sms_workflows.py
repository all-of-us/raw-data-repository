import csv
import datetime
import os
from unittest import mock

from rdr_service import api_util, clock
from rdr_service.api_util import open_cloud_file
from rdr_service.dao.study_nph_sms_dao import SmsSampleDao, SmsN0Dao, SmsN1Mc1Dao, SmsJobRunDao
from rdr_service.data_gen.generators.nph import NphSmsDataGenerator
from rdr_service.workflow_management.nph.sms_pipeline import n1_generation
from tests.helpers.unittest_base import BaseTestCase
from rdr_service.workflow_management.nph.sms_workflows import SmsWorkflow
from tests.test_data import data_path


class NphSmsWorkflowsTest(BaseTestCase):
    def __init__(self, *args, **kwargs):
        super(NphSmsWorkflowsTest, self).__init__(*args, **kwargs)
        self.test_bucket = "test-bucket"
        self.TIME_1 = datetime.datetime(2023, 4, 25, 15, 13)
        self.TIME_2 = datetime.datetime(2023, 4, 26, 15, 13)

    def setUp(self, *args, **kwargs) -> None:
        super(NphSmsWorkflowsTest, self).setUp(*args, **kwargs)

    def tearDown(self):
        super().tearDown()
        self.clear_table_after_test("nph.sms_job_run")
        self.clear_table_after_test("nph.sms_sample")
        self.clear_table_after_test("nph.sms_n0")
        self.clear_table_after_test("nph.sms_n1_mc1")

    def create_cloud_csv(self, test_data_filename, file_name, bucket=None, prefix=None):

        test_file_path = data_path(test_data_filename, os.path.dirname(__file__), "test_data")

        with open(test_file_path) as f:
            lines = f.readlines()
            csv_str = ""
            for line in lines:
                csv_str += line

        bucket = self.test_bucket if bucket is None else bucket
        if prefix is None:
            path = f"/{bucket}/{file_name}"
        else:
            path = f"/{bucket}/{prefix}/{file_name}"

        with open_cloud_file(path, mode='wb') as cloud_file:
            cloud_file.write(csv_str.encode("utf-8"))

        return cloud_file

    def test_sample_list_ingestion(self):

        # Ingestion Test File - RTI Pull List
        self.create_cloud_csv("test_sample_list.csv", "test_sample_list.csv")

        ingestion_data = {
            "job": "FILE_INGESTION",
            "file_type": "SAMPLE_LIST",
            "file_path": f"{self.test_bucket}/test_sample_list.csv"
        }
        workflow = SmsWorkflow(ingestion_data)
        workflow.execute_workflow()

        # Check job run
        run_dao = SmsJobRunDao()
        job_run = run_dao.get(1)

        self.assertEqual(job_run.result, 'SUCCESS')

        sample_dao = SmsSampleDao()
        ingested_record = sample_dao.get(1)

        # Test Data inserted correctly
        self.assertEqual(ingested_record.job_run_id, 1)
        self.assertEqual(ingested_record.sample_id, "10001")
        self.assertEqual(ingested_record.lims_sample_id, "5847307831")
        self.assertEqual(ingested_record.plate_number, "1")
        self.assertEqual(ingested_record.position, "A1")
        self.assertEqual(ingested_record.labware_type, "Matrix96_Blue")
        self.assertEqual(ingested_record.sample_identifier, "C_S_5847307831_M1_L_TP1")
        self.assertEqual(ingested_record.diet, "LMT")
        self.assertEqual(ingested_record.sex_at_birth, "Intersex")
        self.assertEqual(ingested_record.bmi, "38")
        self.assertEqual(ingested_record.age, "21")
        self.assertEqual(ingested_record.race, "Native Hawaiian or other Pacific Islander")
        self.assertEqual(ingested_record.ethnicity, "Black, African American or African")
        self.assertEqual(ingested_record.destination, "UNC_META")

        # Attempt ingestion again to ensure we don't ingest duplicates
        workflow.execute_workflow()
        all_samples = sample_dao.get_all()
        self.assertEqual(len(all_samples), 3)

    @mock.patch("rdr_service.services.ancillary_studies.nph_incident.SlackMessageHandler.send_message_to_webhook")
    def test_sample_list_not_ingested_on_bmi_validation_failure(self, mock_send_message_to_webhook):
        mock_send_message_to_webhook.return_value = True

        self.create_cloud_csv("test_sample_list.csv", "test_sample_list.csv")

        # Define and add new record where bmi violates the validation rule
        new_record = {
            "sample_id": "10004",
            "lims_sample_id": "7013747927",
            "plate_number": "1",
            "position": "C2",
            "labware_type": "Matrix96_Blue",
            "blank": "NA",
            "sample_identifier": "C_S_7013747927_M1_L_TP5",
            "diet": "LMT",
            "sex_at_birth": "Intersex",
            "bmi": "39.888",
            "age": "21",
            "race": "Black, African American or African",
            "ethnicity": "Asian",
            "destination": "UNC_META",
        }
        with open_cloud_file(f'/{self.test_bucket}/test_sample_list.csv', mode="a") as cf:
            cf.write(','.join(new_record.values()))

        ingestion_data = {
            "job": "FILE_INGESTION",
            "file_type": "SAMPLE_LIST",
            "file_path": f"{self.test_bucket}/test_sample_list.csv"
        }

        from rdr_service.resource import main as resource_main
        with self.assertLogs(level="ERROR") as cm:
            self.send_post(
                local_path='NphSmsIngestionTaskApi',
                request_data=ingestion_data,
                prefix="/resource/task/",
                test_client=resource_main.app.test_client(),
            )
            self.assertIn('Validation failed', cm.output[0])

    def test_n0_ingestion(self):

        # Ingestion Test File - Biobank N0 manifest
        self.create_cloud_csv("test_n0.csv", "test_n0_2023-4-20.csv")

        ingestion_data = {
            "job": "FILE_INGESTION",
            "file_type": "N0",
            "file_path": f"{self.test_bucket}/test_n0_2023-4-20.csv"
        }
        from rdr_service.resource import main as resource_main
        self.send_post(
            local_path='NphSmsIngestionTaskApi',
            request_data=ingestion_data,
            prefix="/resource/task/",
            test_client=resource_main.app.test_client(),
        )

        n0_dao = SmsN0Dao()
        ingested_records = n0_dao.get_all()
        self.assertEqual(3, len(ingested_records))

        # Test Data inserted correctly
        self.assertEqual(ingested_records[0].lims_sample_id, "00000000000")
        self.assertEqual(ingested_records[0].matrix_id, "MC8888888888")
        self.assertEqual(ingested_records[0].biobank_id, "N222222222")
        self.assertEqual(ingested_records[0].sample_id, "2222222222")
        self.assertEqual(ingested_records[0].study, None)
        self.assertEqual(ingested_records[0].visit, None)
        self.assertEqual(ingested_records[0].timepoint, None)
        self.assertEqual(ingested_records[0].collection_site, None)
        self.assertEqual(ingested_records[0].collection_date_time, api_util.parse_date("2023-04-06T03:05:55"))
        self.assertEqual(ingested_records[0].sample_type, None)
        self.assertEqual(ingested_records[0].additive_treatment, "EDTA")
        self.assertEqual(ingested_records[0].quantity_ml, "1")
        self.assertEqual(ingested_records[0].manufacturer_lot, "256837")
        self.assertEqual(ingested_records[0].well_box_position, "D8")
        self.assertEqual(ingested_records[0].storage_unit_id, "SU-##########")
        self.assertEqual(ingested_records[0].package_id, "PKG-YYMM-######")
        self.assertEqual(ingested_records[0].tracking_number, "xxxxxxxxxxxx")
        self.assertEqual(ingested_records[0].shipment_storage_temperature, "-80C")
        self.assertEqual(ingested_records[0].sample_comments, "Arrived amibent")
        self.assertEqual(ingested_records[0].age, "32")

    @staticmethod
    def create_data_n1_mc1_generation(destination='UNC_META'):
        sms_datagen = NphSmsDataGenerator()

        # Urine Sample
        sms_datagen.create_database_ordered_sample(
            nph_sample_id=10001,
            supplemental_fields={"color": "Color 4", "clarity": "Clean"}
        )
        # Stool Sample
        sms_datagen.create_database_ordered_sample(
            nph_sample_id=10002,
            supplemental_fields={
                "bowelMovement": "I had normal formed stool, and my stool looks like Type 3 and/or 4",
                "bowelMovementQuality": "I tend to have normal formed stool - Type 3 and 4"
            }
        )

        sms_datagen.create_database_sms_sample(
            ethnicity="test",
            race="test",
            bmi="28",
            diet="LMT",
            sex_at_birth="M",
            sample_identifier="test",
            sample_id=10001,
            lims_sample_id="000200",
            destination=destination
        )
        sms_datagen.create_database_sms_sample(
            ethnicity="test",
            race="test",
            bmi="28",
            diet="LMT",
            sex_at_birth="M",
            sample_identifier="test",
            sample_id=10002,
            lims_sample_id="000200",
            destination=destination
        )
        sms_datagen.create_database_sms_sample(
            ethnicity="test",
            race="test",
            bmi="28",
            diet="LMT",
            sex_at_birth="M",
            sample_identifier="test",
            sample_id=10003,
            lims_sample_id="000200",
            destination=destination
        )

        sms_datagen.create_database_sms_n0(
            sample_id=10001,
            matrix_id=1111,
            package_id="test",
            storage_unit_id="test",
            file_path=f"{destination}_n0_test.csv",
            well_box_position="A1",
            tracking_number="test",
            sample_comments="test",
            study="test",
            visit="1",
            timepoint="LMT",
            collection_site="UNC",
            collection_date_time="2023-04-20T15:54:33",
            sample_type="Urine",
            additive_treatment="test-treatment",
            quantity_ml="120",
            manufacturer_lot='256837',
            age="22",
            biobank_id="test",
        )
        sms_datagen.create_database_sms_n0(
            sample_id=10002,
            matrix_id=1112,
            package_id="test",
            storage_unit_id="test",
            file_path=f"{destination}_n0_test.csv",
            well_box_position="A2",
            tracking_number="test",
            sample_comments="test",
            study="test",
            visit="1",
            timepoint="LMT",
            collection_site="UNC",
            collection_date_time="2023-04-20T15:54:33",
            sample_type="Stool",
            additive_treatment="test-treatment",
            quantity_ml="120",
            manufacturer_lot='256838',
            age="22",
            biobank_id="test",
        )
        sms_datagen.create_database_sms_n0(
            sample_id=10003,
            matrix_id=1111,
            package_id="test",
            storage_unit_id="test",
            file_path=f"{destination}_n0_test.csv",
            well_box_position="A3",
            tracking_number="test",
            sample_comments="test",
            study="test",
            visit="1",
            timepoint="LMT",
            collection_site="UNC",
            collection_date_time="2023-04-20T15:54:33",
            sample_type="Urine",
            additive_treatment="test-treatment",
            quantity_ml="120",
            manufacturer_lot='256837',
            age="22",
            biobank_id="test",
        )

        sms_datagen.create_database_sms_blocklist(
            identifier_value=10003,
            identifier_type='sample_id'
        )

        # blank well
        sms_datagen.create_database_sms_n0(
            package_id="test",
            storage_unit_id="test",
            file_path=f"{destination}_n0_test.csv",
            well_box_position="A4"
        )

    def test_n1_mc1_generation(self):
        self.create_data_n1_mc1_generation()

        generation_data = {
            "job": "FILE_GENERATION",
            "file_type": "N1_MC1",
            "recipient": "UNC_META",
            "package_id": "test"
        }
        with clock.FakeClock(self.TIME_1):
            from rdr_service.resource import main as resource_main
            self.send_post(
                local_path='NphSmsGenerationTaskApi',
                request_data=generation_data,
                prefix="/resource/task/",
                test_client=resource_main.app.test_client(),
            )

        expected_csv_path = "test-bucket-unc-meta/n1_manifests/UNC_META_n1_2023-04-25T15:13:00.csv"

        with open_cloud_file(expected_csv_path, mode='r') as cloud_file:
            csv_reader = csv.DictReader(cloud_file)
            csv_rows = list(csv_reader)

        self.assertEqual(csv_rows[0]['sample_id'], '10001')
        self.assertEqual(csv_rows[0]['matrix_id'], "1111")
        self.assertEqual(csv_rows[0]['urine_color'], '"Color 4"')
        self.assertEqual(csv_rows[0]['urine_clarity'], '"Clean"')
        self.assertEqual(csv_rows[0]['manufacturer_lot'], '256837')

        n1_mcac_dao = SmsN1Mc1Dao()
        manifest_records = n1_mcac_dao.get_all()
        self.assertEqual(len(manifest_records), 3)
        self.assertEqual(manifest_records[0].file_path, expected_csv_path)
        self.assertEqual(manifest_records[0].sample_id, "10001")
        self.assertEqual(manifest_records[0].matrix_id, "1111")
        self.assertEqual(manifest_records[0].bmi, "28")
        self.assertEqual(manifest_records[0].diet, "LMT")
        self.assertEqual(manifest_records[0].collection_site, "UNC")
        self.assertEqual(manifest_records[0].collection_date_time, api_util.parse_date("2023-04-20T15:54:33"))
        self.assertEqual(manifest_records[0].urine_color, '"Color 4"')
        self.assertEqual(manifest_records[0].urine_clarity, '"Clean"')
        self.assertEqual(manifest_records[0].manufacturer_lot, '256837')

        self.assertEqual(manifest_records[1].file_path, expected_csv_path)
        self.assertEqual(manifest_records[1].sample_id, "10002")
        self.assertEqual(manifest_records[1].matrix_id, "1112")
        self.assertEqual(manifest_records[1].bmi, "28")
        self.assertEqual(manifest_records[1].diet, "LMT")
        self.assertEqual(manifest_records[1].collection_site, "UNC")
        self.assertEqual(manifest_records[1].manufacturer_lot, '256838')
        self.assertEqual(manifest_records[1].collection_date_time, api_util.parse_date("2023-04-20T15:54:33"))
        self.assertEqual(manifest_records[1].bowel_movement, '"I had normal formed stool, and my stool looks like Type 3 and/or 4"')
        self.assertEqual(manifest_records[1].bowel_movement_quality, '"I tend to have normal formed stool - Type 3 and 4"')

        self.assertEqual(manifest_records[2].well_box_position, "A4")

        # Test Ignore and rerun
        with n1_mcac_dao.session() as session:
            manifest_records[0].ignore_flag = 1
            session.merge(manifest_records[0])

        with clock.FakeClock(self.TIME_2):
            from rdr_service.resource import main as resource_main
            self.send_post(
                local_path='NphSmsGenerationTaskApi',
                request_data=generation_data,
                prefix="/resource/task/",
                test_client=resource_main.app.test_client(),
            )
        manifest_records = n1_mcac_dao.get_all()
        self.assertEqual(len(manifest_records), 4)

    @mock.patch("rdr_service.services.ancillary_studies.nph_incident.SlackMessageHandler.send_message_to_webhook")
    def test_n1_mc1_raises_error_on_data_validation_failure(self, mock_send_message_to_webhook):
        mock_send_message_to_webhook.return_value = True

        self.create_data_n1_mc1_generation()
        sms_datagen = NphSmsDataGenerator()

        # Create n0 with biobank id, but missing other values, like sample_id, diet, age, etc.
        sms_datagen.create_database_sms_n0(
            package_id="test",
            storage_unit_id="test",
            file_path="UNC_META_n0_test.csv",
            well_box_position="A5",
            biobank_id="test")

        generation_data = {
            "job": "FILE_GENERATION",
            "file_type": "N1_MC1",
            "recipient": "UNC_META",
            "package_id": "test"
        }
        with self.assertLogs(level="ERROR") as cm:
            from rdr_service.resource import main as resource_main
            self.send_post(
                local_path='NphSmsGenerationTaskApi',
                request_data=generation_data,
                prefix="/resource/task/",
                test_client=resource_main.app.test_client(),
            )
            self.assertIn('Validation failed', cm.output[0])

    def test_n1_mcc_tab_delimited(self):
        self.create_data_n1_mc1_generation(destination="UCSD")

        generation_data = {
            "job": "FILE_GENERATION",
            "file_type": "N1_MC1",
            "recipient": "UCSD",
            "package_id": "test"
        }
        with clock.FakeClock(self.TIME_1):
            from rdr_service.resource import main as resource_main
            self.send_post(
                local_path='NphSmsGenerationTaskApi',
                request_data=generation_data,
                prefix="/resource/task/",
                test_client=resource_main.app.test_client(),
            )

        expected_csv_path = "test-bucket-unc-meta/n1_manifests/UCSD_n1_2023-04-25T15:13:00.txt"

        with open_cloud_file(expected_csv_path, mode='r') as cloud_file:
            csv_reader = csv.DictReader(cloud_file, delimiter='\t')
            csv_rows = list(csv_reader)

        self.assertEqual(csv_rows[0]['sample_id'], '10001')
        self.assertEqual(csv_rows[0]['matrix_id'], "1111")
        self.assertEqual(csv_rows[0]['urine_color'], '"Color 4"')
        self.assertEqual(csv_rows[0]['urine_clarity'], '"Clean"')
        self.assertEqual(csv_rows[0]['manufacturer_lot'], '256837')

    @mock.patch('rdr_service.workflow_management.nph.sms_pipeline.GCPCloudTask.execute')
    def test_sms_pipeline_n1_function(self, task_mock):
        self.create_data_n1_mc1_generation(destination="UNC_META")
        data = {
            "file_type": "N1_MC1",
            "job": "FILE_GENERATION",
            "recipient": "UNC_META",
            "package_id": "test"
        }
        n1_generation()
        task_mock.assert_called_with('nph_sms_generation_task',
                                     payload=data,
                                     queue='nph')

    def test_sms_ingestion_filetype_failure(self):

        # Ingestion Test File - RTI Pull List
        self.create_cloud_csv("test_sample_list_fail.txt", "test_sample_list_fail.txt")
        ingestion_data = {
            "job": "FILE_INGESTION",
            "file_type": "SAMPLE_LIST",
            "file_path": f"{self.test_bucket}/test_sample_list_fail.txt"
        }
        workflow = SmsWorkflow(ingestion_data)
        workflow.execute_workflow()

        from rdr_service.resource import main as resource_main
        with self.assertLogs(level="WARNING") as cm:
            self.send_post(
                local_path='NphSmsIngestionTaskApi',
                request_data=ingestion_data,
                prefix="/resource/task/",
                test_client=resource_main.app.test_client(),
            )
            self.assertIn('does not conform', cm.output[0])
