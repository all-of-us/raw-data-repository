import csv
import datetime
import os
from unittest import mock

from rdr_service import config, clock
from rdr_service.api_util import open_cloud_file
from rdr_service.dao.genomics_dao import GenomicDefaultBaseDao, GenomicManifestFileDao, GenomicLongReadDao, \
    GenomicFileProcessedDao, GenomicJobRunDao, GenomicSetMemberDao
from rdr_service.genomic.genomic_job_components import ManifestDefinitionProvider
from rdr_service.genomic_enums import GenomicManifestTypes, GenomicJob, GenomicLongReadPlatform, \
    GenomicSubProcessStatus, GenomicSubProcessResult
from rdr_service.model.genomics import GenomicLRRaw, GenomicL0Raw, GenomicL1Raw, GenomicL2ONTRaw
from rdr_service.offline.genomics import genomic_dispatch, genomic_long_read_pipeline
from rdr_service.participant_enums import QuestionnaireStatus
from tests.genomics_tests.test_genomic_pipeline import create_ingestion_test_file
from tests.helpers.unittest_base import BaseTestCase


class GenomicLongReadPipelineTest(BaseTestCase):
    def setUp(self):
        super().setUp()
        self.manifest_file_dao = GenomicManifestFileDao()
        self.file_processed_dao = GenomicFileProcessedDao()
        self.job_run_dao = GenomicJobRunDao()
        self.gen_set = self.data_generator.create_database_genomic_set(
            genomicSetName=".",
            genomicSetCriteria=".",
            genomicSetVersion=1
        )
        self.long_read_dao = GenomicLongReadDao()
        self.genomic_set_member_dao = GenomicSetMemberDao()

    def base_lr_data_insert(self, **kwargs):
        for num in range(1, kwargs.get('num_set_members', 4)):
            participant_summary = self.data_generator.create_database_participant_summary(
                withdrawalStatus=1,
                suspensionStatus=1,
                consentForStudyEnrollment=1
            )
            member = self.data_generator.create_database_genomic_set_member(
                participantId=participant_summary.participantId,
                genomicSetId=self.gen_set.id,
                biobankId=f"{num}",
                genomeType="aou_array",
                qcStatus=1,
                gcManifestSampleSource="whole blood",
                gcManifestParentSampleId=f"{num}11111111111",
                participantOrigin="vibrent",
                validationStatus=1,
                sexAtBirth="F",
                collectionTubeId=f"{num}2222222222",
                ai_an='Y'
            )
            self.data_generator.create_database_genomic_gc_validation_metrics(
                genomicSetMemberId=member.id,
                processingStatus='Pass'
            )

    def execute_base_lr_ingestion(self, **kwargs):
        test_date = datetime.datetime(2020, 10, 13, 0, 0, 0, 0)
        bucket_name = 'test_lr_bucket'
        subfolder = 'lr_subfolder'

        self.base_lr_data_insert()

        test_file_name = create_ingestion_test_file(
            kwargs.get('test_file'),
            bucket_name,
            folder=subfolder,
            include_timestamp=kwargs.get('include_timestamp', True),
            include_sub_num=kwargs.get('include_sub_num'),
            after_timestamp=kwargs.get('after_timestamp')
        )

        task_data = {
            "job": kwargs.get('job_id'),
            "bucket": bucket_name,
            "file_data": {
                "create_feedback_record": False,
                "upload_date": test_date.isoformat(),
                "manifest_type": kwargs.get('manifest_type'),
                "file_path": f"{bucket_name}/{subfolder}/{test_file_name}"
            }
        }

        # Execute from cloud task
        genomic_dispatch.execute_genomic_manifest_file_pipeline(task_data)

    def test_full_lr_manifest_ingestion(self):

        self.execute_base_lr_ingestion(
            test_file='RDR_AoU_LR_Requests.csv',
            job_id=GenomicJob.LR_LR_WORKFLOW,
            manifest_type=GenomicManifestTypes.LR_LR
        )

        long_read_members = self.long_read_dao.get_all()

        self.assertEqual(len(long_read_members), 3)
        self.assertTrue(all(obj.biobank_id is not None for obj in long_read_members))
        self.assertTrue(all(obj.sample_id is None for obj in long_read_members))
        self.assertTrue(all(obj.genome_type == 'aou_long_read' for obj in long_read_members))
        self.assertTrue(all(obj.collection_tube_id is not None for obj in long_read_members))
        self.assertTrue(all(obj.long_read_platform == GenomicLongReadPlatform.PACBIO_CCS for obj in long_read_members))
        self.assertTrue(all(obj.lr_site_id == 'bcm' for obj in long_read_members))
        self.assertTrue(all(obj.genomic_set_member_id is not None for obj in long_read_members))
        self.assertTrue(all(obj.long_read_set == 1 for obj in long_read_members))
        self.assertTrue(all(obj.created_job_run_id is not None for obj in long_read_members))

        # check collection tube ids
        correct_collection_tube_ids = [obj.collectionTubeId for obj in self.genomic_set_member_dao.get_all()]
        self.assertTrue(all(obj.collection_tube_id in correct_collection_tube_ids for obj in long_read_members))

        # check job run record
        lr_job_runs = list(filter(lambda x: x.jobId == GenomicJob.LR_LR_WORKFLOW, self.job_run_dao.get_all()))

        self.assertIsNotNone(lr_job_runs)
        self.assertEqual(len(lr_job_runs), 1)
        self.assertTrue(all(obj.runStatus == GenomicSubProcessStatus.COMPLETED for obj in lr_job_runs))
        self.assertTrue(all(obj.runResult == GenomicSubProcessResult.SUCCESS for obj in lr_job_runs))

        self.clear_table_after_test('genomic_long_read')

    def test_lr_manifest_ingestion_increments_set(self):

        self.execute_base_lr_ingestion(
            test_file='RDR_AoU_LR_Requests.csv',
            job_id=GenomicJob.LR_LR_WORKFLOW,
            manifest_type=GenomicManifestTypes.LR_LR
        )

        long_read_members = self.long_read_dao.get_all()
        self.assertTrue(all(obj.long_read_set == 1 for obj in long_read_members))

        # check job run record
        lr_job_runs = list(filter(lambda x: x.jobId == GenomicJob.LR_LR_WORKFLOW, self.job_run_dao.get_all()))

        self.assertIsNotNone(lr_job_runs)
        self.assertEqual(len(lr_job_runs), 1)
        self.assertTrue(all(obj.runStatus == GenomicSubProcessStatus.COMPLETED for obj in lr_job_runs))
        self.assertTrue(all(obj.runResult == GenomicSubProcessResult.SUCCESS for obj in lr_job_runs))

        # rerun job should increment set correctly
        self.execute_base_lr_ingestion(
            test_file='RDR_AoU_LR_Requests.csv',
            job_id=GenomicJob.LR_LR_WORKFLOW,
            manifest_type=GenomicManifestTypes.LR_LR,
            bypass_data_insert=True
        )

        long_read_members = self.long_read_dao.get_all()
        self.assertTrue(any(obj.long_read_set == 2 for obj in long_read_members))

        # check job run record
        lr_job_runs = list(filter(lambda x: x.jobId == GenomicJob.LR_LR_WORKFLOW, self.job_run_dao.get_all()))

        self.assertIsNotNone(lr_job_runs)
        self.assertEqual(len(lr_job_runs), 2)
        self.assertTrue(all(obj.runStatus == GenomicSubProcessStatus.COMPLETED for obj in lr_job_runs))
        self.assertTrue(all(obj.runResult == GenomicSubProcessResult.SUCCESS for obj in lr_job_runs))

        self.clear_table_after_test('genomic_long_read')

    def test_lr_manifest_to_raw_ingestion(self):

        self.execute_base_lr_ingestion(
            test_file='RDR_AoU_LR_Requests.csv',
            job_id=GenomicJob.LR_LR_WORKFLOW,
            manifest_type=GenomicManifestTypes.LR_LR,
        )

        lr_raw_dao = GenomicDefaultBaseDao(
            model_type=GenomicLRRaw
        )

        manifest_type = 'lr'
        lr_manifest_file = self.manifest_file_dao.get(1)

        genomic_dispatch.load_manifest_into_raw_table(
            lr_manifest_file.filePath,
            manifest_type
        )

        lr_raw_records = lr_raw_dao.get_all()

        self.assertEqual(len(lr_raw_records), 3)

        for attribute in GenomicLRRaw.__table__.columns:
            self.assertTrue(all(getattr(obj, str(attribute).split('.')[1]) is not None for obj in lr_raw_records))

        # check job run record
        l0_raw_job_runs = list(filter(lambda x: x.jobId == GenomicJob.LOAD_LR_TO_RAW_TABLE, self.job_run_dao.get_all()))

        self.assertIsNotNone(l0_raw_job_runs)
        self.assertEqual(len(l0_raw_job_runs), 1)
        self.assertTrue(all(obj.runStatus == GenomicSubProcessStatus.COMPLETED for obj in l0_raw_job_runs))
        self.assertTrue(all(obj.runResult == GenomicSubProcessResult.SUCCESS for obj in l0_raw_job_runs))

        self.clear_table_after_test('genomic_long_read')

    @mock.patch('rdr_service.genomic.genomic_job_controller.GenomicJobController.execute_cloud_task')
    def test_full_lr_to_l0_cloud_task_manifest(self, cloud_task_mock):

        self.execute_base_lr_ingestion(
            test_file='RDR_AoU_LR_Requests.csv',
            job_id=GenomicJob.LR_LR_WORKFLOW,
            manifest_type=GenomicManifestTypes.LR_LR
        )

        self.assertEqual(cloud_task_mock.called, True)
        self.assertEqual(cloud_task_mock.call_count, 1)

        # manifest type
        self.assertTrue(len(cloud_task_mock.call_args[1]), 1)
        self.assertTrue(cloud_task_mock.call_args[1].get('payload').get('manifest_type') == 'l0')

        # task queue
        self.assertTrue(cloud_task_mock.call_args[1].get('task_queue') == 'genomic-generate-manifest')

        self.clear_table_after_test('genomic_long_read')

    @mock.patch('rdr_service.genomic.genomic_job_controller.GenomicJobController.execute_cloud_task')
    def test_l0_manifest_generation(self, cloud_task):

        # lr always proceeds l0 in workflow
        self.execute_base_lr_ingestion(
            test_file='RDR_AoU_LR_Requests.csv',
            job_id=GenomicJob.LR_LR_WORKFLOW,
            manifest_type=GenomicManifestTypes.LR_LR
        )

        long_read_members = self.long_read_dao.get_all()
        self.assertEqual(len(long_read_members), 3)

        # RESET cloud task mock that was called from LR ingestion for later checks in test
        cloud_task.reset_mock()

        fake_date = datetime.datetime(2020, 8, 3, 0, 0, 0, 0)
        # init l0 workflow from pipeline
        with clock.FakeClock(fake_date):
            genomic_long_read_pipeline.lr_l0_manifest_workflow()

        current_lr_manifests = self.manifest_file_dao.get_all()

        # 1 for LR and 1 for L0
        self.assertEqual(len(current_lr_manifests), 2)

        # check L0 manifest only
        current_l0_manifest = list(filter(lambda x: x.manifestTypeId == GenomicManifestTypes.LR_L0,
                                          current_lr_manifests))

        self.assertEqual(len(current_l0_manifest), 1)
        self.assertTrue(all(obj.recordCount == len(long_read_members) for obj in current_l0_manifest))
        self.assertTrue(all(obj.manifestTypeId == GenomicManifestTypes.LR_L0 for obj in current_l0_manifest))
        self.assertTrue(all(obj.manifestTypeIdStr == GenomicManifestTypes.LR_L0.name for obj in current_l0_manifest))

        manifest_def_provider = ManifestDefinitionProvider(kwargs={})
        columns_expected = manifest_def_provider.manifest_columns_config[GenomicManifestTypes.LR_L0]

        with open_cloud_file(
            os.path.normpath(
                f'{current_l0_manifest[0].filePath}'
            )
        ) as csv_file:
            csv_reader = csv.DictReader(csv_file)
            csv_rows = list(csv_reader)
            self.assertEqual(len(csv_rows), len(long_read_members))

            # check for all columns
            manifest_columns = csv_reader.fieldnames
            self.assertTrue(list(columns_expected) == manifest_columns)

            prefix = config.getSetting(config.BIOBANK_ID_PREFIX)

            for row in csv_rows:
                self.assertIsNotNone(row['biobank_id'])
                self.assertTrue(prefix in row['biobank_id'])
                self.assertIsNotNone(row['collection_tube_id'])
                self.assertIsNotNone(row['sex_at_birth'])
                self.assertIsNotNone(row['validation_passed'])
                self.assertIsNotNone(row['parent_tube_id'])
                self.assertIsNotNone(row['lr_site_id'])
                self.assertIsNotNone(row['long_read_platform'])

                self.assertEqual(row['sex_at_birth'], 'F')
                self.assertEqual(row['ny_flag'], 'N')
                self.assertEqual(row['genome_type'], config.GENOME_TYPE_LR)
                self.assertEqual(row['lr_site_id'], 'bcm')
                self.assertEqual(row['ai_an'], 'Y')
                self.assertEqual(row['long_read_platform'], GenomicLongReadPlatform.PACBIO_CCS.name)
                self.assertEqual(row['validation_passed'], 'Y')

        lr_files_processed = self.file_processed_dao.get_all()

        # 1 for LR and 1 for L0
        self.assertEqual(len(lr_files_processed), 2)

        # check job run record
        l0_job_runs = list(filter(lambda x: x.jobId == GenomicJob.LR_L0_WORKFLOW, self.job_run_dao.get_all()))

        self.assertIsNotNone(l0_job_runs)
        self.assertEqual(len(l0_job_runs), 1)
        self.assertTrue(all(obj.runStatus == GenomicSubProcessStatus.COMPLETED for obj in l0_job_runs))
        self.assertTrue(all(obj.runResult == GenomicSubProcessResult.SUCCESS for obj in l0_job_runs))

        # Cloud task needs to be BYPASSED for updating members @ this time
        self.assertFalse(cloud_task.called)
        self.assertEqual(cloud_task.call_count, 0)

        # check raw records
        l0_raw_dao = GenomicDefaultBaseDao(
            model_type=GenomicL0Raw
        )

        l0_raw_records = l0_raw_dao.get_all()
        self.assertEqual(len(l0_raw_records), len(long_read_members))
        self.assertTrue(all(obj.file_path is not None for obj in l0_raw_records))
        self.assertTrue(all(obj.biobank_id is not None for obj in l0_raw_records))
        self.assertTrue(all(obj.collection_tube_id is not None for obj in l0_raw_records))
        self.assertTrue(all(obj.parent_tube_id is not None for obj in l0_raw_records))
        self.assertTrue(all(obj.sex_at_birth == 'F' for obj in l0_raw_records))
        self.assertTrue(all(obj.ny_flag == 'N' for obj in l0_raw_records))
        self.assertTrue(all(obj.genome_type == config.GENOME_TYPE_LR for obj in l0_raw_records))
        self.assertTrue(all(obj.ai_an == 'Y' for obj in l0_raw_records))
        self.assertTrue(all(obj.lr_site_id == 'bcm' for obj in l0_raw_records))
        self.assertTrue(all(obj.long_read_platform == GenomicLongReadPlatform.PACBIO_CCS.name for obj in
                            l0_raw_records))

        # check job run record
        l0_raw_job_runs = list(filter(lambda x: x.jobId == GenomicJob.LOAD_L0_TO_RAW_TABLE, self.job_run_dao.get_all()))

        self.assertIsNotNone(l0_raw_job_runs)
        self.assertEqual(len(l0_raw_job_runs), 1)
        self.assertTrue(all(obj.runStatus == GenomicSubProcessStatus.COMPLETED for obj in l0_raw_job_runs))
        self.assertTrue(all(obj.runResult == GenomicSubProcessResult.SUCCESS for obj in l0_raw_job_runs))

        self.clear_table_after_test('genomic_long_read')

    def test_l1_manifest_ingestion(self):
        for num in range(1, 4):
            participant_summary = self.data_generator.create_database_participant_summary(
                consentForGenomicsROR=QuestionnaireStatus.SUBMITTED,
                consentForStudyEnrollment=QuestionnaireStatus.SUBMITTED
            )
            genomic_set_member = self.data_generator.create_database_genomic_set_member(
                genomicSetId=self.gen_set.id,
                participantId=participant_summary.participantId,
                biobankId=f"100{num}",
                genomeType="aou_array",
                collectionTubeId=num
            )
            if num < 3:
                self.data_generator.create_database_genomic_long_read(
                    genomic_set_member_id=genomic_set_member.id,
                    biobank_id=genomic_set_member.biobankId,
                    collection_tube_id=f'{num}11111',
                    genome_type="aou_long_read",
                    lr_site_id="bi",
                    long_read_platform=GenomicLongReadPlatform.PACBIO_CCS,
                    long_read_set=1
                )

        self.execute_base_lr_ingestion(
            test_file='RDR_AoU_LR_PKG-0101-123456.csv',
            job_id=GenomicJob.LR_L1_WORKFLOW,
            manifest_type=GenomicManifestTypes.LR_L1,
            include_timestamp=False
        )

        long_read_members = self.long_read_dao.get_all()

        self.assertEqual(len(long_read_members), 2)
        self.assertTrue(all(obj.sample_id is not None for obj in long_read_members))
        self.assertTrue(all(obj.sample_id in ['1111', '1112'] for obj in long_read_members))

        # check job run record
        l1_job_runs = list(filter(lambda x: x.jobId == GenomicJob.LR_L1_WORKFLOW, self.job_run_dao.get_all()))

        self.assertIsNotNone(l1_job_runs)
        self.assertEqual(len(l1_job_runs), 1)

        self.assertTrue(all(obj.runStatus == GenomicSubProcessStatus.COMPLETED for obj in l1_job_runs))
        self.assertTrue(all(obj.runResult == GenomicSubProcessResult.SUCCESS for obj in l1_job_runs))

        self.clear_table_after_test('genomic_long_read')

    def test_l1_manifest_to_raw_ingestion(self):

        self.execute_base_lr_ingestion(
            test_file='RDR_AoU_LR_PKG-0101-123456.csv',
            job_id=GenomicJob.LR_L1_WORKFLOW,
            manifest_type=GenomicManifestTypes.LR_L1,
            include_timestamp=False
        )

        l1_raw_dao = GenomicDefaultBaseDao(
            model_type=GenomicL1Raw
        )

        manifest_type = 'l1'
        l1_manifest_file = self.manifest_file_dao.get(1)

        genomic_dispatch.load_manifest_into_raw_table(
            l1_manifest_file.filePath,
            manifest_type
        )

        l1_raw_records = l1_raw_dao.get_all()
        self.assertEqual(len(l1_raw_records), 3)

        for attribute in GenomicL1Raw.__table__.columns:
            self.assertTrue(all(getattr(obj, str(attribute).split('.')[1]) is not None for obj in l1_raw_records))

        # check job run record
        l1_raw_job_runs = list(filter(lambda x: x.jobId == GenomicJob.LOAD_L1_TO_RAW_TABLE, self.job_run_dao.get_all()))

        self.assertIsNotNone(l1_raw_job_runs)
        self.assertEqual(len(l1_raw_job_runs), 1)
        self.assertTrue(all(obj.runStatus == GenomicSubProcessStatus.COMPLETED for obj in l1_raw_job_runs))
        self.assertTrue(all(obj.runResult == GenomicSubProcessResult.SUCCESS for obj in l1_raw_job_runs))

        self.clear_table_after_test('genomic_long_read')

    def test_l2_ont_manifest_ingestion(self):

        self.execute_base_lr_ingestion(
            test_file='RDR_AoU_l2.csv',
            job_id=GenomicJob.LR_L2_ONT_WORKFLOW,
            manifest_type=GenomicManifestTypes.LR_L2_ONT,
            after_timestamp='_ont_005'
        )

        # check job run record
        l2_ont_job_runs = list(filter(lambda x: x.jobId == GenomicJob.LR_L2_ONT_WORKFLOW, self.job_run_dao.get_all()))

        self.assertIsNotNone(l2_ont_job_runs)
        self.assertEqual(len(l2_ont_job_runs), 1)

        self.assertTrue(all(obj.runStatus == GenomicSubProcessStatus.COMPLETED for obj in l2_ont_job_runs))
        self.assertTrue(all(obj.runResult == GenomicSubProcessResult.SUCCESS for obj in l2_ont_job_runs))

        self.clear_table_after_test('genomic_long_read')

    def test_l2_ont_manifest_to_raw_ingestion(self):

        self.execute_base_lr_ingestion(
            test_file='RDR_AoU_l2.csv',
            job_id=GenomicJob.LR_L2_ONT_WORKFLOW,
            manifest_type=GenomicManifestTypes.LR_L2_ONT,
            after_timestampe='_ont_005'
        )

        l2_ont_raw_dao = GenomicDefaultBaseDao(
            model_type=GenomicL2ONTRaw
        )

        manifest_type = 'l2_ont'
        l1_ont_manifest_file = self.manifest_file_dao.get(1)

        genomic_dispatch.load_manifest_into_raw_table(
            l1_ont_manifest_file.filePath,
            manifest_type
        )

        l2_ont_raw_records = l2_ont_raw_dao.get_all()
        self.assertEqual(len(l2_ont_raw_records), 3)

        for attribute in GenomicL2ONTRaw.__table__.columns:
            self.assertTrue(all(getattr(obj, str(attribute).split('.')[1]) is not None for obj in l2_ont_raw_records))

        # check job run record
        l2_ont_raw_records = list(
            filter(lambda x: x.jobId == GenomicJob.LOAD_L2_ONT_TO_RAW_TABLE, self.job_run_dao.get_all()))

        self.assertIsNotNone(l2_ont_raw_records)
        self.assertEqual(len(l2_ont_raw_records), 1)
        self.assertTrue(all(obj.runStatus == GenomicSubProcessStatus.COMPLETED for obj in l2_ont_raw_records))
        self.assertTrue(all(obj.runResult == GenomicSubProcessResult.SUCCESS for obj in l2_ont_raw_records))

        self.clear_table_after_test('genomic_long_read')

