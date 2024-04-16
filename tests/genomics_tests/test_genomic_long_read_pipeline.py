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
from rdr_service.model.config_utils import get_biobank_id_prefix
from rdr_service.model.genomics import GenomicLRRaw, GenomicL0Raw, GenomicL1Raw, GenomicL2ONTRaw, GenomicL2PBCCSRaw, \
    GenomicL4Raw, GenomicL3Raw, GenomicL5Raw, GenomicL6Raw, GenomicL1FRaw, GenomicL4FRaw, GenomicL6FRaw

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
            # adding set member records for L0 generation
            member = self.data_generator.create_database_genomic_set_member(
                participantId=participant_summary.participantId,
                genomicSetId=self.gen_set.id,
                biobankId=f"{num}",
                genomeType="aou_wgs",  # should always pull wgs sample
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
        self.assertTrue(all(obj.genomic_set_member_id is not None for obj in long_read_members))
        self.assertTrue(all(obj.genome_type == 'aou_long_read' for obj in long_read_members))
        self.assertTrue(all(obj.collection_tube_id is not None for obj in long_read_members))
        self.assertTrue(all(obj.long_read_platform == GenomicLongReadPlatform.PACBIO_CCS for obj in long_read_members))
        self.assertTrue(all(obj.lr_site_id == 'bcm' for obj in long_read_members))
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
            manifest_type=GenomicManifestTypes.LR_LR
        )

        long_read_members = self.long_read_dao.get_all()
        self.assertTrue(any(obj.long_read_set == 2 for obj in long_read_members))

        # check job run record
        lr_job_runs = list(filter(lambda x: x.jobId == GenomicJob.LR_LR_WORKFLOW, self.job_run_dao.get_all()))

        self.assertIsNotNone(lr_job_runs)
        self.assertEqual(len(lr_job_runs), 2)
        self.assertTrue(all(obj.runStatus == GenomicSubProcessStatus.COMPLETED for obj in lr_job_runs))
        self.assertTrue(all(obj.runResult == GenomicSubProcessResult.SUCCESS for obj in lr_job_runs))

    def test_lr_manifest_to_raw_ingestion(self):

        self.execute_base_lr_ingestion(
            test_file='RDR_AoU_LR_Requests.csv',
            job_id=GenomicJob.LR_LR_WORKFLOW,
            manifest_type=GenomicManifestTypes.LR_LR,
        )

        lr_raw_dao = GenomicDefaultBaseDao(
            model_type=GenomicLRRaw
        )

        manifest_type = GenomicJob.LR_LR_WORKFLOW
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

    @mock.patch('rdr_service.genomic.genomic_job_controller.GenomicJobController.execute_cloud_task')
    def test_full_lr_to_l0_cloud_task_manifest(self, cloud_task_mock):

        self.execute_base_lr_ingestion(
            test_file='RDR_AoU_LR_Requests.csv',
            job_id=GenomicJob.LR_LR_WORKFLOW,
            manifest_type=GenomicManifestTypes.LR_LR
        )

        self.assertEqual(cloud_task_mock.called, True)
        self.assertEqual(cloud_task_mock.call_count, 2)

        # manifest type
        self.assertTrue(len(cloud_task_mock.call_args[1]), 1)
        self.assertTrue(cloud_task_mock.call_args[1].get('payload').get('manifest_type') == 'l0')

        # task queue
        self.assertTrue(cloud_task_mock.call_args[1].get('task_queue') == 'genomic-generate-manifest')

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
                genomeType="aou_wgs",
                collectionTubeId=num
            )
            if num < 3:
                # SHOULD NOT add sample_id to long_read member w/ different lr platform
                self.data_generator.create_database_genomic_long_read(
                    genomic_set_member_id=genomic_set_member.id,
                    biobank_id=genomic_set_member.biobankId,
                    collection_tube_id=f'{num}11111',
                    genome_type="aou_long_read",
                    lr_site_id="bi",
                    long_read_platform=GenomicLongReadPlatform.ONT,
                    long_read_set=1
                )
                self.data_generator.create_database_genomic_long_read(
                    genomic_set_member_id=genomic_set_member.id,
                    biobank_id=genomic_set_member.biobankId,
                    collection_tube_id=f'{num}11111',
                    genome_type="aou_long_read",
                    lr_site_id="bi",
                    long_read_platform=GenomicLongReadPlatform.PACBIO_CCS,
                    long_read_set=1
                )
                # SHOULD NOT add sample_id to long_read member w/ different lr site id
                self.data_generator.create_database_genomic_long_read(
                    genomic_set_member_id=genomic_set_member.id,
                    biobank_id=genomic_set_member.biobankId,
                    collection_tube_id=f'{num}11111',
                    genome_type="aou_long_read",
                    lr_site_id="uw",
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

        self.assertEqual(len(long_read_members), 6)

        self.assertTrue(all(obj.sample_id is not None for obj in long_read_members if
                            obj.long_read_platform == GenomicLongReadPlatform.PACBIO_CCS and obj.lr_site_id == 'bi'))
        self.assertTrue(all(obj.sample_id in ['1111', '1112'] for obj in long_read_members  if
                            obj.long_read_platform == GenomicLongReadPlatform.PACBIO_CCS and obj.lr_site_id == 'bi'))

        # ONT platform does not get updated
        self.assertTrue(all(obj.sample_id is None for obj in long_read_members if
                            obj.long_read_platform == GenomicLongReadPlatform.ONT))

        # Different LR site id does not get updated
        self.assertTrue(all(obj.sample_id is None for obj in long_read_members if
                            obj.long_read_platform == GenomicLongReadPlatform.PACBIO_CCS and obj.lr_site_id == 'uw'))

        # check job run record
        l1_job_runs = list(filter(lambda x: x.jobId == GenomicJob.LR_L1_WORKFLOW, self.job_run_dao.get_all()))

        self.assertIsNotNone(l1_job_runs)
        self.assertEqual(len(l1_job_runs), 1)

        self.assertTrue(all(obj.runStatus == GenomicSubProcessStatus.COMPLETED for obj in l1_job_runs))
        self.assertTrue(all(obj.runResult == GenomicSubProcessResult.SUCCESS for obj in l1_job_runs))


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

        manifest_type = GenomicJob.LR_L1_WORKFLOW
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

    def test_l2_ont_manifest_ingestion(self):

        self.execute_base_lr_ingestion(
            test_file='RDR_AoU_l2_1111111.csv',
            job_id=GenomicJob.LR_L2_ONT_WORKFLOW,
            manifest_type=GenomicManifestTypes.LR_L2_ONT,
            include_timestamp=False,
            after_timestamp='_ont_005'
        )

        # check job run record
        l2_ont_job_runs = list(filter(lambda x: x.jobId == GenomicJob.LR_L2_ONT_WORKFLOW, self.job_run_dao.get_all()))

        self.assertIsNotNone(l2_ont_job_runs)
        self.assertEqual(len(l2_ont_job_runs), 1)

        self.assertTrue(all(obj.runStatus == GenomicSubProcessStatus.COMPLETED for obj in l2_ont_job_runs))
        self.assertTrue(all(obj.runResult == GenomicSubProcessResult.SUCCESS for obj in l2_ont_job_runs))

    def test_l2_ont_manifest_to_raw_ingestion(self):

        self.execute_base_lr_ingestion(
            test_file='RDR_AoU_l2_1111111.csv',
            job_id=GenomicJob.LR_L2_ONT_WORKFLOW,
            manifest_type=GenomicManifestTypes.LR_L2_ONT,
            include_timestamp=False,
            after_timestampe='_ont_005'
        )

        l2_ont_raw_dao = GenomicDefaultBaseDao(
            model_type=GenomicL2ONTRaw
        )

        manifest_type = GenomicJob.LR_L2_ONT_WORKFLOW
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
        l2_ont_job_runs = list(
            filter(lambda x: x.jobId == GenomicJob.LOAD_L2_ONT_TO_RAW_TABLE, self.job_run_dao.get_all()))

        self.assertIsNotNone(l2_ont_job_runs)
        self.assertEqual(len(l2_ont_job_runs), 1)
        self.assertTrue(all(obj.runStatus == GenomicSubProcessStatus.COMPLETED for obj in l2_ont_job_runs))
        self.assertTrue(all(obj.runResult == GenomicSubProcessResult.SUCCESS for obj in l2_ont_job_runs))

    def test_l2_pb_ccs_manifest_ingestion(self):

        self.execute_base_lr_ingestion(
            test_file='RDR_AoU_l2_2222222.csv',
            job_id=GenomicJob.LR_L2_PB_CCS_WORKFLOW,
            manifest_type=GenomicManifestTypes.LR_L2_PB_CCS,
            include_timestamp=False,
            after_timestamp='_pbccs_005'
        )

        # check job run record
        l2_pb_ccs_job_runs = list(filter(lambda x: x.jobId == GenomicJob.LR_L2_PB_CCS_WORKFLOW,
                                         self.job_run_dao.get_all()))

        self.assertIsNotNone(l2_pb_ccs_job_runs)
        self.assertEqual(len(l2_pb_ccs_job_runs), 1)

        self.assertTrue(all(obj.runStatus == GenomicSubProcessStatus.COMPLETED for obj in l2_pb_ccs_job_runs))
        self.assertTrue(all(obj.runResult == GenomicSubProcessResult.SUCCESS for obj in l2_pb_ccs_job_runs))

    def test_l2_pb_ccs_manifest_to_raw_ingestion(self):

        self.execute_base_lr_ingestion(
            test_file='RDR_AoU_l2_2222222.csv',
            job_id=GenomicJob.LR_L2_PB_CCS_WORKFLOW,
            manifest_type=GenomicManifestTypes.LR_L2_PB_CCS,
            include_timestamp=False,
            after_timestamp='_pbccs_005'
        )

        l2_pb_ccs_raw_dao = GenomicDefaultBaseDao(
            model_type=GenomicL2PBCCSRaw
        )

        manifest_type = GenomicJob.LR_L2_PB_CCS_WORKFLOW
        l1_pb_ccs_manifest_file = self.manifest_file_dao.get(1)

        genomic_dispatch.load_manifest_into_raw_table(
            l1_pb_ccs_manifest_file.filePath,
            manifest_type
        )

        l2_pb_ccs_raw_records = l2_pb_ccs_raw_dao.get_all()
        self.assertEqual(len(l2_pb_ccs_raw_records), 3)

        for attribute in GenomicL2PBCCSRaw.__table__.columns:
            self.assertTrue(
                all(getattr(obj, str(attribute).split('.')[1]) is not None for obj in l2_pb_ccs_raw_records)
            )

        # check job run record
        l2_pb_ccs_job_runs = list(
            filter(lambda x: x.jobId == GenomicJob.LOAD_L2_PB_CCS_TO_RAW_TABLE, self.job_run_dao.get_all()))

        self.assertIsNotNone(l2_pb_ccs_job_runs)
        self.assertEqual(len(l2_pb_ccs_job_runs), 1)
        self.assertTrue(all(obj.runStatus == GenomicSubProcessStatus.COMPLETED for obj in l2_pb_ccs_job_runs))
        self.assertTrue(all(obj.runResult == GenomicSubProcessResult.SUCCESS for obj in l2_pb_ccs_job_runs))

    def build_lr_l3_data(self):
        for num in range(1, 7):
            # SAME Sample ID cannot be shared between platforms
            if num % 2 != 0:
                long_read_member = self.data_generator.create_database_genomic_long_read(
                    biobank_id=f"100{num}",
                    sample_id=f'{num}11111',
                    collection_tube_id=f'{num}11111',
                    genome_type="aou_long_read",
                    lr_site_id="bi",
                    long_read_platform=GenomicLongReadPlatform.ONT,
                    long_read_set=1
                )
                self.data_generator.create_database_genomic_longread_l1_raw(
                    sex_at_birth='F',
                    sample_id=long_read_member.sample_id,
                    biobank_id=long_read_member.biobank_id,
                    long_read_platform='ont'
                )

            else:
                long_read_member = self.data_generator.create_database_genomic_long_read(
                    biobank_id=f"100{num}",
                    collection_tube_id=f'{num}22222',
                    sample_id=f'{num}22222',
                    genome_type="aou_long_read",
                    lr_site_id="bi",
                    long_read_platform=GenomicLongReadPlatform.PACBIO_CCS,
                    long_read_set=1
                )
                self.data_generator.create_database_genomic_longread_l1_raw(
                    sex_at_birth='F',
                    sample_id=long_read_member.sample_id,
                    biobank_id=long_read_member.biobank_id,
                    long_read_platform='pacbio_ccs'
                )

        ont_platform_members = list(filter(lambda x: x.long_read_platform == GenomicLongReadPlatform.ONT,
                                           self.long_read_dao.get_all()))
        for num, ont in enumerate(ont_platform_members):
            self.data_generator.create_database_genomic_longread_l2_ont_raw(
                biobank_id=ont.biobank_id,
                sample_id=ont.sample_id,
                flowcell_id='na' if num % 2 != 0 else f'PA{num}1212',
                barcode='na' if num % 2 != 0 else f'bc{num}001',
                bam_path='dat_bam_path',
                processing_status='pass',
                read_length_n50=20,
                read_error_rate=20,
                mean_coverage=20,
                genome_coverage=20,
                contamination=0.20,
                basecaller_version='bv1',
                basecaller_model='bvm2',
                mean_read_quality=20,
                sample_source='whole blood'
            )

        pb_ccs_platform_members = list(filter(lambda x: x.long_read_platform == GenomicLongReadPlatform.PACBIO_CCS,
                                              self.long_read_dao.get_all()))
        for ccs in pb_ccs_platform_members:
            # CAN HAVE multiple PB CCS rows on L2 PB CCS per sample_id
            for num in range(1, 3):
                self.data_generator.create_database_genomic_longread_l2_pb_ccs_raw(
                    biobank_id=ccs.biobank_id,
                    sample_id=ccs.sample_id,
                    flowcell_id='na' if num % 2 != 0 else f'PA{num}1212',
                    barcode='na' if num % 2 != 0 else f'bc{num}001',
                    bam_path='dat_bam_path',
                    processing_status='pass',
                    read_length_mean=20,
                    instrument='t200',
                    smrtlink_server_version='v08',
                    instrument_ics_version='v20',
                    read_error_rate=20,
                    mean_coverage=20,
                    genome_coverage=20,
                    contamination=0.20,
                    sample_source='whole blood'
                )

    def test_l3_manifest_generation(self):

        self.build_lr_l3_data()

        # If data is on L2 files, needs to go out on L3
        l2_ont_dao = GenomicDefaultBaseDao(
            model_type=GenomicL2ONTRaw
        )

        l2_pb_ccs_dao = GenomicDefaultBaseDao(
            model_type=GenomicL2PBCCSRaw
        )

        current_l2_ont_records = l2_ont_dao.get_all()
        current_l2_pb_ccs_records = l2_pb_ccs_dao.get_all()

        # init l3 workflow from pipeline
        genomic_long_read_pipeline.lr_l3_manifest_workflow()

        current_l3_manifests = self.manifest_file_dao.get_all()
        self.assertEqual(len(current_l3_manifests), 1)

        self.assertTrue(all(obj.recordCount == len(current_l2_ont_records + current_l2_pb_ccs_records) for obj in current_l3_manifests))
        self.assertTrue(all(obj.manifestTypeId == GenomicManifestTypes.LR_L3 for obj in current_l3_manifests))
        self.assertTrue(all(obj.manifestTypeIdStr == GenomicManifestTypes.LR_L3.name for obj in current_l3_manifests))

        manifest_def_provider = ManifestDefinitionProvider(kwargs={})
        columns_expected = manifest_def_provider.manifest_columns_config[GenomicManifestTypes.LR_L3]

        current_l3_manifest = current_l3_manifests[0]

        with open_cloud_file(
            os.path.normpath(
                f'{current_l3_manifest.filePath}'
            )
        ) as csv_file:
            csv_reader = csv.DictReader(csv_file)
            csv_rows = list(csv_reader)
            self.assertEqual(len(csv_rows), len(current_l2_ont_records + current_l2_pb_ccs_records))
            # check for all columns
            manifest_columns = csv_reader.fieldnames
            self.assertTrue(list(columns_expected) == manifest_columns)

        l3_files_processed = self.file_processed_dao.get_all()
        self.assertEqual(len(l3_files_processed), 1)

        # check raw records
        l3_raw_dao = GenomicDefaultBaseDao(
            model_type=GenomicL3Raw
        )

        l3_raw_records = l3_raw_dao.get_all()
        self.assertEqual(len(l3_raw_records), len(current_l2_ont_records + current_l2_pb_ccs_records))

        self.assertTrue(all(obj.file_path is not None for obj in l3_raw_records))
        self.assertTrue(all(obj.biobank_id is not None for obj in l3_raw_records))
        self.assertTrue(all(get_biobank_id_prefix() in obj.biobank_id for obj in l3_raw_records))
        self.assertTrue(all(obj.sample_id is not None for obj in l3_raw_records))
        self.assertTrue(all(obj.biobankid_sampleid is not None for obj in l3_raw_records))
        self.assertTrue(all(obj.flowcell_id is not None for obj in l3_raw_records))

        self.assertTrue(all(obj.barcode is not None for obj in l3_raw_records))

        self.assertTrue(any(obj.long_read_platform == GenomicLongReadPlatform.ONT.name for obj in l3_raw_records))
        self.assertTrue(any(obj.long_read_platform == GenomicLongReadPlatform.PACBIO_CCS.name for obj in
                            l3_raw_records))

        self.assertTrue(all(obj.long_read_platform is not None for obj in l3_raw_records))
        self.assertTrue(all(obj.bam_path is not None for obj in l3_raw_records))
        self.assertTrue(all(obj.sex_at_birth is not None for obj in l3_raw_records))
        self.assertTrue(all(obj.lr_site_id is not None for obj in l3_raw_records))

        self.assertTrue(any(obj.pacbio_instrument_type is not None for obj in l3_raw_records))
        self.assertTrue(any(obj.smrtlink_server_version is not None for obj in l3_raw_records))
        self.assertTrue(any(obj.pacbio_instrument_ics_version is not None for obj in l3_raw_records))

        self.assertTrue(all(obj.gc_read_error_rate is not None for obj in l3_raw_records))
        self.assertTrue(all(obj.gc_mean_coverage is not None for obj in l3_raw_records))
        self.assertTrue(all(obj.gc_genome_coverage is not None for obj in l3_raw_records))
        self.assertTrue(all(obj.gc_contamination is not None for obj in l3_raw_records))

        self.assertTrue(any(obj.ont_basecaller_version is not None for obj in l3_raw_records))
        self.assertTrue(any(obj.ont_basecaller_model is not None for obj in l3_raw_records))
        self.assertTrue(any(obj.ont_mean_read_qual is not None for obj in l3_raw_records))

        # check raw job run record
        l3_raw_job_runs = list(filter(lambda x: x.jobId == GenomicJob.LOAD_L3_TO_RAW_TABLE, self.job_run_dao.get_all()))

        self.assertIsNotNone(l3_raw_job_runs)
        self.assertEqual(len(l3_raw_job_runs), 1)
        self.assertTrue(all(obj.runStatus == GenomicSubProcessStatus.COMPLETED for obj in l3_raw_job_runs))
        self.assertTrue(all(obj.runResult == GenomicSubProcessResult.SUCCESS for obj in l3_raw_job_runs))

        # check job run record
        l3_job_runs = list(filter(lambda x: x.jobId == GenomicJob.LR_L3_WORKFLOW, self.job_run_dao.get_all()))

        self.assertIsNotNone(l3_job_runs)
        self.assertEqual(len(l3_job_runs), 1)
        self.assertTrue(all(obj.runStatus == GenomicSubProcessStatus.COMPLETED for obj in l3_job_runs))
        self.assertTrue(all(obj.runResult == GenomicSubProcessResult.SUCCESS for obj in l3_job_runs))

    def test_l3_manifest_generation_excludes_sent_samples(self):

        self.build_lr_l3_data()

        # init l3 workflow from pipeline
        genomic_long_read_pipeline.lr_l3_manifest_workflow()

        current_l3_manifests = self.manifest_file_dao.get_all()
        self.assertEqual(len(current_l3_manifests), 1)

        l3_job_runs = list(filter(lambda x: x.jobId == GenomicJob.LR_L3_WORKFLOW, self.job_run_dao.get_all()))

        self.assertIsNotNone(l3_job_runs)
        self.assertEqual(len(l3_job_runs), 1)
        self.assertTrue(all(obj.runStatus == GenomicSubProcessStatus.COMPLETED for obj in l3_job_runs))
        self.assertTrue(all(obj.runResult == GenomicSubProcessResult.SUCCESS for obj in l3_job_runs))

        l2_ont_dao = GenomicDefaultBaseDao(
            model_type=GenomicL2ONTRaw
        )

        l2_pb_ccs_dao = GenomicDefaultBaseDao(
            model_type=GenomicL2PBCCSRaw
        )

        l3_raw_dao = GenomicDefaultBaseDao(
            model_type=GenomicL3Raw
        )

        # check raw records exist for current manifest
        current_l2_ont_records = l2_ont_dao.get_all()
        current_l2_pb_ccs_records = l2_pb_ccs_dao.get_all()
        self.assertEqual(len(l3_raw_dao.get_all()), len(current_l2_ont_records + current_l2_pb_ccs_records))

        # re-init l3 workflow from pipeline
        genomic_long_read_pipeline.lr_l3_manifest_workflow()

        # should find no data since, so should only be one manifest
        current_l3_manifests = self.manifest_file_dao.get_all()
        self.assertEqual(len(current_l3_manifests), 1)

        l3_job_runs = list(filter(lambda x: x.jobId == GenomicJob.LR_L3_WORKFLOW, self.job_run_dao.get_all()))

        self.assertIsNotNone(l3_job_runs)
        self.assertEqual(len(l3_job_runs), 2)
        self.assertTrue(all(obj.runStatus == GenomicSubProcessStatus.COMPLETED for obj in l3_job_runs))
        self.assertTrue(any(obj.runResult == GenomicSubProcessResult.SUCCESS for obj in l3_job_runs))
        self.assertTrue(any(obj.runResult == GenomicSubProcessResult.NO_FILES for obj in l3_job_runs))

        # should only have initial 9 raw records for first L3 manifest
        current_l2_ont_records = l2_ont_dao.get_all()
        current_l2_pb_ccs_records = l2_pb_ccs_dao.get_all()
        self.assertEqual(len(l3_raw_dao.get_all()), len(current_l2_ont_records + current_l2_pb_ccs_records))

    def test_l4_manifest_ingestion(self):

        self.execute_base_lr_ingestion(
            test_file='AoU_L4.csv',
            job_id=GenomicJob.LR_L4_WORKFLOW,
            manifest_type=GenomicManifestTypes.LR_L4,
        )

        # check job run record
        l4_job_runs = list(filter(lambda x: x.jobId == GenomicJob.LR_L4_WORKFLOW,
                                  self.job_run_dao.get_all()))

        self.assertIsNotNone(l4_job_runs)
        self.assertEqual(len(l4_job_runs), 1)

        self.assertTrue(all(obj.runStatus == GenomicSubProcessStatus.COMPLETED for obj in l4_job_runs))
        self.assertTrue(all(obj.runResult == GenomicSubProcessResult.SUCCESS for obj in l4_job_runs))

    def test_l4_manifest_to_raw_ingestion(self):

        self.execute_base_lr_ingestion(
            test_file='AoU_L4.csv',
            job_id=GenomicJob.LR_L4_WORKFLOW,
            manifest_type=GenomicManifestTypes.LR_L4,
        )

        l4_raw_dao = GenomicDefaultBaseDao(
            model_type=GenomicL4Raw
        )

        manifest_type = GenomicJob.LR_L4_WORKFLOW
        l4_manifest_file = self.manifest_file_dao.get(1)

        genomic_dispatch.load_manifest_into_raw_table(
            l4_manifest_file.filePath,
            manifest_type
        )

        l4_manifest_raw_records = l4_raw_dao.get_all()
        self.assertEqual(len(l4_manifest_raw_records), 3)

        for attribute in GenomicL4Raw.__table__.columns:
            self.assertTrue(
                all(getattr(obj, str(attribute).split('.')[1]) is not None for obj in l4_manifest_raw_records)
            )

        # check job run record
        l4_job_runs = list(
            filter(lambda x: x.jobId == GenomicJob.LOAD_L4_TO_RAW_TABLE, self.job_run_dao.get_all()))

        self.assertIsNotNone(l4_job_runs)
        self.assertEqual(len(l4_job_runs), 1)
        self.assertTrue(all(obj.runStatus == GenomicSubProcessStatus.COMPLETED for obj in l4_job_runs))
        self.assertTrue(all(obj.runResult == GenomicSubProcessResult.SUCCESS for obj in l4_job_runs))

    def test_l5_manifest_ingestion(self):

        self.execute_base_lr_ingestion(
            test_file='AoU_L5.csv',
            job_id=GenomicJob.LR_L5_WORKFLOW,
            manifest_type=GenomicManifestTypes.LR_L5,
        )

        # check job run record
        l5_job_runs = list(filter(lambda x: x.jobId == GenomicJob.LR_L5_WORKFLOW,
                                  self.job_run_dao.get_all()))

        self.assertIsNotNone(l5_job_runs)
        self.assertEqual(len(l5_job_runs), 1)

        self.assertTrue(all(obj.runStatus == GenomicSubProcessStatus.COMPLETED for obj in l5_job_runs))
        self.assertTrue(all(obj.runResult == GenomicSubProcessResult.SUCCESS for obj in l5_job_runs))

    def test_l5_manifest_to_raw_ingestion(self):

        self.execute_base_lr_ingestion(
            test_file='AoU_L5.csv',
            job_id=GenomicJob.LR_L5_WORKFLOW,
            manifest_type=GenomicManifestTypes.LR_L5,
        )

        l5_raw_dao = GenomicDefaultBaseDao(
            model_type=GenomicL5Raw
        )

        manifest_type = GenomicJob.LR_L5_WORKFLOW
        l5_manifest_file = self.manifest_file_dao.get(1)

        genomic_dispatch.load_manifest_into_raw_table(
            l5_manifest_file.filePath,
            manifest_type
        )

        l5_manifest_raw_records = l5_raw_dao.get_all()
        self.assertEqual(len(l5_manifest_raw_records), 3)

        for attribute in GenomicL5Raw.__table__.columns:
            self.assertTrue(
                all(getattr(obj, str(attribute).split('.')[1]) is not None for obj in l5_manifest_raw_records)
            )

        # check job run record
        l5_job_runs = list(
            filter(lambda x: x.jobId == GenomicJob.LOAD_L5_TO_RAW_TABLE, self.job_run_dao.get_all()))

        self.assertIsNotNone(l5_job_runs)
        self.assertEqual(len(l5_job_runs), 1)
        self.assertTrue(all(obj.runStatus == GenomicSubProcessStatus.COMPLETED for obj in l5_job_runs))
        self.assertTrue(all(obj.runResult == GenomicSubProcessResult.SUCCESS for obj in l5_job_runs))

    def test_l6_manifest_ingestion(self):

        self.execute_base_lr_ingestion(
            test_file='AoU_L6.csv',
            job_id=GenomicJob.LR_L6_WORKFLOW,
            manifest_type=GenomicManifestTypes.LR_L6,
        )

        # check job run record
        l6_job_runs = list(filter(lambda x: x.jobId == GenomicJob.LR_L6_WORKFLOW,
                                  self.job_run_dao.get_all()))

        self.assertIsNotNone(l6_job_runs)
        self.assertEqual(len(l6_job_runs), 1)

        self.assertTrue(all(obj.runStatus == GenomicSubProcessStatus.COMPLETED for obj in l6_job_runs))
        self.assertTrue(all(obj.runResult == GenomicSubProcessResult.SUCCESS for obj in l6_job_runs))

    def test_l6_manifest_to_raw_ingestion(self):

        self.execute_base_lr_ingestion(
            test_file='AoU_L6.csv',
            job_id=GenomicJob.LR_L6_WORKFLOW,
            manifest_type=GenomicManifestTypes.LR_L6,
        )

        l6_raw_dao = GenomicDefaultBaseDao(
            model_type=GenomicL6Raw
        )

        manifest_type = GenomicJob.LR_L6_WORKFLOW
        l6_manifest_file = self.manifest_file_dao.get(1)

        genomic_dispatch.load_manifest_into_raw_table(
            l6_manifest_file.filePath,
            manifest_type
        )

        l6_manifest_raw_records = l6_raw_dao.get_all()
        self.assertEqual(len(l6_manifest_raw_records), 3)

        for attribute in GenomicL6Raw.__table__.columns:
            self.assertTrue(
                all(getattr(obj, str(attribute).split('.')[1]) is not None for obj in l6_manifest_raw_records)
            )

        # check job run record
        l6_job_runs = list(
            filter(lambda x: x.jobId == GenomicJob.LOAD_L6_TO_RAW_TABLE, self.job_run_dao.get_all()))

        self.assertIsNotNone(l6_job_runs)
        self.assertEqual(len(l6_job_runs), 1)
        self.assertTrue(all(obj.runStatus == GenomicSubProcessStatus.COMPLETED for obj in l6_job_runs))
        self.assertTrue(all(obj.runResult == GenomicSubProcessResult.SUCCESS for obj in l6_job_runs))

    def test_l1f_manifest_ingestion(self):

        self.execute_base_lr_ingestion(
            test_file='RDR_AoU_l1f.csv',
            job_id=GenomicJob.LR_L1F_WORKFLOW,
            manifest_type=GenomicManifestTypes.LR_L1F,
        )

        # check job run record
        l1f_job_runs = list(filter(lambda x: x.jobId == GenomicJob.LR_L1F_WORKFLOW,
                                   self.job_run_dao.get_all()))

        self.assertIsNotNone(l1f_job_runs)
        self.assertEqual(len(l1f_job_runs), 1)

        self.assertTrue(all(obj.runStatus == GenomicSubProcessStatus.COMPLETED for obj in l1f_job_runs))
        self.assertTrue(all(obj.runResult == GenomicSubProcessResult.SUCCESS for obj in l1f_job_runs))

    def test_l1f_manifest_to_raw_ingestion(self):

        self.execute_base_lr_ingestion(
            test_file='RDR_AoU_l1f.csv',
            job_id=GenomicJob.LR_L1F_WORKFLOW,
            manifest_type=GenomicManifestTypes.LR_L1F,
        )

        l1f_raw_dao = GenomicDefaultBaseDao(
            model_type=GenomicL1FRaw
        )

        manifest_type = GenomicJob.LR_L1F_WORKFLOW
        l1f_manifest_file = self.manifest_file_dao.get(1)

        genomic_dispatch.load_manifest_into_raw_table(
            l1f_manifest_file.filePath,
            manifest_type
        )

        l1f_manifest_raw_records = l1f_raw_dao.get_all()
        self.assertEqual(len(l1f_manifest_raw_records), 3)

        for attribute in GenomicL1FRaw.__table__.columns:
            self.assertTrue(
                all(getattr(obj, str(attribute).split('.')[1]) is not None for obj in l1f_manifest_raw_records)
            )

        # check job run record
        l1f_job_runs = list(
            filter(lambda x: x.jobId == GenomicJob.LOAD_L1F_TO_RAW_TABLE, self.job_run_dao.get_all()))

        self.assertIsNotNone(l1f_job_runs)
        self.assertEqual(len(l1f_job_runs), 1)
        self.assertTrue(all(obj.runStatus == GenomicSubProcessStatus.COMPLETED for obj in l1f_job_runs))
        self.assertTrue(all(obj.runResult == GenomicSubProcessResult.SUCCESS for obj in l1f_job_runs))

    def test_l4f_manifest_ingestion(self):

        self.execute_base_lr_ingestion(
            test_file='AoU_L4F.csv',
            job_id=GenomicJob.LR_L4F_WORKFLOW,
            manifest_type=GenomicManifestTypes.LR_L4F,
        )

        # check job run record
        l4f_job_runs = list(filter(lambda x: x.jobId == GenomicJob.LR_L4F_WORKFLOW,
                                   self.job_run_dao.get_all()))

        self.assertIsNotNone(l4f_job_runs)
        self.assertEqual(len(l4f_job_runs), 1)

        self.assertTrue(all(obj.runStatus == GenomicSubProcessStatus.COMPLETED for obj in l4f_job_runs))
        self.assertTrue(all(obj.runResult == GenomicSubProcessResult.SUCCESS for obj in l4f_job_runs))

    def test_l4f_manifest_to_raw_ingestion(self):

        self.execute_base_lr_ingestion(
            test_file='AoU_L4F.csv',
            job_id=GenomicJob.LR_L4F_WORKFLOW,
            manifest_type=GenomicManifestTypes.LR_L4F,
        )

        l4f_raw_dao = GenomicDefaultBaseDao(
            model_type=GenomicL4FRaw
        )

        manifest_type = GenomicJob.LR_L4F_WORKFLOW
        l4f_manifest_file = self.manifest_file_dao.get(1)

        genomic_dispatch.load_manifest_into_raw_table(
            l4f_manifest_file.filePath,
            manifest_type
        )

        l4f_manifest_raw_records = l4f_raw_dao.get_all()
        self.assertEqual(len(l4f_manifest_raw_records), 3)

        for attribute in GenomicL4FRaw.__table__.columns:
            self.assertTrue(
                all(getattr(obj, str(attribute).split('.')[1]) is not None for obj in l4f_manifest_raw_records)
            )

        # check job run record
        l4f_job_runs = list(
            filter(lambda x: x.jobId == GenomicJob.LOAD_L4F_TO_RAW_TABLE, self.job_run_dao.get_all()))

        self.assertIsNotNone(l4f_job_runs)
        self.assertEqual(len(l4f_job_runs), 1)
        self.assertTrue(all(obj.runStatus == GenomicSubProcessStatus.COMPLETED for obj in l4f_job_runs))
        self.assertTrue(all(obj.runResult == GenomicSubProcessResult.SUCCESS for obj in l4f_job_runs))

    def test_l6f_manifest_ingestion(self):

        self.execute_base_lr_ingestion(
            test_file='AoU_L6F.csv',
            job_id=GenomicJob.LR_L6F_WORKFLOW,
            manifest_type=GenomicManifestTypes.LR_L6F,
        )

        # check job run record
        l6f_job_runs = list(filter(lambda x: x.jobId == GenomicJob.LR_L6F_WORKFLOW,
                                   self.job_run_dao.get_all()))

        self.assertIsNotNone(l6f_job_runs)
        self.assertEqual(len(l6f_job_runs), 1)

        self.assertTrue(all(obj.runStatus == GenomicSubProcessStatus.COMPLETED for obj in l6f_job_runs))
        self.assertTrue(all(obj.runResult == GenomicSubProcessResult.SUCCESS for obj in l6f_job_runs))

    def test_l6f_manifest_to_raw_ingestion(self):

        self.execute_base_lr_ingestion(
            test_file='AoU_L6F.csv',
            job_id=GenomicJob.LR_L6F_WORKFLOW,
            manifest_type=GenomicManifestTypes.LR_L6F,
        )

        l6f_raw_dao = GenomicDefaultBaseDao(
            model_type=GenomicL6FRaw
        )

        manifest_type = GenomicJob.LR_L6F_WORKFLOW
        l6f_manifest_file = self.manifest_file_dao.get(1)

        genomic_dispatch.load_manifest_into_raw_table(
            l6f_manifest_file.filePath,
            manifest_type
        )

        l6f_manifest_raw_records = l6f_raw_dao.get_all()
        self.assertEqual(len(l6f_manifest_raw_records), 3)

        for attribute in GenomicL6FRaw.__table__.columns:
            self.assertTrue(
                all(getattr(obj, str(attribute).split('.')[1]) is not None for obj in l6f_manifest_raw_records)
            )

        # check job run record
        l6f_job_runs = list(
            filter(lambda x: x.jobId == GenomicJob.LOAD_L6F_TO_RAW_TABLE, self.job_run_dao.get_all()))

        self.assertIsNotNone(l6f_job_runs)
        self.assertEqual(len(l6f_job_runs), 1)
        self.assertTrue(all(obj.runStatus == GenomicSubProcessStatus.COMPLETED for obj in l6f_job_runs))
        self.assertTrue(all(obj.runResult == GenomicSubProcessResult.SUCCESS for obj in l6f_job_runs))

    def tearDown(self):
        super().tearDown()
        self.clear_table_after_test("participant")
        self.clear_table_after_test("participant_summary")
        self.clear_table_after_test("biobank_stored_sample")
        self.clear_table_after_test("genomic_set_member")
        self.clear_table_after_test('genomic_long_read')
        self.clear_table_after_test('genomic_l2_ont_raw')
        self.clear_table_after_test('genomic_l2_pb_ccs_raw')
        self.clear_table_after_test('genomic_l3_raw')

