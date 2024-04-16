import csv
import datetime
import os
from unittest import mock

from rdr_service import config, clock
from rdr_service.api_util import open_cloud_file
from rdr_service.dao.biobank_stored_sample_dao import BiobankStoredSampleDao
from rdr_service.dao.genomics_dao import GenomicDefaultBaseDao, GenomicManifestFileDao, \
    GenomicFileProcessedDao, GenomicJobRunDao, GenomicPRDao
from rdr_service.genomic.genomic_job_components import ManifestDefinitionProvider
from rdr_service.genomic_enums import GenomicManifestTypes, GenomicJob, \
    GenomicSubProcessStatus, GenomicSubProcessResult
from rdr_service.model.genomics import GenomicPRRaw, GenomicP0Raw, GenomicP1Raw, GenomicP2Raw
from rdr_service.offline.genomics import genomic_dispatch, genomic_proteomics_pipeline
from rdr_service.participant_enums import QuestionnaireStatus
from tests.genomics_tests.test_genomic_pipeline import create_ingestion_test_file
from tests.helpers.unittest_base import BaseTestCase


class GenomicPRPipelineTest(BaseTestCase):
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
        self.pr_dao = GenomicPRDao()
        self.stored_sample_dao = BiobankStoredSampleDao()

    def base_pr_data_insert(self, **kwargs):
        for num in range(1, kwargs.get('num_set_members', 4)):
            participant = self.data_generator.create_database_participant(
                participantOrigin='vibrent',
                biobankId=f"{num}"
            )
            participant_summary = self.data_generator.create_database_participant_summary(
                withdrawalStatus=1,
                suspensionStatus=1,
                consentForStudyEnrollment=1,
                biobankId=participant.biobankId
            )
            self.data_generator.create_database_genomic_set_member(
                participantId=participant_summary.participantId,
                genomicSetId=self.gen_set.id,
                biobankId=participant.biobankId,
                genomeType="aou_array",
                qcStatus=1,
                gcManifestSampleSource="whole blood",
                gcManifestParentSampleId=f"{num}11111111111",
                participantOrigin="vibrent",
                validationStatus=1,
                sexAtBirth="F",
                collectionTubeId=f"{num}2222222222",
                ai_an='N'
            )
            # should be two stored bio samples per biobank_id
            self.data_generator.create_database_biobank_stored_sample(
                biobankId=participant.biobankId,
                biobankOrderIdentifier=self.fake.pyint(),
                biobankStoredSampleId=f"{num}11111",
                confirmed=clock.CLOCK.now(),
                test='1ED10'
            )
            self.data_generator.create_database_biobank_stored_sample(
                biobankId=participant.biobankId,
                biobankOrderIdentifier=self.fake.pyint(),
                biobankStoredSampleId=f"{num + 10}11111",
                confirmed=clock.CLOCK.now(),
                test='1ED04'
            )

    def execute_base_pr_ingestion(self, **kwargs):
        test_date = datetime.datetime(2020, 10, 13, 0, 0, 0, 0)
        bucket_name = 'test_pr_bucket'
        subfolder = 'pr_subfolder'

        if not kwargs.get('bypass_data_insert'):
            self.base_pr_data_insert()

        test_file_name = create_ingestion_test_file(
            kwargs.get('test_file'),
            bucket_name,
            folder=subfolder,
            include_timestamp=kwargs.get('include_timestamp', True),
            include_sub_num=kwargs.get('include_sub_num')
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

    def test_full_pr_manifest_ingestion(self):

        self.execute_base_pr_ingestion(
            test_file='RDR_AoU_PR_Requests.csv',
            job_id=GenomicJob.PR_PR_WORKFLOW,
            manifest_type=GenomicManifestTypes.PR_PR
        )

        pr_members = self.pr_dao.get_all()

        self.assertEqual(len(pr_members), 3)
        self.assertTrue(all(obj.biobank_id is not None for obj in pr_members))
        self.assertTrue(all(obj.sample_id is None for obj in pr_members))
        self.assertTrue(all(obj.genome_type == 'aou_proteomics' for obj in pr_members))
        self.assertTrue(all(obj.collection_tube_id is not None for obj in pr_members))
        self.assertTrue(all(obj.p_site_id == 'bi' for obj in pr_members))
        self.assertTrue(all(obj.genomic_set_member_id is not None for obj in pr_members))
        self.assertTrue(all(obj.proteomics_set == 1 for obj in pr_members))
        self.assertTrue(all(obj.created_job_run_id is not None for obj in pr_members))

        # check collection tube ids
        correct_collection_tube_ids = [obj.biobankStoredSampleId for obj in self.stored_sample_dao.get_all() if
                                       obj.test == '1ED10']
        self.assertTrue(all(obj.collection_tube_id in correct_collection_tube_ids for obj in pr_members))

        # check job run record
        pr_job_runs = list(filter(lambda x: x.jobId == GenomicJob.PR_PR_WORKFLOW, self.job_run_dao.get_all()))

        self.assertIsNotNone(pr_job_runs)
        self.assertEqual(len(pr_job_runs), 1)
        self.assertTrue(all(obj.created_job_run_id == pr_job_runs[0].id for obj in pr_members))

        self.assertTrue(all(obj.runStatus == GenomicSubProcessStatus.COMPLETED for obj in pr_job_runs))
        self.assertTrue(all(obj.runResult == GenomicSubProcessResult.SUCCESS for obj in pr_job_runs))

        self.clear_table_after_test('genomic_proteomics')

    def test_pr_manifest_ingestion_increments_set(self):

        self.execute_base_pr_ingestion(
            test_file='RDR_AoU_PR_Requests.csv',
            job_id=GenomicJob.PR_PR_WORKFLOW,
            manifest_type=GenomicManifestTypes.PR_PR
        )

        pr_members = self.pr_dao.get_all()
        self.assertTrue(all(obj.proteomics_set == 1 for obj in pr_members))

        # check job run record
        pr_job_runs = list(filter(lambda x: x.jobId == GenomicJob.PR_PR_WORKFLOW, self.job_run_dao.get_all()))

        self.assertIsNotNone(pr_job_runs)
        self.assertEqual(len(pr_job_runs), 1)
        self.assertTrue(all(obj.runStatus == GenomicSubProcessStatus.COMPLETED for obj in pr_job_runs))
        self.assertTrue(all(obj.runResult == GenomicSubProcessResult.SUCCESS for obj in pr_job_runs))

        # rerun job should increment set correctly
        self.execute_base_pr_ingestion(
            test_file='RDR_AoU_PR_Requests.csv',
            job_id=GenomicJob.PR_PR_WORKFLOW,
            manifest_type=GenomicManifestTypes.PR_PR,
            bypass_data_insert=True
        )

        pr_members = self.pr_dao.get_all()
        self.assertTrue(any(obj.proteomics_set == 2 for obj in pr_members))

        # check job run record
        pr_job_runs = list(filter(lambda x: x.jobId == GenomicJob.PR_PR_WORKFLOW, self.job_run_dao.get_all()))

        self.assertIsNotNone(pr_job_runs)
        self.assertEqual(len(pr_job_runs), 2)
        self.assertTrue(all(obj.runStatus == GenomicSubProcessStatus.COMPLETED for obj in pr_job_runs))
        self.assertTrue(all(obj.runResult == GenomicSubProcessResult.SUCCESS for obj in pr_job_runs))

        self.clear_table_after_test('genomic_proteomics')

    def test_pr_manifest_to_raw_ingestion(self):

        self.execute_base_pr_ingestion(
            test_file='RDR_AoU_PR_Requests.csv',
            job_id=GenomicJob.PR_PR_WORKFLOW,
            manifest_type=GenomicManifestTypes.PR_PR,
        )

        pr_raw_dao = GenomicDefaultBaseDao(
            model_type=GenomicPRRaw
        )

        manifest_type = GenomicJob.PR_PR_WORKFLOW
        pr_manifest_file = self.manifest_file_dao.get(1)

        genomic_dispatch.load_manifest_into_raw_table(
            pr_manifest_file.filePath,
            manifest_type
        )

        pr_raw_records = pr_raw_dao.get_all()

        self.assertEqual(len(pr_raw_records), 3)

        for attribute in GenomicPRRaw.__table__.columns:
            self.assertTrue(all(getattr(obj, str(attribute).split('.')[1]) is not None for obj in pr_raw_records))

        # check job run record
        pr_raw_job_runs = list(filter(lambda x: x.jobId == GenomicJob.LOAD_PR_TO_RAW_TABLE, self.job_run_dao.get_all()))

        self.assertIsNotNone(pr_raw_job_runs)
        self.assertEqual(len(pr_raw_job_runs), 1)
        self.assertTrue(all(obj.runStatus == GenomicSubProcessStatus.COMPLETED for obj in pr_raw_job_runs))
        self.assertTrue(all(obj.runResult == GenomicSubProcessResult.SUCCESS for obj in pr_raw_job_runs))

        self.clear_table_after_test('genomic_proteomics')

    @mock.patch('rdr_service.genomic.genomic_job_controller.GenomicJobController.execute_cloud_task')
    def test_full_pr_to_p0_cloud_task_manifest(self, cloud_task_mock):

        self.execute_base_pr_ingestion(
            test_file='RDR_AoU_PR_Requests.csv',
            job_id=GenomicJob.PR_PR_WORKFLOW,
            manifest_type=GenomicManifestTypes.PR_PR
        )

        self.assertEqual(cloud_task_mock.called, True)
        self.assertEqual(cloud_task_mock.call_count, 2)

        # manifest type
        self.assertTrue(len(cloud_task_mock.call_args[1]), 1)
        self.assertTrue(cloud_task_mock.call_args[1].get('payload').get('manifest_type') == 'p0')

        # task queue
        self.assertTrue(cloud_task_mock.call_args[1].get('task_queue') == 'genomic-generate-manifest')

        self.clear_table_after_test('genomic_proteomics')

    @mock.patch('rdr_service.genomic.genomic_job_controller.GenomicJobController.execute_cloud_task')
    def test_p0_manifest_generation(self, cloud_task):

        # pr always proceeds p0 in workflow
        self.execute_base_pr_ingestion(
            test_file='RDR_AoU_PR_Requests.csv',
            job_id=GenomicJob.PR_PR_WORKFLOW,
            manifest_type=GenomicManifestTypes.PR_PR
        )

        pr_members = self.pr_dao.get_all()
        self.assertEqual(len(pr_members), 3)

        # RESET cloud task mock that was called from LR ingestion for later checks in test
        cloud_task.reset_mock()

        fake_date = datetime.datetime(2020, 8, 3, 0, 0, 0, 0)

        # init p0 workflow from pipeline
        with clock.FakeClock(fake_date):
            genomic_proteomics_pipeline.pr_p0_manifest_workflow()

        current_pr_manifests = self.manifest_file_dao.get_all()

        # 1 for PR and 1 for P0
        self.assertEqual(len(current_pr_manifests), 2)

        # check P0 manifest only
        current_p0_manifest = list(filter(lambda x: x.manifestTypeId == GenomicManifestTypes.PR_P0,
                                          current_pr_manifests))

        self.assertEqual(len(current_p0_manifest), 1)
        self.assertTrue(all(obj.recordCount == len(pr_members) for obj in current_p0_manifest))
        self.assertTrue(all(obj.manifestTypeId == GenomicManifestTypes.PR_P0 for obj in current_p0_manifest))
        self.assertTrue(all(obj.manifestTypeIdStr == GenomicManifestTypes.PR_P0.name for obj in current_p0_manifest))

        manifest_def_provider = ManifestDefinitionProvider(kwargs={})
        columns_expected = manifest_def_provider.manifest_columns_config[GenomicManifestTypes.PR_P0]

        with open_cloud_file(
            os.path.normpath(
                f'{current_p0_manifest[0].filePath}'
            )
        ) as csv_file:
            csv_reader = csv.DictReader(csv_file)
            csv_rows = list(csv_reader)
            self.assertEqual(len(csv_rows), len(pr_members))

            # check for all columns
            manifest_columns = csv_reader.fieldnames
            self.assertTrue(list(columns_expected) == manifest_columns)

            prefix = config.getSetting(config.BIOBANK_ID_PREFIX)
            correct_collection_tube_ids = [obj.biobankStoredSampleId for obj in self.stored_sample_dao.get_all() if
                                           obj.test == '1ED10']

            for row in csv_rows:
                self.assertIsNotNone(row['biobank_id'])
                self.assertTrue(prefix in row['biobank_id'])
                self.assertIsNotNone(row['collection_tube_id'])
                self.assertIsNotNone(row['sex_at_birth'])
                self.assertIsNotNone(row['validation_passed'])
                self.assertIsNotNone(row['ai_an'])
                self.assertIsNotNone(row['ny_flag'])
                self.assertIsNotNone(row['p_site_id'])

                self.assertEqual(row['sex_at_birth'], 'F')
                self.assertEqual(row['ny_flag'], 'N')
                self.assertEqual(row['genome_type'], config.GENOME_TYPE_PR)
                self.assertEqual(row['p_site_id'], 'bi')
                self.assertEqual(row['ai_an'], 'N')
                self.assertEqual(row['validation_passed'], 'Y')

                # check collection tube ids
                self.assertTrue(row['collection_tube_id'] in correct_collection_tube_ids)

        pr_files_processed = self.file_processed_dao.get_all()

        # 1 for PR and 1 for P0
        self.assertEqual(len(pr_files_processed), 2)

        # check job run record
        p0_job_runs = list(filter(lambda x: x.jobId == GenomicJob.PR_P0_WORKFLOW, self.job_run_dao.get_all()))

        self.assertIsNotNone(p0_job_runs)
        self.assertEqual(len(p0_job_runs), 1)
        self.assertTrue(all(obj.runStatus == GenomicSubProcessStatus.COMPLETED for obj in p0_job_runs))
        self.assertTrue(all(obj.runResult == GenomicSubProcessResult.SUCCESS for obj in p0_job_runs))

        # Cloud task needs to be BYPASSED for updating members @ this time
        self.assertFalse(cloud_task.called)
        self.assertEqual(cloud_task.call_count, 0)

        # check raw records
        p0_raw_dao = GenomicDefaultBaseDao(
            model_type=GenomicP0Raw
        )

        p0_raw_records = p0_raw_dao.get_all()

        for attribute in GenomicP0Raw.__table__.columns:
            self.assertTrue(all(getattr(obj, str(attribute).split('.')[1]) is not None for obj in p0_raw_records))

        self.assertEqual(len(p0_raw_records), len(pr_members))
        self.assertTrue(all(obj.sex_at_birth == 'F' for obj in p0_raw_records))
        self.assertTrue(all(obj.ny_flag == 'N' for obj in p0_raw_records))
        self.assertTrue(all(obj.genome_type == config.GENOME_TYPE_PR for obj in p0_raw_records))
        self.assertTrue(all(obj.ai_an == 'N' for obj in p0_raw_records))
        self.assertTrue(all(obj.p_site_id == 'bi' for obj in p0_raw_records))

        # check job run record
        p0_raw_job_runs = list(filter(lambda x: x.jobId == GenomicJob.LOAD_P0_TO_RAW_TABLE, self.job_run_dao.get_all()))

        self.assertIsNotNone(p0_raw_job_runs)
        self.assertEqual(len(p0_raw_job_runs), 1)
        self.assertTrue(all(obj.runStatus == GenomicSubProcessStatus.COMPLETED for obj in p0_raw_job_runs))
        self.assertTrue(all(obj.runResult == GenomicSubProcessResult.SUCCESS for obj in p0_raw_job_runs))

        self.clear_table_after_test('genomic_proteomics')

    def test_p1_manifest_ingestion(self):
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
                collectionTubeId=num,
                ai_an="N"
            )
            if num < 3:
                self.data_generator.create_database_genomic_proteomics(
                    genomic_set_member_id=genomic_set_member.id,
                    biobank_id=genomic_set_member.biobankId,
                    collection_tube_id=f'{num}11111',
                    genome_type="aou_proteomics",
                    p_site_id="bi",
                    proteomics_set=1
                )

        self.execute_base_pr_ingestion(
            test_file='RDR_AoU_Proteomics_PKG-2301-123456.csv',
            job_id=GenomicJob.PR_P1_WORKFLOW,
            manifest_type=GenomicManifestTypes.PR_P1,
            include_timestamp=False
        )

        pr_members = self.pr_dao.get_all()

        self.assertEqual(len(pr_members), 2)
        self.assertTrue(all(obj.sample_id is not None for obj in pr_members))
        self.assertTrue(all(obj.sample_id in ['1111', '1112'] for obj in pr_members))

        # check job run record
        p1_job_runs = list(filter(lambda x: x.jobId == GenomicJob.PR_P1_WORKFLOW, self.job_run_dao.get_all()))

        self.assertIsNotNone(p1_job_runs)
        self.assertEqual(len(p1_job_runs), 1)

        self.assertTrue(all(obj.runStatus == GenomicSubProcessStatus.COMPLETED for obj in p1_job_runs))
        self.assertTrue(all(obj.runResult == GenomicSubProcessResult.SUCCESS for obj in p1_job_runs))

        self.clear_table_after_test('genomic_proteomics')

    def test_p1_manifest_to_raw_ingestion(self):

        self.execute_base_pr_ingestion(
            test_file='RDR_AoU_Proteomics_PKG-2301-123456.csv',
            job_id=GenomicJob.PR_P1_WORKFLOW,
            manifest_type=GenomicManifestTypes.PR_P1,
            include_timestamp=False
        )

        p1_raw_dao = GenomicDefaultBaseDao(
            model_type=GenomicP1Raw
        )

        manifest_type = GenomicJob.PR_P1_WORKFLOW
        p1_manifest_file = self.manifest_file_dao.get(1)

        genomic_dispatch.load_manifest_into_raw_table(
            p1_manifest_file.filePath,
            manifest_type
        )

        p1_raw_records = p1_raw_dao.get_all()
        self.assertEqual(len(p1_raw_records), 3)

        for attribute in GenomicP1Raw.__table__.columns:
            self.assertTrue(all(getattr(obj, str(attribute).split('.')[1]) is not None for obj in p1_raw_records))

        # check job run record
        p1_raw_job_runs = list(filter(lambda x: x.jobId == GenomicJob.LOAD_P1_TO_RAW_TABLE, self.job_run_dao.get_all()))

        self.assertIsNotNone(p1_raw_job_runs)
        self.assertEqual(len(p1_raw_job_runs), 1)
        self.assertTrue(all(obj.runStatus == GenomicSubProcessStatus.COMPLETED for obj in p1_raw_job_runs))
        self.assertTrue(all(obj.runResult == GenomicSubProcessResult.SUCCESS for obj in p1_raw_job_runs))

        self.clear_table_after_test('genomic_proteomics')

    def test_p2_manifest_ingestion(self):

        self.execute_base_pr_ingestion(
            test_file='RDR_AoU_P2.csv',
            job_id=GenomicJob.PR_P2_WORKFLOW,
            manifest_type=GenomicManifestTypes.PR_P2
        )

        # check job run record
        p2_job_runs = list(filter(lambda x: x.jobId == GenomicJob.PR_P2_WORKFLOW, self.job_run_dao.get_all()))

        self.assertIsNotNone(p2_job_runs)
        self.assertEqual(len(p2_job_runs), 1)

        self.assertTrue(all(obj.runStatus == GenomicSubProcessStatus.COMPLETED for obj in p2_job_runs))
        self.assertTrue(all(obj.runResult == GenomicSubProcessResult.SUCCESS for obj in p2_job_runs))

    def test_p2_manifest_to_raw_ingestion(self):

        self.execute_base_pr_ingestion(
            test_file='RDR_AoU_P2.csv',
            job_id=GenomicJob.PR_P2_WORKFLOW,
            manifest_type=GenomicManifestTypes.PR_P2
        )

        p2_raw_dao = GenomicDefaultBaseDao(
            model_type=GenomicP2Raw
        )

        manifest_type = GenomicJob.PR_P2_WORKFLOW
        p2_manifest_file = self.manifest_file_dao.get(1)

        genomic_dispatch.load_manifest_into_raw_table(
            p2_manifest_file.filePath,
            manifest_type
        )

        p2_raw_records = p2_raw_dao.get_all()
        self.assertEqual(len(p2_raw_records), 1)

        for attribute in GenomicP2Raw.__table__.columns:
            self.assertTrue(all(getattr(obj, str(attribute).split('.')[1]) is not None for obj in p2_raw_records))

        # check job run record
        p2_raw_job_runs = list(filter(lambda x: x.jobId == GenomicJob.LOAD_P2_TO_RAW_TABLE, self.job_run_dao.get_all()))

        self.assertIsNotNone(p2_raw_job_runs)
        self.assertEqual(len(p2_raw_job_runs), 1)
        self.assertTrue(all(obj.runStatus == GenomicSubProcessStatus.COMPLETED for obj in p2_raw_job_runs))
        self.assertTrue(all(obj.runResult == GenomicSubProcessResult.SUCCESS for obj in p2_raw_job_runs))

