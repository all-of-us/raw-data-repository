import csv
import datetime
import os
import mock

import pytz

from rdr_service import clock, config
from rdr_service.api_util import open_cloud_file, list_blobs
from rdr_service.code_constants import BIOBANK_TESTS
from rdr_service.config import GENOMIC_GEM_A3_MANIFEST_SUBFOLDER
from rdr_service.dao.biobank_order_dao import BiobankOrderDao
from rdr_service.dao.biobank_stored_sample_dao import BiobankStoredSampleDao
from rdr_service.dao.genomics_dao import (
    GenomicSetDao,
    GenomicSetMemberDao,
    GenomicJobRunDao,
    GenomicFileProcessedDao,
    GenomicGCValidationMetricsDao,
)
from rdr_service.dao.participant_dao import ParticipantDao
from rdr_service.dao.participant_summary_dao import ParticipantSummaryDao, ParticipantRaceAnswersDao
from rdr_service.dao.site_dao import SiteDao
from rdr_service.dao.code_dao import CodeDao, CodeType
from rdr_service.model.biobank_dv_order import BiobankDVOrder
from rdr_service.model.biobank_order import (
    BiobankOrder,
    BiobankOrderIdentifier,
    BiobankOrderedSample
)
from rdr_service.model.biobank_stored_sample import BiobankStoredSample
from rdr_service.model.genomics import (
    GenomicSet,
    GenomicSetMember,
    GenomicJobRun,
    GenomicGCValidationMetrics)
from rdr_service.model.participant import Participant
from rdr_service.model.code import Code
from rdr_service.model.participant_summary import ParticipantRaceAnswers
from rdr_service.offline import genomic_pipeline
from rdr_service.participant_enums import (
    SampleStatus,
    GenomicSetStatus,
    GenomicSetMemberStatus,
    GenomicSubProcessStatus,
    GenomicSubProcessResult,
    GenomicJob,
    Race,
    QuestionnaireStatus, GenomicWorkflowState)
from tests import test_data
from tests.helpers.unittest_base import BaseTestCase

_BASELINE_TESTS = list(BIOBANK_TESTS)
_FAKE_BUCKET = "rdr_fake_bucket"
_FAKE_BIOBANK_SAMPLE_BUCKET = "rdr_fake_biobank_sample_bucket"
_FAKE_BUCKET_FOLDER = "rdr_fake_sub_folder"
_FAKE_BUCKET_RESULT_FOLDER = "rdr_fake_sub_result_folder"
_FAKE_GENOMIC_CENTER_BUCKET_A = 'rdr_fake_genomic_center_a_bucket'
_FAKE_GENOMIC_CENTER_BUCKET_B = 'rdr_fake_genomic_center_b_bucket'
_FAKE_GENOMIC_CENTER_DATA_BUCKET_A = 'rdr_fake_genomic_center_a_data_bucket'
_FAKE_GENOTYPING_FOLDER = 'genotyping_sample_manifests'
_FAKE_SEQUENCING_FOLDER = 'sequencing_sample_manifests'
_FAKE_CVL_REPORT_FOLDER = 'fake_cvl_reconciliation_reports'
_FAKE_CVL_MANIFEST_FOLDER = 'fake_cvl_manifest_folder'
_FAKE_GEM_BUCKET = 'fake_gem_bucket'
_FAKE_FAILURE_FOLDER = 'post_accessioning_results'
_OUTPUT_CSV_TIME_FORMAT = "%Y-%m-%d-%H-%M-%S"
_US_CENTRAL = pytz.timezone("US/Central")
_UTC = pytz.utc


# noinspection DuplicatedCode
class GenomicPipelineTest(BaseTestCase):
    def setUp(self):
        super(GenomicPipelineTest, self).setUp()
        # Everything is stored as a list, so override bucket name as a 1-element list.
        config.override_setting(config.GENOMIC_SET_BUCKET_NAME, [_FAKE_BUCKET])
        config.override_setting(config.BIOBANK_SAMPLES_BUCKET_NAME, [_FAKE_BIOBANK_SAMPLE_BUCKET])
        config.override_setting(config.GENOMIC_BIOBANK_MANIFEST_FOLDER_NAME, [_FAKE_BUCKET_FOLDER])
        config.override_setting(config.GENOMIC_BIOBANK_MANIFEST_RESULT_FOLDER_NAME, [_FAKE_BUCKET_RESULT_FOLDER])
        config.override_setting(config.GENOMIC_CENTER_BUCKET_NAME, [_FAKE_GENOMIC_CENTER_BUCKET_A,
                                                                    _FAKE_GENOMIC_CENTER_BUCKET_B])
        config.override_setting(config.GENOMIC_CENTER_DATA_BUCKET_NAME, [_FAKE_GENOMIC_CENTER_BUCKET_A,
                                                                         _FAKE_GENOMIC_CENTER_BUCKET_B])
        config.override_setting(config.GENOMIC_CVL_BUCKET_NAME, [_FAKE_BUCKET])

        config.override_setting(config.GENOMIC_GENOTYPING_SAMPLE_MANIFEST_FOLDER_NAME,
                                [_FAKE_GENOTYPING_FOLDER])
        config.override_setting(config.GENOMIC_CVL_RECONCILIATION_REPORT_SUBFOLDER,
                                [_FAKE_CVL_REPORT_FOLDER])
        config.override_setting(config.CVL_W1_MANIFEST_SUBFOLDER,
                                [_FAKE_CVL_MANIFEST_FOLDER])
        config.override_setting(config.GENOMIC_GEM_BUCKET_NAME, [_FAKE_GEM_BUCKET])
        config.override_setting(config.GENOMIC_AW1F_SUBFOLDER, [_FAKE_FAILURE_FOLDER])

        self.participant_dao = ParticipantDao()
        self.summary_dao = ParticipantSummaryDao()
        self.race_dao = ParticipantRaceAnswersDao()
        self.job_run_dao = GenomicJobRunDao()
        self.file_processed_dao = GenomicFileProcessedDao()
        self.set_dao = GenomicSetDao()
        self.member_dao = GenomicSetMemberDao()
        self.metrics_dao = GenomicGCValidationMetricsDao()
        self.sample_dao = BiobankStoredSampleDao()
        self.site_dao = SiteDao()
        self.code_dao = CodeDao()
        self._participant_i = 1

    mock_bucket_paths = [_FAKE_BUCKET,
                         _FAKE_BIOBANK_SAMPLE_BUCKET,
                         _FAKE_BIOBANK_SAMPLE_BUCKET + os.sep + _FAKE_BUCKET_FOLDER,
                         _FAKE_BIOBANK_SAMPLE_BUCKET + os.sep + _FAKE_BUCKET_RESULT_FOLDER
                         ]

    def _write_cloud_csv(self, file_name, contents_str, bucket=None, folder=None):
        bucket = _FAKE_BUCKET if bucket is None else bucket
        if folder is None:
            path = "/%s/%s" % (bucket, file_name)
        else:
            path = "/%s/%s/%s" % (bucket, folder, file_name)
        with open_cloud_file(path, mode='wb') as cloud_file:
            cloud_file.write(contents_str.encode("utf-8"))

    def _make_participant(self, **kwargs):
        """
    Make a participant with custom settings.
    default should create a valid participant.
    """
        i = self._participant_i
        self._participant_i += 1
        bid = kwargs.pop('biobankId', i)
        participant = Participant(participantId=i, biobankId=bid, **kwargs)
        self.participant_dao.insert(participant)
        return participant

    def _make_biobank_order(self, **kwargs):
        """Makes a new BiobankOrder (same values every time) with valid/complete defaults.

    Kwargs pass through to BiobankOrder constructor, overriding defaults.
    """
        participant_id = kwargs["participantId"]

        for k, default_value in (
            ("biobankOrderId", "1"),
            ("created", clock.CLOCK.now()),
            ("sourceSiteId", 1),
            ("sourceUsername", "fred@pmi-ops.org"),
            ("collectedSiteId", 1),
            ("collectedUsername", "joe@pmi-ops.org"),
            ("processedSiteId", 1),
            ("processedUsername", "sue@pmi-ops.org"),
            ("finalizedSiteId", 2),
            ("finalizedUsername", "bob@pmi-ops.org"),
            ("version", 1),
            ("identifiers", [BiobankOrderIdentifier(system="a", value="c")]),
            ("samples", [BiobankOrderedSample(test="1SAL2", description="description", processingRequired=True)]),
            ("dvOrders", [BiobankDVOrder(participantId=participant_id, version=1)]),
        ):
            if k not in kwargs:
                kwargs[k] = default_value

        biobank_order = BiobankOrderDao().insert(BiobankOrder(**kwargs))
        return biobank_order

    def _make_stored_sample(self, **kwargs):
        """Makes BiobankStoredSamples for a biobank_id"""
        return BiobankStoredSampleDao().insert(BiobankStoredSample(**kwargs))

    def _make_summary(self, participant, **override_kwargs):
        """
    Make a summary with custom settings.
    default should create a valid summary.
    """
        valid_kwargs = dict(
            participantId=participant.participantId,
            biobankId=participant.biobankId,
            withdrawalStatus=participant.withdrawalStatus,
            dateOfBirth=datetime.datetime(2000, 1, 1),
            firstName="foo",
            lastName="bar",
            zipCode="12345",
            sampleStatus1ED04=SampleStatus.RECEIVED,
            sampleStatus1SAL2=SampleStatus.RECEIVED,
            samplesToIsolateDNA=SampleStatus.RECEIVED,
            consentForStudyEnrollmentTime=datetime.datetime(2019, 1, 1),
            consentForGenomicsROR=QuestionnaireStatus.SUBMITTED,
        )
        kwargs = dict(valid_kwargs, **override_kwargs)
        summary = self.data_generator._participant_summary_with_defaults(**kwargs)
        self.summary_dao.insert(summary)
        return summary

    def test_gc_validation_metrics_end_to_end(self):
        # Create the fake Google Cloud CSV files to ingest
        bucket_name = _FAKE_GENOMIC_CENTER_BUCKET_A
        # add to subfolder
        end_to_end_test_files = (
            'RDR_AoU_GEN_TestDataManifest.csv',
            'test_empty_wells.csv'
        )
        for test_file in end_to_end_test_files:
            self._create_ingestion_test_file(test_file, bucket_name,
                                             folder=config.GENOMIC_AW2_SUBFOLDERS[1])

        self._create_fake_datasets_for_gc_tests(2, arr_override=True,
                                                array_participants=(1, 2))

        self._update_test_sample_ids()

        # run the GC Metrics Ingestion workflow
        genomic_pipeline.ingest_genomic_centers_metrics_files()

        # test file processing queue
        files_processed = self.file_processed_dao.get_all()
        self.assertEqual(len(files_processed), 1)
        self._gc_files_processed_test_cases(files_processed)

        # Test the fields against the DB
        gc_metrics = self.metrics_dao.get_all()

        self.assertEqual(len(gc_metrics), 2)
        self._gc_metrics_ingested_data_test_cases([gc_metrics[0]])

        # Test Genomic State updated
        member = self.member_dao.get(1)
        self.assertEqual(GenomicWorkflowState.AW2, member.genomicWorkflowState)

        # Test successful run result
        run_obj = self.job_run_dao.get(1)
        self.assertEqual(GenomicSubProcessResult.SUCCESS, run_obj.runResult)

    def _update_test_sample_ids(self):
        # update sample ID (mock AW1 manifest)
        for m in self.member_dao.get_all():
            m.sampleId = f"100{m.collectionTubeId}"
            self.member_dao.update(m)

    def _gc_files_processed_test_cases(self, files_processed):
        """ sub tests for the GC Metrics end to end test """

        for f in files_processed:
            if "SEQ" in f.fileName:
                self.assertEqual(
                    f.fileName,
                    'RDR_AoU_SEQ_TestDataManifest_11192019.csv'
                )
                self.assertEqual(
                    f.filePath,
                    f'/{_FAKE_GENOMIC_CENTER_BUCKET_A}/'
                    f'{config.GENOMIC_AW2_SUBFOLDERS[0]}/'
                    f'/RDR_AoU_SEQ_TestDataManifest_11192019.csv'
                )
            else:
                self.assertEqual(
                    f.fileName,
                    'RDR_AoU_GEN_TestDataManifest_11192019.csv'
                )
                self.assertEqual(
                    f.filePath,
                    f'/{_FAKE_GENOMIC_CENTER_BUCKET_A}/'
                    f'{config.GENOMIC_AW2_SUBFOLDERS[1]}/'
                    f'RDR_AoU_GEN_TestDataManifest_11192019.csv'
                )

            self.assertEqual(f.fileStatus,
                             GenomicSubProcessStatus.COMPLETED)
            self.assertEqual(f.fileResult,
                             GenomicSubProcessResult.SUCCESS)

    def _gc_metrics_ingested_data_test_cases(self, gc_metrics):
        """Sub tests for the end-to-end metrics test"""
        for record in gc_metrics:
            # Test GEN file data inserted correctly
            self.assertEqual(1, record.genomicSetMemberId)
            self.assertEqual('10001', record.limsId)
            self.assertEqual('10001_R01C01', record.chipwellbarcode)
            self.assertEqual('0.996', record.callRate)
            self.assertEqual('True', record.sexConcordance)
            self.assertEqual('0.00654', record.contamination)
            self.assertEqual('Pass', record.processingStatus)
            self.assertEqual('JH', record.siteId)
            self.assertEqual('This sample passed', record.notes)

    def test_gc_metrics_ingestion_bad_files(self):
        # Create the fake Google Cloud CSV files to ingest
        bucket_name = _FAKE_GENOMIC_CENTER_BUCKET_A
        end_to_end_test_files = (
            'RDR_AoU_SEQ_TestBadStructureDataManifest.csv',
            'RDR-AoU-TestBadFilename-DataManifest.csv',
            'test_empty_wells.csv'
        )
        for test_file in end_to_end_test_files:
            self._create_ingestion_test_file(test_file, bucket_name,
                                             folder=config.GENOMIC_AW2_SUBFOLDERS[0])

        # run the GC Metrics Ingestion workflow
        genomic_pipeline.ingest_genomic_centers_metrics_files()

        # test file processing queue
        files_processed = self.file_processed_dao.get_all()

        # Test bad filename, invalid columns
        for f in files_processed:
            if "TestBadFilename" in f.fileName:
                self.assertEqual(f.fileResult,
                                 GenomicSubProcessResult.INVALID_FILE_NAME)
            if "TestBadStructure" in f.fileName:
                self.assertEqual(f.fileResult,
                                 GenomicSubProcessResult.INVALID_FILE_STRUCTURE)
        # Test Unsuccessful run
        run_obj = self.job_run_dao.get(1)
        self.assertEqual(GenomicSubProcessResult.ERROR, run_obj.runResult)

    def test_gc_metrics_ingestion_no_files(self):
        # run the GC Metrics Ingestion workflow
        genomic_pipeline.ingest_genomic_centers_metrics_files()

        # Test No Files run (should be success)
        run_obj = self.job_run_dao.get(1)
        self.assertEqual(GenomicSubProcessResult.SUCCESS, run_obj.runResult)

    def test_aw2_wgs_gc_metrics_ingestion(self):
        # Create the fake ingested data
        self._create_fake_datasets_for_gc_tests(2)
        bucket_name = _FAKE_GENOMIC_CENTER_BUCKET_A
        self._create_ingestion_test_file('RDR_AoU_SEQ_TestDataManifest.csv',
                                         bucket_name,
                                         folder=config.GENOMIC_AW2_SUBFOLDERS[0])

        self._update_test_sample_ids()

        genomic_pipeline.ingest_genomic_centers_metrics_files()  # run_id = 1

        # Test the fields against the DB
        gc_metrics = self.metrics_dao.get_all()

        self.assertEqual(len(gc_metrics), 1)
        self.assertEqual(gc_metrics[0].genomicSetMemberId, 2)
        self.assertEqual(gc_metrics[0].genomicFileProcessedId, 1)
        self.assertEqual(gc_metrics[0].limsId, '10002')
        self.assertEqual(gc_metrics[0].meanCoverage, '2')
        self.assertEqual(gc_metrics[0].genomeCoverage, '2')
        self.assertEqual(gc_metrics[0].contamination, '3')
        self.assertEqual(gc_metrics[0].sexConcordance, 'True')
        self.assertEqual(gc_metrics[0].sexPloidy, 'XY')
        self.assertEqual(gc_metrics[0].alignedQ20Bases, 4)
        self.assertEqual(gc_metrics[0].processingStatus, 'Pass')
        self.assertEqual(gc_metrics[0].notes, 'This sample passed')

        # Test file processing queue
        files_processed = self.file_processed_dao.get_all()
        self.assertEqual(len(files_processed), 1)

        # Test the end-to-end result code
        self.assertEqual(GenomicSubProcessResult.SUCCESS, self.job_run_dao.get(1).runResult)

    def _create_ingestion_test_file(self,
                                    test_data_filename,
                                    bucket_name,
                                    folder=None,
                                    include_timestamp=True):
        test_data_file = test_data.open_genomic_set_file(test_data_filename)

        input_filename = '{}{}.csv'.format(
            test_data_filename.replace('.csv', ''),
            '_11192019' if include_timestamp else ''
        )

        self._write_cloud_csv(input_filename,
                              test_data_file,
                              folder=folder,
                              bucket=bucket_name)

    def _create_fake_genomic_set(self,
                                 genomic_set_name,
                                 genomic_set_criteria,
                                 genomic_set_filename
                                 ):
        now = clock.CLOCK.now()
        genomic_set = GenomicSet()
        genomic_set.genomicSetName = genomic_set_name
        genomic_set.genomicSetCriteria = genomic_set_criteria
        genomic_set.genomicSetFile = genomic_set_filename
        genomic_set.genomicSetFileTime = now
        genomic_set.genomicSetStatus = GenomicSetStatus.INVALID

        set_dao = GenomicSetDao()
        genomic_set.genomicSetVersion = set_dao.get_new_version_number(genomic_set.genomicSetName)

        set_dao.insert(genomic_set)

        return genomic_set

    def _create_fake_genomic_member(
        self,
        genomic_set_id,
        participant_id,
        biobank_order_id,
        validation_status=GenomicSetMemberStatus.VALID,
        validation_flags=None,
        sex_at_birth="F",
        biobankId=None,
        genome_type="aou_array",
        ny_flag="Y",
        sequencing_filename=None,
        recon_bb_manifest_job_id=None,
        recon_gc_manifest_job_id=None,
        recon_sequencing_job_id=None,
        recon_cvl_job_id=None,
        cvl_manifest_wgs_job_id=None,
        gem_a1_manifest_job_id=None,
        cvl_w1_manifest_job_id=None,
        genomic_workflow_state=None,
    ):
        genomic_set_member = GenomicSetMember()
        genomic_set_member.genomicSetId = genomic_set_id
        genomic_set_member.validationStatus = validation_status
        genomic_set_member.validationFlags = validation_flags
        genomic_set_member.participantId = participant_id
        genomic_set_member.sexAtBirth = sex_at_birth
        genomic_set_member.biobankId = biobankId
        genomic_set_member.collectionTubeId = participant_id
        genomic_set_member.genomeType = genome_type
        genomic_set_member.nyFlag = 1 if ny_flag == "Y" else 0
        genomic_set_member.biobankOrderId = biobank_order_id
        genomic_set_member.sequencingFileName = sequencing_filename
        genomic_set_member.reconcileMetricsBBManifestJobRunId = recon_bb_manifest_job_id
        genomic_set_member.reconcileGCManifestJobRunId = recon_gc_manifest_job_id
        genomic_set_member.reconcileMetricsSequencingJobRunId = recon_sequencing_job_id
        genomic_set_member.reconcileCvlJobRunId = recon_cvl_job_id
        genomic_set_member.cvlW1ManifestJobRunId = cvl_manifest_wgs_job_id
        genomic_set_member.gemA1ManifestJobRunId = gem_a1_manifest_job_id
        genomic_set_member.cvlW1ManifestJobRunId = cvl_w1_manifest_job_id
        genomic_set_member.genomicWorkflowState = genomic_workflow_state

        member_dao = GenomicSetMemberDao()
        member_dao.insert(genomic_set_member)

    def _naive_utc_to_naive_central(self, naive_utc_date):
        utc_date = pytz.utc.localize(naive_utc_date)
        central_date = utc_date.astimezone(pytz.timezone("US/Central"))
        return central_date.replace(tzinfo=None)

    def _find_latest_genomic_set_csv(self, cloud_bucket_name, keyword=None):
        bucket_stat_list = list_blobs(cloud_bucket_name)
        if not bucket_stat_list:
            raise RuntimeError("No files in cloud bucket %r." % cloud_bucket_name)
        bucket_stat_list = [s for s in bucket_stat_list if s.name.lower().endswith(".csv")]
        if not bucket_stat_list:
            raise RuntimeError("No CSVs in cloud bucket %r (all files: %s)." % (cloud_bucket_name, bucket_stat_list))
        if keyword:
            buckt_stat_keyword_list = []
            for item in bucket_stat_list:
                if keyword in item.name:
                    buckt_stat_keyword_list.append(item)
            if buckt_stat_keyword_list:
                buckt_stat_keyword_list.sort(key=lambda s: s.updated)
                return buckt_stat_keyword_list[-1].name
            else:
                raise RuntimeError(
                    "No CSVs in cloud bucket %r with keyword %s (all files: %s)."
                    % (cloud_bucket_name, keyword, bucket_stat_list)
                )
        bucket_stat_list.sort(key=lambda s: s.updated)
        return bucket_stat_list[-1].name

    def _create_fake_datasets_for_gc_tests(self, count,
                                           arr_override=False,
                                           **kwargs):
        # fake genomic_set
        genomic_test_set = self._create_fake_genomic_set(
            genomic_set_name="genomic-test-set-cell-line",
            genomic_set_criteria=".",
            genomic_set_filename="genomic-test-set-cell-line.csv"
        )
        # make necessary fake participant data
        for p in range(1, count + 1):
            participant = self._make_participant()
            self._make_summary(participant)
            biobank_order = self._make_biobank_order(
                participantId=participant.participantId,
                biobankOrderId=p,
                identifiers=[BiobankOrderIdentifier(
                    system=u'c', value=u'e{}'.format(
                        participant.participantId))]
            )
            sample_args = {
                'test': '1SAL2',
                'confirmed': clock.CLOCK.now(),
                'created': clock.CLOCK.now(),
                'biobankId': p,
                'biobankOrderIdentifier': f'e{participant.participantId}',
                'biobankStoredSampleId': p,
            }
            with clock.FakeClock(clock.CLOCK.now()):
                self._make_stored_sample(**sample_args)
            # Fake genomic set members.
            gt = 'aou_wgs'
            if arr_override and p in kwargs.get('array_participants'):
                gt = 'aou_array'
            if kwargs.get('cvl'):
                gt = 'aou_cvl'
            self._create_fake_genomic_member(
                genomic_set_id=genomic_test_set.id,
                participant_id=participant.participantId,
                biobank_order_id=biobank_order.biobankOrderId,
                validation_status=GenomicSetMemberStatus.VALID,
                validation_flags=None,
                biobankId=p,
                sex_at_birth='F', genome_type=gt, ny_flag='Y',
                sequencing_filename=kwargs.get('sequencing_filename'),
                recon_bb_manifest_job_id=kwargs.get('bb_man_id'),
                recon_sequencing_job_id=kwargs.get('recon_seq_id'),
                recon_gc_manifest_job_id=kwargs.get('recon_gc_man_id'),
                gem_a1_manifest_job_id=kwargs.get('gem_a1_run_id'),
                cvl_w1_manifest_job_id=kwargs.get('cvl_w1_run_id'),
                genomic_workflow_state=kwargs.get('genomic_workflow_state')
            )

    def _update_site_states(self):
        sites = [self.site_dao.get(i) for i in range(1, 3)]
        sites[0].state = 'NY'
        sites[1].state = 'AZ'
        for site in sites:
            self.site_dao.update(site)

    def _setup_fake_sex_at_birth_codes(self, sex_code='n'):
        if sex_code.lower() == 'f':
            c_val = "SexAtBirth_Female"
        elif sex_code.lower() == 'm':
            c_val = "SexAtBirth_Male"
        else:
            c_val = "SexAtBirth_Intersex"
        code_to_insert = Code(
            system="a",
            value=c_val,
            display="c",
            topic="d",
            codeType=CodeType.ANSWER, mapped=True)
        return self.code_dao.insert(code_to_insert).codeId

    def _setup_fake_race_codes(self, native=False):
        c_val = "WhatRaceEthnicity_Hispanic"
        if native:
            c_val = "WhatRaceEthnicity_AIAN"
        code_to_insert = Code(
            system="a",
            value=c_val,
            display="c",
            topic="d",
            codeType=CodeType.ANSWER, mapped=True)
        return self.code_dao.insert(code_to_insert).codeId

    def test_gc_metrics_reconciliation_vs_manifest(self):
        # Create the fake Google Cloud CSV files to ingest
        self._create_fake_datasets_for_gc_tests(1, arr_override=True, array_participants=[1])
        bucket_name = _FAKE_GENOMIC_CENTER_BUCKET_A
        self._create_ingestion_test_file('RDR_AoU_GEN_TestDataManifest.csv',
                                         bucket_name,
                                         folder=config.GENOMIC_AW2_SUBFOLDERS[1])

        self._update_test_sample_ids()

        # Run the GC Metrics Ingestion workflow
        genomic_pipeline.ingest_genomic_centers_metrics_files()  # run_id = 1

        # Run the GC Metrics Reconciliation
        genomic_pipeline.reconcile_metrics_vs_manifest()  # run_id = 2
        test_set_member = self.member_dao.get(1)
        gc_metric_record = self.metrics_dao.get(1)

        # Test the gc_metrics were updated with reconciliation data
        self.assertEqual(test_set_member.id, gc_metric_record.genomicSetMemberId)
        self.assertEqual(2, test_set_member.reconcileMetricsBBManifestJobRunId)

        run_obj = self.job_run_dao.get(2)

        self.assertEqual(GenomicSubProcessResult.SUCCESS, run_obj.runResult)

    @mock.patch('rdr_service.genomic.genomic_job_components.GenomicAlertHandler')
    def test_gc_metrics_reconciliation_vs_genotyping_data(self, patched_handler):
        mock_alert_handler = patched_handler.return_value
        mock_alert_handler._jira_handler = 'fake_jira_handler'
        mock_alert_handler.make_genomic_alert.return_value = 1

        # Create the fake ingested data
        self._create_fake_datasets_for_gc_tests(2, arr_override=True, array_participants=[1,2])
        bucket_name = _FAKE_GENOMIC_CENTER_BUCKET_A
        self._create_ingestion_test_file('RDR_AoU_GEN_TestDataManifest.csv',
                                         bucket_name,
                                         folder=config.GENOMIC_AW2_SUBFOLDERS[1])

        self._update_test_sample_ids()

        genomic_pipeline.ingest_genomic_centers_metrics_files()  # run_id = 1
        manifest_file = self.file_processed_dao.get(1)

        # Test the reconciliation process
        sequencing_test_files = (
            f'test_data_folder/10001_R01C01.vcf.gz',
            f'test_data_folder/10001_R01C01.vcf.gz.tbi',
            f'test_data_folder/10001_R01C01.red.idat.gz',
            f'test_data_folder/10002_R01C02.vcf.gz',
            f'test_data_folder/10002_R01C02.vcf.gz.tbi',
            f'test_data_folder/10002_R01C02.red.idat.gz',
            f'test_data_folder/10002_R01C02.grn.idat.md5',
        )
        for f in sequencing_test_files:
            self._write_cloud_csv(f, 'attagc', bucket=bucket_name)

        genomic_pipeline.reconcile_metrics_vs_genotyping_data()  # run_id = 2

        gc_record = self.metrics_dao.get(1)

        # Test the gc_metrics were updated with reconciliation data
        self.assertEqual(1, gc_record.vcfReceived)
        self.assertEqual(1, gc_record.tbiReceived)
        self.assertEqual(1, gc_record.idatRedReceived)
        self.assertEqual(0, gc_record.idatGreenReceived)

        gc_record = self.metrics_dao.get(2)

        # Test the gc_metrics were updated with reconciliation data
        self.assertEqual(1, gc_record.vcfReceived)
        self.assertEqual(1, gc_record.tbiReceived)
        self.assertEqual(1, gc_record.idatRedReceived)
        self.assertEqual(1, gc_record.idatGreenReceived)

        # Test member updated with job ID
        member = self.member_dao.get(1)
        self.assertEqual(2, member.reconcileMetricsSequencingJobRunId)
        self.assertEqual(GenomicWorkflowState.AW2_MISSING, member.genomicWorkflowState)

        # Test member updated with job ID
        member = self.member_dao.get(2)
        self.assertEqual(2, member.reconcileMetricsSequencingJobRunId)
        self.assertEqual(GenomicWorkflowState.GEM_READY, member.genomicWorkflowState)

        # Fake alert
        summary = '[Genomic System Alert] Missing AW2 Array Manifest Files'
        description = "The following AW2 manifest file listed missing data."
        description += f"\nManifest File: {manifest_file.fileName}"
        description += "\nGenomic Job Run ID: 2"
        description += "\nMissing Genotype Data: ['10001_R01C01.grn.idat.md5']"

        mock_alert_handler.make_genomic_alert.assert_called_with(summary, description)

        run_obj = self.job_run_dao.get(2)

        self.assertEqual(GenomicSubProcessResult.SUCCESS, run_obj.runResult)

    @mock.patch('rdr_service.genomic.genomic_job_components.GenomicAlertHandler')
    def test_aw2_wgs_reconciliation_vs_sequencing_data(self, patched_handler):
        mock_alert_handler = patched_handler.return_value
        mock_alert_handler._jira_handler = 'fake_jira_handler'
        mock_alert_handler.make_genomic_alert.return_value = 1

        # Create the fake ingested data
        self._create_fake_datasets_for_gc_tests(2)
        bucket_name = _FAKE_GENOMIC_CENTER_BUCKET_A
        self._create_ingestion_test_file('RDR_AoU_SEQ_TestDataManifest.csv',
                                         bucket_name,
                                         folder=config.GENOMIC_AW2_SUBFOLDERS[0])

        self._update_test_sample_ids()

        genomic_pipeline.ingest_genomic_centers_metrics_files()  # run_id = 1
        manifest_file = self.file_processed_dao.get(1)

        # Test the reconciliation process
        sequencing_test_files = (
            f'test_data_folder/RDR_2_1002_LocalID_InternalRevisionNumber.hard-filtered.vcf.gz',
            f'test_data_folder/RDR_2_1002_LocalID_InternalRevisionNumber.hard-filtered.vcf.gz.tbi',
            f'test_data_folder/RDR_2_1002_LocalID_InternalRevisionNumber.hard-filtered.vcf.md5sum',
            f'test_data_folder/RDR_2_1002_LocalID_InternalRevisionNumber.vcf.gz',
            f'test_data_folder/RDR_2_1002_LocalID_InternalRevisionNumber.vcf.gz.tbi',
            f'test_data_folder/RDR_2_1002_LocalID_InternalRevisionNumber.vcf.md5sum',
            f'test_data_folder/RDR_2_1002_LocalID_InternalRevisionNumber.cram',
            f'test_data_folder/RDR_2_1002_LocalID_InternalRevisionNumber.cram.md5sum',
            f'test_data_folder/RDR_2_1002_LocalID_InternalRevisionNumber.crai.md5sum',
        )
        for f in sequencing_test_files:
            self._write_cloud_csv(f, 'attagc', bucket=bucket_name)

        genomic_pipeline.reconcile_metrics_vs_sequencing_data()  # run_id = 2

        gc_record = self.metrics_dao.get(1)

        # Test the gc_metrics were updated with reconciliation data
        self.assertEqual(1, gc_record.hfVcfReceived)
        self.assertEqual(1, gc_record.hfVcfTbiReceived)
        self.assertEqual(1, gc_record.hfVcfMd5Received)
        self.assertEqual(1, gc_record.rawVcfReceived)
        self.assertEqual(1, gc_record.rawVcfTbiReceived)
        self.assertEqual(1, gc_record.rawVcfMd5Received)
        self.assertEqual(1, gc_record.cramReceived)
        self.assertEqual(1, gc_record.cramMd5Received)
        self.assertEqual(0, gc_record.craiReceived)
        self.assertEqual(1, gc_record.craiMd5Received)

        # Test member updated with job ID and state
        member = self.member_dao.get(2)
        self.assertEqual(2, member.reconcileMetricsSequencingJobRunId)
        self.assertEqual(GenomicWorkflowState.AW2_MISSING, member.genomicWorkflowState)

        # Fake alert
        summary = '[Genomic System Alert] Missing AW2 WGS Manifest Files'
        description = "The following AW2 manifest file listed missing data."
        description += f"\nManifest File: {manifest_file.fileName}"
        description += "\nGenomic Job Run ID: 2"
        description += "\nMissing Genotype Data: ['RDR_2_1002_LocalID_InternalRevisionNumber.crai']"

        mock_alert_handler.make_genomic_alert.assert_called_with(summary, description)

        run_obj = self.job_run_dao.get(2)

        self.assertEqual(GenomicSubProcessResult.SUCCESS, run_obj.runResult)

    def test_new_participant_workflow(self):
        # Test for Cohort 3 workflow
        # create test samples
        test_biobank_ids = (100001, 100002, 100003, 100004, 100005, 100006, 100007)
        fake_datetime_old = datetime.datetime(2019, 12, 31, tzinfo=pytz.utc)
        fake_datetime_new = datetime.datetime(2020, 1, 5, tzinfo=pytz.utc)
        # update the sites' States for the state test (NY or AZ)
        self._update_site_states()

        # setup sex_at_birth code for unittests
        female_code = self._setup_fake_sex_at_birth_codes('f')
        intersex_code = self._setup_fake_sex_at_birth_codes()

        # Setup race codes for unittests
        non_native_code = self._setup_fake_race_codes(native=False)
        native_code = self._setup_fake_race_codes(native=True)

        # Setup the biobank order backend
        for bid in test_biobank_ids:
            p = self._make_participant(biobankId=bid)
            self._make_summary(p, sexId=intersex_code if bid == 100004 else female_code,
                               consentForStudyEnrollment=0 if bid == 100006 else 1,
                               sampleStatus1ED04=0,
                               sampleStatus1SAL2=0 if bid == 100005 else 1,
                               samplesToIsolateDNA=0,
                               race=Race.HISPANIC_LATINO_OR_SPANISH,
                               consentCohort=3)
            # Insert participant races
            race_answer = ParticipantRaceAnswers(
                participantId=p.participantId,
                codeId=native_code if bid == 100007 else non_native_code
            )
            self.race_dao.insert(race_answer)
            test_identifier = BiobankOrderIdentifier(
                    system=u'c',
                    value=u'e{}'.format(bid))
            self._make_biobank_order(biobankOrderId=f'W{bid}',
                                     participantId=p.participantId,
                                     collectedSiteId=1 if bid == 100002 else 2,
                                     identifiers=[test_identifier])
            sample_args = {
                'test': '1UR10' if bid == 100005 else '1SAL2',
                'confirmed': fake_datetime_new,
                'created': fake_datetime_old,
                'biobankId': bid,
                'biobankOrderIdentifier': test_identifier.value,
                'biobankStoredSampleId': bid,
            }
            insert_dtm = fake_datetime_new
            if bid == 100001:
                insert_dtm = fake_datetime_old
            with clock.FakeClock(insert_dtm):
                self._make_stored_sample(**sample_args)

        # insert an 'already ran' workflow to test proper exclusions
        self.job_run_dao.insert(GenomicJobRun(
            id=1,
            jobId=GenomicJob.NEW_PARTICIPANT_WORKFLOW,
            startTime=datetime.datetime(2020, 1, 1),
            endTime=datetime.datetime(2020, 1, 1),
            runStatus=GenomicSubProcessStatus.COMPLETED,
            runResult=GenomicSubProcessResult.SUCCESS
        ))

        # run new participant workflow and test results
        genomic_pipeline.new_participant_workflow()

        new_genomic_set = self.set_dao.get_all()
        self.assertEqual(1, len(new_genomic_set))

        # Should be a aou_wgs and aou_array for each
        new_genomic_members = self.member_dao.get_all()
        self.assertEqual(10, len(new_genomic_members))

        # Test GenomicMember's data
        # 100001 : Excluded, created before last run,
        # 100005 : Excluded, no DNA sample
        member_genome_types = {_member.biobankId: list() for _member in new_genomic_members}
        for member in new_genomic_members:
            member_genome_types[member.biobankId].append(member.genomeType)
            if member.biobankId == '100002':
                # 100002 : Included, Valid
                self.assertEqual(1, member.nyFlag)
                self.assertEqual('100002', member.collectionTubeId)
                self.assertEqual('F', member.sexAtBirth)
                self.assertEqual(GenomicSetMemberStatus.VALID, member.validationStatus)
                self.assertEqual('N', member.ai_an)
            if member.biobankId == '100003':
                # 100003 : Included, Valid
                self.assertEqual(0, member.nyFlag)
                self.assertEqual('100003', member.collectionTubeId)
                self.assertEqual('F', member.sexAtBirth)
                self.assertEqual(GenomicSetMemberStatus.VALID, member.validationStatus)
                self.assertEqual('N', member.ai_an)
            if member.biobankId == '100004':
                # 100004 : Included, Invalid SAB
                self.assertEqual(0, member.nyFlag)
                self.assertEqual('100004', member.collectionTubeId)
                self.assertEqual('NA', member.sexAtBirth)
                self.assertEqual(GenomicSetMemberStatus.INVALID, member.validationStatus)
                self.assertEqual('N', member.ai_an)
            if member.biobankId == '100006':
                # 100006 : Included, Invalid consent
                self.assertEqual(0, member.nyFlag)
                self.assertEqual('100006', member.collectionTubeId)
                self.assertEqual('F', member.sexAtBirth)
                self.assertEqual(GenomicSetMemberStatus.INVALID, member.validationStatus)
                self.assertEqual('N', member.ai_an)
            if member.biobankId == '100007':
                # 100007 : Included, Invalid Indian/Native
                self.assertEqual(0, member.nyFlag)
                self.assertEqual('100007', member.collectionTubeId)
                self.assertEqual('F', member.sexAtBirth)
                self.assertEqual(GenomicSetMemberStatus.INVALID, member.validationStatus)
                self.assertEqual('Y', member.ai_an)
        for bbid in member_genome_types.keys():
            self.assertIn('aou_array', member_genome_types[bbid])
            self.assertIn('aou_wgs', member_genome_types[bbid])

        # Test manifest file was created correctly
        bucket_name = config.getSetting(config.BIOBANK_SAMPLES_BUCKET_NAME)

        class ExpectedCsvColumns(object):
            VALUE = "value"
            BIOBANK_ID = "biobank_id"
            SAMPLE_ID = "sample_id"
            SEX_AT_BIRTH = "sex_at_birth"
            GENOME_TYPE = "genome_type"
            NY_FLAG = "ny_flag"
            REQUEST_ID = "request_id"
            PACKAGE_ID = "package_id"
            VALIDATION_PASSED = 'validation_passed'
            AI_AN = 'ai_an'

            ALL = (VALUE, SEX_AT_BIRTH, GENOME_TYPE, NY_FLAG,
                   REQUEST_ID, PACKAGE_ID, VALIDATION_PASSED, AI_AN)

        blob_name = self._find_latest_genomic_set_csv(bucket_name, _FAKE_BUCKET_FOLDER)
        with open_cloud_file(os.path.normpath(bucket_name + '/' + blob_name)) as csv_file:
            csv_reader = csv.DictReader(csv_file, delimiter=",")
            missing_cols = set(ExpectedCsvColumns.ALL) - set(csv_reader.fieldnames)
            self.assertEqual(0, len(missing_cols))
            rows = list(csv_reader)

            self.assertEqual("T100002", rows[0][ExpectedCsvColumns.BIOBANK_ID])
            self.assertEqual(100002, int(rows[0][ExpectedCsvColumns.SAMPLE_ID]))
            self.assertEqual("F", rows[0][ExpectedCsvColumns.SEX_AT_BIRTH])
            self.assertEqual("Y", rows[0][ExpectedCsvColumns.NY_FLAG])
            self.assertEqual("Y", rows[0][ExpectedCsvColumns.VALIDATION_PASSED])
            self.assertEqual("N", rows[0][ExpectedCsvColumns.AI_AN])
            self.assertEqual("aou_array", rows[0][ExpectedCsvColumns.GENOME_TYPE])

            self.assertEqual("T100002", rows[1][ExpectedCsvColumns.BIOBANK_ID])
            self.assertEqual(100002, int(rows[1][ExpectedCsvColumns.SAMPLE_ID]))
            self.assertEqual("F", rows[1][ExpectedCsvColumns.SEX_AT_BIRTH])
            self.assertEqual("Y", rows[1][ExpectedCsvColumns.NY_FLAG])
            self.assertEqual("Y", rows[1][ExpectedCsvColumns.VALIDATION_PASSED])
            self.assertEqual("N", rows[1][ExpectedCsvColumns.AI_AN])
            self.assertEqual("aou_wgs", rows[1][ExpectedCsvColumns.GENOME_TYPE])

            self.assertEqual("T100003", rows[2][ExpectedCsvColumns.BIOBANK_ID])
            self.assertEqual(100003, int(rows[2][ExpectedCsvColumns.SAMPLE_ID]))
            self.assertEqual("F", rows[2][ExpectedCsvColumns.SEX_AT_BIRTH])
            self.assertEqual("N", rows[2][ExpectedCsvColumns.NY_FLAG])
            self.assertEqual("Y", rows[2][ExpectedCsvColumns.VALIDATION_PASSED])
            self.assertEqual("N", rows[2][ExpectedCsvColumns.AI_AN])
            self.assertEqual("aou_array", rows[2][ExpectedCsvColumns.GENOME_TYPE])

            self.assertEqual("T100003", rows[3][ExpectedCsvColumns.BIOBANK_ID])
            self.assertEqual(100003, int(rows[3][ExpectedCsvColumns.SAMPLE_ID]))
            self.assertEqual("F", rows[3][ExpectedCsvColumns.SEX_AT_BIRTH])
            self.assertEqual("N", rows[3][ExpectedCsvColumns.NY_FLAG])
            self.assertEqual("Y", rows[3][ExpectedCsvColumns.VALIDATION_PASSED])
            self.assertEqual("N", rows[3][ExpectedCsvColumns.AI_AN])
            self.assertEqual("aou_wgs", rows[3][ExpectedCsvColumns.GENOME_TYPE])

            self.assertEqual("T100004", rows[4][ExpectedCsvColumns.BIOBANK_ID])
            self.assertEqual(100004, int(rows[4][ExpectedCsvColumns.SAMPLE_ID]))
            self.assertEqual("NA", rows[4][ExpectedCsvColumns.SEX_AT_BIRTH])
            self.assertEqual("N", rows[4][ExpectedCsvColumns.NY_FLAG])
            self.assertEqual("N", rows[4][ExpectedCsvColumns.VALIDATION_PASSED])
            self.assertEqual("N", rows[4][ExpectedCsvColumns.AI_AN])
            self.assertEqual("aou_array", rows[4][ExpectedCsvColumns.GENOME_TYPE])

            self.assertEqual("T100004", rows[5][ExpectedCsvColumns.BIOBANK_ID])
            self.assertEqual(100004, int(rows[5][ExpectedCsvColumns.SAMPLE_ID]))
            self.assertEqual("NA", rows[5][ExpectedCsvColumns.SEX_AT_BIRTH])
            self.assertEqual("N", rows[5][ExpectedCsvColumns.NY_FLAG])
            self.assertEqual("N", rows[5][ExpectedCsvColumns.VALIDATION_PASSED])
            self.assertEqual("N", rows[5][ExpectedCsvColumns.AI_AN])
            self.assertEqual("aou_wgs", rows[5][ExpectedCsvColumns.GENOME_TYPE])

            self.assertEqual("T100006", rows[6][ExpectedCsvColumns.BIOBANK_ID])
            self.assertEqual(100006, int(rows[6][ExpectedCsvColumns.SAMPLE_ID]))
            self.assertEqual("F", rows[6][ExpectedCsvColumns.SEX_AT_BIRTH])
            self.assertEqual("N", rows[6][ExpectedCsvColumns.NY_FLAG])
            self.assertEqual("N", rows[6][ExpectedCsvColumns.VALIDATION_PASSED])
            self.assertEqual("N", rows[6][ExpectedCsvColumns.AI_AN])
            self.assertEqual("aou_array", rows[6][ExpectedCsvColumns.GENOME_TYPE])

            self.assertEqual("T100006", rows[7][ExpectedCsvColumns.BIOBANK_ID])
            self.assertEqual(100006, int(rows[7][ExpectedCsvColumns.SAMPLE_ID]))
            self.assertEqual("F", rows[7][ExpectedCsvColumns.SEX_AT_BIRTH])
            self.assertEqual("N", rows[7][ExpectedCsvColumns.NY_FLAG])
            self.assertEqual("N", rows[7][ExpectedCsvColumns.VALIDATION_PASSED])
            self.assertEqual("N", rows[7][ExpectedCsvColumns.AI_AN])
            self.assertEqual("aou_wgs", rows[7][ExpectedCsvColumns.GENOME_TYPE])

            self.assertEqual("T100007", rows[8][ExpectedCsvColumns.BIOBANK_ID])
            self.assertEqual(100007, int(rows[8][ExpectedCsvColumns.SAMPLE_ID]))
            self.assertEqual("F", rows[8][ExpectedCsvColumns.SEX_AT_BIRTH])
            self.assertEqual("N", rows[8][ExpectedCsvColumns.NY_FLAG])
            self.assertEqual("N", rows[8][ExpectedCsvColumns.VALIDATION_PASSED])
            self.assertEqual("Y", rows[8][ExpectedCsvColumns.AI_AN])
            self.assertEqual("aou_array", rows[8][ExpectedCsvColumns.GENOME_TYPE])

            self.assertEqual("T100007", rows[9][ExpectedCsvColumns.BIOBANK_ID])
            self.assertEqual(100007, int(rows[9][ExpectedCsvColumns.SAMPLE_ID]))
            self.assertEqual("F", rows[9][ExpectedCsvColumns.SEX_AT_BIRTH])
            self.assertEqual("N", rows[9][ExpectedCsvColumns.NY_FLAG])
            self.assertEqual("N", rows[9][ExpectedCsvColumns.VALIDATION_PASSED])
            self.assertEqual("Y", rows[9][ExpectedCsvColumns.AI_AN])
            self.assertEqual("aou_wgs", rows[9][ExpectedCsvColumns.GENOME_TYPE])

        # Test the end-to-end result code
        self.assertEqual(GenomicSubProcessResult.SUCCESS, self.job_run_dao.get(2).runResult)

    def test_c2_participant_workflow(self):
        # Test for Cohort 2 workflow
        # create test samples
        test_biobank_ids = (100001, 100002, 100003)
        fake_datetime_old = datetime.datetime(2019, 12, 31, tzinfo=pytz.utc)
        fake_datetime_new = datetime.datetime(2020, 1, 5, tzinfo=pytz.utc)

        # update the sites' States for the state test (NY or AZ)
        self._update_site_states()

        # setup sex_at_birth code for unittests
        female_code = self._setup_fake_sex_at_birth_codes('f')

        # Setup race codes for unittests
        non_native_code = self._setup_fake_race_codes(native=False)

        # Setup the biobank order backend
        for bid in test_biobank_ids:
            p = self._make_participant(biobankId=bid)
            self._make_summary(p, sexId=female_code,
                               consentForStudyEnrollment=1,
                               sampleStatus1ED04=0,
                               sampleStatus1SAL2=1,
                               consentCohort=3 if bid == 100003 else 2,
                               questionnaireOnDnaProgram=QuestionnaireStatus.SUBMITTED if bid != 100003 else None,
                               questionnaireOnDnaProgramAuthored=clock.CLOCK.now() if bid != 100003 else None,
                               race=Race.HISPANIC_LATINO_OR_SPANISH)
            # Insert participant races
            race_answer = ParticipantRaceAnswers(
                participantId=p.participantId,
                codeId=non_native_code
            )

            self.race_dao.insert(race_answer)
            test_identifier = BiobankOrderIdentifier(
                system=u'c',
                value=u'e{}'.format(bid))
            self._make_biobank_order(biobankOrderId=f'W{bid}',
                                     participantId=p.participantId,
                                     collectedSiteId=1 if bid == 100002 else 2,
                                     identifiers=[test_identifier])
            sample_args = {
                'test': '1SAL2',
                'confirmed': fake_datetime_new,
                'created': fake_datetime_old,
                'biobankId': bid,
                'biobankOrderIdentifier': test_identifier.value,
                'biobankStoredSampleId': bid,
            }
            insert_dtm = fake_datetime_new
            if bid == 100001:
                insert_dtm = fake_datetime_old
            with clock.FakeClock(insert_dtm):
                self._make_stored_sample(**sample_args)

        # run C2 participant workflow and test results
        genomic_pipeline.c2_participant_workflow()

        new_genomic_set = self.set_dao.get_all()
        self.assertEqual(1, len(new_genomic_set))

        # Should be a aou_wgs and aou_array for each pid
        new_genomic_members = self.member_dao.get_all()
        self.assertEqual(4, len(new_genomic_members))

        # Test member data
        member_genome_types = {_member.biobankId: list() for _member in new_genomic_members}
        for member in new_genomic_members:
            member_genome_types[member.biobankId].append(member.genomeType)

            if member.biobankId == '100001':
                # 100002 : Included, Valid
                self.assertEqual(0, member.nyFlag)
                self.assertEqual('100001', member.collectionTubeId)
                self.assertEqual('F', member.sexAtBirth)
                self.assertEqual(GenomicSetMemberStatus.VALID, member.validationStatus)
                self.assertEqual('N', member.ai_an)

            if member.biobankId == '100002':
                # 100003 : Included, Valid
                self.assertEqual(1, member.nyFlag)
                self.assertEqual('100002', member.collectionTubeId)
                self.assertEqual('F', member.sexAtBirth)
                self.assertEqual(GenomicSetMemberStatus.VALID, member.validationStatus)
                self.assertEqual('N', member.ai_an)

        for bbid in member_genome_types.keys():
            self.assertIn('aou_array', member_genome_types[bbid])
            self.assertIn('aou_wgs', member_genome_types[bbid])

            # Test manifest file was created correctly
            bucket_name = config.getSetting(config.BIOBANK_SAMPLES_BUCKET_NAME)

            class ExpectedCsvColumns(object):
                VALUE = "value"
                BIOBANK_ID = "biobank_id"
                SAMPLE_ID = "sample_id"
                SEX_AT_BIRTH = "sex_at_birth"
                GENOME_TYPE = "genome_type"
                NY_FLAG = "ny_flag"
                REQUEST_ID = "request_id"
                PACKAGE_ID = "package_id"
                VALIDATION_PASSED = 'validation_passed'
                AI_AN = 'ai_an'

                ALL = (VALUE, SEX_AT_BIRTH, GENOME_TYPE, NY_FLAG,
                       REQUEST_ID, PACKAGE_ID, VALIDATION_PASSED, AI_AN)

            blob_name = self._find_latest_genomic_set_csv(bucket_name, _FAKE_BUCKET_FOLDER)
            with open_cloud_file(os.path.normpath(bucket_name + '/' + blob_name)) as csv_file:
                csv_reader = csv.DictReader(csv_file, delimiter=",")
                missing_cols = set(ExpectedCsvColumns.ALL) - set(csv_reader.fieldnames)
                self.assertEqual(0, len(missing_cols))
                rows = list(csv_reader)

                self.assertEqual("T100001", rows[0][ExpectedCsvColumns.BIOBANK_ID])
                self.assertEqual(100001, int(rows[0][ExpectedCsvColumns.SAMPLE_ID]))
                self.assertEqual("F", rows[0][ExpectedCsvColumns.SEX_AT_BIRTH])
                self.assertEqual("N", rows[0][ExpectedCsvColumns.NY_FLAG])
                self.assertEqual("Y", rows[0][ExpectedCsvColumns.VALIDATION_PASSED])
                self.assertEqual("N", rows[0][ExpectedCsvColumns.AI_AN])
                self.assertEqual("aou_array", rows[0][ExpectedCsvColumns.GENOME_TYPE])

                self.assertEqual("T100001", rows[1][ExpectedCsvColumns.BIOBANK_ID])
                self.assertEqual(100001, int(rows[1][ExpectedCsvColumns.SAMPLE_ID]))
                self.assertEqual("F", rows[1][ExpectedCsvColumns.SEX_AT_BIRTH])
                self.assertEqual("N", rows[1][ExpectedCsvColumns.NY_FLAG])
                self.assertEqual("Y", rows[1][ExpectedCsvColumns.VALIDATION_PASSED])
                self.assertEqual("N", rows[1][ExpectedCsvColumns.AI_AN])
                self.assertEqual("aou_wgs", rows[1][ExpectedCsvColumns.GENOME_TYPE])

                self.assertEqual("T100002", rows[2][ExpectedCsvColumns.BIOBANK_ID])
                self.assertEqual(100002, int(rows[2][ExpectedCsvColumns.SAMPLE_ID]))
                self.assertEqual("F", rows[2][ExpectedCsvColumns.SEX_AT_BIRTH])
                self.assertEqual("Y", rows[2][ExpectedCsvColumns.NY_FLAG])
                self.assertEqual("Y", rows[2][ExpectedCsvColumns.VALIDATION_PASSED])
                self.assertEqual("N", rows[2][ExpectedCsvColumns.AI_AN])
                self.assertEqual("aou_array", rows[2][ExpectedCsvColumns.GENOME_TYPE])

                self.assertEqual("T100002", rows[3][ExpectedCsvColumns.BIOBANK_ID])
                self.assertEqual(100002, int(rows[3][ExpectedCsvColumns.SAMPLE_ID]))
                self.assertEqual("F", rows[3][ExpectedCsvColumns.SEX_AT_BIRTH])
                self.assertEqual("Y", rows[3][ExpectedCsvColumns.NY_FLAG])
                self.assertEqual("Y", rows[3][ExpectedCsvColumns.VALIDATION_PASSED])
                self.assertEqual("N", rows[3][ExpectedCsvColumns.AI_AN])
                self.assertEqual("aou_wgs", rows[3][ExpectedCsvColumns.GENOME_TYPE])

    def test_gc_manifest_ingestion_workflow(self):
        self._create_fake_datasets_for_gc_tests(3, arr_override=True,
                                                array_participants=range(1, 4),
                                                genomic_workflow_state=GenomicWorkflowState.AW0)

        # Setup Test file
        gc_manifest_file = test_data.open_genomic_set_file("Genomic-GC-Manifest-Workflow-Test-1.csv")

        gc_manifest_filename = "RDR_AoU_GEN_PKG-1908-218051.csv"

        self._write_cloud_csv(
            gc_manifest_filename,
            gc_manifest_file,
            bucket=_FAKE_GENOMIC_CENTER_BUCKET_A,
            folder=_FAKE_GENOTYPING_FOLDER,
        )

        genomic_pipeline.genomic_centers_manifest_workflow()

        # Test the data was ingested OK
        for member in self.member_dao.get_all():
            if member.id in [1, 2]:
                self.assertEqual(1, member.reconcileGCManifestJobRunId)
                self.assertEqual('rdr', member.gcSiteId)
                # Package ID represents that BB sample was reconciled to GC Manifest
                self.assertEqual("PKG-1908-218051", member.packageId)
                self.assertEqual("SU-0026388097", member.gcManifestBoxStorageUnitId)
                self.assertEqual("BX-00299188", member.gcManifestBoxPlateId)
                self.assertEqual(f"A0{member.id}", member.gcManifestWellPosition)
                # self.assertEqual("", member.gcManifestParentSampleId)
                # self.assertEqual("", member.gcManifestMatrixId)
                self.assertEqual("TE", member.gcManifestTreatments)
                self.assertEqual(40, member.gcManifestQuantity_ul)
                self.assertEqual(60, member.gcManifestTotalConcentration_ng_per_ul)
                self.assertEqual(2400, member.gcManifestTotalDNA_ng)
                self.assertEqual("All", member.gcManifestVisitDescription)
                self.assertEqual("Other", member.gcManifestSampleSource)
                self.assertEqual("PMI Coriell Samples Only", member.gcManifestStudy)
                self.assertEqual("475523957339", member.gcManifestTrackingNumber)
                self.assertEqual("Samantha Wirkus", member.gcManifestContact)
                self.assertEqual("Wirkus.Samantha@mayo.edu", member.gcManifestEmail)
                self.assertEqual("Josh Denny", member.gcManifestStudyPI)
                self.assertEqual("aou_array", member.gcManifestTestName)
                self.assertEqual("", member.gcManifestFailureMode)
                self.assertEqual("", member.gcManifestFailureDescription)
                self.assertEqual(GenomicWorkflowState.AW1, member.genomicWorkflowState)
            if member.id == 3:
                self.assertNotEqual(1, member.reconcileGCManifestJobRunId)

        # Test file processing queue
        files_processed = self.file_processed_dao.get_all()
        self.assertEqual(len(files_processed), 1)

        # Test the end-to-end result code
        self.assertEqual(GenomicSubProcessResult.SUCCESS, self.job_run_dao.get(1).runResult)

    def test_aw1f_ingestion_workflow(self):
        # Setup test data: 1 aou_array, 1 aou_wgs
        self._create_fake_datasets_for_gc_tests(2, arr_override=True,
                                                array_participants=[1],
                                                genomic_workflow_state=GenomicWorkflowState.AW0)

        # Setup Test AW1 file
        gc_manifest_file = test_data.open_genomic_set_file("Genomic-GC-Manifest-Workflow-Test-2.csv")
        gc_manifest_filename = "RDR_AoU_GEN_PKG-1908-218051.csv"
        self._write_cloud_csv(
            gc_manifest_filename,
            gc_manifest_file,
            bucket=_FAKE_GENOMIC_CENTER_BUCKET_A,
            folder=_FAKE_GENOTYPING_FOLDER,
        )

        # Setup Test AW1F file
        gc_manifest_file = test_data.open_genomic_set_file("Genomic-AW1F-Workflow-Test-1.csv")
        gc_manifest_filename = "RDR_AoU_SEQ_PKG-1908-218051_FAILURE.csv"
        self._write_cloud_csv(
            gc_manifest_filename,
            gc_manifest_file,
            bucket=_FAKE_GENOMIC_CENTER_BUCKET_A,
            folder=_FAKE_FAILURE_FOLDER,
        )

        # Ingest AW1
        genomic_pipeline.genomic_centers_manifest_workflow()

        # Ingest AW1F
        genomic_pipeline.genomic_centers_aw1f_manifest_workflow()

        # Test db updated
        members = sorted(self.member_dao.get_all(), key=lambda x: x.id)
        self.assertEqual(members[1].gcManifestFailureMode, 'damaged')
        self.assertEqual(members[1].gcManifestFailureDescription, 'Arrived and damaged')
        self.assertEqual(members[1].genomicWorkflowState, GenomicWorkflowState.AW1F_POST)

        # Test file processing queue
        files_processed = self.file_processed_dao.get_all()
        self.assertEqual(len(files_processed), 2)

        # Test the end-to-end result code
        self.assertEqual(GenomicSubProcessResult.SUCCESS, self.job_run_dao.get(2).runResult)

    def test_gem_a1_manifest_end_to_end(self):
        # Need GC Manifest for source query : run_id = 1
        self.job_run_dao.insert(GenomicJobRun(jobId=GenomicJob.AW1_MANIFEST,
                                              startTime=clock.CLOCK.now(),
                                              runStatus=GenomicSubProcessStatus.COMPLETED,
                                              runResult=GenomicSubProcessResult.SUCCESS))
        self._create_fake_datasets_for_gc_tests(3, arr_override=True,
                                                array_participants=range(1, 4),
                                                recon_gc_man_id=1)
        bucket_name = _FAKE_GENOMIC_CENTER_BUCKET_A
        self._create_ingestion_test_file('RDR_AoU_GEN_TestDataManifest.csv',
                                         bucket_name,
                                         folder=config.GENOMIC_AW2_SUBFOLDERS[1])

        self._update_test_sample_ids()

        genomic_pipeline.ingest_genomic_centers_metrics_files()  # run_id = 2

        # Test sequencing file (required for GEM)
        sequencing_test_files = (
            f'test_data_folder/10001_R01C01.vcf.gz',
            f'test_data_folder/10001_R01C01.vcf.gz.tbi',
            f'test_data_folder/10001_R01C01.red.idat.gz',
            f'test_data_folder/10001_R01C01.grn.idat.md5',
            f'test_data_folder/10002_R01C02.vcf.gz',
            f'test_data_folder/10002_R01C02.vcf.gz.tbi',
            f'test_data_folder/10002_R01C02.red.idat.gz',
            f'test_data_folder/10002_R01C02.grn.idat.md5',
        )
        for f in sequencing_test_files:
            self._write_cloud_csv(f, 'attagc', bucket=bucket_name)

        genomic_pipeline.reconcile_metrics_vs_manifest()  # run_id = 3
        genomic_pipeline.reconcile_metrics_vs_genotyping_data()  # run_id = 4

        # finally run the manifest workflow
        bucket_name = config.getSetting(config.GENOMIC_GEM_BUCKET_NAME)
        a1_time = datetime.datetime(2020, 4, 1, 0, 0, 0, 0)
        with clock.FakeClock(a1_time):
            genomic_pipeline.gem_a1_manifest_workflow()  # run_id = 5
        a1f = a1_time.strftime("%Y-%m-%d-%H-%M-%S")
        # Test Genomic Set Member updated with GEM Array Manifest job run
        with self.member_dao.session() as member_session:
            test_member_1 = member_session.query(
                GenomicSet.genomicSetName,
                GenomicSetMember.biobankId,
                GenomicSetMember.sampleId,
                GenomicSetMember.sexAtBirth,
                GenomicSetMember.nyFlag,
                GenomicSetMember.gemA1ManifestJobRunId,
                GenomicGCValidationMetrics.siteId,
                GenomicSetMember.genomicWorkflowState).filter(
                GenomicGCValidationMetrics.genomicSetMemberId == GenomicSetMember.id,
                GenomicSet.id == GenomicSetMember.genomicSetId,
                GenomicSetMember.id == 1
            ).one()

        self.assertEqual(5, test_member_1.gemA1ManifestJobRunId)
        self.assertEqual(GenomicWorkflowState.A1, test_member_1.genomicWorkflowState)

        # Test the manifest file contents
        expected_gem_columns = (
            "biobank_id",
            "sample_id",
            "sex_at_birth",
        )
        sub_folder = config.GENOMIC_GEM_A1_MANIFEST_SUBFOLDER
        with open_cloud_file(os.path.normpath(f'{bucket_name}/{sub_folder}/AoU_GEM_Manifest_{a1f}.csv')) as csv_file:
            csv_reader = csv.DictReader(csv_file)
            missing_cols = set(expected_gem_columns) - set(csv_reader.fieldnames)
            self.assertEqual(0, len(missing_cols))
            rows = list(csv_reader)
            self.assertEqual(2, len(rows))
            self.assertEqual(test_member_1.biobankId, rows[0]['biobank_id'])
            self.assertEqual(test_member_1.sampleId, rows[0]['sample_id'])
            self.assertEqual(test_member_1.sexAtBirth, rows[0]['sex_at_birth'])

        # Array
        file_record = self.file_processed_dao.get(2)  # remember, GC Metrics is #1
        self.assertEqual(5, file_record.runId)
        self.assertEqual(f'{sub_folder}/AoU_GEM_Manifest_{a1f}.csv', file_record.fileName)

        # Test the job result
        run_obj = self.job_run_dao.get(4)
        self.assertEqual(GenomicSubProcessResult.SUCCESS, run_obj.runResult)

        # Test Withdrawn and then Reconsented
        # Do withdraw GROR
        withdraw_time = datetime.datetime(2020, 4, 2, 0, 0, 0, 0)
        summary1 = self.summary_dao.get(1)
        summary1.consentForGenomicsROR = QuestionnaireStatus.SUBMITTED_NO_CONSENT
        summary1.consentForGenomicsRORAuthored = withdraw_time
        self.summary_dao.update(summary1)
        # Run A3 manifest
        with clock.FakeClock(withdraw_time):
            genomic_pipeline.gem_a3_manifest_workflow()  # run_id 6

        # Do Reconsent ROR
        reconsent_time = datetime.datetime(2020, 4, 3, 0, 0, 0, 0)
        summary1.consentForGenomicsROR = QuestionnaireStatus.SUBMITTED
        summary1.consentForGenomicsRORAuthored = reconsent_time
        self.summary_dao.update(summary1)
        # Run A1 Again
        with clock.FakeClock(reconsent_time):
            genomic_pipeline.gem_a1_manifest_workflow()  # run_id 7
        a1f = reconsent_time.strftime("%Y-%m-%d-%H-%M-%S")
        # Test record was included again
        with open_cloud_file(os.path.normpath(f'{bucket_name}/{sub_folder}/AoU_GEM_Manifest_{a1f}.csv')) as csv_file:
            csv_reader = csv.DictReader(csv_file)
            rows = list(csv_reader)
            self.assertEqual(1, len(rows))
            self.assertEqual(test_member_1.biobankId, rows[0]['biobank_id'])

    def test_gem_a2_manifest_workflow(self):
        # Create A1 manifest job run: id = 1
        self.job_run_dao.insert(GenomicJobRun(jobId=GenomicJob.GEM_A1_MANIFEST,
                                              startTime=clock.CLOCK.now(),
                                              runStatus=GenomicSubProcessStatus.COMPLETED,
                                              runResult=GenomicSubProcessResult.SUCCESS))
        # Create genomic set members
        self._create_fake_datasets_for_gc_tests(3, arr_override=True,
                                                array_participants=range(1, 4),
                                                gem_a1_run_id=1)

        self._update_test_sample_ids()

        # Set up test A2 manifest
        bucket_name = config.getSetting(config.GENOMIC_GEM_BUCKET_NAME)
        sub_folder = config.GENOMIC_GEM_A2_MANIFEST_SUBFOLDER
        self._create_ingestion_test_file('AoU_GEM_Manifest_2.csv',
                                         bucket_name, folder=sub_folder,
                                         include_timestamp=False)
        # Run Workflow
        genomic_pipeline.gem_a2_manifest_workflow()  # run_id 2

        # Test gem_pass field
        members = self.member_dao.get_all()
        for member in members:
            if member.id in (1, 2):
                self.assertEqual("Y", member.gemPass)
                self.assertEqual(2, member.gemA2ManifestJobRunId)
            if member.id == 3:
                self.assertEqual("N", member.gemPass)

        # Test Files Processed
        file_record = self.file_processed_dao.get(1)
        self.assertEqual(2, file_record.runId)
        self.assertEqual(f'/{bucket_name}/{sub_folder}/AoU_GEM_Manifest_2.csv', file_record.filePath)
        self.assertEqual('AoU_GEM_Manifest_2.csv', file_record.fileName)

        # Test the job result
        run_obj = self.job_run_dao.get(2)
        self.assertEqual(GenomicSubProcessResult.SUCCESS, run_obj.runResult)

    def test_gem_a3_manifest_workflow(self):
        # Create A1 manifest job run: id = 1
        self.job_run_dao.insert(GenomicJobRun(jobId=GenomicJob.GEM_A1_MANIFEST,
                                              startTime=clock.CLOCK.now(),
                                              runStatus=GenomicSubProcessStatus.COMPLETED,
                                              runResult=GenomicSubProcessResult.SUCCESS))

        # Create genomic set members
        self._create_fake_datasets_for_gc_tests(3, arr_override=True,
                                                array_participants=range(1, 4),
                                                gem_a1_run_id=1,
                                                genomic_workflow_state=GenomicWorkflowState.GEM_RPT_READY)

        self._update_test_sample_ids()

        p3 = self.summary_dao.get(3)
        p3.consentForGenomicsROR = QuestionnaireStatus.SUBMITTED_NO_CONSENT
        p3.consentForGenomicsRORAuthored = datetime.datetime(2020, 5, 25, 0, 0, 0)
        self.summary_dao.update(p3)

        # Run Workflow
        fake_now = datetime.datetime.utcnow()
        out_time = fake_now.strftime("%Y-%m-%d-%H-%M-%S")
        with clock.FakeClock(fake_now):
            genomic_pipeline.gem_a3_manifest_workflow()  # run_id 2

        # Test the member job run ID
        test_member = self.member_dao.get(3)
        self.assertEqual(2, test_member.gemA3ManifestJobRunId)
        self.assertEqual(GenomicWorkflowState.GEM_RPT_DELETED, test_member.genomicWorkflowState)

        # Test the manifest file contents
        bucket_name = config.getSetting(config.GENOMIC_GEM_BUCKET_NAME)
        sub_folder = GENOMIC_GEM_A3_MANIFEST_SUBFOLDER

        expected_gem_columns = (
            "biobank_id",
            "sample_id",
        )
        with open_cloud_file(os.path.normpath(f'{bucket_name}/{sub_folder}/AoU_GEM_WD_{out_time}.csv')) as csv_file:
            csv_reader = csv.DictReader(csv_file)
            missing_cols = set(expected_gem_columns) - set(csv_reader.fieldnames)
            self.assertEqual(0, len(missing_cols))
            rows = list(csv_reader)
            self.assertEqual(1, len(rows))
            self.assertEqual(test_member.biobankId, rows[0]['biobank_id'])
            self.assertEqual(test_member.sampleId, rows[0]['sample_id'])

        # Array
        file_record = self.file_processed_dao.get(1)  # remember, GC Metrics is #1
        self.assertEqual(2, file_record.runId)
        self.assertEqual(f'{sub_folder}/AoU_GEM_WD_{out_time}.csv', file_record.fileName)

        # Test the job result
        run_obj = self.job_run_dao.get(2)
        self.assertEqual(GenomicSubProcessResult.SUCCESS, run_obj.runResult)

    def test_cvl_w1_manifest(self):
        # Need GC Manifest for source query : run_id = 1
        self.job_run_dao.insert(GenomicJobRun(jobId=GenomicJob.AW1_MANIFEST,
                                              startTime=clock.CLOCK.now(),
                                              runStatus=GenomicSubProcessStatus.COMPLETED,
                                              runResult=GenomicSubProcessResult.SUCCESS))

        self._create_fake_datasets_for_gc_tests(3, arr_override=False, recon_gc_man_id=1)

        bucket_name = _FAKE_GENOMIC_CENTER_BUCKET_A
        self._create_ingestion_test_file('RDR_AoU_SEQ_TestDataManifest.csv', bucket_name,
                                         folder=config.GENOMIC_AW2_SUBFOLDERS[0])

        self._update_test_sample_ids()

        genomic_pipeline.ingest_genomic_centers_metrics_files()  # run_id = 2

        # Create the Sequencing test files
        sequencing_test_files = (
            f'test_data_folder/RDR_2_1002_LocalID_InternalRevisionNumber.hard-filtered.vcf.gz',
            f'test_data_folder/RDR_2_1002_LocalID_InternalRevisionNumber.hard-filtered.vcf.gz.tbi',
            f'test_data_folder/RDR_2_1002_LocalID_InternalRevisionNumber.hard-filtered.vcf.md5sum',
            f'test_data_folder/RDR_2_1002_LocalID_InternalRevisionNumber.vcf.gz',
            f'test_data_folder/RDR_2_1002_LocalID_InternalRevisionNumber.vcf.gz.tbi',
            f'test_data_folder/RDR_2_1002_LocalID_InternalRevisionNumber.vcf.md5sum',
            f'test_data_folder/RDR_2_1002_LocalID_InternalRevisionNumber.cram',
            f'test_data_folder/RDR_2_1002_LocalID_InternalRevisionNumber.crai',
            f'test_data_folder/RDR_2_1002_LocalID_InternalRevisionNumber.cram.md5sum',
            f'test_data_folder/RDR_2_1002_LocalID_InternalRevisionNumber.crai.md5sum',
        )

        for f in sequencing_test_files:
            self._write_cloud_csv(f, 'attagc', bucket=bucket_name)

        genomic_pipeline.reconcile_metrics_vs_manifest()  # run_id = 3
        genomic_pipeline.reconcile_metrics_vs_sequencing_data()  # run_id = 4

        # Run the W1 manifest workflow
        fake_dt = datetime.datetime(2020, 4, 3, 0, 0, 0, 0)

        with clock.FakeClock(fake_dt):
            genomic_pipeline.create_cvl_w1_manifest()  # run_id 5

        w1_dtf = fake_dt.strftime("%Y-%m-%d-%H-%M-%S")

        # Test member was updated
        member = self.member_dao.get(2)
        self.assertEqual(5, member.cvlW1ManifestJobRunId)
        self.assertEqual(GenomicWorkflowState.W1, member.genomicWorkflowState)

        # Test the manifest file contents
        expected_w1_columns = (
            "genomic_set_name",
            "biobank_id",
            "sample_id",
            "sex_at_birth",
            "ny_flag",
            "site_id",
            "secondary_validation",
            "date_submitted",
            "test_name"
        )

        sub_folder = config.CVL_W1_MANIFEST_SUBFOLDER
        bucket_name = config.getSetting(config.GENOMIC_CVL_BUCKET_NAME)

        with open_cloud_file(os.path.normpath(f'{bucket_name}/{sub_folder}/AoU_CVL_Manifest_{w1_dtf}.csv')) as csv_file:
            csv_reader = csv.DictReader(csv_file)
            missing_cols = set(expected_w1_columns) - set(csv_reader.fieldnames)
            self.assertEqual(0, len(missing_cols))

            rows = list(csv_reader)

            self.assertEqual(1, len(rows))
            self.assertEqual(member.biobankId, rows[0]['biobank_id'])
            self.assertEqual(member.sampleId, rows[0]['sample_id'])
            self.assertEqual("", rows[0]['secondary_validation'])

        # Test file processed is recorded
        file_record = self.file_processed_dao.get(2)  # remember, GC Metrics is #1
        self.assertEqual(5, file_record.runId)
        self.assertEqual(f'{sub_folder}/AoU_CVL_Manifest_{w1_dtf}.csv', file_record.fileName)

        run_obj = self.job_run_dao.get(5)

        self.assertEqual(GenomicSubProcessResult.SUCCESS, run_obj.runResult)

    def test_cvl_w2_manifest_ingestion(self):
        # Create W1 Manifest Job: run_id = 1
        self.job_run_dao.insert(GenomicJobRun(jobId=GenomicJob.CREATE_CVL_W1_MANIFESTS,
                                              startTime=clock.CLOCK.now(),
                                              runStatus=GenomicSubProcessStatus.COMPLETED,
                                              runResult=GenomicSubProcessResult.SUCCESS))

        self._create_fake_datasets_for_gc_tests(3, recon_gc_man_id=1)

        # Set up test W2 manifest
        bucket_name = config.getSetting(config.GENOMIC_CVL_BUCKET_NAME)
        sub_folder = config.CVL_W2_MANIFEST_SUBFOLDER

        self._create_ingestion_test_file('RDR_AoU_CVL_RequestValidation_20200519.csv',
                                         bucket_name, folder=sub_folder,
                                         include_timestamp=False)

        members = self.member_dao.get_all()
        for m in members:
            self.member_dao.update_member_state(m, GenomicWorkflowState.W1)

        # Run Workflow
        genomic_pipeline.ingest_cvl_w2_manifest()  # run_id 2

        # Test Member
        # Test gem_pass field
        members = self.member_dao.get_all()

        for member in members:
            self.assertEqual(2, member.cvlW2ManifestJobRunID)
            self.assertEqual(GenomicWorkflowState.W2, member.genomicWorkflowState)

            if member.id in (1, 2):
                self.assertEqual("aou_cvl", member.genomeType)

            if member.id == 3:
                self.assertEqual("aou_wgs", member.genomeType)

        # Test File Processed
        file_record = self.file_processed_dao.get(1)
        self.assertEqual(2, file_record.runId)

        self.assertEqual(f'/{bucket_name}/{sub_folder}/RDR_AoU_CVL_RequestValidation_20200519.csv',
                         file_record.filePath)

        self.assertEqual('RDR_AoU_CVL_RequestValidation_20200519.csv', file_record.fileName)

        # Test the job result
        run_obj = self.job_run_dao.get(2)
        self.assertEqual(GenomicSubProcessResult.SUCCESS, run_obj.runResult)

    def test_cvl_w3_manifest_generation(self):
        # Create W1 manifest job run: id = 1
        self.job_run_dao.insert(GenomicJobRun(jobId=GenomicJob.CREATE_CVL_W1_MANIFESTS,
                                              startTime=clock.CLOCK.now(),
                                              runStatus=GenomicSubProcessStatus.COMPLETED,
                                              runResult=GenomicSubProcessResult.SUCCESS))

        # Create genomic set members
        self._create_fake_datasets_for_gc_tests(3, arr_override=False,
                                                cvl_w1_run_id=1,
                                                cvl=True,
                                                genomic_workflow_state=GenomicWorkflowState.W2)

        self._update_test_sample_ids()

        # Run Workflow with specific time
        fake_now = datetime.datetime.utcnow()
        out_time = fake_now.strftime("%Y-%m-%d-%H-%M-%S")
        with clock.FakeClock(fake_now):
            genomic_pipeline.create_cvl_w3_manifest()  # run_id 2

        # Test member was updated
        member = self.member_dao.get(1)
        self.assertEqual(2, member.cvlW3ManifestJobRunID)
        self.assertEqual(GenomicWorkflowState.W3, member.genomicWorkflowState)

        # Test the manifest file contents
        bucket_name = config.getSetting(config.GENOMIC_CVL_BUCKET_NAME)
        sub_folder = config.CVL_W3_MANIFEST_SUBFOLDER

        # Test the manifest file contents
        expected_w3_columns = (
            "value",
            "sample_id",
            "biobank_id",
            "sex_at_birth",
            "genome_type",
            "ny_flag",
            "request_id",
            "package_id",
            "ai_an",
            "site_ID",
            "secondary_validation",
        )

        with open_cloud_file(os.path.normpath(f'{bucket_name}/{sub_folder}/AoU_CVL_W1_{out_time}.csv')) as csv_file:
            csv_reader = csv.DictReader(csv_file)
            missing_cols = set(expected_w3_columns) - set(csv_reader.fieldnames)
            self.assertEqual(0, len(missing_cols))

            rows = list(csv_reader)

            self.assertEqual(3, len(rows))
            self.assertEqual(member.biobankId, rows[0]['biobank_id'])
            self.assertEqual(member.sampleId, rows[0]['sample_id'])
            self.assertEqual("Y", rows[0]['secondary_validation'])

        # Test Manifest File Record Created
        file_record = self.file_processed_dao.get(1)  # remember, GC Metrics is #1
        self.assertEqual(2, file_record.runId)
        self.assertEqual(f'{sub_folder}/AoU_CVL_W1_{out_time}.csv', file_record.fileName)

        # Test the job result
        run_obj = self.job_run_dao.get(2)
        self.assertEqual(GenomicSubProcessResult.SUCCESS, run_obj.runResult)
