import csv
import datetime
import os
import mock

import pytz
from dateutil.parser import parse


from rdr_service import clock, config
from rdr_service.api_util import open_cloud_file, list_blobs
from rdr_service.code_constants import (
    BIOBANK_TESTS, COHORT_1_REVIEW_CONSENT_YES_CODE, COHORT_1_REVIEW_CONSENT_NO_CODE)
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
from rdr_service.dao.questionnaire_dao import QuestionnaireDao, QuestionnaireQuestionDao
from rdr_service.dao.questionnaire_response_dao import QuestionnaireResponseDao, QuestionnaireResponseAnswerDao
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
from rdr_service.model.participant_summary import ParticipantRaceAnswers, ParticipantSummary
from rdr_service.model.questionnaire import Questionnaire, QuestionnaireQuestion
from rdr_service.model.questionnaire_response import QuestionnaireResponse, QuestionnaireResponseAnswer
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
_FAKE_GENOTYPING_FOLDER = 'AW1_genotyping_sample_manifests'
_FAKE_SEQUENCING_FOLDER = 'AW1_wgs_sample_manifests'
_FAKE_CVL_REPORT_FOLDER = 'fake_cvl_reconciliation_reports'
_FAKE_CVL_MANIFEST_FOLDER = 'fake_cvl_manifest_folder'
_FAKE_GEM_BUCKET = 'fake_gem_bucket'
_FAKE_FAILURE_FOLDER = 'AW1F_genotyping_accessioning_results'
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
        self.q_dao = QuestionnaireDao()
        self.qr_dao = QuestionnaireResponseDao()
        self.qra_dao = QuestionnaireResponseAnswerDao()
        self.qq_dao = QuestionnaireQuestionDao()
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

    def _make_ordered_sample(self, _test='1SAL2', _description='description',
                             _processing_req=True, _collected=None, _processed=None,
                             _finalized=None):
        """Makes BiobankOrderedSample for insert with biobank order"""

        return BiobankOrderedSample(test=_test, description=_description,
                                    processingRequired=_processing_req,
                                    collected=_collected, processed=_processed,
                                    finalized=_finalized)


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
                                             folder=config.getSetting(config.GENOMIC_AW2_SUBFOLDERS[1]),
                                             include_sub_num=True)

        self._create_fake_datasets_for_gc_tests(2, arr_override=True,
                                                array_participants=(1, 2),
                                                genomic_workflow_state=GenomicWorkflowState.AW1)

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
        self.assertEqual('1001', member.sampleId)

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
                    'RDR_AoU_GEN_TestDataManifest_11192019_1.csv'
                )
                self.assertEqual(
                    f.filePath,
                    f'/{_FAKE_GENOMIC_CENTER_BUCKET_A}/'
                    f'{config.getSetting(config.GENOMIC_AW2_SUBFOLDERS[1])}/'
                    f'RDR_AoU_GEN_TestDataManifest_11192019_1.csv'
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
            self.assertEqual('0.34567890', record.callRate)
            self.assertEqual('True', record.sexConcordance)
            self.assertEqual('Pass', record.processingStatus)
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
                                             folder=config.getSetting(config.GENOMIC_AW2_SUBFOLDERS[0]))

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
        self._create_fake_datasets_for_gc_tests(2, genomic_workflow_state=GenomicWorkflowState.AW1)
        bucket_name = _FAKE_GENOMIC_CENTER_BUCKET_A
        self._create_ingestion_test_file('RDR_AoU_SEQ_TestDataManifest.csv',
                                         bucket_name,
                                         folder=config.getSetting(config.GENOMIC_AW2_SUBFOLDERS[0]))

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
        self.assertEqual(gc_metrics[0].aouHdrCoverage, '2')
        self.assertEqual(gc_metrics[0].contamination, '3')
        self.assertEqual(gc_metrics[0].sexConcordance, 'True')
        self.assertEqual(gc_metrics[0].arrayConcordance, 'True')
        self.assertEqual(gc_metrics[0].sexPloidy, 'XY')
        self.assertEqual(gc_metrics[0].alignedQ30Bases, 4)
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
                                    include_timestamp=True,
                                    include_sub_num=False,):
        test_data_file = test_data.open_genomic_set_file(test_data_filename)

        input_filename = '{}{}{}.csv'.format(
            test_data_filename.replace('.csv', ''),
            '_11192019' if include_timestamp else '',
            '_1' if include_sub_num else ''
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
        genome_center=None,
        aw3_job_id=None,
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
        genomic_set_member.gcSiteId = genome_center
        genomic_set_member.aw3ManifestJobRunID = aw3_job_id

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
                genomic_workflow_state=kwargs.get('genomic_workflow_state'),
                genome_center=kwargs.get('genome_center'),
                aw3_job_id=kwargs.get('aw3_job_id'),
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

    def _setup_fake_reconsent_question_code(self):
        code_to_insert = Code(
            system="a",
            value="ReviewConsentAgree_Question",
            display="c",
            topic="d",
            codeType=CodeType.QUESTION, mapped=True)
        return self.code_dao.insert(code_to_insert).codeId

    def _setup_fake_reconsent_codes(self, reconsent=True):
        code_to_insert = Code(
            system="a",
            value=COHORT_1_REVIEW_CONSENT_YES_CODE if reconsent else COHORT_1_REVIEW_CONSENT_NO_CODE,
            display="c",
            topic="d",
            codeType=CodeType.ANSWER, mapped=True)
        return self.code_dao.insert(code_to_insert).codeId

    def test_gc_metrics_reconciliation_vs_manifest(self):
        # Create the fake Google Cloud CSV files to ingest
        self._create_fake_datasets_for_gc_tests(1, arr_override=True, array_participants=[1],
                                                genomic_workflow_state=GenomicWorkflowState.AW1)
        bucket_name = _FAKE_GENOMIC_CENTER_BUCKET_A
        self._create_ingestion_test_file('RDR_AoU_GEN_TestDataManifest.csv',
                                         bucket_name,
                                         folder=config.getSetting(config.GENOMIC_AW2_SUBFOLDERS[1]))

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
        self._create_fake_datasets_for_gc_tests(2, arr_override=True, array_participants=[1,2],
                                                genomic_workflow_state=GenomicWorkflowState.AW1)
        bucket_name = _FAKE_GENOMIC_CENTER_BUCKET_A
        self._create_ingestion_test_file('RDR_AoU_GEN_TestDataManifest.csv',
                                         bucket_name,
                                         folder=config.getSetting(config.GENOMIC_AW2_SUBFOLDERS[1]))

        self._update_test_sample_ids()

        genomic_pipeline.ingest_genomic_centers_metrics_files()  # run_id = 1
        manifest_file = self.file_processed_dao.get(1)

        # Test the reconciliation process
        sequencing_test_files = (
            f'test_data_folder/10001_R01C01.vcf.gz',
            f'test_data_folder/10001_R01C01.vcf.gz.tbi',
            f'test_data_folder/10001_R01C01.vcf.gz.md5sum',
            f'test_data_folder/10001_R01C01_Red.idat',
            f'test_data_folder/10001_R01C01_Grn.idat',
            f'test_data_folder/10001_R01C01_Red.idat.md5sum',
            f'test_data_folder/10002_R01C02.vcf.gz',
            f'test_data_folder/10002_R01C02.vcf.gz.tbi',
            f'test_data_folder/10002_R01C02.vcf.gz.md5sum',
            f'test_data_folder/10002_R01C02_Red.idat',
            f'test_data_folder/10002_R01C02_Grn.idat',
            f'test_data_folder/10002_R01C02_Red.idat.md5sum',
            f'test_data_folder/10002_R01C02_Grn.idat.md5sum',
        )
        for f in sequencing_test_files:
            self._write_cloud_csv(f, 'attagc', bucket=bucket_name)

        genomic_pipeline.reconcile_metrics_vs_genotyping_data()  # run_id = 2

        gc_record = self.metrics_dao.get(1)

        # Test the gc_metrics were updated with reconciliation data
        self.assertEqual(1, gc_record.vcfReceived)
        self.assertEqual(1, gc_record.vcfTbiReceived)
        self.assertEqual(1, gc_record.vcfMd5Received)
        self.assertEqual(1, gc_record.idatRedReceived)
        self.assertEqual(1, gc_record.idatGreenReceived)
        self.assertEqual(1, gc_record.idatRedMd5Received)
        self.assertEqual(0, gc_record.idatGreenMd5Received)

        self.assertEqual(f"gs://{bucket_name}/{sequencing_test_files[0]}", gc_record.vcfPath)
        self.assertEqual(f"gs://{bucket_name}/{sequencing_test_files[1]}", gc_record.vcfTbiPath)
        self.assertEqual(f"gs://{bucket_name}/{sequencing_test_files[2]}", gc_record.vcfMd5Path)
        self.assertEqual(f"gs://{bucket_name}/{sequencing_test_files[3]}", gc_record.idatRedPath)
        self.assertEqual(f"gs://{bucket_name}/{sequencing_test_files[4]}", gc_record.idatGreenPath)
        self.assertEqual(f"gs://{bucket_name}/{sequencing_test_files[5]}", gc_record.idatRedMd5Path)

        gc_record = self.metrics_dao.get(2)

        # Test the gc_metrics were updated with reconciliation data
        self.assertEqual(1, gc_record.vcfReceived)
        self.assertEqual(1, gc_record.vcfTbiReceived)
        self.assertEqual(1, gc_record.vcfMd5Received)
        self.assertEqual(1, gc_record.idatRedReceived)
        self.assertEqual(1, gc_record.idatGreenReceived)
        self.assertEqual(1, gc_record.idatRedMd5Received)
        self.assertEqual(1, gc_record.idatGreenMd5Received)

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
        description = "The following AW2 manifests are missing data files."
        description += "\nGenomic Job Run ID: 2"
        description += f"\n\tManifest File: {manifest_file.fileName}"
        description += "\n\tMissing Genotype Data: ['10001_R01C01_grn.idat.md5sum']"

        mock_alert_handler.make_genomic_alert.assert_called_with(summary, description)

        run_obj = self.job_run_dao.get(2)

        self.assertEqual(GenomicSubProcessResult.SUCCESS, run_obj.runResult)

    @mock.patch('rdr_service.genomic.genomic_job_components.GenomicAlertHandler')
    def test_aw2_wgs_reconciliation_vs_sequencing_data(self, patched_handler):
        mock_alert_handler = patched_handler.return_value
        mock_alert_handler._jira_handler = 'fake_jira_handler'
        mock_alert_handler.make_genomic_alert.return_value = 1

        # Create the fake ingested data
        self._create_fake_datasets_for_gc_tests(2, genomic_workflow_state=GenomicWorkflowState.AW1)
        bucket_name = _FAKE_GENOMIC_CENTER_BUCKET_A
        self._create_ingestion_test_file('RDR_AoU_SEQ_TestDataManifest.csv',
                                         bucket_name,
                                         folder=config.getSetting(config.GENOMIC_AW2_SUBFOLDERS[0]))

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

        self.assertEqual(f"gs://{bucket_name}/{sequencing_test_files[0]}", gc_record.hfVcfPath)
        self.assertEqual(f"gs://{bucket_name}/{sequencing_test_files[1]}", gc_record.hfVcfTbiPath)
        self.assertEqual(f"gs://{bucket_name}/{sequencing_test_files[2]}", gc_record.hfVcfMd5Path)
        self.assertEqual(f"gs://{bucket_name}/{sequencing_test_files[3]}", gc_record.rawVcfPath)
        self.assertEqual(f"gs://{bucket_name}/{sequencing_test_files[4]}", gc_record.rawVcfTbiPath)
        self.assertEqual(f"gs://{bucket_name}/{sequencing_test_files[5]}", gc_record.rawVcfMd5Path)
        self.assertEqual(f"gs://{bucket_name}/{sequencing_test_files[6]}", gc_record.cramPath)
        self.assertEqual(f"gs://{bucket_name}/{sequencing_test_files[7]}", gc_record.cramMd5Path)

        # Test member updated with job ID and state
        member = self.member_dao.get(2)
        self.assertEqual(2, member.reconcileMetricsSequencingJobRunId)
        self.assertEqual(GenomicWorkflowState.AW2_MISSING, member.genomicWorkflowState)

        # Fake alert
        summary = '[Genomic System Alert] Missing AW2 WGS Manifest Files'
        description = "The following AW2 manifests are missing data files."
        description += "\nGenomic Job Run ID: 2"
        description += f"\n\tManifest File: {manifest_file.fileName}"
        description += "\n\tMissing Genotype Data: ['RDR_2_1002_LocalID_InternalRevisionNumber.crai']"

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
                # 100004 : Included, NA is now a valid SAB
                self.assertEqual(0, member.nyFlag)
                self.assertEqual('100004', member.collectionTubeId)
                self.assertEqual('NA', member.sexAtBirth)
                self.assertEqual(GenomicSetMemberStatus.VALID, member.validationStatus)
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
            self.assertEqual("Y", rows[4][ExpectedCsvColumns.VALIDATION_PASSED])
            self.assertEqual("N", rows[4][ExpectedCsvColumns.AI_AN])
            self.assertEqual("aou_array", rows[4][ExpectedCsvColumns.GENOME_TYPE])

            self.assertEqual("T100004", rows[5][ExpectedCsvColumns.BIOBANK_ID])
            self.assertEqual(100004, int(rows[5][ExpectedCsvColumns.SAMPLE_ID]))
            self.assertEqual("NA", rows[5][ExpectedCsvColumns.SEX_AT_BIRTH])
            self.assertEqual("N", rows[5][ExpectedCsvColumns.NY_FLAG])
            self.assertEqual("Y", rows[5][ExpectedCsvColumns.VALIDATION_PASSED])
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
        """Test for Cohort 2 workflow"""

        # Setup for C2 Test
        self._setup_c1_c2_tests(2)

        # run C2 participant workflow and test results
        genomic_pipeline.c2_participant_workflow()

        new_genomic_set = self.set_dao.get_all()
        self.assertEqual(1, len(new_genomic_set))

        # Should be a aou_wgs and aou_array for each pid
        new_genomic_members = self.member_dao.get_all()
        self.assertEqual(6, len(new_genomic_members))

        # Test member data
        member_genome_types = {_member.biobankId: list() for _member in new_genomic_members}
        for member in new_genomic_members:
            member_genome_types[member.biobankId].append(member.genomeType)

            if member.biobankId == '100001':
                # 100001 : Included, Valid
                self.assertEqual(0, member.nyFlag)
                self.assertEqual('10000102', member.collectionTubeId)
                self.assertEqual('F', member.sexAtBirth)
                self.assertEqual(GenomicSetMemberStatus.VALID, member.validationStatus)
                self.assertEqual('N', member.ai_an)

            if member.biobankId == '100002':
                # 100002 : Included, Valid
                self.assertEqual(1, member.nyFlag)
                self.assertEqual('10000201', member.collectionTubeId)
                self.assertEqual('F', member.sexAtBirth)
                self.assertEqual(GenomicSetMemberStatus.VALID, member.validationStatus)
                self.assertEqual('N', member.ai_an)

            if member.biobankId == '100005':
                # 100005 : Included, Valid
                self.assertEqual(1, member.nyFlag)
                self.assertEqual('10000501', member.collectionTubeId)
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
                self.assertEqual(10000102, int(rows[0][ExpectedCsvColumns.SAMPLE_ID]))
                self.assertEqual("F", rows[0][ExpectedCsvColumns.SEX_AT_BIRTH])
                self.assertEqual("N", rows[0][ExpectedCsvColumns.NY_FLAG])
                self.assertEqual("Y", rows[0][ExpectedCsvColumns.VALIDATION_PASSED])
                self.assertEqual("N", rows[0][ExpectedCsvColumns.AI_AN])
                self.assertEqual("aou_array", rows[0][ExpectedCsvColumns.GENOME_TYPE])

                self.assertEqual("T100001", rows[1][ExpectedCsvColumns.BIOBANK_ID])
                self.assertEqual(10000102, int(rows[1][ExpectedCsvColumns.SAMPLE_ID]))
                self.assertEqual("F", rows[1][ExpectedCsvColumns.SEX_AT_BIRTH])
                self.assertEqual("N", rows[1][ExpectedCsvColumns.NY_FLAG])
                self.assertEqual("Y", rows[1][ExpectedCsvColumns.VALIDATION_PASSED])
                self.assertEqual("N", rows[1][ExpectedCsvColumns.AI_AN])
                self.assertEqual("aou_wgs", rows[1][ExpectedCsvColumns.GENOME_TYPE])

                self.assertEqual("T100002", rows[2][ExpectedCsvColumns.BIOBANK_ID])
                self.assertEqual(10000201, int(rows[2][ExpectedCsvColumns.SAMPLE_ID]))
                self.assertEqual("F", rows[2][ExpectedCsvColumns.SEX_AT_BIRTH])
                self.assertEqual("Y", rows[2][ExpectedCsvColumns.NY_FLAG])
                self.assertEqual("Y", rows[2][ExpectedCsvColumns.VALIDATION_PASSED])
                self.assertEqual("N", rows[2][ExpectedCsvColumns.AI_AN])
                self.assertEqual("aou_array", rows[2][ExpectedCsvColumns.GENOME_TYPE])

                self.assertEqual("T100002", rows[3][ExpectedCsvColumns.BIOBANK_ID])
                self.assertEqual(10000201, int(rows[3][ExpectedCsvColumns.SAMPLE_ID]))
                self.assertEqual("F", rows[3][ExpectedCsvColumns.SEX_AT_BIRTH])
                self.assertEqual("Y", rows[3][ExpectedCsvColumns.NY_FLAG])
                self.assertEqual("Y", rows[3][ExpectedCsvColumns.VALIDATION_PASSED])
                self.assertEqual("N", rows[3][ExpectedCsvColumns.AI_AN])
                self.assertEqual("aou_wgs", rows[3][ExpectedCsvColumns.GENOME_TYPE])

    def test_c1_participant_workflow(self):
        """Test for Cohort 1 workflow"""

        # Setup for C1 Test
        self._setup_c1_c2_tests(1)

        # run C1 participant workflow and test results
        genomic_pipeline.c1_participant_workflow()

        new_genomic_set = self.set_dao.get_all()
        self.assertEqual(1, len(new_genomic_set))

        # Should be a aou_wgs and aou_array for each pid
        new_genomic_members = self.member_dao.get_all()
        self.assertEqual(6, len(new_genomic_members))

        # Test member data
        member_genome_types = {_member.biobankId: list() for _member in new_genomic_members}
        for member in new_genomic_members:
            member_genome_types[member.biobankId].append(member.genomeType)

            if member.biobankId == '100001':
                # 100001 : Included, Valid
                self.assertEqual(0, member.nyFlag)
                self.assertEqual('10000102', member.collectionTubeId)
                self.assertEqual('F', member.sexAtBirth)
                self.assertEqual(GenomicSetMemberStatus.VALID, member.validationStatus)
                self.assertEqual('N', member.ai_an)

            if member.biobankId == '100002':
                # 100002 : Included, Valid
                self.assertEqual(1, member.nyFlag)
                self.assertEqual('10000201', member.collectionTubeId)
                self.assertEqual('F', member.sexAtBirth)
                self.assertEqual(GenomicSetMemberStatus.VALID, member.validationStatus)
                self.assertEqual('N', member.ai_an)

            if member.biobankId == '100005':
                # 100005 : Included, Valid
                self.assertEqual(1, member.nyFlag)
                self.assertEqual('10000501', member.collectionTubeId)
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
                self.assertEqual(10000102, int(rows[0][ExpectedCsvColumns.SAMPLE_ID]))
                self.assertEqual("F", rows[0][ExpectedCsvColumns.SEX_AT_BIRTH])
                self.assertEqual("N", rows[0][ExpectedCsvColumns.NY_FLAG])
                self.assertEqual("Y", rows[0][ExpectedCsvColumns.VALIDATION_PASSED])
                self.assertEqual("N", rows[0][ExpectedCsvColumns.AI_AN])
                self.assertEqual("aou_array", rows[0][ExpectedCsvColumns.GENOME_TYPE])

                self.assertEqual("T100001", rows[1][ExpectedCsvColumns.BIOBANK_ID])
                self.assertEqual(10000102, int(rows[1][ExpectedCsvColumns.SAMPLE_ID]))
                self.assertEqual("F", rows[1][ExpectedCsvColumns.SEX_AT_BIRTH])
                self.assertEqual("N", rows[1][ExpectedCsvColumns.NY_FLAG])
                self.assertEqual("Y", rows[1][ExpectedCsvColumns.VALIDATION_PASSED])
                self.assertEqual("N", rows[1][ExpectedCsvColumns.AI_AN])
                self.assertEqual("aou_wgs", rows[1][ExpectedCsvColumns.GENOME_TYPE])

                self.assertEqual("T100002", rows[2][ExpectedCsvColumns.BIOBANK_ID])
                self.assertEqual(10000201, int(rows[2][ExpectedCsvColumns.SAMPLE_ID]))
                self.assertEqual("F", rows[2][ExpectedCsvColumns.SEX_AT_BIRTH])
                self.assertEqual("Y", rows[2][ExpectedCsvColumns.NY_FLAG])
                self.assertEqual("Y", rows[2][ExpectedCsvColumns.VALIDATION_PASSED])
                self.assertEqual("N", rows[2][ExpectedCsvColumns.AI_AN])
                self.assertEqual("aou_array", rows[2][ExpectedCsvColumns.GENOME_TYPE])

                self.assertEqual("T100002", rows[3][ExpectedCsvColumns.BIOBANK_ID])
                self.assertEqual(10000201, int(rows[3][ExpectedCsvColumns.SAMPLE_ID]))
                self.assertEqual("F", rows[3][ExpectedCsvColumns.SEX_AT_BIRTH])
                self.assertEqual("Y", rows[3][ExpectedCsvColumns.NY_FLAG])
                self.assertEqual("Y", rows[3][ExpectedCsvColumns.VALIDATION_PASSED])
                self.assertEqual("N", rows[3][ExpectedCsvColumns.AI_AN])
                self.assertEqual("aou_wgs", rows[3][ExpectedCsvColumns.GENOME_TYPE])

    def _setup_c1_c2_tests(self, c_test):
        # create test samples
        test_biobank_ids = (100001, 100002, 100003, 100004, 100005)
        fake_datetime_old = datetime.datetime(2019, 12, 31, tzinfo=pytz.utc)
        fake_datetime_new = datetime.datetime(2020, 1, 5, tzinfo=pytz.utc)

        # update the sites' States for the state test (NY or AZ)
        self._update_site_states()

        # setup sex_at_birth code for unittests
        female_code = self._setup_fake_sex_at_birth_codes('f')

        # Setup race codes for unittests
        non_native_code = self._setup_fake_race_codes(native=False)
        native_code = self._setup_fake_race_codes(native=True)

        # setup reconsent codes and questionnaire
        recon_yes_code = self._setup_fake_reconsent_codes()
        #recon_no_code = self._setup_fake_reconsent_codes(reconsent=False)
        recon_question_code = self._setup_fake_reconsent_question_code()

        recon_questionnaire = None
        recon_qq = None

        if c_test == 1:
            reconsent_questionnaire = Questionnaire(version=1,
                                                    semanticVersion='1',
                                                    resource='{"version": 1}')
            recon_questionnaire = self.q_dao.insert(reconsent_questionnaire)

            qq = QuestionnaireQuestion(
                questionnaireId=recon_questionnaire.questionnaireId,
                questionnaireVersion=1,
                codeId=recon_question_code,
                repeats=False
            )
            recon_qq = self.qq_dao.insert(qq)


        # Setup the biobank order backend
        for bid in test_biobank_ids:
            p = self._make_participant(biobankId=bid)

            self._make_summary(p, sexId=female_code,
                               consentForStudyEnrollment=1,
                               sampleStatus1ED04=0,
                               sampleStatus1SAL2=1,
                               consentCohort=3 if bid == 100003 else c_test,
                               questionnaireOnDnaProgram=QuestionnaireStatus.SUBMITTED if bid != 100003 else None,
                               questionnaireOnDnaProgramAuthored=clock.CLOCK.now() if bid != 100003 else None,
                               race=Race.HISPANIC_LATINO_OR_SPANISH)

            # Insert participant races
            race_answer = ParticipantRaceAnswers(
                participantId=p.participantId,
                codeId=native_code if bid == 100004 else non_native_code
            )

            if recon_questionnaire is not None and recon_qq is not None:
                # Insert Questionnaire Response and Answers for Reconsent
                qr_to_insert = QuestionnaireResponse(
                    questionnaireId=recon_questionnaire.questionnaireId,
                    questionnaireVersion=1,
                    questionnaireSemanticVersion='1',
                    participantId=p.participantId,
                    resource='{"resourceType": "QuestionnaireResponse"}',
                )
                qr = self.qr_dao.insert(qr_to_insert)

                qra_to_insert = QuestionnaireResponseAnswer(
                    questionnaireResponseId=qr.questionnaireResponseId,
                    questionId=recon_qq.questionnaireQuestionId,
                    valueCodeId=recon_yes_code,
                )
                self.qra_dao.insert(qra_to_insert)

            self.race_dao.insert(race_answer)
            test_identifier = BiobankOrderIdentifier(
                system=u'c',
                value=u'e{}'.format(bid))

            insert_dtm = fake_datetime_new

            # Testing sample business logic (prioritize newer collected samples then 1ED04)
            col_date_1 = datetime.datetime(2018, 6, 30, 0, 0, 0, 0)
            col_date_2 = datetime.datetime(2019, 6, 30, 0, 0, 0, 0)

            if bid == 100001:
                # SAL2 newer than ED04 -> Use SAL2
                sample_1 = self._make_ordered_sample(_test="1ED04", _collected=col_date_1)
                sample_2 = self._make_ordered_sample(_test="1SAL2", _collected=col_date_2)

                test_identifier2 = BiobankOrderIdentifier(
                    system=u'c',
                    value=u'e{}-02'.format(bid))

                self._make_biobank_order(biobankOrderId=f'W{bid}-01',
                                         participantId=p.participantId,
                                         collectedSiteId=2,
                                         identifiers=[test_identifier],
                                         samples=[sample_1])

                self._make_biobank_order(biobankOrderId=f'W{bid}-02',
                                         participantId=p.participantId,
                                         collectedSiteId=2,
                                         identifiers=[test_identifier2],
                                         samples=[sample_2])

                insert_dtm = fake_datetime_old

                sample_args1 = {
                    'test': '1ED04',
                    'confirmed': fake_datetime_new,
                    'created': fake_datetime_old,
                    'biobankId': bid,
                    'biobankOrderIdentifier': test_identifier.value,
                    'biobankStoredSampleId': 10000101,
                }

                sample_args2 = {
                    'test': '1SAL2',
                    'confirmed': fake_datetime_new,
                    'created': fake_datetime_old,
                    'biobankId': bid,
                    'biobankOrderIdentifier': test_identifier2.value,
                    'biobankStoredSampleId': 10000102,
                }

                with clock.FakeClock(insert_dtm):
                    self._make_stored_sample(**sample_args1)
                    self._make_stored_sample(**sample_args2)

            elif bid == 100002:
                # ED04 and SAL2 same collected date -> Use ED04
                sample_1 = self._make_ordered_sample(_test="1ED04", _collected=col_date_1)
                sample_2 = self._make_ordered_sample(_test="1SAL2", _collected=col_date_1)

                test_identifier2 = BiobankOrderIdentifier(
                    system=u'c',
                    value=u'e{}-02'.format(bid))

                self._make_biobank_order(biobankOrderId=f'W{bid}-01',
                                         participantId=p.participantId,
                                         collectedSiteId=1,
                                         identifiers=[test_identifier],
                                         samples=[sample_1])

                self._make_biobank_order(biobankOrderId=f'W{bid}-02',
                                         participantId=p.participantId,
                                         collectedSiteId=1,
                                         identifiers=[test_identifier2],
                                         samples=[sample_2])

                insert_dtm = fake_datetime_old

                sample_args1 = {
                    'test': '1ED04',
                    'confirmed': fake_datetime_new,
                    'created': fake_datetime_old,
                    'biobankId': bid,
                    'biobankOrderIdentifier': test_identifier.value,
                    'biobankStoredSampleId': 10000201,
                }

                sample_args2 = {
                    'test': '1SAL2',
                    'confirmed': fake_datetime_new,
                    'created': fake_datetime_old,
                    'biobankId': bid,
                    'biobankOrderIdentifier': test_identifier2.value,
                    'biobankStoredSampleId': 10000202,
                }

                with clock.FakeClock(insert_dtm):
                    self._make_stored_sample(**sample_args1)
                    self._make_stored_sample(**sample_args2)

            elif bid == 100005:
                # ED04 and no SAL2 -> Use ED04
                sample_1 = self._make_ordered_sample(_test="1ED04", _collected=col_date_1)

                self._make_biobank_order(biobankOrderId=f'W{bid}-01',
                                         participantId=p.participantId,
                                         collectedSiteId=1,
                                         identifiers=[test_identifier],
                                         samples=[sample_1])

                insert_dtm = fake_datetime_old

                sample_args1 = {
                    'test': '1ED04',
                    'confirmed': fake_datetime_new,
                    'created': fake_datetime_old,
                    'biobankId': bid,
                    'biobankOrderIdentifier': test_identifier.value,
                    'biobankStoredSampleId': 10000501,
                }

                with clock.FakeClock(insert_dtm):
                    self._make_stored_sample(**sample_args1)

            else:
                self._make_biobank_order(biobankOrderId=f'W{bid}',
                                         participantId=p.participantId,
                                         collectedSiteId=2,
                                         identifiers=[test_identifier])

                sample_args = {
                    'test': '1SAL2',
                    'confirmed': fake_datetime_new,
                    'created': fake_datetime_old,
                    'biobankId': bid,
                    'biobankOrderIdentifier': test_identifier.value,
                    'biobankStoredSampleId': bid,
                }

                with clock.FakeClock(insert_dtm):
                    self._make_stored_sample(**sample_args)

    @mock.patch('rdr_service.genomic.genomic_job_components.GenomicFileIngester._check_if_control_sample')
    def test_gc_manifest_ingestion_workflow(self, control_check_mock):
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

        # test control samples
        control_check_mock.assert_called_with(1234)

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
        gc_manifest_filename = "RDR_AoU_GEN_PKG-1908-218051_FAILURE.csv"
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

    @mock.patch('rdr_service.genomic.genomic_job_controller.GenomicJobController._send_email_with_sendgrid')
    def test_aw1f_alerting_emails(self, send_email_mock):
        gc_manifest_filename = "RDR_AoU_SEQ_PKG-1908-218051_FAILURE.csv"

        self._write_cloud_csv(
            gc_manifest_filename,
            ".",
            bucket=_FAKE_GENOMIC_CENTER_BUCKET_A,
            folder=_FAKE_FAILURE_FOLDER,
        )

        genomic_pipeline.genomic_centers_accessioning_failures_workflow()

        # Todo: change to expected response code
        # send_email_mock.return_value = "SUCCESS"

        # Set up expected SendGrid request
        email_message = "New AW1 Failure manifests have been found:\n"
        email_message += f"\t{_FAKE_GENOMIC_CENTER_BUCKET_A}:\n"
        email_message += f"\t\t{_FAKE_FAILURE_FOLDER}/{gc_manifest_filename}:\n"

        expected_email_req = {
            "personalizations": [
                {
                    "to": [{"email": "test-genomic@vumc.org"}],
                    "subject": "All of Us GC Manifest Failure Alert"
                }
            ],
            "from": {
                "email": "no-reply@pmi-ops.org"
            },
            "content": [
                {
                    "type": "text/plain",
                    "value": email_message
                }
            ]
        }

        send_email_mock.assert_called_with(expected_email_req)

    def test_gem_a1_manifest_end_to_end(self):
        # Need GC Manifest for source query : run_id = 1
        self.job_run_dao.insert(GenomicJobRun(jobId=GenomicJob.AW1_MANIFEST,
                                              startTime=clock.CLOCK.now(),
                                              runStatus=GenomicSubProcessStatus.COMPLETED,
                                              runResult=GenomicSubProcessResult.SUCCESS))

        self._create_fake_datasets_for_gc_tests(3, arr_override=True,
                                                array_participants=range(1, 4),
                                                recon_gc_man_id=1,
                                                genome_center='JH',
                                                genomic_workflow_state=GenomicWorkflowState.AW1)

        # Set starting RoR authored
        ps_list = self.summary_dao.get_all()
        ror_start = datetime.datetime(2020, 7, 11, 0, 0, 0, 0)
        for p in ps_list:
            p.consentForGenomicsRORAuthored = ror_start
            self.summary_dao.update(p)

        bucket_name = _FAKE_GENOMIC_CENTER_BUCKET_A

        self._create_ingestion_test_file('RDR_AoU_GEN_TestDataManifest.csv',
                                         bucket_name,
                                         folder=config.getSetting(config.GENOMIC_AW2_SUBFOLDERS[1]))

        self._update_test_sample_ids()

        genomic_pipeline.ingest_genomic_centers_metrics_files()  # run_id = 2

        # Test sequencing file (required for GEM)
        sequencing_test_files = (
            f'test_data_folder/10001_R01C01.vcf.gz',
            f'test_data_folder/10001_R01C01.vcf.gz.tbi',
            f'test_data_folder/10001_R01C01.vcf.gz.md5sum',
            f'test_data_folder/10001_R01C01_red.idat',
            f'test_data_folder/10001_R01C01_grn.idat',
            f'test_data_folder/10001_R01C01_red.idat.md5sum',
            f'test_data_folder/10001_R01C01_grn.idat.md5sum',
            f'test_data_folder/10002_R01C02.vcf.gz',
            f'test_data_folder/10002_R01C02.vcf.gz.tbi',
            f'test_data_folder/10002_R01C02.vcf.gz.md5sum',
            f'test_data_folder/10002_R01C02_red.idat',
            f'test_data_folder/10002_R01C02_grn.idat',
            f'test_data_folder/10002_R01C02_red.idat.md5sum',
            f'test_data_folder/10002_R01C02_grn.idat.md5sum',
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
                ParticipantSummary.consentForGenomicsROR,
                ParticipantSummary.consentForGenomicsRORAuthored,
                GenomicSetMember.nyFlag,
                GenomicSetMember.gemA1ManifestJobRunId,
                GenomicSetMember.gcSiteId,
                GenomicGCValidationMetrics.chipwellbarcode,
                GenomicSetMember.genomicWorkflowState).filter(
                GenomicGCValidationMetrics.genomicSetMemberId == GenomicSetMember.id,
                GenomicSet.id == GenomicSetMember.genomicSetId,
                ParticipantSummary.participantId == GenomicSetMember.participantId,
                GenomicSetMember.id == 1
            ).one()

        self.assertEqual(5, test_member_1.gemA1ManifestJobRunId)
        self.assertEqual(GenomicWorkflowState.A1, test_member_1.genomicWorkflowState)

        # Test the manifest file contents
        expected_gem_columns = (
            "biobank_id",
            "sample_id",
            "sex_at_birth",
            "consent_for_ror",
            "date_of_consent_for_ror",
            "chipwellbarcode",
            "genome_center",
        )
        sub_folder = config.GENOMIC_GEM_A1_MANIFEST_SUBFOLDER
        with open_cloud_file(os.path.normpath(f'{bucket_name}/{sub_folder}/AoU_GEM_A1_manifest_{a1f}.csv')) as csv_file:
            csv_reader = csv.DictReader(csv_file)
            missing_cols = set(expected_gem_columns) - set(csv_reader.fieldnames)
            self.assertEqual(0, len(missing_cols))
            rows = list(csv_reader)
            self.assertEqual(2, len(rows))
            self.assertEqual(test_member_1.biobankId, rows[0]['biobank_id'])
            self.assertEqual(test_member_1.sampleId, rows[0]['sample_id'])
            self.assertEqual(test_member_1.sexAtBirth, rows[0]['sex_at_birth'])
            self.assertEqual("yes", rows[0]['consent_for_ror'])
            self.assertEqual(test_member_1.consentForGenomicsRORAuthored, parse(rows[0]['date_of_consent_for_ror']))
            self.assertEqual(test_member_1.chipwellbarcode, rows[0]['chipwellbarcode'])
            self.assertEqual(test_member_1.gcSiteId, rows[0]['genome_center'])

        # Array
        file_record = self.file_processed_dao.get(2)  # remember, GC Metrics is #1
        self.assertEqual(5, file_record.runId)
        self.assertEqual(f'{sub_folder}/AoU_GEM_A1_manifest_{a1f}.csv', file_record.fileName)

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
        with open_cloud_file(os.path.normpath(f'{bucket_name}/{sub_folder}/AoU_GEM_A1_manifest_{a1f}.csv')) as csv_file:
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
                                                gem_a1_run_id=1,
                                                genomic_workflow_state=GenomicWorkflowState.A1)

        self._update_test_sample_ids()

        # Set up test A2 manifest
        bucket_name = config.getSetting(config.GENOMIC_GEM_BUCKET_NAME)
        sub_folder = config.GENOMIC_GEM_A2_MANIFEST_SUBFOLDER
        self._create_ingestion_test_file('AoU_GEM_A2_manifest_2020-07-11-00-00-00.csv',
                                         bucket_name, folder=sub_folder,
                                         include_timestamp=False)
        # Run Workflow
        genomic_pipeline.gem_a2_manifest_workflow()  # run_id 2

        # Test A2 fields and genomic state
        members = self.member_dao.get_all()
        for member in members:
            self.assertEqual(datetime.datetime(2020, 4, 29, 0, 0, 0), member.gemDateOfImport)
            if member.id in (1, 2):
                self.assertEqual("Y", member.gemPass)
                self.assertEqual(2, member.gemA2ManifestJobRunId)
                self.assertEqual(GenomicWorkflowState.GEM_RPT_READY, member.genomicWorkflowState)
            if member.id == 3:
                self.assertEqual("N", member.gemPass)
                self.assertEqual(GenomicWorkflowState.A2F, member.genomicWorkflowState)

        # Test Files Processed
        file_record = self.file_processed_dao.get(1)
        self.assertEqual(2, file_record.runId)
        self.assertEqual(f'/{bucket_name}/{sub_folder}/AoU_GEM_A2_manifest_2020-07-11-00-00-00.csv', file_record.filePath)
        self.assertEqual('AoU_GEM_A2_manifest_2020-07-11-00-00-00.csv', file_record.fileName)

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

        self._create_fake_datasets_for_gc_tests(3, arr_override=False,
                                                recon_gc_man_id=1,
                                                genomic_workflow_state=GenomicWorkflowState.AW1)

        bucket_name = _FAKE_GENOMIC_CENTER_BUCKET_A
        self._create_ingestion_test_file('RDR_AoU_SEQ_TestDataManifest.csv', bucket_name,
                                         folder=config.getSetting(config.GENOMIC_AW2_SUBFOLDERS[0]))

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
                                                genome_center='JH',
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
            "biobank_id",
            "collection_tubeid",
            "sample_id",
            "sex_at_birth",
            "genome_type",
            "ny_flag",
            "request_id",
            "package_id",
            "ai_an",
            "site_id",
        )

        with open_cloud_file(os.path.normpath(f'{bucket_name}/{sub_folder}/AoU_CVL_W1_{out_time}.csv')) as csv_file:
            csv_reader = csv.DictReader(csv_file)
            missing_cols = set(expected_w3_columns) - set(csv_reader.fieldnames)
            self.assertEqual(0, len(missing_cols))

            rows = list(csv_reader)

            self.assertEqual(3, len(rows))
            self.assertEqual(member.biobankId, rows[0]['biobank_id'])
            self.assertEqual(member.collectionTubeId, rows[0]['collection_tubeid'])
            self.assertEqual(member.sampleId, rows[0]['sample_id'])
            self.assertEqual(member.gcSiteId, rows[0]['site_id'])

        # Test Manifest File Record Created
        file_record = self.file_processed_dao.get(1)  # remember, GC Metrics is #1
        self.assertEqual(2, file_record.runId)
        self.assertEqual(f'{sub_folder}/AoU_CVL_W1_{out_time}.csv', file_record.fileName)

        # Test the job result
        run_obj = self.job_run_dao.get(2)
        self.assertEqual(GenomicSubProcessResult.SUCCESS, run_obj.runResult)

    def test_aw3_array_manifest_generation(self):
        # Need GC Manifest for source query : run_id = 1
        self.job_run_dao.insert(GenomicJobRun(jobId=GenomicJob.AW1_MANIFEST,
                                              startTime=clock.CLOCK.now(),
                                              runStatus=GenomicSubProcessStatus.COMPLETED,
                                              runResult=GenomicSubProcessResult.SUCCESS))

        self._create_fake_datasets_for_gc_tests(3, arr_override=True,
                                                array_participants=range(1, 4),
                                                recon_gc_man_id=1,
                                                genome_center='JH',
                                                genomic_workflow_state=GenomicWorkflowState.AW1)

        bucket_name = _FAKE_GENOMIC_CENTER_BUCKET_A

        self._create_ingestion_test_file('RDR_AoU_GEN_TestDataManifest.csv',
                                         bucket_name,
                                         folder=config.getSetting(config.GENOMIC_AW2_SUBFOLDERS[1]))

        self._update_test_sample_ids()

        genomic_pipeline.ingest_genomic_centers_metrics_files()  # run_id = 2

        # Test sequencing file (required for GEM)
        sequencing_test_files = (
            f'test_data_folder/10001_R01C01.vcf.gz',
            f'test_data_folder/10001_R01C01.vcf.gz.tbi',
            f'test_data_folder/10001_R01C01.vcf.gz.md5sum',
            f'test_data_folder/10001_R01C01_red.idat',
            f'test_data_folder/10001_R01C01_grn.idat',
            f'test_data_folder/10001_R01C01_red.idat.md5sum',
            f'test_data_folder/10001_R01C01_grn.idat.md5sum',
            f'test_data_folder/10002_R01C02.vcf.gz',
            f'test_data_folder/10002_R01C02.vcf.gz.tbi',
            f'test_data_folder/10002_R01C02.vcf.gz.md5sum',
            f'test_data_folder/10002_R01C02_red.idat',
            f'test_data_folder/10002_R01C02_grn.idat',
            f'test_data_folder/10002_R01C02_red.idat.md5sum',
            f'test_data_folder/10002_R01C02_grn.idat.md5sum',
        )
        for f in sequencing_test_files:
            self._write_cloud_csv(f, 'attagc', bucket=bucket_name)

        genomic_pipeline.reconcile_metrics_vs_manifest()  # run_id = 3
        genomic_pipeline.reconcile_metrics_vs_genotyping_data()  # run_id = 4

        # finally run the AW3 manifest workflow
        fake_dt = datetime.datetime(2020, 8, 3, 0, 0, 0, 0)

        with clock.FakeClock(fake_dt):
            genomic_pipeline.aw3_array_manifest_workflow()  # run_id = 5

        aw3_dtf = fake_dt.strftime("%Y-%m-%d-%H-%M-%S")

        # Test member was updated
        member = self.member_dao.get(2)

        self.assertEqual(5, member.aw3ManifestJobRunID)
        self.assertEqual(GenomicWorkflowState.GEM_READY, member.genomicWorkflowState)

        # Test the manifest file contents
        expected_aw3_columns = (
            "chipwellbarcode",
            "biobank_id",
            "sample_id",
            "sex_at_birth",
            "site_id",
            "red_idat_path",
            "red_idat_md5_path",
            "green_idat_path",
            "green_idat_md5_path",
            "vcf_path",
            "vcf_index_path",
            "research_id"
        )

        bucket_name = config.getSetting(config.DRC_BROAD_BUCKET_NAME)
        sub_folder = config.GENOMIC_AW3_ARRAY_SUBFOLDER

        with open_cloud_file(os.path.normpath(f'{bucket_name}/{sub_folder}/AoU_DRCV_GEN_{aw3_dtf}.csv')) as csv_file:
            csv_reader = csv.DictReader(csv_file)
            missing_cols = set(expected_aw3_columns) - set(csv_reader.fieldnames)
            self.assertEqual(0, len(missing_cols))

            rows = list(csv_reader)

            self.assertEqual(2, len(rows))
            self.assertEqual(member.biobankId, rows[1]['biobank_id'])
            self.assertEqual(member.sampleId, rows[1]['sample_id'])
            self.assertEqual(member.sexAtBirth, rows[1]['sex_at_birth'])
            self.assertEqual(member.gcSiteId, rows[1]['site_id'])

            # Test File Paths
            metric = self.metrics_dao.get(2)
            self.assertEqual(metric.idatRedPath, rows[1]['red_idat_path'])
            self.assertEqual(metric.idatRedMd5Path, rows[1]['red_idat_md5_path'])
            self.assertEqual(metric.idatGreenPath, rows[1]['green_idat_path'])
            self.assertEqual(metric.idatGreenMd5Path, rows[1]['green_idat_md5_path'])
            self.assertEqual(metric.vcfPath, rows[1]['vcf_path'])
            self.assertEqual(metric.vcfTbiPath, rows[1]['vcf_index_path'])
            self.assertEqual(metric.vcfMd5Path, rows[1]['vcf_md5_path'])

            # Test run record is success
            run_obj = self.job_run_dao.get(5)

            self.assertEqual(GenomicSubProcessResult.SUCCESS, run_obj.runResult)

    def test_aw3_wgs_manifest_generation(self):
        # Need GC Manifest for source query : run_id = 1
        self.job_run_dao.insert(GenomicJobRun(jobId=GenomicJob.AW1_MANIFEST,
                                              startTime=clock.CLOCK.now(),
                                              runStatus=GenomicSubProcessStatus.COMPLETED,
                                              runResult=GenomicSubProcessResult.SUCCESS))

        self._create_fake_datasets_for_gc_tests(3, arr_override=False,
                                                recon_gc_man_id=1,
                                                genome_center='JH',
                                                genomic_workflow_state=GenomicWorkflowState.AW1)

        bucket_name = _FAKE_GENOMIC_CENTER_BUCKET_A

        self._create_ingestion_test_file('RDR_AoU_SEQ_TestDataManifest.csv',
                                         bucket_name,
                                         folder=config.getSetting(config.GENOMIC_AW2_SUBFOLDERS[0]))

        self._update_test_sample_ids()

        genomic_pipeline.ingest_genomic_centers_metrics_files()  # run_id = 2

        # Test sequencing file (required for GEM)
        sequencing_test_files = (
            f'test_data_folder/RDR_2_1002_LocalID_InternalRevisionNumber.hard-filtered.vcf.gz',
            f'test_data_folder/RDR_2_1002_LocalID_InternalRevisionNumber.hard-filtered.vcf.gz.tbi',
            f'test_data_folder/RDR_2_1002_LocalID_InternalRevisionNumber.hard-filtered.vcf.md5sum',
            f'test_data_folder/RDR_2_1002_LocalID_InternalRevisionNumber.vcf.gz',
            f'test_data_folder/RDR_2_1002_LocalID_InternalRevisionNumber.vcf.gz.tbi',
            f'test_data_folder/RDR_2_1002_LocalID_InternalRevisionNumber.vcf.md5sum',
            f'test_data_folder/RDR_2_1002_LocalID_InternalRevisionNumber.cram',
            f'test_data_folder/RDR_2_1002_LocalID_InternalRevisionNumber.cram.md5sum',
            f'test_data_folder/RDR_2_1002_LocalID_InternalRevisionNumber.crai',

        )
        for f in sequencing_test_files:
            self._write_cloud_csv(f, 'attagc', bucket=bucket_name)

        genomic_pipeline.reconcile_metrics_vs_manifest()  # run_id = 3
        genomic_pipeline.reconcile_metrics_vs_sequencing_data()  # run_id = 4

        # finally run the AW3 manifest workflow
        fake_dt = datetime.datetime(2020, 8, 3, 0, 0, 0, 0)

        with clock.FakeClock(fake_dt):
            genomic_pipeline.aw3_wgs_manifest_workflow()  # run_id = 5

        aw3_dtf = fake_dt.strftime("%Y-%m-%d-%H-%M-%S")

        # Test member was updated
        member = self.member_dao.get(2)

        self.assertEqual(5, member.aw3ManifestJobRunID)
        self.assertEqual(GenomicWorkflowState.CVL_READY, member.genomicWorkflowState)

        # Test the manifest file contents
        expected_aw3_columns = (
            "biobank_id",
            "sample_id",
            "biobankidsampleid",
            "sex_at_birth",
            "site_id",
            "vcf_hf_path",
            "vcf_hf_index_path",
            # "vcf_hf_md5_path",
            "vcf_raw_path",
            "vcf_raw_index_path",
            # "vcf_raw_md5_path",
            "cram_path",
            "cram_md5_path",
            "crai_path",
            "research_id"
        )

        bucket_name = config.getSetting(config.DRC_BROAD_BUCKET_NAME)
        sub_folder = config.GENOMIC_AW3_WGS_SUBFOLDER

        with open_cloud_file(os.path.normpath(f'{bucket_name}/{sub_folder}/AoU_DRCV_SEQ_{aw3_dtf}.csv')) as csv_file:
            csv_reader = csv.DictReader(csv_file)
            missing_cols = set(expected_aw3_columns) - set(csv_reader.fieldnames)
            self.assertEqual(0, len(missing_cols))

            rows = list(csv_reader)

            self.assertEqual(1, len(rows))
            self.assertEqual(f'{member.biobankId}_{member.sampleId}', rows[0]['biobankidsampleid'])
            self.assertEqual(member.sexAtBirth, rows[0]['sex_at_birth'])
            self.assertEqual(member.gcSiteId, rows[0]['site_id'])

            # Test File Paths
            metric = self.metrics_dao.get(1)
            self.assertEqual(metric.hfVcfPath, rows[0]["vcf_hf_path"])
            self.assertEqual(metric.hfVcfTbiPath, rows[0]["vcf_hf_index_path"])
            # self.assertEqual(metric.hfVcfTbiPath, rows[0]["vcf_hf_md5_path"])
            self.assertEqual(metric.rawVcfPath, rows[0]["vcf_raw_path"])
            self.assertEqual(metric.rawVcfTbiPath, rows[0]["vcf_raw_index_path"])
            # self.assertEqual(metric.rawVcfTbiPath, rows[0]["vcf_raw_md5_path"])
            self.assertEqual(metric.cramPath, rows[0]["cram_path"])
            self.assertEqual(metric.cramMd5Path, rows[0]["cram_md5_path"])
            self.assertEqual(metric.craiPath, rows[0]["crai_path"])

            # Test run record is success
            run_obj = self.job_run_dao.get(5)

            self.assertEqual(GenomicSubProcessResult.SUCCESS, run_obj.runResult)

    def test_aw4_array_manifest_ingest(self):
        # Create AW3 array manifest job run: id = 1
        self.job_run_dao.insert(GenomicJobRun(jobId=GenomicJob.AW3_ARRAY_WORKFLOW,
                                              startTime=clock.CLOCK.now(),
                                              runStatus=GenomicSubProcessStatus.COMPLETED,
                                              runResult=GenomicSubProcessResult.SUCCESS))
        # Create genomic set members
        self._create_fake_datasets_for_gc_tests(2, arr_override=True,
                                                array_participants=range(1, 3),
                                                aw3_job_id=1,
                                                genomic_workflow_state=GenomicWorkflowState.A1)

        # simulates the AW1 (sample_ids come from Biobank)
        self._update_test_sample_ids()

        # Set up test A2 manifest
        bucket_name = config.getSetting(config.DRC_BROAD_BUCKET_NAME)
        sub_folder = config.getSetting(config.DRC_BROAD_AW4_SUBFOLDERS[0])

        self._create_ingestion_test_file('AoU_DRCB_GEN_2020-07-11-00-00-00.csv',
                                         bucket_name, folder=sub_folder,
                                         include_timestamp=False)
        # Run Workflow
        genomic_pipeline.aw4_array_manifest_workflow()  # run_id 2

        # Test AW4 manifest ID updated field
        members = self.member_dao.get_all()
        for member in members:
            if member.id in (1, 2):
                self.assertEqual(2, member.aw4ManifestJobRunID)

        # Test Files Processed
        file_record = self.file_processed_dao.get(1)
        self.assertEqual(2, file_record.runId)
        self.assertEqual(f'/{bucket_name}/{sub_folder}/AoU_DRCB_GEN_2020-07-11-00-00-00.csv',
                         file_record.filePath)
        self.assertEqual('AoU_DRCB_GEN_2020-07-11-00-00-00.csv', file_record.fileName)

        # Test the job result
        run_obj = self.job_run_dao.get(2)
        self.assertEqual(GenomicSubProcessResult.SUCCESS, run_obj.runResult)

    def test_aw4_wgs_manifest_ingest(self):
        # Create AW3 WGS manifest job run: id = 1
        self.job_run_dao.insert(GenomicJobRun(jobId=GenomicJob.AW3_WGS_WORKFLOW,
                                              startTime=clock.CLOCK.now(),
                                              runStatus=GenomicSubProcessStatus.COMPLETED,
                                              runResult=GenomicSubProcessResult.SUCCESS))
        # Create genomic set members
        self._create_fake_datasets_for_gc_tests(2, arr_override=False,
                                                aw3_job_id=1,
                                                genomic_workflow_state=GenomicWorkflowState.A1)

        # simulates the AW1 (sample_ids come from Biobank)
        self._update_test_sample_ids()

        # Set up test A2 manifest
        bucket_name = config.getSetting(config.DRC_BROAD_BUCKET_NAME)
        sub_folder = config.getSetting(config.DRC_BROAD_AW4_SUBFOLDERS[1])

        self._create_ingestion_test_file('AoU_DRCB_SEQ_2020-07-11-00-00-00.csv',
                                         bucket_name, folder=sub_folder,
                                         include_timestamp=False)
        # Run Workflow
        genomic_pipeline.aw4_wgs_manifest_workflow()  # run_id 2

        # Test AW4 manifest ID updated field
        members = self.member_dao.get_all()
        for member in members:
            if member.id in (1, 2):
                self.assertEqual(2, member.aw4ManifestJobRunID)

        # Test Files Processed
        file_record = self.file_processed_dao.get(1)
        self.assertEqual(2, file_record.runId)
        self.assertEqual(f'/{bucket_name}/{sub_folder}/AoU_DRCB_SEQ_2020-07-11-00-00-00.csv',
                         file_record.filePath)
        self.assertEqual('AoU_DRCB_SEQ_2020-07-11-00-00-00.csv', file_record.fileName)

        # Test the job result
        run_obj = self.job_run_dao.get(2)
        self.assertEqual(GenomicSubProcessResult.SUCCESS, run_obj.runResult)
