import csv
import datetime
import mock
import os
import operator
import pytz
import random
import time

from copy import deepcopy
from dateutil.parser import parse
from itertools import chain

from rdr_service import clock, config, storage
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
    GenomicManifestFileDao,
    GenomicManifestFeedbackDao,
    GenomicAW1RawDao,
    GenomicAW2RawDao,
    GenomicIncidentDao,
    GenomicGcDataFileDao,
    GenomicGcDataFileMissingDao, UserEventMetricsDao, GenomicAW4RawDao, GenomicAW3RawDao)
from rdr_service.dao.mail_kit_order_dao import MailKitOrderDao
from rdr_service.dao.participant_dao import ParticipantDao
from rdr_service.dao.participant_summary_dao import ParticipantSummaryDao, ParticipantRaceAnswersDao
from rdr_service.dao.questionnaire_dao import QuestionnaireDao, QuestionnaireQuestionDao
from rdr_service.dao.questionnaire_response_dao import QuestionnaireResponseDao, QuestionnaireResponseAnswerDao
from rdr_service.dao.site_dao import SiteDao
from rdr_service.dao.code_dao import CodeDao, CodeType
from rdr_service.model.biobank_mail_kit_order import BiobankMailKitOrder
from rdr_service.model.biobank_order import (
    BiobankOrder,
    BiobankOrderIdentifier,
    BiobankOrderedSample
)
from rdr_service.model.config_utils import get_biobank_id_prefix
from rdr_service.model.biobank_stored_sample import BiobankStoredSample
from rdr_service.model.genomics import (
    GenomicSet,
    GenomicSetMember,
    GenomicJobRun,
    GenomicGCValidationMetrics,
    GenomicSampleContamination, GenomicAW3Raw
)
from rdr_service.model.participant import Participant
from rdr_service.model.code import Code
from rdr_service.model.participant_summary import ParticipantRaceAnswers, ParticipantSummary
from rdr_service.model.questionnaire import Questionnaire, QuestionnaireQuestion
from rdr_service.model.questionnaire_response import QuestionnaireResponse, QuestionnaireResponseAnswer
from rdr_service.genomic.genomic_job_controller import GenomicJobController
from rdr_service.genomic.genomic_job_components import GenomicFileIngester
from rdr_service.genomic.genomic_mappings import array_file_types_attributes, wgs_file_types_attributes
from rdr_service.offline import genomic_pipeline
from rdr_service.participant_enums import (
    SampleStatus,
    Race,
    QuestionnaireStatus,
    WithdrawalStatus
)
from rdr_service.genomic_enums import GenomicSetStatus, GenomicSetMemberStatus, GenomicJob, GenomicWorkflowState, \
    GenomicSubProcessStatus, GenomicSubProcessResult, GenomicManifestTypes, GenomicContaminationCategory, \
    GenomicQcStatus, GenomicIncidentCode, GenomicIncidentStatus

from tests.helpers.unittest_base import BaseTestCase
from tests.test_data import data_path

_BASELINE_TESTS = list(BIOBANK_TESTS)
_FAKE_BUCKET = "rdr_fake_bucket"
_FAKE_BIOBANK_SAMPLE_BUCKET = "rdr_fake_biobank_sample_bucket"
_FAKE_BUCKET_FOLDER = "rdr_fake_sub_folder"
_FAKE_BUCKET_RESULT_FOLDER = "rdr_fake_sub_result_folder"
_FAKE_GENOMIC_CENTER_BUCKET_A = 'rdr_fake_genomic_center_a_bucket'
_FAKE_GENOMIC_CENTER_BUCKET_B = 'rdr_fake_genomic_center_b_bucket'
_FAKE_GENOMIC_CENTER_BUCKET_BAYLOR = 'fake_genomic_center_bucket-baylor'
_FAKE_GENOMIC_CENTER_DATA_BUCKET_A = 'rdr_fake_genomic_center_a_data_bucket'
_FAKE_GENOMIC_CENTER_BUCKET_RDR = 'rdr_fake_bucket'
_FAKE_GENOTYPING_FOLDER = 'AW1_genotyping_sample_manifests'
_FAKE_SEQUENCING_FOLDER = 'AW1_wgs_sample_manifests'
_FAKE_CVL_REPORT_FOLDER = 'fake_cvl_reconciliation_reports'
_FAKE_CVL_MANIFEST_FOLDER = 'fake_cvl_manifest_folder'
_FAKE_GEM_BUCKET = 'fake_gem_bucket'
_FAKE_FAILURE_FOLDER = 'AW1F_genotyping_accessioning_results'
_FAKE_CVL_SITE_BUCKET_MAP = {"rdr": "rdr_bucket_name"}
_OUTPUT_CSV_TIME_FORMAT = "%Y-%m-%d-%H-%M-%S"
_US_CENTRAL = pytz.timezone("US/Central")
_UTC = pytz.utc


class ExpectedCsvColumns(object):
    BIOBANK_ID = "biobank_id"
    COLLECTION_TUBE_ID = "collection_tube_id"
    SEX_AT_BIRTH = "sex_at_birth"
    GENOME_TYPE = "genome_type"
    NY_FLAG = "ny_flag"
    REQUEST_ID = "request_id"
    PACKAGE_ID = "package_id"
    VALIDATION_PASSED = 'validation_passed'
    AI_AN = 'ai_an'
    ALL = (SEX_AT_BIRTH, GENOME_TYPE, NY_FLAG, VALIDATION_PASSED, AI_AN)


def create_ingestion_test_file(
    test_data_filename,
    bucket_name,
    folder=None,
    include_timestamp=True,
    include_sub_num=False,
    extension=None
):
    test_data_file = open_genomic_set_file(test_data_filename)

    input_filename = '{}{}{}{}'.format(
        test_data_filename.replace('.csv', ''),
        '_11192019' if include_timestamp else '',
        '_1' if include_sub_num else '',
        '.csv' if not extension else extension
    )
    write_cloud_csv(
        input_filename,
        test_data_file,
        folder=folder,
        bucket=bucket_name
    )

    return input_filename


def open_genomic_set_file(test_filename):
    with open(data_path(test_filename)) as f:
        lines = f.readlines()
        csv_str = ""
        for line in lines:
            csv_str += line

        return csv_str


def write_cloud_csv(
    file_name,
    contents_str,
    bucket=None,
    folder=None,
):
    bucket = _FAKE_BUCKET if bucket is None else bucket
    if folder is None:
        path = "/%s/%s" % (bucket, file_name)
    else:
        path = "/%s/%s/%s" % (bucket, folder, file_name)
    with open_cloud_file(path, mode='wb') as cloud_file:
        cloud_file.write(contents_str.encode("utf-8"))

    # handle update time of test files
    provider = storage.get_storage_provider()
    n = clock.CLOCK.now()
    ntime = time.mktime(n.timetuple())
    os.utime(provider.get_local_path(path), (ntime, ntime))
    return cloud_file


# noinspection DuplicatedCode
class GenomicPipelineTest(BaseTestCase):
    def setUp(self):
        super(GenomicPipelineTest, self).setUp()

        self.slack_webhooks = {
            "rdr_genomic_alerts": "https://hooks.slack.com/services/T00000000/B00000000/XXXXXXXXXXXXXXXXXXXXXXXX",
        }
        # Everything is stored as a list, so override bucket name as a 1-element list.
        config.override_setting(config.GENOMIC_SET_BUCKET_NAME, [_FAKE_BUCKET])
        config.override_setting(config.BIOBANK_SAMPLES_BUCKET_NAME, [_FAKE_BIOBANK_SAMPLE_BUCKET])
        config.override_setting(config.GENOMIC_BIOBANK_MANIFEST_FOLDER_NAME, [_FAKE_BUCKET_FOLDER])
        config.override_setting(config.GENOMIC_BIOBANK_MANIFEST_RESULT_FOLDER_NAME, [_FAKE_BUCKET_RESULT_FOLDER])
        config.override_setting(config.GENOMIC_CENTER_BUCKET_NAME, [_FAKE_GENOMIC_CENTER_BUCKET_A,
                                                                    _FAKE_GENOMIC_CENTER_BUCKET_B,
                                                                    _FAKE_GENOMIC_CENTER_BUCKET_BAYLOR,
                                                                    _FAKE_GENOMIC_CENTER_BUCKET_RDR])
        config.override_setting(config.GENOMIC_CENTER_DATA_BUCKET_NAME, [_FAKE_GENOMIC_CENTER_BUCKET_A,
                                                                         _FAKE_GENOMIC_CENTER_BUCKET_B,
                                                                         _FAKE_GENOMIC_CENTER_BUCKET_BAYLOR,
                                                                         _FAKE_GENOMIC_CENTER_BUCKET_RDR])
        config.override_setting(config.GENOMIC_CVL_BUCKET_NAME, [_FAKE_BUCKET])

        config.override_setting(config.GENOMIC_GENOTYPING_SAMPLE_MANIFEST_FOLDER_NAME,
                                [_FAKE_GENOTYPING_FOLDER])
        config.override_setting(config.GENOMIC_CVL_RECONCILIATION_REPORT_SUBFOLDER,
                                [_FAKE_CVL_REPORT_FOLDER])
        config.override_setting(config.CVL_W1_MANIFEST_SUBFOLDER,
                                [_FAKE_CVL_MANIFEST_FOLDER])
        config.override_setting(config.GENOMIC_GEM_BUCKET_NAME, [_FAKE_GEM_BUCKET])
        config.override_setting(config.GENOMIC_AW1F_SUBFOLDER, [_FAKE_FAILURE_FOLDER])
        config.override_setting(config.RDR_SLACK_WEBHOOKS, self.slack_webhooks)
        config.override_setting(config.GENOMIC_CVL_SITE_BUCKET_MAP, _FAKE_CVL_SITE_BUCKET_MAP)
        config.override_setting('rdr_bucket_name', [_FAKE_BUCKET])
        config.override_setting(config.GENOMIC_CVL_SITE_PREFIX_MAP, {'rdr': {
            'cram': 'Wgs_sample_raw_data/CRAMs_CRAIs',
            'cram.crai': 'Wgs_sample_raw_data/CRAMs_CRAIs',
            'cram.md5sum': 'Wgs_sample_raw_data/CRAMs_CRAIs',
            'hard-filtered.vcf.gz': 'Wgs_sample_raw_data/SS_VCF_clinical',
            'hard-filtered.vcf.gz.md5sum': 'Wgs_sample_raw_data/SS_VCF_clinical',
            'hard-filtered.vcf.gz.tbi': 'Wgs_sample_raw_data/SS_VCF_clinical',
            'hard-filtered.gvcf.gz': 'Wgs_sample_raw_data/SS_VCF_research',
            'hard-filtered.gvcf.gz.md5sum': 'Wgs_sample_raw_data/SS_VCF_research'
        }})

        self.participant_dao = ParticipantDao()
        self.summary_dao = ParticipantSummaryDao()
        self.race_dao = ParticipantRaceAnswersDao()
        self.job_run_dao = GenomicJobRunDao()
        self.manifest_file_dao = GenomicManifestFileDao()
        self.manifest_feedback_dao = GenomicManifestFeedbackDao()
        self.file_processed_dao = GenomicFileProcessedDao()
        self.incident_dao = GenomicIncidentDao()
        self.set_dao = GenomicSetDao()
        self.member_dao = GenomicSetMemberDao()
        self.metrics_dao = GenomicGCValidationMetricsDao()
        self.data_file_dao = GenomicGcDataFileDao()
        self.missing_file_dao = GenomicGcDataFileMissingDao()
        self.sample_dao = BiobankStoredSampleDao()
        self.order_dao = BiobankOrderDao()
        self.mk_dao = MailKitOrderDao()
        self.site_dao = SiteDao()
        self.code_dao = CodeDao()
        self.q_dao = QuestionnaireDao()
        self.qr_dao = QuestionnaireResponseDao()
        self.qra_dao = QuestionnaireResponseAnswerDao()
        self.qq_dao = QuestionnaireQuestionDao()
        self.aw1_raw_dao = GenomicAW1RawDao()
        self.aw2_raw_dao = GenomicAW2RawDao()

        self._participant_i = 1

    mock_bucket_paths = [_FAKE_BUCKET,
                         _FAKE_BIOBANK_SAMPLE_BUCKET,
                         _FAKE_BIOBANK_SAMPLE_BUCKET + os.sep + _FAKE_BUCKET_FOLDER,
                         _FAKE_BIOBANK_SAMPLE_BUCKET + os.sep + _FAKE_BUCKET_RESULT_FOLDER
                         ]

    def _make_participant(self, **kwargs):
        """
    Make a participant with custom settings.
    default should create a valid participant.
    """
        i = self._participant_i
        self._participant_i += 1
        bid = kwargs.pop('biobankId', i)
        participant = Participant(participantId=i, biobankId=bid, researchId=1000000 + i, **kwargs)
        self.participant_dao.insert(participant)
        return participant

    @staticmethod
    def _make_biobank_order(**kwargs):
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
            ("finalizedTime", clock.CLOCK.now()),
            ("version", 1),
            ("identifiers", [BiobankOrderIdentifier(system="a", value="c")]),
            ("samples", [BiobankOrderedSample(test="1SAL2", description="description", processingRequired=True)]),
            ("mailKitOrders", [BiobankMailKitOrder(participantId=participant_id, version=1)]),
        ):
            if k not in kwargs:
                kwargs[k] = default_value

        biobank_order = BiobankOrderDao().insert(BiobankOrder(**kwargs))
        return biobank_order

    @staticmethod
    def _make_stored_sample(**kwargs):
        """Makes BiobankStoredSamples for a biobank_id"""
        return BiobankStoredSampleDao().insert(BiobankStoredSample(**kwargs))

    @staticmethod
    def _make_ordered_sample(_test='1SAL2', _description='description',
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
            consentForStudyEnrollmentAuthored=datetime.datetime(2019, 1, 1),
            consentForStudyEnrollment=QuestionnaireStatus.SUBMITTED,
            consentForGenomicsROR=QuestionnaireStatus.SUBMITTED,
        )
        kwargs = dict(valid_kwargs, **override_kwargs)
        summary = self.data_generator._participant_summary_with_defaults(**kwargs)
        self.summary_dao.insert(summary)
        return summary

    def _insert_control_sample_genomic_set_member(self, sample_id, genome_type):
        # Create genomic_set for control sample

        genomic_test_set = self._create_fake_genomic_set(
            genomic_set_name="control-samples",
            genomic_set_criteria=".",
            genomic_set_filename="."
        )

        self._create_fake_genomic_member(
            genomic_set_id=genomic_test_set.id,
            participant_id=0,
            validation_status=GenomicSetMemberStatus.VALID,
            validation_flags=None,
            biobankId=None,
            sample_id=sample_id,
            genome_type=genome_type,
            genomic_workflow_state=GenomicWorkflowState.CONTROL_SAMPLE,
        )

    @staticmethod
    def _create_fake_genomic_set(
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

    @staticmethod
    def _create_fake_genomic_member(
        genomic_set_id,
        participant_id,
        validation_status=GenomicSetMemberStatus.VALID,
        validation_flags=None,
        sex_at_birth="F",
        biobankId=None,
        sample_id=None,
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
        gc_manifest_parent_sample_id=None,
        sample_source=None,
        ai_an=None,
        block_research=None,
        block_research_reason=None,
        block_results=None,
        block_results_reason=None
    ):
        genomic_set_member = GenomicSetMember()
        genomic_set_member.genomicSetId = genomic_set_id
        genomic_set_member.validationStatus = validation_status
        genomic_set_member.validationFlags = validation_flags
        genomic_set_member.participantId = participant_id
        genomic_set_member.sampleId = sample_id
        genomic_set_member.sexAtBirth = sex_at_birth
        genomic_set_member.biobankId = biobankId
        genomic_set_member.collectionTubeId = participant_id
        genomic_set_member.genomeType = genome_type
        genomic_set_member.nyFlag = 1 if ny_flag == "Y" else 0
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
        genomic_set_member.gcManifestParentSampleId = gc_manifest_parent_sample_id
        genomic_set_member.gcManifestSampleSource = sample_source
        genomic_set_member.ai_an = ai_an
        genomic_set_member.blockResearch = block_research
        genomic_set_member.blockResearchReason = block_research_reason
        genomic_set_member.blockResults = block_results
        genomic_set_member.blockResultsReason = block_results_reason
        member_dao = GenomicSetMemberDao()
        member_dao.insert(genomic_set_member)

    @staticmethod
    def _naive_utc_to_naive_central(naive_utc_date):
        utc_date = pytz.utc.localize(naive_utc_date)
        central_date = utc_date.astimezone(pytz.timezone("US/Central"))
        return central_date.replace(tzinfo=None)

    @staticmethod
    def _find_latest_genomic_set_csv(cloud_bucket_name, keyword=None):
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
        # pylint: disable=unused-variable
        # fake genomic_set
        genomic_test_set = self._create_fake_genomic_set(
            genomic_set_name="genomic-test-set-cell-line",
            genomic_set_criteria=".",
            genomic_set_filename="genomic-test-set-cell-line.csv"
        )

        # make necessary fake participant data
        id_start_from = kwargs.get('id_start_from', 0)
        for p in range(1 + id_start_from, count + 1 + id_start_from):
            participant = self._make_participant()
            self._make_summary(participant)
            self._make_biobank_order(participantId=participant.participantId,
                                     biobankOrderId=p,
                                     identifiers=[BiobankOrderIdentifier(
                                         system=u'c', value=u'e{}'.format(
                                             participant.participantId))])
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
            if "genome_type" in kwargs:
                gt = kwargs.get('genome_type')

            self._create_fake_genomic_member(
                genomic_set_id=genomic_test_set.id,
                participant_id=participant.participantId,
                validation_status=GenomicSetMemberStatus.VALID,
                validation_flags=None,
                biobankId=p,
                sex_at_birth='F',
                genome_type=gt,
                ny_flag='Y',
                sequencing_filename=kwargs.get('sequencing_filename'),
                recon_bb_manifest_job_id=kwargs.get('bb_man_id'),
                recon_sequencing_job_id=kwargs.get('recon_seq_id'),
                recon_gc_manifest_job_id=kwargs.get('recon_gc_man_id'),
                gem_a1_manifest_job_id=kwargs.get('gem_a1_run_id'),
                cvl_w1_manifest_job_id=kwargs.get('cvl_w1_run_id'),
                genomic_workflow_state=kwargs.get('genomic_workflow_state'),
                genome_center=kwargs.get('genome_center'),
                aw3_job_id=kwargs.get('aw3_job_id'),
                gc_manifest_parent_sample_id=1000+p,
                sample_source=kwargs.get('sample_source'),
                ai_an=kwargs.get('ai_an'),
                block_research=kwargs.get('block_research'),
                block_research_reason=kwargs.get('block_research_reason'),
                block_results=kwargs.get('block_results'),
                block_results_reason=kwargs.get('block_results_reason')
            )

    def _update_site_states(self):
        sites = [self.site_dao.get(i) for i in range(1, 3)]
        sites[0].state = 'NY'
        sites[1].state = 'AZ'
        for site in sites:
            self.site_dao.update(site)

    def _update_site_type_div_pouch(self, site_id):
        site = self.site_dao.get(site_id)
        site.siteType = "Diversion Pouch"
        self.site_dao.update(site)

    def _update_biobank_order_collected_site(self, biobank_order_id, site_id):
        biobank_order = self.order_dao.get(biobank_order_id)
        biobank_order.collectedSiteId = site_id
        with self.order_dao.session() as session:
            session.merge(biobank_order)

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

    def test_ingest_array_aw2_end_to_end(self):
        # Create the fake Google Cloud CSV files to ingest
        bucket_name = _FAKE_GENOMIC_CENTER_BUCKET_A
        subfolder = config.getSetting(config.GENOMIC_AW2_SUBFOLDERS[1])
        # add to subfolder
        test_file = 'RDR_AoU_GEN_TestDataManifest.csv'

        test_date = datetime.datetime(2020, 10, 13, 0, 0, 0, 0)

        pytz.timezone('US/Central').localize(test_date)

        with clock.FakeClock(test_date):
            test_file_name = create_ingestion_test_file(test_file, bucket_name,
                                                              folder=subfolder,
                                                              include_sub_num=True)

        self._create_fake_datasets_for_gc_tests(2, arr_override=True,
                                                array_participants=(1, 2),
                                                genomic_workflow_state=GenomicWorkflowState.AW1)

        self._update_test_sample_ids()

        # run the GC Metrics Ingestion workflow via cloud task
        # Set up file/JSON
        task_data = {
            "job": GenomicJob.METRICS_INGESTION,
            "bucket": bucket_name,
            "file_data": {
                "create_feedback_record": False,
                "upload_date": test_date.isoformat(),
                "manifest_type": GenomicManifestTypes.AW2,
                "file_path": f"{bucket_name}/{subfolder}/{test_file_name}"
            }
        }

        self._create_stored_samples([
            (1, 1001),
            (2, 1002)
        ])

        # Execute from cloud task
        genomic_pipeline.execute_genomic_manifest_file_pipeline(task_data)

        # test file processing queue
        file_processed = self.file_processed_dao.get(1)
        self.assertEqual(test_date.astimezone(pytz.utc), pytz.utc.localize(file_processed.uploadDate))
        self.assertEqual(1, file_processed.genomicManifestFileId)

        manifest_file_obj = self.manifest_file_dao.get(1)
        self.assertEqual('AW2', manifest_file_obj.manifestTypeIdStr)

        self.assertEqual(
            file_processed.fileName,
            'RDR_AoU_GEN_TestDataManifest_11192019_1.csv'
        )
        self.assertEqual(
            file_processed.filePath,
            f'{_FAKE_GENOMIC_CENTER_BUCKET_A}/'
            f'{config.getSetting(config.GENOMIC_AW2_SUBFOLDERS[1])}/'
            f'RDR_AoU_GEN_TestDataManifest_11192019_1.csv'
        )

        # Test the fields against the DB
        gc_metrics = self.metrics_dao.get_all()

        self.assertEqual(len(gc_metrics), 2)
        self._gc_metrics_ingested_data_test_cases([gc_metrics[0]])

        # Test Genomic State updated
        member = self.member_dao.get(1)
        self.assertEqual(GenomicWorkflowState.GEM_READY, member.genomicWorkflowState)
        self.assertEqual('1001', member.sampleId)
        self.assertEqual(1, member.aw2FileProcessedId)

        # Test new members created for re-extraction
        members = self.member_dao.get_all()
        self.assertEqual(len(members), 5)

        # Test successful run result
        self.assertEqual(GenomicSubProcessResult.SUCCESS, self.job_run_dao.get(1).runResult)
        self.assertEqual(GenomicSubProcessResult.SUCCESS, self.job_run_dao.get(2).runResult)

        # Test Inserted metrics are not re-inserted
        # Setup Test file (reusing test file)
        updated_aw2_file = open_genomic_set_file('RDR_AoU_GEN_TestDataManifest.csv')
        updated_aw2_file = updated_aw2_file.replace('10002', '11002')
        updated_aw2_file = updated_aw2_file.replace('0.1345', '-0.005')

        updated_aw2_filename = "RDR_AoU_GEN_TestDataManifest_11192020.csv"

        write_cloud_csv(
            updated_aw2_filename,
            updated_aw2_file,
            bucket=bucket_name,
            folder=subfolder,
        )

        # run the GC Metrics Ingestion workflow again
        task_data['file_data']['file_path'] = f"{bucket_name}/{subfolder}/{updated_aw2_filename}"

        # Simulate new file uploaded
        genomic_pipeline.execute_genomic_manifest_file_pipeline(task_data)

        gc_metrics = self.metrics_dao.get_all()
        # Test that no new records were inserted
        self.assertEqual(len(gc_metrics), 2)
        # Test the data was updated
        for m in gc_metrics:
            if m.genomicSetMemberId == 2:
                self.assertEqual(m.limsId, '11002')

                # Test negative contamination is 0
                self.assertEqual('0', gc_metrics[1].contamination)

        # Test new members created for re-extraction again
        members = self.member_dao.get_all()
        self.assertEqual(len(members), 5)

    def test_ingest_specific_aw2_file(self):
        self._create_fake_datasets_for_gc_tests(3, arr_override=True,
                                                array_participants=(1, 3),
                                                genomic_workflow_state=GenomicWorkflowState.AW1)

        self._update_test_sample_ids()

        # Setup Test file
        aw2_manifest_file = open_genomic_set_file("RDR_AoU_GEN_TestDataManifest.csv")

        aw2_manifest_filename = "RDR_AoU_GEN_TestDataManifest_11192019_1.csv"

        test_date = datetime.datetime(2020, 10, 13, 0, 0, 0, 0)
        pytz.timezone('US/Central').localize(test_date)

        subfolder = config.getSetting(config.GENOMIC_AW2_SUBFOLDERS[1])
        with clock.FakeClock(test_date):
            write_cloud_csv(
                aw2_manifest_filename,
                aw2_manifest_file,
                bucket=_FAKE_GENOMIC_CENTER_BUCKET_A,
                folder=subfolder,
            )

        # Get bucket, subfolder, and filename from argument
        bucket_name = _FAKE_GENOMIC_CENTER_BUCKET_A
        file_name = subfolder + '/' + aw2_manifest_filename

        # Use the Controller to run the job
        with GenomicJobController(GenomicJob.METRICS_INGESTION) as controller:
            controller.bucket_name = bucket_name
            controller.ingest_specific_manifest(file_name)

        files_processed = self.file_processed_dao.get_all()
        self.assertEqual(test_date.astimezone(pytz.utc), pytz.utc.localize(files_processed[0].uploadDate))

        # Test the data was ingested OK
        self._gc_files_processed_test_cases(files_processed)

        # Test the end result code is recorded
        self.assertEqual(GenomicSubProcessResult.SUCCESS, self.job_run_dao.get(1).runResult)

    def test_ingest_aw5_file(self):
        # Create the fake ingested data
        self._create_fake_datasets_for_gc_tests(2, genomic_workflow_state=GenomicWorkflowState.AW1)
        self._create_fake_datasets_for_gc_tests(2, arr_override=True, id_start_from=2, array_participants=[3, 4],
                                                genomic_workflow_state=GenomicWorkflowState.AW1)
        bucket_name = _FAKE_GENOMIC_CENTER_BUCKET_A
        subfolder = config.getSetting(config.GENOMIC_AW2_SUBFOLDERS[0])

        test_date = datetime.datetime(2020, 10, 13, 0, 0, 0, 0)
        pytz.timezone('US/Central').localize(test_date)
        self._update_test_sample_ids()
        self._create_stored_samples([(1, 1001), (2, 1002), (3, 1003), (4, 1004)])

        with clock.FakeClock(test_date):
            test_file_name_seq = create_ingestion_test_file(
                'RDR_AoU_SEQ_TestDataManifest_for_aw5.csv',
                bucket_name,
                folder=subfolder
            )
            test_file_name_gen = create_ingestion_test_file(
                'RDR_AoU_GEN_TestDataManifest_for_aw5.csv',
                bucket_name,
                folder=subfolder
            )

        task_data_seq = {
            "job": GenomicJob.METRICS_INGESTION,
            "bucket": bucket_name,
            "file_data": {
                "create_feedback_record": False,
                "upload_date": test_date.isoformat(),
                "manifest_type": GenomicManifestTypes.AW1,
                "file_path": f"{bucket_name}/{subfolder}/{test_file_name_seq}"
            }
        }

        task_data_gen = {
            "job": GenomicJob.METRICS_INGESTION,
            "bucket": bucket_name,
            "file_data": {
                "create_feedback_record": False,
                "upload_date": test_date.isoformat(),
                "manifest_type": GenomicManifestTypes.AW1,
                "file_path": f"{bucket_name}/{subfolder}/{test_file_name_gen}"
            }
        }

        # Execute from cloud task
        genomic_pipeline.execute_genomic_manifest_file_pipeline(task_data_seq)
        genomic_pipeline.execute_genomic_manifest_file_pipeline(task_data_gen)

        # ingest AW5 files
        with clock.FakeClock(test_date):
            test_file_name_aw5_array = create_ingestion_test_file('aw5_deletion_array.csv',
                                                                        bucket_name, folder=subfolder)
            test_file_name_aw5_wgs = create_ingestion_test_file('aw5_deletion_wgs.csv',
                                                                      bucket_name, folder=subfolder)
        task_data_aw5_wgs = {
            "job": GenomicJob.AW5_WGS_MANIFEST,
            "bucket": bucket_name,
            "file_data": {
                "create_feedback_record": False,
                "upload_date": test_date.isoformat(),
                "manifest_type": GenomicManifestTypes.AW5_WGS,
                "file_path": f"{bucket_name}/{subfolder}/{test_file_name_aw5_wgs}"
            }
        }

        task_data_aw5_array = {
            "job": GenomicJob.AW5_ARRAY_MANIFEST,
            "bucket": bucket_name,
            "file_data": {
                "create_feedback_record": False,
                "upload_date": test_date.isoformat(),
                "manifest_type": GenomicManifestTypes.AW5_ARRAY,
                "file_path": f"{bucket_name}/{subfolder}/{test_file_name_aw5_array}"
            }
        }

        # Test investigation genome types still ingest
        for member in self.member_dao.get_all():
            if member.id in (2, 4):
                member.genomeType += "_investigation"
                self.member_dao.update(member)

        # Execute from cloud task
        genomic_pipeline.execute_genomic_manifest_file_pipeline(task_data_aw5_wgs)
        genomic_pipeline.execute_genomic_manifest_file_pipeline(task_data_aw5_array)

        # Test the fields against the DB
        gc_metrics = self.metrics_dao.get_all()

        self.assertEqual(len(gc_metrics), 4)
        for metrics_record in gc_metrics:
            self.assertIn(metrics_record.limsId, ['10001', '10002', '10003', '10004'])
            if metrics_record.limsId == '10001':
                self.assertEqual(metrics_record.hfVcfDeleted, 1)
                self.assertEqual(metrics_record.hfVcfTbiDeleted, 1)
                self.assertEqual(metrics_record.hfVcfMd5Deleted, 1)
                self.assertEqual(metrics_record.rawVcfDeleted, 1)
                self.assertEqual(metrics_record.rawVcfTbiDeleted, 1)
                self.assertEqual(metrics_record.rawVcfMd5Deleted, 1)
                self.assertEqual(metrics_record.cramDeleted, 1)
                self.assertEqual(metrics_record.cramMd5Deleted, 1)
                self.assertEqual(metrics_record.craiDeleted, 1)
            elif metrics_record.limsId == '10002':
                self.assertEqual(metrics_record.hfVcfDeleted, 1)
                self.assertEqual(metrics_record.hfVcfTbiDeleted, 0)
                self.assertEqual(metrics_record.hfVcfMd5Deleted, 1)
                self.assertEqual(metrics_record.rawVcfDeleted, 1)
                self.assertEqual(metrics_record.rawVcfTbiDeleted, 0)
                self.assertEqual(metrics_record.rawVcfMd5Deleted, 1)
                self.assertEqual(metrics_record.cramDeleted, 1)
                self.assertEqual(metrics_record.cramMd5Deleted, 0)
                self.assertEqual(metrics_record.craiDeleted, 1)
            elif metrics_record.limsId == '10003':
                self.assertEqual(metrics_record.idatRedDeleted, 1)
                self.assertEqual(metrics_record.idatGreenDeleted, 1)
                self.assertEqual(metrics_record.idatRedMd5Deleted, 1)
                self.assertEqual(metrics_record.idatGreenMd5Deleted, 1)
                self.assertEqual(metrics_record.vcfDeleted, 1)
                self.assertEqual(metrics_record.vcfMd5Deleted, 1)
                self.assertEqual(metrics_record.vcfTbiDeleted, 1)
            elif metrics_record.limsId == '10004':
                self.assertEqual(metrics_record.idatRedDeleted, 1)
                self.assertEqual(metrics_record.idatGreenDeleted, 1)
                self.assertEqual(metrics_record.idatRedMd5Deleted, 0)
                self.assertEqual(metrics_record.idatGreenMd5Deleted, 1)
                self.assertEqual(metrics_record.vcfDeleted, 0)
                self.assertEqual(metrics_record.vcfMd5Deleted, 1)
                self.assertEqual(metrics_record.vcfTbiDeleted, 1)

        # Test file processing queue
        files_processed = self.file_processed_dao.get_all()
        self.assertEqual(len(files_processed), 4)
        self.assertEqual(test_date.astimezone(pytz.utc), pytz.utc.localize(files_processed[0].uploadDate))

        # Test the end-to-end result code
        self.assertEqual(GenomicSubProcessResult.SUCCESS, self.job_run_dao.get(1).runResult)
        self.assertEqual(GenomicSubProcessResult.SUCCESS, self.job_run_dao.get(2).runResult)
        self.assertEqual(GenomicSubProcessResult.SUCCESS, self.job_run_dao.get(3).runResult)
        self.assertEqual(GenomicSubProcessResult.SUCCESS, self.job_run_dao.get(4).runResult)

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
                    f'{_FAKE_GENOMIC_CENTER_BUCKET_A}/'
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
                    f'{_FAKE_GENOMIC_CENTER_BUCKET_A}/'
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
            self.assertEqual('0.01', record.contamination)
            self.assertEqual(GenomicContaminationCategory.EXTRACT_WGS, record.contaminationCategory)
            self.assertEqual(GenomicContaminationCategory.EXTRACT_WGS.name, record.contaminationCategoryStr)
            self.assertEqual('Pass', record.processingStatus)
            self.assertEqual('This sample passed', record.notes)

    def test_gc_metrics_ingestion_bad_files(self):
        # Create the fake Google Cloud CSV files to ingest
        bucket_name = _FAKE_GENOMIC_CENTER_BUCKET_A
        end_to_end_test_files = (
            'RDR_AoU_SEQ_TestNoHeadersDataManifest.csv',
            'RDR_AoU_SEQ_TestBadStructureDataManifest.csv',
            'RDR-AoU-TestBadFilename-DataManifest.csv',
            'test_empty_wells.csv',
            'RDR_AoU_GEN_TestExt_DataManifest.csv',
        )
        test_files_names = []
        for test_file in end_to_end_test_files:
            test_file_name = create_ingestion_test_file(
                test_file,
                bucket_name,
                folder=config.getSetting(config.GENOMIC_AW2_SUBFOLDERS[0]),
                extension='.tsv' if 'TestExt' in test_file else None
            )
            test_files_names.append(test_file_name)

        # run the GC Metrics Ingestion workflow
        genomic_pipeline.ingest_genomic_centers_metrics_files()

        # test file processing queue
        processed_files = self.file_processed_dao.get_all()
        should_be_processed = [test_files_names[0], test_files_names[1]]

        self.assertEqual(len(processed_files), len(should_be_processed))

        for processed in processed_files:
            # Test bad filename, invalid columns
            incident = self.incident_dao.get_by_source_file_id(processed.id)[0]
            if "TestNoHeaders" in processed.fileName:
                self.assertEqual(0, incident.slack_notification)
                self.assertIsNone(incident.slack_notification_date)
                self.assertEqual(incident.code, GenomicIncidentCode.FILE_VALIDATION_FAILED_STRUCTURE.name)
            if "TestBadStructure" in processed.fileName:
                self.assertEqual(1, incident.slack_notification)
                self.assertIsNotNone(incident.slack_notification_date)
                self.assertEqual(incident.code, GenomicIncidentCode.FILE_VALIDATION_FAILED_STRUCTURE.name)

        # Test Unsuccessful run
        run_obj = self.job_run_dao.get(1)
        self.assertEqual(GenomicSubProcessResult.ERROR, run_obj.runResult)
        self.assertEqual(GenomicSubProcessResult.ERROR.name, run_obj.runResultStr)

        should_not_be_processed = [test_files_names[2], test_files_names[3], test_files_names[4]]

        for processed in should_not_be_processed:
            self.assertIsNone(self.file_processed_dao.get_record_from_filename(processed))

    def test_ingestion_bad_files_no_incident_created_seven_days(self):
        # Create the fake Google Cloud CSV files to ingest
        bucket_name = _FAKE_GENOMIC_CENTER_BUCKET_A
        create_ingestion_test_file(
            'RDR_AoU_SEQ_TestBadStructureDataManifest.csv',
            bucket_name,
            folder=config.getSetting(config.GENOMIC_AW2_SUBFOLDERS[0]),
        )

        # run the GC Metrics Ingestion workflow
        genomic_pipeline.ingest_genomic_centers_metrics_files()

        current_incident = self.incident_dao.get(1)
        message = current_incident.message

        current_incidents_with_message = self.incident_dao.get_by_message(message)

        self.assertIsNotNone(current_incidents_with_message)

        today_plus_6 = datetime.datetime.utcnow() \
                            + datetime.timedelta(days=6)

        # run the GC Metrics Ingestion workflow + 6 days same bad file
        with clock.FakeClock(today_plus_6):
            genomic_pipeline.ingest_genomic_centers_metrics_files()

            all_incidents = self.incident_dao.get_all()
            count = 0
            for incident in all_incidents:
                if incident.message == message:
                    count += 1

            self.assertEqual(count, 1)

        today_plus_8 = datetime.datetime.utcnow() \
                           + datetime.timedelta(days=8)

        # run the GC Metrics Ingestion workflow + 8 days same bad file
        with clock.FakeClock(today_plus_8):
            genomic_pipeline.ingest_genomic_centers_metrics_files()

            all_incidents = self.incident_dao.get_all()
            count = 0
            for incident in all_incidents:
                if incident.message == message:
                    count += 1

            self.assertEqual(count, 2)

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
        subfolder = config.getSetting(config.GENOMIC_AW2_SUBFOLDERS[0])

        test_date = datetime.datetime(2020, 10, 13, 0, 0, 0, 0)
        pytz.timezone('US/Central').localize(test_date)

        with clock.FakeClock(test_date):
            test_file_name = create_ingestion_test_file('RDR_AoU_SEQ_TestDataManifest.csv',
                                                              bucket_name,
                                                              folder=subfolder)

        self._update_test_sample_ids()

        # run the GC Metrics Ingestion workflow via cloud task
        # Set up file/JSON
        task_data = {
            "job": GenomicJob.METRICS_INGESTION,
            "bucket": bucket_name,
            "file_data": {
                "create_feedback_record": False,
                "upload_date": test_date.isoformat(),
                "manifest_type": GenomicManifestTypes.AW1,
                "file_path": f"{bucket_name}/{subfolder}/{test_file_name}"
            }
        }

        self._create_stored_samples([(2, 1002)])

        # Execute from cloud task
        genomic_pipeline.execute_genomic_manifest_file_pipeline(task_data)  # run_id = 1 & 2

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
        self.assertEqual(gc_metrics[0].contaminationCategory, GenomicContaminationCategory.EXTRACT_BOTH)
        self.assertEqual(gc_metrics[0].contaminationCategoryStr, 'EXTRACT_BOTH')
        self.assertEqual(gc_metrics[0].mappedReadsPct, '88.8888888')
        self.assertEqual(gc_metrics[0].sexConcordance, 'True')
        self.assertEqual(gc_metrics[0].arrayConcordance, 'True')
        self.assertEqual(gc_metrics[0].sexPloidy, 'XY')
        self.assertEqual(gc_metrics[0].alignedQ30Bases, 1000000000004)
        self.assertEqual(gc_metrics[0].processingStatus, 'Pass')
        self.assertEqual(gc_metrics[0].notes, 'This sample passed')

        # Test file processing queue
        files_processed = self.file_processed_dao.get_all()
        self.assertEqual(len(files_processed), 1)
        self.assertEqual(test_date.astimezone(pytz.utc), pytz.utc.localize(files_processed[0].uploadDate))

        # Test the end-to-end result code
        self.assertEqual(GenomicSubProcessResult.SUCCESS, self.job_run_dao.get(1).runResult)
        self.assertEqual(GenomicSubProcessResult.SUCCESS, self.job_run_dao.get(2).runResult)
        self.assertEqual("SUCCESS", self.job_run_dao.get(2).runResultStr)

    # DA-2934 Reconciliation process deprecated
    # def test_gc_metrics_reconciliation_vs_array_data(self):
    #
    #     # Create the fake ingested data
    #     self._create_fake_datasets_for_gc_tests(3, arr_override=True, array_participants=[1, 2, 3],
    #                                             genome_center='rdr',
    #                                             genomic_workflow_state=GenomicWorkflowState.AW1)
    #     bucket_name = _FAKE_GENOMIC_CENTER_BUCKET_BAYLOR
    #     create_ingestion_test_file('RDR_AoU_GEN_TestDataManifestWithFailure.csv',
    #                                      bucket_name,
    #                                      folder=config.getSetting(config.GENOMIC_AW2_SUBFOLDERS[1]))
    #
    #     self._update_test_sample_ids()
    #
    #     self._create_stored_samples([
    #         (1, 1001),
    #         (2, 1002),
    #         (3, 1003)
    #     ])
    #
    #     genomic_pipeline.ingest_genomic_centers_metrics_files()  # run_id = 1
    #
    #     # JH sample files
    #     array_test_files_jh = (
    #         f'test_data_folder/10001_R01C01.vcf.gz',
    #         f'test_data_folder/10001_R01C01.vcf.gz.tbi',
    #         f'test_data_folder/10001_R01C01.vcf.gz.md5sum',
    #         f'test_data_folder/10001_R01C01_Red.idat',
    #         f'test_data_folder/10001_R01C01_Grn.idat',
    #         f'test_data_folder/10001_R01C01_Red.idat.md5sum',
    #         f'test_data_folder/10002_R01C02.vcf.gz',
    #         f'test_data_folder/10002_R01C02.vcf.gz.tbi',
    #         f'test_data_folder/10002_R01C02.vcf.gz.md5sum',
    #         f'test_data_folder/10002_R01C02_Red.idat',
    #         f'test_data_folder/10002_R01C02_Grn.idat',
    #         f'test_data_folder/10002_R01C02_Red.idat.md5sum',
    #         f'test_data_folder/10002_R01C02_Grn.idat.md5sum',
    #     )
    #
    #     test_date = datetime.datetime(2021, 7, 12, 0, 0, 0, 0)
    #
    #     # create test records in GenomicGcDataFile
    #     with clock.FakeClock(test_date):
    #         for f in array_test_files_jh:
    #             # Set file type
    #             if "idat" in f.lower():
    #                 file_type = f.split('/')[-1].split("_")[-1]
    #             else:
    #                 file_type = '.'.join(f.split('.')[1:])
    #
    #             test_file_dict = {
    #                 'file_path': f'{bucket_name}/{f}',
    #                 'gc_site_id': 'jh',
    #                 'bucket_name': bucket_name,
    #                 'file_prefix': f'Genotyping_sample_raw_data',
    #                 'file_name': f,
    #                 'file_type': file_type,
    #                 'identifier_type': 'chipwellbarcode',
    #                 'identifier_value': "_".join(f.split('/')[1].split('_')[0:2]).split('.')[0],
    #             }
    #
    #             self.data_generator.create_database_gc_data_file_record(**test_file_dict)
    #
    #     # change member 2 gc_site_id
    #     member = self.member_dao.get(2)
    #     member.gcSiteId = 'jh'
    #
    #     with self.member_dao.session() as s:
    #         s.merge(member)
    #
    #     genomic_pipeline.reconcile_metrics_vs_array_data()  # run_id = 2
    #
    #     gc_record = self.metrics_dao.get(1)
    #
    #     # Test the gc_metrics were updated with reconciliation data
    #     self.assertEqual(1, gc_record.vcfReceived)
    #     self.assertEqual(1, gc_record.vcfTbiReceived)
    #     self.assertEqual(1, gc_record.vcfMd5Received)
    #     self.assertEqual(1, gc_record.idatRedReceived)
    #     self.assertEqual(1, gc_record.idatGreenReceived)
    #     self.assertEqual(1, gc_record.idatRedMd5Received)
    #     self.assertEqual(0, gc_record.idatGreenMd5Received)
    #
    #     self.assertEqual(f"gs://{bucket_name}/{array_test_files_jh[0]}", gc_record.vcfPath)
    #     self.assertEqual(f"gs://{bucket_name}/{array_test_files_jh[1]}", gc_record.vcfTbiPath)
    #     self.assertEqual(f"gs://{bucket_name}/{array_test_files_jh[2]}", gc_record.vcfMd5Path)
    #     self.assertEqual(f"gs://{bucket_name}/{array_test_files_jh[3]}", gc_record.idatRedPath)
    #     self.assertEqual(f"gs://{bucket_name}/{array_test_files_jh[4]}", gc_record.idatGreenPath)
    #     self.assertEqual(f"gs://{bucket_name}/{array_test_files_jh[5]}", gc_record.idatRedMd5Path)
    #
    #     gc_record = self.metrics_dao.get(2)
    #
    #     # Test the gc_metrics were updated with reconciliation data
    #     self.assertEqual(1, gc_record.vcfReceived)
    #     self.assertEqual(1, gc_record.vcfTbiReceived)
    #     self.assertEqual(1, gc_record.vcfMd5Received)
    #     self.assertEqual(1, gc_record.idatRedReceived)
    #     self.assertEqual(1, gc_record.idatGreenReceived)
    #     self.assertEqual(1, gc_record.idatRedMd5Received)
    #     self.assertEqual(1, gc_record.idatGreenMd5Received)
    #
    #     # Test member updated with job ID
    #     member = self.member_dao.get(1)
    #     self.assertEqual(GenomicWorkflowState.GC_DATA_FILES_MISSING, member.genomicWorkflowState)
    #     self.assertEqual('GC_DATA_FILES_MISSING', member.genomicWorkflowStateStr)
    #
    #     # Test member updated with job ID
    #     member = self.member_dao.get(2)
    #     self.assertEqual(GenomicWorkflowState.GEM_READY, member.genomicWorkflowState)
    #     self.assertEqual('GEM_READY', member.genomicWorkflowStateStr)
    #
    #     missing_file = self.missing_file_dao.get(1)
    #     self.assertEqual("rdr", missing_file.gc_site_id)
    #     self.assertEqual("Grn.idat.md5sum", missing_file.file_type)
    #     self.assertEqual(2, missing_file.run_id)
    #     self.assertEqual(1, missing_file.gc_validation_metric_id)
    #     self.assertEqual(0, missing_file.resolved)
    #
    #     processed_file = self.file_processed_dao.get(1)
    #     incident = self.incident_dao.get_by_source_file_id(processed_file.id)[0]
    #     self.assertTrue(incident.code == 'MISSING_FILES')
    #     self.assertTrue(incident.slack_notification == 1)
    #     self.assertTrue("Grn.idat.md5sum" in incident.message)
    #
    #     run_obj = self.job_run_dao.get(2)
    #
    #     self.assertEqual(GenomicSubProcessResult.SUCCESS, run_obj.runResult)

    # DA-2934 Reconciliation process deprecated
    # def test_gc_metrics_reconciliation_vs_array_investigation_data(self):
    #
    #     # Create the fake ingested data
    #     self._create_fake_datasets_for_gc_tests(3, arr_override=True, array_participants=[1, 2, 3],
    #                                             genome_center='rdr',
    #                                             genomic_workflow_state=GenomicWorkflowState.AW1,
    #                                             genome_type='aou_array_investigation',
    #                                             block_research=1,
    #                                             block_research_reason='Created from AW1 with investigation genome type',
    #                                             block_results=1,
    #                                             block_results_reason='Created from AW1 with investigation genome type')
    #
    #     bucket_name = _FAKE_GENOMIC_CENTER_BUCKET_BAYLOR
    #     create_ingestion_test_file('RDR_AoU_GEN_TestDataManifestWithFailure.csv',
    #                                      bucket_name,
    #                                      folder=config.getSetting(config.GENOMIC_AW2_SUBFOLDERS[1]))
    #
    #     self._update_test_sample_ids()
    #
    #     self._create_stored_samples([
    #         (1, 1001),
    #         (2, 1002),
    #         (3, 1003)
    #     ])
    #
    #     genomic_pipeline.ingest_genomic_centers_metrics_files()  # run_id = 1
    #
    #     # JH sample files
    #     array_test_files_jh = (
    #         f'test_data_folder/10001_R01C01.vcf.gz',
    #         f'test_data_folder/10001_R01C01.vcf.gz.tbi',
    #         f'test_data_folder/10001_R01C01.vcf.gz.md5sum',
    #         f'test_data_folder/10001_R01C01_Red.idat',
    #         f'test_data_folder/10001_R01C01_Grn.idat',
    #         f'test_data_folder/10001_R01C01_Red.idat.md5sum',
    #         f'test_data_folder/10002_R01C02.vcf.gz',
    #         f'test_data_folder/10002_R01C02.vcf.gz.tbi',
    #         f'test_data_folder/10002_R01C02.vcf.gz.md5sum',
    #         f'test_data_folder/10002_R01C02_Red.idat',
    #         f'test_data_folder/10002_R01C02_Grn.idat',
    #         f'test_data_folder/10002_R01C02_Red.idat.md5sum',
    #         f'test_data_folder/10002_R01C02_Grn.idat.md5sum',
    #     )
    #
    #     test_date = datetime.datetime(2021, 7, 12, 0, 0, 0, 0)
    #
    #     # create test records in GenomicGcDataFile
    #     with clock.FakeClock(test_date):
    #         for f in array_test_files_jh:
    #             # Set file type
    #             if "idat" in f.lower():
    #                 file_type = f.split('/')[-1].split("_")[-1]
    #             else:
    #                 file_type = '.'.join(f.split('.')[1:])
    #
    #             test_file_dict = {
    #                 'file_path': f'{bucket_name}/{f}',
    #                 'gc_site_id': 'jh',
    #                 'bucket_name': bucket_name,
    #                 'file_prefix': f'Genotyping_sample_raw_data',
    #                 'file_name': f,
    #                 'file_type': file_type,
    #                 'identifier_type': 'chipwellbarcode',
    #                 'identifier_value': "_".join(f.split('/')[1].split('_')[0:2]).split('.')[0],
    #             }
    #
    #             self.data_generator.create_database_gc_data_file_record(**test_file_dict)
    #
    #     # change member 2 gc_site_id
    #     member = self.member_dao.get(2)
    #     member.gcSiteId = 'jh'
    #
    #     with self.member_dao.session() as s:
    #         s.merge(member)
    #
    #     genomic_pipeline.reconcile_metrics_vs_array_data()  # run_id = 2
    #
    #     gc_record = self.metrics_dao.get(1)
    #
    #     # Test the gc_metrics were updated with reconciliation data
    #     self.assertEqual(1, gc_record.vcfReceived)
    #     self.assertEqual(1, gc_record.vcfTbiReceived)
    #     self.assertEqual(1, gc_record.vcfMd5Received)
    #     self.assertEqual(1, gc_record.idatRedReceived)
    #     self.assertEqual(1, gc_record.idatGreenReceived)
    #     self.assertEqual(1, gc_record.idatRedMd5Received)
    #     self.assertEqual(0, gc_record.idatGreenMd5Received)
    #
    #     self.assertEqual(f"gs://{bucket_name}/{array_test_files_jh[0]}", gc_record.vcfPath)
    #     self.assertEqual(f"gs://{bucket_name}/{array_test_files_jh[1]}", gc_record.vcfTbiPath)
    #     self.assertEqual(f"gs://{bucket_name}/{array_test_files_jh[2]}", gc_record.vcfMd5Path)
    #     self.assertEqual(f"gs://{bucket_name}/{array_test_files_jh[3]}", gc_record.idatRedPath)
    #     self.assertEqual(f"gs://{bucket_name}/{array_test_files_jh[4]}", gc_record.idatGreenPath)
    #     self.assertEqual(f"gs://{bucket_name}/{array_test_files_jh[5]}", gc_record.idatRedMd5Path)
    #
    #     gc_record = self.metrics_dao.get(2)
    #
    #     # Test the gc_metrics were updated with reconciliation data
    #     self.assertEqual(1, gc_record.vcfReceived)
    #     self.assertEqual(1, gc_record.vcfTbiReceived)
    #     self.assertEqual(1, gc_record.vcfMd5Received)
    #     self.assertEqual(1, gc_record.idatRedReceived)
    #     self.assertEqual(1, gc_record.idatGreenReceived)
    #     self.assertEqual(1, gc_record.idatRedMd5Received)
    #     self.assertEqual(1, gc_record.idatGreenMd5Received)
    #
    #     # Test member updated with job ID
    #     member = self.member_dao.get(1)
    #     self.assertEqual(GenomicWorkflowState.GC_DATA_FILES_MISSING, member.genomicWorkflowState)
    #     self.assertEqual('GC_DATA_FILES_MISSING', member.genomicWorkflowStateStr)
    #
    #     # Test member updated with job ID
    #     member = self.member_dao.get(2)
    #     self.assertEqual(GenomicWorkflowState.GEM_READY, member.genomicWorkflowState)
    #     self.assertEqual('GEM_READY', member.genomicWorkflowStateStr)
    #
    #     missing_file = self.missing_file_dao.get(1)
    #     self.assertEqual("rdr", missing_file.gc_site_id)
    #     self.assertEqual("Grn.idat.md5sum", missing_file.file_type)
    #     self.assertEqual(2, missing_file.run_id)
    #     self.assertEqual(1, missing_file.gc_validation_metric_id)
    #     self.assertEqual(0, missing_file.resolved)
    #
    #     processed_file = self.file_processed_dao.get(1)
    #     incident = self.incident_dao.get_by_source_file_id(processed_file.id)[0]
    #     self.assertTrue(incident.code == 'MISSING_FILES')
    #     self.assertTrue(incident.slack_notification == 1)
    #     self.assertTrue("Grn.idat.md5sum" in incident.message)
    #
    #     run_obj = self.job_run_dao.get(2)
    #
    #     self.assertEqual(GenomicSubProcessResult.SUCCESS, run_obj.runResult)

    # DA-2934 Reconciliation process deprecated
    # def test_reconciliation_array_data_with_pipeline_config(self):
    #
    #     # Create the fake ingested data
    #     self._create_fake_datasets_for_gc_tests(3, arr_override=True, array_participants=[1, 2, 3],
    #                                             genome_center='rdr',
    #                                             genomic_workflow_state=GenomicWorkflowState.AW1)
    #
    #     bucket_name = _FAKE_GENOMIC_CENTER_BUCKET_BAYLOR
    #     create_ingestion_test_file(
    #         'RDR_AoU_GEN_TestDataManifest_2.csv',
    #         bucket_name,
    #         folder=config.getSetting(config.GENOMIC_AW2_SUBFOLDERS[1])
    #     )
    #
    #     self._update_test_sample_ids()
    #
    #     self._create_stored_samples([
    #         (1, 1001),
    #         (2, 1002),
    #         (3, 1003)
    #     ])
    #
    #     genomic_pipeline.ingest_genomic_centers_metrics_files()  # run_id = 1
    #
    #     # rdr sample files
    #     array_test_files_jh = (
    #         f'test_data_folder/10001_R01C01.vcf.gz',
    #         f'test_data_folder/10001_R01C01.vcf.gz.tbi',
    #         f'test_data_folder/10001_R01C01.vcf.gz.md5sum',
    #         f'test_data_folder/10001_R01C01_Red.idat',
    #         f'test_data_folder/10001_R01C01_Grn.idat',
    #         f'test_data_folder/10001_R01C01_Red.idat.md5sum',
    #         f'test_data_folder/10001_R01C01_Grn.idat.md5sum',
    #         f'test_data_folder/10002_R01C02.vcf.gz',
    #         f'test_data_folder/10002_R01C02.vcf.gz.tbi',
    #         f'test_data_folder/10002_R01C02.vcf.gz.md5sum',
    #         f'test_data_folder/10002_R01C02_Red.idat',
    #         f'test_data_folder/10002_R01C02_Grn.idat',
    #         f'test_data_folder/10002_R01C02_Red.idat.md5sum',
    #         f'test_data_folder/10002_R01C02_Grn.idat.md5sum',
    #         f'test_data_folder/10003_R01C03.vcf.gz',
    #         f'test_data_folder/10003_R01C03.vcf.gz.tbi',
    #         f'test_data_folder/10003_R01C03.vcf.gz.md5sum',
    #         f'test_data_folder/10003_R01C03_Red.idat',
    #         f'test_data_folder/10003_R01C03_Grn.idat',
    #         f'test_data_folder/10003_R01C03_Red.idat.md5sum',
    #         f'test_data_folder/10003_R01C03_Grn.idat.md5sum',
    #     )
    #
    #     test_date = datetime.datetime(2021, 7, 12, 0, 0, 0, 0)
    #
    #     # create test records in GenomicGcDataFile
    #     with clock.FakeClock(test_date):
    #         for f in array_test_files_jh:
    #             # Set file type
    #             file_type = f.split('/')[-1].split("_")[-1] if "idat" in f.lower() else '.'.join(f.split('.')[1:])
    #
    #             test_file_dict = {
    #                 'file_path': f'{bucket_name}/{f}',
    #                 'gc_site_id': 'rdr',
    #                 'bucket_name': bucket_name,
    #                 'file_prefix': f'Genotyping_sample_raw_data',
    #                 'file_name': f,
    #                 'file_type': file_type,
    #                 'identifier_type': 'chipwellbarcode',
    #                 'identifier_value': "_".join(f.split('/')[1].split('_')[0:2]).split('.')[0],
    #             }
    #             self.data_generator.create_database_gc_data_file_record(**test_file_dict)
    #
    #     pipeline_id_config = {
    #         "aou_array": ["test_pipeline_id"]
    #     }
    #
    #     config.override_setting(config.GENOMIC_PIPELINE_IDS, pipeline_id_config)
    #
    #     # all data files for each metric record are present
    #     all_metrics = self.metrics_dao.get_all()
    #     for metric in all_metrics:
    #         if metric.id < len(all_metrics):
    #             metric.pipelineId = 'test_pipeline_id'
    #             self.metrics_dao.upsert(metric)
    #
    #     genomic_pipeline.reconcile_metrics_vs_array_data()  # run_id = 2
    #
    #     all_metrics = self.metrics_dao.get_all()
    #
    #     # reconcile should only happen for pipelineId == 'test_pipeline_id'
    #     for metric in all_metrics:
    #         if metric.pipelineId == 'test_pipeline_id':
    #             self.assertEqual(1, metric.vcfReceived)
    #             self.assertEqual(1, metric.vcfTbiReceived)
    #             self.assertEqual(1, metric.vcfMd5Received)
    #             self.assertEqual(1, metric.idatRedReceived)
    #             self.assertEqual(1, metric.idatGreenReceived)
    #             self.assertEqual(1, metric.idatRedMd5Received)
    #             self.assertEqual(1, metric.idatGreenMd5Received)
    #         else:
    #             self.assertEqual(0, metric.vcfReceived)
    #             self.assertEqual(0, metric.vcfTbiReceived)
    #             self.assertEqual(0, metric.vcfMd5Received)
    #             self.assertEqual(0, metric.idatRedReceived)
    #             self.assertEqual(0, metric.idatGreenReceived)
    #             self.assertEqual(0, metric.idatRedMd5Received)
    #             self.assertEqual(0, metric.idatGreenMd5Received)
    #
    #     run_obj = self.job_run_dao.get(2)
    #     self.assertEqual(GenomicSubProcessResult.SUCCESS, run_obj.runResult)
    #
    #     # clear current set metric records
    #     with self.metrics_dao.session() as session:
    #         session.query(GenomicGCValidationMetrics).delete()
    #
    #     # clear current config
    #     config.override_setting(config.GENOMIC_PIPELINE_IDS, {})
    #
    #     genomic_pipeline.ingest_genomic_centers_metrics_files()
    #
    #     all_metrics = self.metrics_dao.get_all()
    #     for metric in all_metrics:
    #         if metric.id < (len(all_metrics) * 2):
    #             metric.pipelineId = 'test_pipeline_id'
    #             self.metrics_dao.upsert(metric)
    #
    #     genomic_pipeline.reconcile_metrics_vs_array_data()
    #
    #     all_metrics = self.metrics_dao.get_all()
    #
    #     # reconcile should only happen for all regardless of pipelineId
    #     for metric in all_metrics:
    #         self.assertEqual(1, metric.vcfReceived)
    #         self.assertEqual(1, metric.vcfTbiReceived)
    #         self.assertEqual(1, metric.vcfMd5Received)
    #         self.assertEqual(1, metric.idatRedReceived)
    #         self.assertEqual(1, metric.idatGreenReceived)
    #         self.assertEqual(1, metric.idatRedMd5Received)
    #         self.assertEqual(1, metric.idatGreenMd5Received)
    #
    #     run_obj = self.job_run_dao.get(4)
    #     self.assertEqual(GenomicSubProcessResult.SUCCESS, run_obj.runResult)

    # DA-2934 Reconciliation process deprecated
    # def test_aw2_wgs_reconciliation_vs_wgs_data(self):
    #
    #     # Create the fake ingested data
    #     self._create_fake_datasets_for_gc_tests(3, genome_center='rdr', genomic_workflow_state=GenomicWorkflowState.AW1)
    #     bucket_name = _FAKE_GENOMIC_CENTER_BUCKET_A
    #     create_ingestion_test_file('RDR_AoU_SEQ_TestDataManifestWithFailure.csv',
    #                                      bucket_name,
    #                                      folder=config.getSetting(config.GENOMIC_AW2_SUBFOLDERS[0]))
    #
    #     self._update_test_sample_ids()
    #
    #     self._create_stored_samples([
    #         (2, 1002),
    #         (3, 1003)
    #     ])
    #
    #     genomic_pipeline.ingest_genomic_centers_metrics_files()  # run_id = 1
    #
    #     # Test the reconciliation process
    #     sequencing_test_files = (
    #         'test_data_folder/RDR_2_1002_10002_1.hard-filtered.vcf.gz',
    #         'test_data_folder/RDR_2_1002_10002_1.hard-filtered.vcf.gz.tbi',
    #         'test_data_folder/RDR_2_1002_10002_1.hard-filtered.vcf.gz.md5sum',
    #         'test_data_folder/RDR_2_1002_10002_v2.cram',
    #         'test_data_folder/RDR_2_1002_10002_v2.cram.md5sum',
    #     )
    #
    #     test_date = datetime.datetime(2021, 7, 12, 0, 0, 0, 0)
    #
    #     # create test records in GenomicGcDataFile
    #     with clock.FakeClock(test_date):
    #         for f in sequencing_test_files:
    #             if "cram" in f:
    #                 file_prefix = "CRAMs_CRAIs"
    #             else:
    #                 file_prefix = "SS_VCF_CLINICAL"
    #
    #             test_file_dict = {
    #                 'file_path': f'{bucket_name}/{f}',
    #                 'gc_site_id': 'rdr',
    #                 'bucket_name': bucket_name,
    #                 'file_prefix': f'Wgs_sample_raw_data/{file_prefix}',
    #                 'file_name': f,
    #                 'file_type': '.'.join(f.split('.')[1:]),
    #                 'identifier_type': 'sample_id',
    #                 'identifier_value': '1002',
    #             }
    #
    #             self.data_generator.create_database_gc_data_file_record(**test_file_dict)
    #
    #     genomic_pipeline.reconcile_metrics_vs_wgs_data()  # run_id = 2
    #
    #     gc_record = self.metrics_dao.get(1)
    #
    #     # Test the gc_metrics were updated with reconciliation data
    #     self.assertEqual(1, gc_record.hfVcfReceived)
    #     self.assertEqual(1, gc_record.hfVcfTbiReceived)
    #     self.assertEqual(1, gc_record.hfVcfMd5Received)
    #     self.assertEqual(0, gc_record.rawVcfReceived)
    #     self.assertEqual(0, gc_record.rawVcfTbiReceived)
    #     self.assertEqual(0, gc_record.rawVcfMd5Received)
    #     self.assertEqual(1, gc_record.cramReceived)
    #     self.assertEqual(1, gc_record.cramMd5Received)
    #     self.assertEqual(0, gc_record.craiReceived)
    #
    #     self.assertEqual(f"gs://{bucket_name}/{sequencing_test_files[0]}", gc_record.hfVcfPath)
    #     self.assertEqual(f"gs://{bucket_name}/{sequencing_test_files[1]}", gc_record.hfVcfTbiPath)
    #     self.assertEqual(f"gs://{bucket_name}/{sequencing_test_files[2]}", gc_record.hfVcfMd5Path)
    #     self.assertEqual(f"gs://{bucket_name}/{sequencing_test_files[3]}", gc_record.cramPath)
    #     self.assertEqual(f"gs://{bucket_name}/{sequencing_test_files[4]}", gc_record.cramMd5Path)
    #
    #     # Test member updated with job ID and state
    #     member = self.member_dao.get(2)
    #     self.assertEqual(GenomicWorkflowState.GC_DATA_FILES_MISSING, member.genomicWorkflowState)
    #     self.assertEqual('GC_DATA_FILES_MISSING', member.genomicWorkflowStateStr)
    #
    #     missing_file = self.missing_file_dao.get(1)
    #     self.assertEqual("rdr", missing_file.gc_site_id)
    #     self.assertEqual("cram.crai", missing_file.file_type)
    #     self.assertEqual(2, missing_file.run_id)
    #     self.assertEqual(1, missing_file.gc_validation_metric_id)
    #     self.assertEqual(0, missing_file.resolved)
    #
    #     processed_file = self.file_processed_dao.get(1)
    #     incident = self.incident_dao.get_by_source_file_id(processed_file.id)[0]
    #     self.assertTrue(incident.code == 'MISSING_FILES')
    #     self.assertTrue(incident.slack_notification == 1)
    #     self.assertTrue("cram.crai" in incident.message)
    #
    #     run_obj = self.job_run_dao.get(2)
    #
    #     self.assertEqual(GenomicSubProcessResult.SUCCESS, run_obj.runResult)

    # DA-2934 Reconciliation process deprecated
    # def test_aw2_wgs_reconciliation_vs_wgs_investigation_data(self):
    #
    #     # Create the fake ingested data
    #     self._create_fake_datasets_for_gc_tests(3, genome_center='rdr', genomic_workflow_state=GenomicWorkflowState.AW1,
    #                                             genome_type='aou_wgs_investigation',
    #                                             block_research=1,
    #                                             block_research_reason='Created from AW1 with investigation genome type',
    #                                             block_results=1,
    #                                             block_results_reason='Created from AW1 with investigation genome type'
    #                                             )
    #     bucket_name = _FAKE_GENOMIC_CENTER_BUCKET_A
    #     create_ingestion_test_file('RDR_AoU_SEQ_TestDataManifestWithFailure.csv',
    #                                      bucket_name,
    #                                      folder=config.getSetting(config.GENOMIC_AW2_SUBFOLDERS[0]))
    #
    #     self._update_test_sample_ids()
    #
    #     self._create_stored_samples([
    #         (2, 1002),
    #         (3, 1003)
    #     ])
    #
    #     genomic_pipeline.ingest_genomic_centers_metrics_files()  # run_id = 1
    #
    #     # Test the reconciliation process
    #     sequencing_test_files = (
    #         'test_data_folder/RDR_2_1002_10002_1.hard-filtered.vcf.gz',
    #         'test_data_folder/RDR_2_1002_10002_1.hard-filtered.vcf.gz.tbi',
    #         'test_data_folder/RDR_2_1002_10002_1.hard-filtered.vcf.gz.md5sum',
    #         'test_data_folder/RDR_2_1002_10002_v2.cram',
    #         'test_data_folder/RDR_2_1002_10002_v2.cram.md5sum',
    #     )
    #
    #     test_date = datetime.datetime(2021, 7, 12, 0, 0, 0, 0)
    #
    #     # create test records in GenomicGcDataFile
    #     with clock.FakeClock(test_date):
    #         for f in sequencing_test_files:
    #             if "cram" in f:
    #                 file_prefix = "CRAMs_CRAIs"
    #             else:
    #                 file_prefix = "SS_VCF_CLINICAL"
    #
    #             test_file_dict = {
    #                 'file_path': f'{bucket_name}/{f}',
    #                 'gc_site_id': 'rdr',
    #                 'bucket_name': bucket_name,
    #                 'file_prefix': f'Wgs_sample_raw_data/{file_prefix}',
    #                 'file_name': f,
    #                 'file_type': '.'.join(f.split('.')[1:]),
    #                 'identifier_type': 'sample_id',
    #                 'identifier_value': '1002',
    #             }
    #
    #             self.data_generator.create_database_gc_data_file_record(**test_file_dict)
    #
    #     genomic_pipeline.reconcile_metrics_vs_wgs_data()  # run_id = 2
    #
    #     gc_record = self.metrics_dao.get(1)
    #
    #     # Test the gc_metrics were updated with reconciliation data
    #     self.assertEqual(1, gc_record.hfVcfReceived)
    #     self.assertEqual(1, gc_record.hfVcfTbiReceived)
    #     self.assertEqual(1, gc_record.hfVcfMd5Received)
    #     self.assertEqual(0, gc_record.rawVcfReceived)
    #     self.assertEqual(0, gc_record.rawVcfTbiReceived)
    #     self.assertEqual(0, gc_record.rawVcfMd5Received)
    #     self.assertEqual(1, gc_record.cramReceived)
    #     self.assertEqual(1, gc_record.cramMd5Received)
    #     self.assertEqual(0, gc_record.craiReceived)
    #
    #     self.assertEqual(f"gs://{bucket_name}/{sequencing_test_files[0]}", gc_record.hfVcfPath)
    #     self.assertEqual(f"gs://{bucket_name}/{sequencing_test_files[1]}", gc_record.hfVcfTbiPath)
    #     self.assertEqual(f"gs://{bucket_name}/{sequencing_test_files[2]}", gc_record.hfVcfMd5Path)
    #     self.assertEqual(f"gs://{bucket_name}/{sequencing_test_files[3]}", gc_record.cramPath)
    #     self.assertEqual(f"gs://{bucket_name}/{sequencing_test_files[4]}", gc_record.cramMd5Path)
    #
    #     # Test member updated with job ID and state
    #     member = self.member_dao.get(2)
    #     self.assertEqual(GenomicWorkflowState.GC_DATA_FILES_MISSING, member.genomicWorkflowState)
    #     self.assertEqual('GC_DATA_FILES_MISSING', member.genomicWorkflowStateStr)
    #
    #     missing_file = self.missing_file_dao.get(1)
    #     self.assertEqual("rdr", missing_file.gc_site_id)
    #     self.assertEqual("cram.crai", missing_file.file_type)
    #     self.assertEqual(2, missing_file.run_id)
    #     self.assertEqual(1, missing_file.gc_validation_metric_id)
    #     self.assertEqual(0, missing_file.resolved)
    #
    #     processed_file = self.file_processed_dao.get(1)
    #     incident = self.incident_dao.get_by_source_file_id(processed_file.id)[0]
    #     self.assertTrue(incident.code == 'MISSING_FILES')
    #     self.assertTrue(incident.slack_notification == 1)
    #     self.assertTrue("cram.crai" in incident.message)
    #
    #     run_obj = self.job_run_dao.get(2)
    #
    #     self.assertEqual(GenomicSubProcessResult.SUCCESS, run_obj.runResult)

    # DA-2934 Missing files process deprecated
    # def test_reconciliation_wgs_data_config_missing_files_incident_creation(self):
    #     # Create the fake ingested data
    #     self._create_fake_datasets_for_gc_tests(3, genome_center='rdr', genomic_workflow_state=GenomicWorkflowState.AW1)
    #     bucket_name = _FAKE_GENOMIC_CENTER_BUCKET_A
    #     create_ingestion_test_file('RDR_AoU_SEQ_TestDataManifestWithFailure.csv',
    #                                      bucket_name,
    #                                      folder=config.getSetting(config.GENOMIC_AW2_SUBFOLDERS[0]))
    #
    #     self._update_test_sample_ids()
    #
    #     self._create_stored_samples([
    #         (2, 1002),
    #         (3, 1003)
    #     ])
    #
    #     genomic_pipeline.ingest_genomic_centers_metrics_files()  # run_id = 1
    #
    #     # Test the reconciliation process
    #     sequencing_test_files = (
    #         'test_data_folder/RDR_2_1002_10002_1.hard-filtered.vcf.gz.tbi',
    #         'test_data_folder/RDR_2_1002_10002_1.hard-filtered.vcf.gz.md5sum',
    #         'test_data_folder/RDR_2_1002_10002_v2.cram',
    #         'test_data_folder/RDR_2_1002_10002_v2.cram.md5sum',
    #     )
    #
    #     test_date = datetime.datetime(2021, 7, 12, 0, 0, 0, 0)
    #
    #     # create test records in GenomicGcDataFile
    #     with clock.FakeClock(test_date):
    #         for f in sequencing_test_files:
    #             if "cram" in f:
    #                 file_prefix = "CRAMs_CRAIs"
    #             else:
    #                 file_prefix = "SS_VCF_CLINICAL"
    #
    #             test_file_dict = {
    #                 'file_path': f'{bucket_name}/{f}',
    #                 'gc_site_id': 'rdr',
    #                 'bucket_name': bucket_name,
    #                 'file_prefix': f'Wgs_sample_raw_data/{file_prefix}',
    #                 'file_name': f,
    #                 'file_type': '.'.join(f.split('.')[1:]),
    #                 'identifier_type': 'sample_id',
    #                 'identifier_value': '1002',
    #             }
    #
    #             self.data_generator.create_database_gc_data_file_record(**test_file_dict)
    #
    #     missing_file_config = {
    #         "aou_wgs": ["hard-filtered.vcf.gz"]
    #     }
    #     config.override_setting(
    #         config.GENOMIC_SKIP_MISSING_FILETYPES,
    #         missing_file_config
    #     )
    #
    #     genomic_pipeline.reconcile_metrics_vs_wgs_data()  # run_id = 2
    #
    #     # should be 2 missing file records => ('hard-filtered.vcf.gz', 'cram.crai')
    #     missing_files = self.missing_file_dao.get_all()
    #     self.assertTrue(all(obj.gc_site_id == 'rdr' for obj in missing_files))
    #     self.assertTrue(all(obj.file_type in ("hard-filtered.vcf.gz", "cram.crai") for obj in missing_files))
    #
    #     processed_file = self.file_processed_dao.get(1)
    #     incident = self.incident_dao.get_by_source_file_id(processed_file.id)[0]
    #
    #     # should only be 1 missing file in incident => ('cram.crai')
    #     self.assertTrue(incident.code == 'MISSING_FILES')
    #     self.assertTrue("cram.crai" in incident.message)
    #     self.assertFalse("hard-filtered.vcf.gz" in incident.message)
    #
    #     run_obj = self.job_run_dao.get(2)
    #
    #     self.assertEqual(GenomicSubProcessResult.SUCCESS, run_obj.runResult)

    def test_new_participant_workflow(self):
        # Test for Cohort 3 workflow
        # create test samples
        test_biobank_ids = (100001, 100002, 100003, 100004, 100005, 100006, 100007, 100008, 100009)
        fake_datetime_old = datetime.datetime(2019, 12, 31, tzinfo=pytz.utc)
        fake_datetime_new = datetime.datetime(2020, 1, 5, tzinfo=pytz.utc)
        participant_origins = ['careevolution', 'example']

        # update the sites' States for the state test (NY or AZ)
        self._update_site_states()

        # setup sex_at_birth code for unittests
        female_code = self._setup_fake_sex_at_birth_codes('f')
        intersex_code = self._setup_fake_sex_at_birth_codes()

        # Setup race codes for unittests
        non_native_code = self._setup_fake_race_codes(native=False)
        native_code = self._setup_fake_race_codes(native=True)

        # Setup the biobank order backend
        for i, bid in enumerate(test_biobank_ids):
            p = self._make_participant(biobankId=bid)
            self._make_summary(p, sexId=intersex_code if bid == 100004 else female_code,
                               consentForStudyEnrollment=0 if bid == 100006 else 1,
                               sampleStatus1ED04=0,
                               sampleStatus1ED10=1 if bid == 100003 else 0,
                               sampleStatus1SAL2=0 if bid == 100005 else 1,
                               samplesToIsolateDNA=0,
                               race=Race.HISPANIC_LATINO_OR_SPANISH,
                               consentCohort=3,
                               participantOrigin=participant_origins[0 if i % 2 == 0 else 1])
            # Insert participant races
            race_answer = ParticipantRaceAnswers(
                participantId=p.participantId,
                codeId=native_code if bid == 100007 else non_native_code
            )
            self.race_dao.insert(race_answer)
            test_identifier = BiobankOrderIdentifier(
                    system=u'c',
                    value=u'e{}'.format(bid))

            # collected site
            if bid == 100002:
                # NY
                col_site = 1

            elif bid == 100008:
                # mail kit, NY
                col_site = None

            else:
                # not NY
                col_site = 2

            self._make_biobank_order(biobankOrderId=f'W{bid}',
                                     participantId=p.participantId,
                                     collectedSiteId=col_site,
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

            # Make a 1ED10 test for 100003 (in addition to the 1SAL2 test for that participant)
            if bid == 100003:
                self._make_stored_sample(
                    test='1ED10',
                    confirmed=fake_datetime_new,
                    created=fake_datetime_old,
                    biobankId=bid,
                    biobankOrderIdentifier=test_identifier.value,
                    biobankStoredSampleId='003_1ED10',
                )

            # Make Mail Kit NY participant
            if bid == 100008:
                code_to_insert = Code(
                    system="http://terminology.pmi-ops.org/CodeSystem/ppi",
                    value="State_NY",
                    display="c",
                    topic="d",
                    codeType=CodeType.ANSWER, mapped=True)

                ny_code = self.code_dao.insert(code_to_insert)

                mko = self.mk_dao.get(8)
                mko.stateId = ny_code.codeId

                self.mk_dao.update(mko)

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
        self.assertEqual(14, len(new_genomic_members))

        all_ps_origins = [self.summary_dao.get_by_participant_id(obj.participantId).participantOrigin
                       for obj in new_genomic_members]
        self.assertEqual(len(set(all_ps_origins)), len(participant_origins))

        all_member_origins = [obj.participantOrigin for obj in new_genomic_members]
        self.assertEqual(len(set(all_member_origins)), len(participant_origins))

        new_manifest_created = self.manifest_file_dao.get_all()
        self.assertIsNotNone(new_manifest_created)
        self.assertEqual(len(new_manifest_created), 1)

        new_manifest_created = new_manifest_created[0]
        self.assertEqual(new_manifest_created.recordCount, len(new_genomic_members))
        self.assertEqual(new_manifest_created.manifestTypeId, GenomicManifestTypes.AW0)

        self.assertTrue(all(obj.aw0ManifestFileId == new_manifest_created.id for obj in new_genomic_members))

        # Test GenomicMember's data
        # 100001 : Excluded, created before last run,
        # 100005 : Excluded, no DNA sample
        member_genome_types = {_member.biobankId: list() for _member in new_genomic_members}
        for member in new_genomic_members:
            member_genome_types[member.biobankId].append(member.genomeType)

            self.assertIsNotNone(member.participantOrigin)
            self.assertIsNotNone(member.created)
            self.assertIsNotNone(member.modified)

            if member.biobankId == 100002:
                # 100002 : Included, Valid
                self.assertEqual(1, member.nyFlag)
                self.assertEqual('100002', member.collectionTubeId)
                self.assertEqual('F', member.sexAtBirth)
                self.assertEqual(GenomicSetMemberStatus.VALID, member.validationStatus)
                self.assertEqual(GenomicWorkflowState.AW0.name, member.genomicWorkflowStateStr)
                self.assertEqual('N', member.ai_an)

            if member.biobankId == 100003:
                # 100003 : Included, Valid
                self.assertEqual(0, member.nyFlag)
                self.assertEqual('003_1ED10', member.collectionTubeId)
                self.assertEqual('F', member.sexAtBirth)
                self.assertEqual(GenomicSetMemberStatus.VALID, member.validationStatus)
                self.assertEqual(GenomicWorkflowState.AW0.name, member.genomicWorkflowStateStr)
                self.assertEqual('N', member.ai_an)

            if member.biobankId == 100004:
                # 100004 : Included, NA is now a valid SAB
                self.assertEqual(0, member.nyFlag)
                self.assertEqual('100004', member.collectionTubeId)
                self.assertEqual('NA', member.sexAtBirth)
                self.assertEqual(GenomicSetMemberStatus.VALID, member.validationStatus)
                self.assertEqual(GenomicWorkflowState.AW0.name, member.genomicWorkflowStateStr)
                self.assertEqual('N', member.ai_an)

            if member.biobankId == 100006:
                # 100006 : Included, Invalid consent
                self.assertEqual(0, member.nyFlag)
                self.assertEqual('100006', member.collectionTubeId)
                self.assertEqual('F', member.sexAtBirth)
                self.assertEqual(GenomicSetMemberStatus.INVALID, member.validationStatus)
                self.assertEqual(GenomicWorkflowState.AW0.name, member.genomicWorkflowStateStr)
                self.assertEqual('N', member.ai_an)

            if member.biobankId == 100007:
                # 100007 : Included, Invalid Indian/Native
                self.assertEqual(0, member.nyFlag)
                self.assertEqual('100007', member.collectionTubeId)
                self.assertEqual('F', member.sexAtBirth)
                self.assertEqual(GenomicSetMemberStatus.INVALID, member.validationStatus)
                self.assertEqual(GenomicWorkflowState.AW0.name, member.genomicWorkflowStateStr)
                self.assertEqual('Y', member.ai_an)

            if member.biobankId == 100008:
                # 100008 : Included, Invalid Indian/Native
                self.assertEqual(1, member.nyFlag)
                self.assertEqual('100008', member.collectionTubeId)
                self.assertEqual('F', member.sexAtBirth)
                self.assertEqual(GenomicSetMemberStatus.VALID, member.validationStatus)
                self.assertEqual(GenomicWorkflowState.AW0.name, member.genomicWorkflowStateStr)
                self.assertEqual('N', member.ai_an)

        for bid in member_genome_types.keys():
            self.assertIn('aou_array', member_genome_types[bid])
            self.assertIn('aou_wgs', member_genome_types[bid])

        # Test manifest file was created correctly
        bucket_name = config.getSetting(config.BIOBANK_SAMPLES_BUCKET_NAME)

        blob_name = self._find_latest_genomic_set_csv(bucket_name, _FAKE_BUCKET_FOLDER)
        with open_cloud_file(os.path.normpath(bucket_name + '/' + blob_name)) as csv_file:
            csv_reader = csv.DictReader(csv_file, delimiter=",")
            missing_cols = set(ExpectedCsvColumns.ALL) - set(csv_reader.fieldnames)
            self.assertEqual(0, len(missing_cols))
            rows = list(csv_reader)

            rows.sort(key=operator.itemgetter(ExpectedCsvColumns.BIOBANK_ID, ExpectedCsvColumns.GENOME_TYPE ))

            self.assertEqual("T100002", rows[0][ExpectedCsvColumns.BIOBANK_ID])
            self.assertEqual(100002, int(rows[0][ExpectedCsvColumns.COLLECTION_TUBE_ID]))
            self.assertEqual("F", rows[0][ExpectedCsvColumns.SEX_AT_BIRTH])
            self.assertEqual("Y", rows[0][ExpectedCsvColumns.NY_FLAG])
            self.assertEqual("Y", rows[0][ExpectedCsvColumns.VALIDATION_PASSED])
            self.assertEqual("N", rows[0][ExpectedCsvColumns.AI_AN])
            self.assertEqual("aou_array", rows[0][ExpectedCsvColumns.GENOME_TYPE])

            self.assertEqual("T100002", rows[1][ExpectedCsvColumns.BIOBANK_ID])
            self.assertEqual(100002, int(rows[1][ExpectedCsvColumns.COLLECTION_TUBE_ID]))
            self.assertEqual("F", rows[1][ExpectedCsvColumns.SEX_AT_BIRTH])
            self.assertEqual("Y", rows[1][ExpectedCsvColumns.NY_FLAG])
            self.assertEqual("Y", rows[1][ExpectedCsvColumns.VALIDATION_PASSED])
            self.assertEqual("N", rows[1][ExpectedCsvColumns.AI_AN])
            self.assertEqual("aou_wgs", rows[1][ExpectedCsvColumns.GENOME_TYPE])

            self.assertEqual("T100003", rows[2][ExpectedCsvColumns.BIOBANK_ID])
            self.assertEqual('003_1ED10', rows[2][ExpectedCsvColumns.COLLECTION_TUBE_ID])
            self.assertEqual("F", rows[2][ExpectedCsvColumns.SEX_AT_BIRTH])
            self.assertEqual("N", rows[2][ExpectedCsvColumns.NY_FLAG])
            self.assertEqual("Y", rows[2][ExpectedCsvColumns.VALIDATION_PASSED])
            self.assertEqual("N", rows[2][ExpectedCsvColumns.AI_AN])
            self.assertEqual("aou_array", rows[2][ExpectedCsvColumns.GENOME_TYPE])

            self.assertEqual("T100003", rows[3][ExpectedCsvColumns.BIOBANK_ID])
            self.assertEqual('003_1ED10', rows[3][ExpectedCsvColumns.COLLECTION_TUBE_ID])
            self.assertEqual("F", rows[3][ExpectedCsvColumns.SEX_AT_BIRTH])
            self.assertEqual("N", rows[3][ExpectedCsvColumns.NY_FLAG])
            self.assertEqual("Y", rows[3][ExpectedCsvColumns.VALIDATION_PASSED])
            self.assertEqual("N", rows[3][ExpectedCsvColumns.AI_AN])
            self.assertEqual("aou_wgs", rows[3][ExpectedCsvColumns.GENOME_TYPE])

            self.assertEqual("T100004", rows[4][ExpectedCsvColumns.BIOBANK_ID])
            self.assertEqual(100004, int(rows[4][ExpectedCsvColumns.COLLECTION_TUBE_ID]))
            self.assertEqual("NA", rows[4][ExpectedCsvColumns.SEX_AT_BIRTH])
            self.assertEqual("N", rows[4][ExpectedCsvColumns.NY_FLAG])
            self.assertEqual("Y", rows[4][ExpectedCsvColumns.VALIDATION_PASSED])
            self.assertEqual("N", rows[4][ExpectedCsvColumns.AI_AN])
            self.assertEqual("aou_array", rows[4][ExpectedCsvColumns.GENOME_TYPE])

            self.assertEqual("T100004", rows[5][ExpectedCsvColumns.BIOBANK_ID])
            self.assertEqual(100004, int(rows[5][ExpectedCsvColumns.COLLECTION_TUBE_ID]))
            self.assertEqual("NA", rows[5][ExpectedCsvColumns.SEX_AT_BIRTH])
            self.assertEqual("N", rows[5][ExpectedCsvColumns.NY_FLAG])
            self.assertEqual("Y", rows[5][ExpectedCsvColumns.VALIDATION_PASSED])
            self.assertEqual("N", rows[5][ExpectedCsvColumns.AI_AN])
            self.assertEqual("aou_wgs", rows[5][ExpectedCsvColumns.GENOME_TYPE])

            self.assertEqual("T100006", rows[6][ExpectedCsvColumns.BIOBANK_ID])
            self.assertEqual(100006, int(rows[6][ExpectedCsvColumns.COLLECTION_TUBE_ID]))
            self.assertEqual("F", rows[6][ExpectedCsvColumns.SEX_AT_BIRTH])
            self.assertEqual("N", rows[6][ExpectedCsvColumns.NY_FLAG])
            self.assertEqual("N", rows[6][ExpectedCsvColumns.VALIDATION_PASSED])
            self.assertEqual("N", rows[6][ExpectedCsvColumns.AI_AN])
            self.assertEqual("aou_array", rows[6][ExpectedCsvColumns.GENOME_TYPE])

            self.assertEqual("T100006", rows[7][ExpectedCsvColumns.BIOBANK_ID])
            self.assertEqual(100006, int(rows[7][ExpectedCsvColumns.COLLECTION_TUBE_ID]))
            self.assertEqual("F", rows[7][ExpectedCsvColumns.SEX_AT_BIRTH])
            self.assertEqual("N", rows[7][ExpectedCsvColumns.NY_FLAG])
            self.assertEqual("N", rows[7][ExpectedCsvColumns.VALIDATION_PASSED])
            self.assertEqual("N", rows[7][ExpectedCsvColumns.AI_AN])
            self.assertEqual("aou_wgs", rows[7][ExpectedCsvColumns.GENOME_TYPE])

            self.assertEqual("T100007", rows[8][ExpectedCsvColumns.BIOBANK_ID])
            self.assertEqual(100007, int(rows[8][ExpectedCsvColumns.COLLECTION_TUBE_ID]))
            self.assertEqual("F", rows[8][ExpectedCsvColumns.SEX_AT_BIRTH])
            self.assertEqual("N", rows[8][ExpectedCsvColumns.NY_FLAG])
            self.assertEqual("Y", rows[8][ExpectedCsvColumns.VALIDATION_PASSED])
            self.assertEqual("Y", rows[8][ExpectedCsvColumns.AI_AN])
            self.assertEqual("aou_array", rows[8][ExpectedCsvColumns.GENOME_TYPE])

            self.assertEqual("T100007", rows[9][ExpectedCsvColumns.BIOBANK_ID])
            self.assertEqual(100007, int(rows[9][ExpectedCsvColumns.COLLECTION_TUBE_ID]))
            self.assertEqual("F", rows[9][ExpectedCsvColumns.SEX_AT_BIRTH])
            self.assertEqual("N", rows[9][ExpectedCsvColumns.NY_FLAG])
            self.assertEqual("Y", rows[9][ExpectedCsvColumns.VALIDATION_PASSED])
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
        self.assertEqual(8, len(new_genomic_members))

        new_manifest_created = self.manifest_file_dao.get_all()
        self.assertIsNotNone(new_manifest_created)
        self.assertEqual(len(new_manifest_created), 1)

        new_manifest_created = new_manifest_created[0]
        self.assertEqual(new_manifest_created.recordCount, len(new_genomic_members))
        self.assertEqual(new_manifest_created.manifestTypeId, GenomicManifestTypes.AW0)

        self.assertTrue(all(obj.aw0ManifestFileId == new_manifest_created.id for obj in new_genomic_members))

        # Test member data
        member_genome_types = {_member.biobankId: list() for _member in new_genomic_members}
        for member in new_genomic_members:
            member_genome_types[member.biobankId].append(member.genomeType)

            self.assertIsNotNone(member.created)
            self.assertIsNotNone(member.modified)

            if member.biobankId == '100001':
                # 100001 : Included, Valid
                self.assertEqual(0, member.nyFlag)
                self.assertEqual('10000101', member.collectionTubeId)
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

            blob_name = self._find_latest_genomic_set_csv(bucket_name, _FAKE_BUCKET_FOLDER)
            with open_cloud_file(os.path.normpath(bucket_name + '/' + blob_name)) as csv_file:
                csv_reader = csv.DictReader(csv_file, delimiter=",")
                missing_cols = set(ExpectedCsvColumns.ALL) - set(csv_reader.fieldnames)
                self.assertEqual(0, len(missing_cols))
                rows = list(csv_reader)

                self.assertEqual("T100001", rows[0][ExpectedCsvColumns.BIOBANK_ID])
                self.assertEqual(10000101, int(rows[0][ExpectedCsvColumns.COLLECTION_TUBE_ID]))
                self.assertEqual("F", rows[0][ExpectedCsvColumns.SEX_AT_BIRTH])
                self.assertEqual("N", rows[0][ExpectedCsvColumns.NY_FLAG])
                self.assertEqual("Y", rows[0][ExpectedCsvColumns.VALIDATION_PASSED])
                self.assertEqual("N", rows[0][ExpectedCsvColumns.AI_AN])
                self.assertEqual("aou_array", rows[0][ExpectedCsvColumns.GENOME_TYPE])

                self.assertEqual("T100001", rows[1][ExpectedCsvColumns.BIOBANK_ID])
                self.assertEqual(10000101, int(rows[1][ExpectedCsvColumns.COLLECTION_TUBE_ID]))
                self.assertEqual("F", rows[1][ExpectedCsvColumns.SEX_AT_BIRTH])
                self.assertEqual("N", rows[1][ExpectedCsvColumns.NY_FLAG])
                self.assertEqual("Y", rows[1][ExpectedCsvColumns.VALIDATION_PASSED])
                self.assertEqual("N", rows[1][ExpectedCsvColumns.AI_AN])
                self.assertEqual("aou_wgs", rows[1][ExpectedCsvColumns.GENOME_TYPE])

                self.assertEqual("T100002", rows[2][ExpectedCsvColumns.BIOBANK_ID])
                self.assertEqual(10000201, int(rows[2][ExpectedCsvColumns.COLLECTION_TUBE_ID]))
                self.assertEqual("F", rows[2][ExpectedCsvColumns.SEX_AT_BIRTH])
                self.assertEqual("Y", rows[2][ExpectedCsvColumns.NY_FLAG])
                self.assertEqual("Y", rows[2][ExpectedCsvColumns.VALIDATION_PASSED])
                self.assertEqual("N", rows[2][ExpectedCsvColumns.AI_AN])
                self.assertEqual("aou_array", rows[2][ExpectedCsvColumns.GENOME_TYPE])

                self.assertEqual("T100002", rows[3][ExpectedCsvColumns.BIOBANK_ID])
                self.assertEqual(10000201, int(rows[3][ExpectedCsvColumns.COLLECTION_TUBE_ID]))
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
        self.assertEqual(8, len(new_genomic_members))

        new_manifest_created = self.manifest_file_dao.get_all()
        self.assertIsNotNone(new_manifest_created)
        self.assertEqual(len(new_manifest_created), 1)

        new_manifest_created = new_manifest_created[0]
        self.assertEqual(new_manifest_created.recordCount, len(new_genomic_members))
        self.assertEqual(new_manifest_created.manifestTypeId, GenomicManifestTypes.AW0)

        self.assertTrue(all(obj.aw0ManifestFileId == new_manifest_created.id for obj in new_genomic_members))

        # Test member data
        member_genome_types = {_member.biobankId: list() for _member in new_genomic_members}
        for member in new_genomic_members:
            member_genome_types[member.biobankId].append(member.genomeType)

            self.assertIsNotNone(member.created)
            self.assertIsNotNone(member.modified)

            if member.biobankId == '100001':
                # 100001 : Included, Valid
                self.assertEqual(0, member.nyFlag)
                self.assertEqual('10000101', member.collectionTubeId)
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

            blob_name = self._find_latest_genomic_set_csv(bucket_name, _FAKE_BUCKET_FOLDER)
            with open_cloud_file(os.path.normpath(bucket_name + '/' + blob_name)) as csv_file:
                csv_reader = csv.DictReader(csv_file, delimiter=",")
                missing_cols = set(ExpectedCsvColumns.ALL) - set(csv_reader.fieldnames)
                self.assertEqual(0, len(missing_cols))
                rows = list(csv_reader)

                self.assertEqual("T100001", rows[0][ExpectedCsvColumns.BIOBANK_ID])
                self.assertEqual(10000101, int(rows[0][ExpectedCsvColumns.COLLECTION_TUBE_ID]))
                self.assertEqual("F", rows[0][ExpectedCsvColumns.SEX_AT_BIRTH])
                self.assertEqual("N", rows[0][ExpectedCsvColumns.NY_FLAG])
                self.assertEqual("Y", rows[0][ExpectedCsvColumns.VALIDATION_PASSED])
                self.assertEqual("N", rows[0][ExpectedCsvColumns.AI_AN])
                self.assertEqual("aou_array", rows[0][ExpectedCsvColumns.GENOME_TYPE])

                self.assertEqual("T100001", rows[1][ExpectedCsvColumns.BIOBANK_ID])
                self.assertEqual(10000101, int(rows[1][ExpectedCsvColumns.COLLECTION_TUBE_ID]))
                self.assertEqual("F", rows[1][ExpectedCsvColumns.SEX_AT_BIRTH])
                self.assertEqual("N", rows[1][ExpectedCsvColumns.NY_FLAG])
                self.assertEqual("Y", rows[1][ExpectedCsvColumns.VALIDATION_PASSED])
                self.assertEqual("N", rows[1][ExpectedCsvColumns.AI_AN])
                self.assertEqual("aou_wgs", rows[1][ExpectedCsvColumns.GENOME_TYPE])

                self.assertEqual("T100002", rows[2][ExpectedCsvColumns.BIOBANK_ID])
                self.assertEqual(10000201, int(rows[2][ExpectedCsvColumns.COLLECTION_TUBE_ID]))
                self.assertEqual("F", rows[2][ExpectedCsvColumns.SEX_AT_BIRTH])
                self.assertEqual("Y", rows[2][ExpectedCsvColumns.NY_FLAG])
                self.assertEqual("Y", rows[2][ExpectedCsvColumns.VALIDATION_PASSED])
                self.assertEqual("N", rows[2][ExpectedCsvColumns.AI_AN])
                self.assertEqual("aou_array", rows[2][ExpectedCsvColumns.GENOME_TYPE])

                self.assertEqual("T100002", rows[3][ExpectedCsvColumns.BIOBANK_ID])
                self.assertEqual(10000201, int(rows[3][ExpectedCsvColumns.COLLECTION_TUBE_ID]))
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

            col_date_1 = datetime.datetime(2018, 6, 30, 0, 0, 0, 0)
            col_date_2 = datetime.datetime(2019, 6, 30, 0, 0, 0, 0)

            if bid == 100001:
                # ED04 with bad status and SAL2 -> Use SAL2
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
                    'status': SampleStatus.SAMPLE_NOT_RECEIVED
                }

                sample_args2 = {
                    'test': '1SAL2',
                    'confirmed': fake_datetime_new,
                    'created': fake_datetime_old,
                    'biobankId': bid,
                    'biobankOrderIdentifier': test_identifier2.value,
                    'biobankStoredSampleId': 10000102,
                    'status': SampleStatus.RECEIVED
                }

                with clock.FakeClock(insert_dtm):
                    self._make_stored_sample(**sample_args1)
                    self._make_stored_sample(**sample_args2)

            elif bid == 100002:
                # ED10 and SAL2 -> Use ED10
                sample_1 = self._make_ordered_sample(_test="1ED10", _collected=col_date_1)
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
                    'test': '1ED10',
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

    def test_gc_manifest_ingestion_workflow(self):
        self._create_fake_datasets_for_gc_tests(3, arr_override=True,
                                                array_participants=range(1, 4),
                                                genomic_workflow_state=GenomicWorkflowState.AW0)

        self._insert_control_sample_genomic_set_member(sample_id=30003, genome_type="aou_array")

        # Add extra sample for collection_tube_id test
        sample_args = {
            'test': '1ED10',
            'confirmed': clock.CLOCK.now(),
            'created': clock.CLOCK.now(),
            'biobankId': 2,
            'biobankOrderIdentifier': f'e2',
            'biobankStoredSampleId': 100002,
        }

        self._make_stored_sample(**sample_args)

        # Setup Test file
        gc_manifest_file = open_genomic_set_file("Genomic-GC-Manifest-Workflow-Test-3.csv")

        gc_manifest_filename = "RDR_AoU_GEN_PKG-1908-218051.csv"
        test_date = datetime.datetime(2020, 10, 13, 0, 0, 0, 0)
        pytz.timezone('US/Central').localize(test_date)

        with clock.FakeClock(test_date):
            write_cloud_csv(
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
                self.assertEqual(1, member.aw1FileProcessedId)

            if member.id == 2:
                self.assertEqual("100002", member.collectionTubeId)

            if member.id == 3:
                self.assertNotEqual(1, member.reconcileGCManifestJobRunId)

        # Test file processing queue
        files_processed = self.file_processed_dao.get_all()
        self.assertEqual(len(files_processed), 1)
        self.assertEqual(test_date.astimezone(pytz.utc), pytz.utc.localize(files_processed[0].uploadDate))

        # Test the end-to-end result code
        self.assertEqual(GenomicSubProcessResult.SUCCESS, self.job_run_dao.get(1).runResult)

        # Test that swapping out collection tube ids records the old id as contaminated
        self.session.query(GenomicSampleContamination).filter(
            GenomicSampleContamination.sampleId == 2,  # Participant 2's collection tube id is replaced with 100002
            GenomicSampleContamination.failedInJob == GenomicJob.AW1_MANIFEST
        ).one()

    def test_ingest_specific_aw1_manifest(self):
        self._create_fake_datasets_for_gc_tests(3,
                                                arr_override=True,
                                                array_participants=range(1, 4),
                                                genomic_workflow_state=GenomicWorkflowState.AW0
                                                )

        self._insert_control_sample_genomic_set_member(sample_id=30003, genome_type="aou_array")

        # set site 2 to diversion pouch
        self._update_site_type_div_pouch(2)
        self._update_biobank_order_collected_site(1, 2)

        # Setup Test file
        gc_manifest_file = open_genomic_set_file("Genomic-GC-Manifest-Workflow-Test-6.csv")
        gc_manifest_filename = "RDR_AoU_GEN_PKG-1908-218051.csv"

        test_date = datetime.datetime(2020, 10, 13, 0, 0, 0, 0)
        pytz.timezone('US/Central').localize(test_date)

        bucket_name = _FAKE_GENOMIC_CENTER_BUCKET_A

        with clock.FakeClock(test_date):
            write_cloud_csv(
                gc_manifest_filename,
                gc_manifest_file,
                bucket=bucket_name,
                folder=_FAKE_GENOTYPING_FOLDER,
            )

        # Get   subfolder, and filename from argument
        file_name = _FAKE_GENOTYPING_FOLDER + '/' + gc_manifest_filename

        # Set up file/JSON
        task_data = {
            "job": GenomicJob.AW1_MANIFEST,
            "bucket": bucket_name,
            "file_data": {
                "create_feedback_record": True,
                "upload_date": "2020-10-13 00:00:00",
                "manifest_type": GenomicManifestTypes.AW1,
                "file_path": f"{bucket_name}/{file_name}"
            }
        }

        # Call pipeline function
        genomic_pipeline.execute_genomic_manifest_file_pipeline(task_data)  # job_id 1 & 2

        # Test the data was ingested OK
        for member in self.member_dao.get_all():
            if member.id in [1, 2]:
                self.assertEqual(2, member.reconcileGCManifestJobRunId)
                self.assertEqual('rdr', member.gcSiteId)
                self.assertEqual("aou_array", member.gcManifestTestName)

                # Test div pouch flag updated correctly
                if member.participantId == 1:
                    self.assertEqual(member.diversionPouchSiteFlag, 1)
                else:
                    self.assertEqual(member.diversionPouchSiteFlag, 0)

        files_processed = self.file_processed_dao.get_all()
        self.assertEqual(test_date.astimezone(pytz.utc), pytz.utc.localize(files_processed[0].uploadDate))

        # Check record count for manifest record
        manifest_record = self.manifest_file_dao.get(1)
        self.assertEqual(file_name.split('/')[1], manifest_record.fileName)
        self.assertEqual(2, manifest_record.recordCount)

        # Test the end result code is recorded
        self.assertEqual(GenomicSubProcessResult.SUCCESS, self.job_run_dao.get(2).runResult)

    def test_ingest_investigation_aw1_manifest(self):
        self._create_fake_datasets_for_gc_tests(3,
                                                genome_type='aou_array',
                                                array_participants=range(1, 4),
                                                genomic_workflow_state=GenomicWorkflowState.AW0
                                                )

        for m in self.member_dao.get_all():
            m.collectionTubeId = f'replated_{m.collectionTubeId}'
            self.member_dao.update(m)

        # Setup Array Test file
        gc_manifest_file = open_genomic_set_file("AW1-array-investigation-test.csv")
        gc_manifest_filename = "RDR_AoU_GEN_PKG-1908-218051.csv"

        test_date = datetime.datetime(2020, 10, 13, 0, 0, 0, 0)
        pytz.timezone('US/Central').localize(test_date)

        bucket_name = _FAKE_GENOMIC_CENTER_BUCKET_A

        with clock.FakeClock(test_date):
            write_cloud_csv(
                gc_manifest_filename,
                gc_manifest_file,
                bucket=bucket_name,
                folder=_FAKE_GENOTYPING_FOLDER,
            )

        # Get   subfolder, and filename from argument
        file_name = _FAKE_GENOTYPING_FOLDER + '/' + gc_manifest_filename

        # Set up file/JSON
        task_data = {
            "job": GenomicJob.AW1_MANIFEST,
            "bucket": bucket_name,
            "file_data": {
                "create_feedback_record": True,
                "upload_date": "2020-10-13 00:00:00",
                "manifest_type": GenomicManifestTypes.AW1,
                "file_path": f"{bucket_name}/{file_name}"
            }
        }

        # Call pipeline function
        genomic_pipeline.execute_genomic_manifest_file_pipeline(task_data)  # job_id 1 & 2

        # Test the data was ingested OK
        members = [m for m in self.member_dao.get_all() if m.id in [4, 5]]
        self.assertEqual(2, len(members))

        for member in members:
            self.assertEqual(2, member.reconcileGCManifestJobRunId)
            self.assertEqual('rdr', member.gcSiteId)
            self.assertEqual("aou_array_investigation", member.gcManifestTestName)
            self.assertEqual("aou_array_investigation", member.genomeType)
            self.assertEqual(GenomicWorkflowState.AW1, member.genomicWorkflowState)
            self.assertEqual(1, member.blockResearch)
            self.assertEqual("Created from AW1 with investigation genome type.", member.blockResearchReason)
            self.assertEqual(1, member.blockResults)
            self.assertEqual("Created from AW1 with investigation genome type.", member.blockResultsReason)
            self.assertEqual("F", member.sexAtBirth)

        # Test the end result code is recorded
        self.assertEqual(GenomicSubProcessResult.SUCCESS, self.job_run_dao.get(2).runResult)

    def test_ingest_aw1_with_replated_samples(self):
        self._create_fake_datasets_for_gc_tests(3,
                                                arr_override=True,
                                                array_participants=range(1, 4),
                                                genomic_workflow_state=GenomicWorkflowState.EXTRACT_REQUESTED,
                                                )
        test_members = self.member_dao.get_all()
        for test_member in test_members:
            test_member.collectionTubeId = None
            self.member_dao.update(test_member)

        # Setup Test file
        gc_manifest_file = open_genomic_set_file("Genomic-GC-Manifest-Workflow-Test-4.csv")
        gc_manifest_filename = "RDR_AoU_GEN_PKG-1908-218051.csv"

        test_date = datetime.datetime(2020, 10, 13, 0, 0, 0, 0)
        pytz.timezone('US/Central').localize(test_date)

        bucket_name = _FAKE_GENOMIC_CENTER_BUCKET_A

        with clock.FakeClock(test_date):
            write_cloud_csv(
                gc_manifest_filename,
                gc_manifest_file,
                bucket=bucket_name,
                folder=_FAKE_GENOTYPING_FOLDER,
            )

        # Get   subfolder, and filename from argument
        file_name = _FAKE_GENOTYPING_FOLDER + '/' + gc_manifest_filename

        # Set up file/JSON
        task_data = {
            "job": GenomicJob.AW1_MANIFEST,
            "bucket": bucket_name,
            "file_data": {
                "create_feedback_record": True,
                "upload_date": "2020-10-13 00:00:00",
                "manifest_type": GenomicManifestTypes.AW1,
                "file_path": f"{bucket_name}/{file_name}"
            }
        }

        # Call pipeline function
        genomic_pipeline.execute_genomic_manifest_file_pipeline(task_data)  # job_id 1 & 2

        members = self.member_dao.get_members_with_non_null_sample_ids()

        self.assertEqual(3, len(members))
        for member in members:
            self.assertEqual(GenomicWorkflowState.AW1, member.genomicWorkflowState)

    def test_control_sample_insert(self):
        # Create member record for base control sample
        self._insert_control_sample_genomic_set_member(sample_id=10001, genome_type="aou_wgs")

        # Ingest an AW1 with control sample as parent_sample_id
        # Setup Test file
        gc_manifest_file = open_genomic_set_file("AW1-Control-Sample-Test.csv")
        fake_filenames = ("RDR_AoU_GEN_PKG-1908-218051.csv", "JH_AoU_GEN_PKG-1908-218051.csv")

        for gc_manifest_filename in fake_filenames:
            write_cloud_csv(
                gc_manifest_filename,
                gc_manifest_file,
                bucket=_FAKE_GENOMIC_CENTER_BUCKET_A,
                folder=_FAKE_GENOTYPING_FOLDER,
            )

        # Get bucket, subfolder, and filename from argument
        bucket_name = _FAKE_GENOMIC_CENTER_BUCKET_A
        file_name = _FAKE_GENOTYPING_FOLDER + '/' + fake_filenames[0]

        # Set up file/JSON
        task_data = {
            "job": GenomicJob.AW1_MANIFEST,
            "bucket": bucket_name,
            "file_data": {
                "create_feedback_record": True,
                "upload_date": "2020-10-13 00:00:00",
                "manifest_type": GenomicManifestTypes.AW1,
                "file_path": f"{bucket_name}/{file_name}"
            }
        }

        # Call pipeline function
        genomic_pipeline.execute_genomic_manifest_file_pipeline(task_data)  # job_id 1 & 2

        # Test member was created
        new_member = self.member_dao.get_member_from_collection_tube(1, 'aou_wgs')

        self.assertEqual('HG-002', new_member.biobankId)

        # Test new control sample inserted again for different GC
        file_name = _FAKE_GENOTYPING_FOLDER + '/' + fake_filenames[1]
        task_data['file_data']['file_path'] = f"{bucket_name}/{file_name}"

        # Call pipeline function twice
        genomic_pipeline.execute_genomic_manifest_file_pipeline(task_data)  # job_id 3 & 4

        # No new GenomicSetMembers should be inserted in this run
        genomic_pipeline.execute_genomic_manifest_file_pipeline(task_data)  # job_id 5 & 6

        members = self.member_dao.get_all()
        members.sort(key=lambda x: x.id)

        self.assertEqual(3, len(members))
        self.assertEqual('rdr', members[1].gcSiteId)
        self.assertEqual('jh', members[2].gcSiteId)

    def test_aw1f_ingestion_workflow(self):
        # Setup test data: 1 aou_array, 1 aou_wgs
        self._create_fake_datasets_for_gc_tests(2, arr_override=True,
                                                array_participants=[1],
                                                genomic_workflow_state=GenomicWorkflowState.AW0)
        # Setup Test file
        gc_manifest_file = open_genomic_set_file("Genomic-GC-Manifest-Workflow-Test-2.csv")

        gc_manifest_filename = "RDR_AoU_GEN_PKG-1908-218051.csv"

        test_date = datetime.datetime(2020, 10, 13, 0, 0, 0, 0)
        pytz.timezone('US/Central').localize(test_date)

        with clock.FakeClock(test_date):
            write_cloud_csv(
                gc_manifest_filename,
                gc_manifest_file,
                bucket=_FAKE_GENOMIC_CENTER_BUCKET_A,
                folder=_FAKE_GENOTYPING_FOLDER,
            )

        # Get bucket, subfolder, and filename from argument
        bucket_name = _FAKE_GENOMIC_CENTER_BUCKET_A
        file_name = _FAKE_GENOTYPING_FOLDER + '/' + gc_manifest_filename

        # Set up file/JSON
        task_data = {
            "job": GenomicJob.AW1_MANIFEST,
            "bucket": bucket_name,
            "file_data": {
                "create_feedback_record": True,
                "upload_date": "2020-10-13 00:00:00",
                "manifest_type": GenomicManifestTypes.AW1,
                "file_path": f"{bucket_name}/{file_name}"
            }
        }

        # Call pipeline function
        genomic_pipeline.execute_genomic_manifest_file_pipeline(task_data)  # job_id 1 & 2

        # Setup Test AW1F file
        gc_manifest_file = open_genomic_set_file("Genomic-AW1F-Workflow-Test-1.csv")
        gc_manifest_filename = "RDR_AoU_GEN_PKG-1908-218051_FAILURE.csv"
        write_cloud_csv(
            gc_manifest_filename,
            gc_manifest_file,
            bucket=_FAKE_GENOMIC_CENTER_BUCKET_A,
            folder=_FAKE_FAILURE_FOLDER,
        )
        failure_file_name = _FAKE_FAILURE_FOLDER + '/' + gc_manifest_filename

        # Set up file/JSON
        task_data = {
            "job": GenomicJob.AW1F_MANIFEST,
            "bucket": bucket_name,
            "file_data": {
                "create_feedback_record": False,
                "upload_date": "2020-10-13 00:00:00",
                "manifest_type": GenomicManifestTypes.AW1F,
                "file_path": f"{bucket_name}/{failure_file_name}"
            }
        }

        # Ingest AW1F
        genomic_pipeline.execute_genomic_manifest_file_pipeline(task_data)  # job_id 3 & 4

        # Test db updated
        members = sorted(self.member_dao.get_all(), key=lambda x: x.id)
        self.assertEqual(members[1].gcManifestFailureMode, 'damaged')
        self.assertEqual(members[1].gcManifestFailureDescription, 'Arrived and damaged')
        self.assertEqual(members[1].genomicWorkflowState, GenomicWorkflowState.AW1F_POST)
        self.assertEqual(members[1].aw1FileProcessedId, 1)

        # Test file processing queue
        files_processed = self.file_processed_dao.get_all()
        self.assertEqual(len(files_processed), 2)

        # Test AW1F manifest file record inserted correctly
        aw1f_mf = self.manifest_file_dao.get(2)
        self.assertEqual(GenomicManifestTypes.AW1F, aw1f_mf.manifestTypeId)

        # Test AW1 record count
        aw1_mf = self.manifest_file_dao.get(1)
        self.assertEqual(2, aw1_mf.recordCount)

        # Test the end-to-end result code
        self.assertEqual(GenomicSubProcessResult.SUCCESS, self.job_run_dao.get(2).runResult)

    def test_gem_a1_manifest_end_to_end(self):
        # Need GC Manifest for source query : run_id = 1
        self.job_run_dao.insert(GenomicJobRun(jobId=GenomicJob.AW1_MANIFEST,
                                              startTime=clock.CLOCK.now(),
                                              runStatus=GenomicSubProcessStatus.COMPLETED,
                                              runResult=GenomicSubProcessResult.SUCCESS))

        self._create_fake_datasets_for_gc_tests(4, arr_override=True,
                                                array_participants=range(1, 5),
                                                recon_gc_man_id=1,
                                                genome_center='jh',
                                                genomic_workflow_state=GenomicWorkflowState.AW1)

        # Set starting RoR and Primary authored
        ps_list = self.summary_dao.get_all()
        ror_start = datetime.datetime(2020, 7, 11, 0, 0, 0, 0)
        for p in ps_list:
            p.consentForGenomicsRORAuthored = ror_start
            if p.participantId == 2:
                p.consentForStudyEnrollmentAuthored = ror_start
                p.consentForStudyEnrollment = QuestionnaireStatus.SUBMITTED
            self.summary_dao.update(p)

        # exclude based on block result in GEM A1 query
        bib_member = list(filter(lambda x: x.biobankId == '3', self.member_dao.get_all()))[0]
        bib_member.blockResults = 1
        bib_member.blockResultsReason = 'test_reason'
        self.member_dao.update(bib_member)

        bucket_name = _FAKE_GENOMIC_CENTER_BUCKET_BAYLOR

        create_ingestion_test_file('RDR_AoU_GEN_TestDataManifest_2.csv',
                                   bucket_name,
                                   folder=config.getSetting(config.GENOMIC_AW2_SUBFOLDERS[1]))

        self._update_test_sample_ids()

        self._create_stored_samples([
            (1, 1001),
            (2, 1002),
            (3, 1003)
        ])

        genomic_pipeline.ingest_genomic_centers_metrics_files()  # run_id = 2

        # Test sequencing file (required for GEM)
        sequencing_test_files = (
            f'test_data_folder/10001_R01C01.vcf.gz',
            f'test_data_folder/10001_R01C01.vcf.gz.tbi',
            f'test_data_folder/10001_R01C01.vcf.gz.md5sum',
            f'test_data_folder/10001_R01C01_Red.idat',
            f'test_data_folder/10001_R01C01_Grn.idat',
            f'test_data_folder/10001_R01C01_Red.idat.md5sum',
            f'test_data_folder/10001_R01C01_Grn.idat.md5sum',
            f'test_data_folder/10002_R01C02.vcf.gz',
            f'test_data_folder/10002_R01C02.vcf.gz.tbi',
            f'test_data_folder/10002_R01C02.vcf.gz.md5sum',
            f'test_data_folder/10002_R01C02_Red.idat',
            f'test_data_folder/10002_R01C02_Grn.idat',
            f'test_data_folder/10002_R01C02_Red.idat.md5sum',
            f'test_data_folder/10002_R01C02_Grn.idat.md5sum',
            f'test_data_folder/10003_R01C03.vcf.gz',
            f'test_data_folder/10003_R01C03.vcf.gz.tbi',
            f'test_data_folder/10003_R01C03.vcf.gz.md5sum',
            f'test_data_folder/10003_R01C03_Red.idat',
            f'test_data_folder/10003_R01C03_Grn.idat',
            f'test_data_folder/10003_R01C03_Red.idat.md5sum',
            f'test_data_folder/10003_R01C03_Grn.idat.md5sum',
        )

        fake_dt = datetime.datetime(2020, 8, 3, 0, 0, 0, 0)
        with clock.FakeClock(fake_dt):
            for f in sequencing_test_files:
                # Set file type
                if "idat" in f.lower():
                    file_type = f.split('/')[-1].split("_")[-1]
                else:
                    file_type = '.'.join(f.split('.')[1:])

                test_file_dict = {
                    'file_path': f'{bucket_name}/{f}',
                    'gc_site_id': 'jh',
                    'bucket_name': bucket_name,
                    'file_prefix': f'Genotyping_sample_raw_data',
                    'file_name': f,
                    'file_type': file_type,
                    'identifier_type': 'chipwellbarcode',
                    'identifier_value': "_".join(f.split('/')[1].split('_')[0:2]).split('.')[0],
                }

                self.data_generator.create_database_gc_data_file_record(**test_file_dict)

        # finally run the manifest workflow
        bucket_name = config.getSetting(config.GENOMIC_GEM_BUCKET_NAME)
        a1_time = datetime.datetime(2020, 4, 1, 0, 0, 0, 0)
        with clock.FakeClock(a1_time):
            genomic_pipeline.gem_a1_manifest_workflow()  # run_id = 3
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
                GenomicSetMember.genomicWorkflowState,
                GenomicSetMember.genomicWorkflowStateStr).filter(
                GenomicGCValidationMetrics.genomicSetMemberId == GenomicSetMember.id,
                GenomicSet.id == GenomicSetMember.genomicSetId,
                ParticipantSummary.participantId == GenomicSetMember.participantId,
                GenomicSetMember.id == 1
            ).one()

        self.assertEqual(GenomicWorkflowState.A1, test_member_1.genomicWorkflowState)
        self.assertEqual('A1', test_member_1.genomicWorkflowStateStr)

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
            missing_cols = len(set(expected_gem_columns)) - len(set(csv_reader.fieldnames))
            self.assertEqual(0, missing_cols)
            rows = list(csv_reader)
            self.assertEqual(2, len(rows))
            self.assertIn(test_member_1.biobankId, [rows[0]['biobank_id'], rows[1]['biobank_id']])
            for row in rows:
                if test_member_1.biobankId == row['biobank_id']:
                    self.assertEqual(test_member_1.biobankId, row['biobank_id'])
                    self.assertEqual(test_member_1.sampleId, row['sample_id'])
                    self.assertEqual(test_member_1.sexAtBirth, row['sex_at_birth'])
                    self.assertEqual("yes", row['consent_for_ror'])
                    self.assertEqual(test_member_1.consentForGenomicsRORAuthored, parse(row['date_of_consent_for_ror']))
                    self.assertEqual(test_member_1.chipwellbarcode, row['chipwellbarcode'])
                    self.assertEqual('JH', row['genome_center'])

        # Array
        file_record = self.file_processed_dao.get(2)  # remember, GC Metrics is #1
        self.assertEqual(3, file_record.runId)
        self.assertEqual(f'{sub_folder}/AoU_GEM_A1_manifest_{a1f}.csv', file_record.fileName)

        # Test the job result
        run_obj = self.job_run_dao.get(3)
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

        # Do Reconsent ROR | DEFERRING This until a later sprint
        # reconsent_time = datetime.datetime(2020, 4, 3, 0, 0, 0, 0)
        # summary1.consentForGenomicsROR = QuestionnaireStatus.SUBMITTED
        # summary1.consentForGenomicsRORAuthored = reconsent_time
        # self.summary_dao.update(summary1)
        # # Run A1 Again
        # with clock.FakeClock(reconsent_time):
        #     genomic_pipeline.gem_a1_manifest_workflow()  # run_id 7
        # a1f = reconsent_time.strftime("%Y-%m-%d-%H-%M-%S")
        # # Test record was included again
        # with open_cloud_file(os.path.normpath(f'{bucket_name}/{sub_folder}/AoU_GEM_A1_manifest_{a1f}.csv')) as csv_file:
        #     csv_reader = csv.DictReader(csv_file)
        #     rows = list(csv_reader)
        #     self.assertEqual(1, len(rows))
        #     self.assertEqual(test_member_1.biobankId, rows[0]['biobank_id'])

    def test_gem_a1_block_results(self):
        # Need GC Manifest for source query : run_id = 1
        self.job_run_dao.insert(GenomicJobRun(jobId=GenomicJob.AW1_MANIFEST,
                                              startTime=clock.CLOCK.now(),
                                              runStatus=GenomicSubProcessStatus.COMPLETED,
                                              runResult=GenomicSubProcessResult.SUCCESS))

        self._create_fake_datasets_for_gc_tests(5, arr_override=True,
                                                array_participants=range(1, 6),
                                                recon_gc_man_id=1,
                                                genome_center='jh',
                                                genomic_workflow_state=GenomicWorkflowState.GEM_READY)

        self._update_test_sample_ids()

        self._create_stored_samples([
            (1, 1001),
            (2, 1002),
            (3, 1003),
            (4, 1004),
            (5, 1005),
        ])

        for i in range(1, 6):
            self.data_generator.create_database_genomic_gc_validation_metrics(
                genomicSetMemberId=i,
                processingStatus='pass',
            )

        # update ignore_flags for test
        members = self.member_dao.get_all()
        members[2].ignoreFlag = 1
        self.member_dao.update(members[2])

        # update block_results for test
        members[3].blockResults = 1
        self.member_dao.update(members[3])

        # Add participant that has already been sent
        members[4].biobankId = 4
        members[4].participantId = 4
        members[4].gemA1ManifestJobRunId = 1
        self.member_dao.update(members[4])

        genomic_pipeline.gem_a1_manifest_workflow()  # run_id = 4

        members = self.member_dao.get_all()
        a1_members = [x for x in members if x.genomicWorkflowState == GenomicWorkflowState.A1]
        self.assertEqual(2, len(a1_members))

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
        create_ingestion_test_file('AoU_GEM_A2_manifest_2020-07-11-00-00-00.csv',
                                         bucket_name, folder=sub_folder,
                                         include_timestamp=False)

        # Run Workflow at fake time
        fake_now = datetime.datetime(2020, 7, 11, 0, 0, 0, 0)
        with clock.FakeClock(fake_now):
            genomic_pipeline.gem_a2_manifest_workflow()  # run_id 2

        # Test A2 fields and genomic state
        members = self.member_dao.get_all()
        for member in members:
            self.assertEqual(datetime.datetime(2020, 4, 29, 0, 0, 0), member.gemDateOfImport)
            self.assertEqual(fake_now, member.genomicWorkflowStateModifiedTime)

            if member.id in (1, 2):
                self.assertEqual("Y", member.gemPass)
                self.assertEqual(2, member.gemA2ManifestJobRunId)
                self.assertEqual(GenomicWorkflowState.GEM_RPT_READY, member.genomicWorkflowState)
                self.assertEqual('GEM_RPT_READY', member.genomicWorkflowStateStr)
            if member.id == 3:
                self.assertEqual("N", member.gemPass)
                self.assertEqual(GenomicWorkflowState.A2F, member.genomicWorkflowState)
                self.assertEqual('A2F', member.genomicWorkflowStateStr)

        # Test Files Processed
        file_record = self.file_processed_dao.get(1)
        self.assertEqual(2, file_record.runId)
        self.assertEqual(f'{bucket_name}/{sub_folder}/AoU_GEM_A2_manifest_2020-07-11-00-00-00.csv', file_record.filePath)
        self.assertEqual('AoU_GEM_A2_manifest_2020-07-11-00-00-00.csv', file_record.fileName)

        # Test the job result
        run_obj = self.job_run_dao.get(2)
        self.assertEqual(GenomicSubProcessResult.SUCCESS, run_obj.runResult)

    @mock.patch('rdr_service.genomic.genomic_job_controller.GenomicJobController.execute_cloud_task')
    def test_gem_a3_manifest_workflow(self, cloud_task):
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

        # Change GROR (Should be included in A3)
        p3 = self.summary_dao.get(3)
        p3.consentForGenomicsROR = QuestionnaireStatus.SUBMITTED_NO_CONSENT
        p3.consentForGenomicsRORAuthored = datetime.datetime(2020, 5, 25, 0, 0, 0)
        self.summary_dao.update(p3)

        # Change Withdrawal (Should be included in A3)
        p3 = self.summary_dao.get(2)
        p3.consentForStudyEnrollment = QuestionnaireStatus.SUBMITTED_NO_CONSENT
        p3.consentForStudyEnrollmentAuthored = datetime.datetime(2020, 5, 26, 0, 0, 0)
        p3.withdrawalStatus = WithdrawalStatus.NO_USE
        self.summary_dao.update(p3)

        # Run Workflow
        fake_now = datetime.datetime.utcnow()
        out_time = fake_now.strftime("%Y-%m-%d-%H-%M-%S")
        with clock.FakeClock(fake_now):
            genomic_pipeline.update_report_state_for_consent_removal()  # run_id 2
            genomic_pipeline.gem_a3_manifest_workflow()  # run_id 3

        self.assertTrue(cloud_task.called)
        cloud_task_args = cloud_task.call_args.args[0]
        req_keys = ['member_ids', 'is_job_run', 'field', 'value']
        self.assertTrue(set(cloud_task_args.keys()) == set(req_keys))

        member_ids = cloud_task_args['member_ids']
        self.assertIsNotNone(member_ids)
        self.assertTrue(len(set(member_ids)) == len(member_ids))

        # Test the members' job run ID
        # Picked up by job
        test_member_3 = self.member_dao.get(3)
        self.assertEqual(GenomicWorkflowState.GEM_RPT_DELETED, test_member_3.genomicWorkflowState)
        self.assertEqual('GEM_RPT_DELETED', test_member_3.genomicWorkflowStateStr)

        # picked up by job
        test_member_2 = self.member_dao.get(2)
        self.assertEqual(GenomicWorkflowState.GEM_RPT_DELETED, test_member_2.genomicWorkflowState)
        self.assertEqual('GEM_RPT_DELETED', test_member_2.genomicWorkflowStateStr)

        # Test the manifest file contents
        bucket_name = config.getSetting(config.GENOMIC_GEM_BUCKET_NAME)
        sub_folder = GENOMIC_GEM_A3_MANIFEST_SUBFOLDER

        expected_gem_columns = (
            "biobank_id",
            "sample_id",
            "date_of_consent_removal",
        )
        with open_cloud_file(os.path.normpath(f'{bucket_name}/{sub_folder}/AoU_GEM_A3_manifest_{out_time}.csv')) as csv_file:
            csv_reader = csv.DictReader(csv_file)
            missing_cols = len(set(expected_gem_columns)) - len(set(csv_reader.fieldnames))
            self.assertEqual(0, missing_cols)
            rows = list(csv_reader)
            self.assertEqual(2, len(rows))
            self.assertEqual(test_member_2.biobankId, rows[0]['biobank_id'])
            self.assertEqual(test_member_2.sampleId, rows[0]['sample_id'])
            self.assertEqual('2020-05-26T00:00:00Z', rows[0]['date_of_consent_removal'])
            self.assertEqual(test_member_3.biobankId, rows[1]['biobank_id'])
            self.assertEqual(test_member_3.sampleId, rows[1]['sample_id'])
            self.assertEqual('2020-05-25T00:00:00Z', rows[1]['date_of_consent_removal'])

        # Array
        file_record = self.file_processed_dao.get(1)  # remember, GC Metrics is #1
        self.assertEqual(3, file_record.runId)
        self.assertEqual(f'{sub_folder}/AoU_GEM_A3_manifest_{out_time}.csv', file_record.fileName)

        # Test the job result
        run_obj = self.job_run_dao.get(3)
        self.assertEqual(GenomicSubProcessResult.SUCCESS, run_obj.runResult)

    def test_gem_a1_limit(self):
        # Need GC Manifest for source query : run_id = 1
        self.job_run_dao.insert(GenomicJobRun(jobId=GenomicJob.AW1_MANIFEST,
                                              startTime=clock.CLOCK.now(),
                                              runStatus=GenomicSubProcessStatus.COMPLETED,
                                              runResult=GenomicSubProcessResult.SUCCESS))

        self._create_fake_datasets_for_gc_tests(4, arr_override=True,
                                                array_participants=range(1, 5),
                                                recon_gc_man_id=1,
                                                genome_center='jh',
                                                genomic_workflow_state=GenomicWorkflowState.GEM_READY)

        self._update_test_sample_ids()

        self._create_stored_samples([
            (1, 1001),
            (2, 1002),
            (3, 1003)
        ])

        for i in range(1, 5):
            self.data_generator.create_database_genomic_gc_validation_metrics(
                genomicSetMemberId=i,
                processingStatus='pass',
            )

        config.override_setting(config.A1_LIMIT, [1])

        genomic_pipeline.gem_a1_manifest_workflow()  # run_id = 4

        members = self.member_dao.get_all()
        a1_members = [x for x in members if x.genomicWorkflowState == GenomicWorkflowState.A1]
        self.assertEqual(1, len(a1_members))
        config.override_setting(config.A1_LIMIT, [1000])  # reset for full testing

    @mock.patch('rdr_service.genomic.genomic_job_controller.GenomicJobController.execute_cloud_task')
    def test_aw3_array_manifest_generation(self, cloud_task):
        # Need GC Manifest for source query : run_id = 1
        self.job_run_dao.insert(GenomicJobRun(jobId=GenomicJob.AW1_MANIFEST,
                                              startTime=clock.CLOCK.now(),
                                              runStatus=GenomicSubProcessStatus.COMPLETED,
                                              runResult=GenomicSubProcessResult.SUCCESS))

        self._create_fake_datasets_for_gc_tests(3, arr_override=True,
                                                array_participants=range(1, 4),
                                                recon_gc_man_id=1,
                                                genome_center='jh',
                                                genomic_workflow_state=GenomicWorkflowState.AW1,
                                                sample_source="Whole Blood",
                                                ai_an='N')

        bucket_name = _FAKE_GENOMIC_CENTER_BUCKET_BAYLOR

        create_ingestion_test_file('RDR_AoU_GEN_TestDataManifest.csv',
                                         bucket_name,
                                         folder=config.getSetting(config.GENOMIC_AW2_SUBFOLDERS[1]))

        self._update_test_sample_ids()

        self._create_stored_samples([
            (1, 1001),
            (2, 1002)
        ])

        genomic_pipeline.ingest_genomic_centers_metrics_files()  # run_id = 2

        # Test sequencing file (required for GEM)
        sequencing_test_files = (
            f'test_data_folder/10001_R01C01.vcf.gz',
            f'test_data_folder/10001_R01C01.vcf.gz.tbi',
            f'test_data_folder/10001_R01C01.vcf.gz.md5sum',
            f'test_data_folder/10001_R01C01_Red.idat',
            f'test_data_folder/10001_R01C01_Grn.idat',
            f'test_data_folder/10001_R01C01_Red.idat.md5sum',
            f'test_data_folder/10001_R01C01_Grn.idat.md5sum',
            f'test_data_folder/10002_R01C02.vcf.gz',
            f'test_data_folder/10002_R01C02.vcf.gz.tbi',
            f'test_data_folder/10002_R01C02.vcf.gz.md5sum',
            f'test_data_folder/10002_R01C02_Red.idat',
            f'test_data_folder/10002_R01C02_Grn.idat',
            f'test_data_folder/10002_R01C02_Red.idat.md5sum',
            f'test_data_folder/10002_R01C02_Grn.idat.md5sum',
        )

        fake_dt = datetime.datetime(2020, 8, 3, 0, 0, 0, 0)
        with clock.FakeClock(fake_dt):
            for f in sequencing_test_files:
                # Set file type
                if "idat" in f.lower():
                    file_type = f.split('/')[-1].split("_")[-1]
                else:
                    file_type = '.'.join(f.split('.')[1:])

                test_file_dict = {
                    'file_path': f'{bucket_name}/{f}',
                    'gc_site_id': 'jh',
                    'bucket_name': bucket_name,
                    'file_prefix': f'Genotyping_sample_raw_data',
                    'file_name': f,
                    'file_type': file_type,
                    'identifier_type': 'chipwellbarcode',
                    'identifier_value': "_".join(f.split('/')[1].split('_')[0:2]).split('.')[0],
                }

                self.data_generator.create_database_gc_data_file_record(**test_file_dict)

        # finally run the AW3 manifest workflow
        fake_dt = datetime.datetime(2020, 8, 3, 0, 0, 0, 0)

        with clock.FakeClock(fake_dt):
            genomic_pipeline.aw3_array_manifest_workflow()  # run_id = 4

        manifest_records = self.manifest_file_dao.get_all()
        self.assertEqual(len(manifest_records), 1)
        self.assertEqual(manifest_records[0].recordCount, 2)
        self.assertIsNotNone(manifest_records[0].fileName)
        self.assertIsNotNone(manifest_records[0].filePath)

        self.assertTrue(cloud_task.called)
        cloud_task_args = cloud_task.call_args.args[0]
        self.assertEqual(cloud_task_args['field'], 'aw3ManifestFileId')

        member_ids = cloud_task_args['member_ids']
        self.assertIsNotNone(member_ids)
        self.assertTrue(len(set(member_ids)) == len(member_ids))

        aw3_dtf = fake_dt.strftime("%Y-%m-%d-%H-%M-%S")

        # Test member was updated
        member = self.member_dao.get(2)
        self.assertEqual(GenomicWorkflowState.GEM_READY, member.genomicWorkflowState)

        # Test the manifest file contents
        expected_aw3_columns = (
            "chipwellbarcode",
            "biobank_id",
            "sample_id",
            "biobankidsampleid",
            "sex_at_birth",
            "site_id",
            "red_idat_path",
            "red_idat_md5_path",
            "green_idat_path",
            "green_idat_md5_path",
            "vcf_path",
            "vcf_index_path",
            "vcf_md5_path",
            "callrate",
            "sex_concordance",
            "contamination",
            "processing_status",
            "research_id",
            "sample_source",
            "pipeline_id",
            "ai_an",
            "blocklisted",
            "blocklisted_reason"
        )

        bucket_name = config.getSetting(config.DRC_BROAD_BUCKET_NAME)
        sub_folder = config.GENOMIC_AW3_ARRAY_SUBFOLDER

        with open_cloud_file(os.path.normpath(f'{bucket_name}/{sub_folder}/AoU_DRCV_GEN_{aw3_dtf}.csv')) as csv_file:
            csv_reader = csv.DictReader(csv_file)
            missing_cols = len(set(expected_aw3_columns)) - len(set(csv_reader.fieldnames))
            self.assertEqual(0, missing_cols)

            rows = list(csv_reader)

            self.assertEqual(2, len(rows))
            self.assertEqual(f'{get_biobank_id_prefix()}{member.biobankId}', rows[1]['biobank_id'])
            self.assertEqual(member.sampleId, rows[1]['sample_id'])
            self.assertEqual(f'{get_biobank_id_prefix()}{member.biobankId}_{member.sampleId}',
                             rows[1]['biobankidsampleid'])
            self.assertEqual(member.sexAtBirth, rows[1]['sex_at_birth'])
            self.assertEqual(member.gcSiteId, rows[1]['site_id'])
            self.assertEqual(1000002, int(rows[1]['research_id']))
            self.assertEqual('Whole Blood', rows[1]['sample_source'])
            self.assertEqual('cidr_egt_1', rows[1]['pipeline_id'])
            self.assertEqual('False', rows[1]['ai_an'])

            # Test File Paths
            metric = self.metrics_dao.get(2)
            self.assertEqual(metric.idatRedPath, rows[1]['red_idat_path'])
            self.assertEqual(metric.idatRedMd5Path, rows[1]['red_idat_md5_path'])
            self.assertEqual(metric.idatGreenPath, rows[1]['green_idat_path'])
            self.assertEqual(metric.idatGreenMd5Path, rows[1]['green_idat_md5_path'])
            self.assertEqual(metric.vcfPath, rows[1]['vcf_path'])
            self.assertEqual(metric.vcfTbiPath, rows[1]['vcf_index_path'])
            self.assertEqual(metric.vcfMd5Path, rows[1]['vcf_md5_path'])

            # Test processing GC metrics columns
            self.assertEqual(metric.callRate, rows[1]['callrate'])
            self.assertEqual(metric.sexConcordance, rows[1]['sex_concordance'])
            self.assertEqual(metric.contamination, rows[1]['contamination'])
            self.assertEqual(metric.processingStatus, rows[1]['processing_status'])

            self.assertEqual(metric.pipelineId, rows[1]['pipeline_id'])

            # Test AW3 loaded into raw table
            aw3_dao = GenomicAW3RawDao()
            raw_records = aw3_dao.get_all()
            raw_records.sort(key=lambda x: x.biobank_id)

            # Check rows in file against records in raw table
            self.assertEqual(len(rows), len(raw_records))

            for file_row in rows:
                i = int(file_row['biobank_id'][1:])-1
                for field in file_row.keys():
                    self.assertEqual(file_row[field], getattr(raw_records[i], field.lower()))

                self.assertEqual("aou_array", raw_records[i].genome_type)

        # Test run record is success
        run_obj = self.job_run_dao.get(4)
        self.assertEqual(GenomicSubProcessResult.SUCCESS, run_obj.runResult)

        # Set up 'investigation' test
        investigation_member = member
        investigation_member.genomeType = 'aou_array_investigation'
        investigation_member.blockResearch = 1
        self.member_dao.update(investigation_member)

        fake_dt = datetime.datetime(2020, 8, 4, 0, 0, 0, 0)
        with clock.FakeClock(fake_dt):
            genomic_pipeline.aw3_array_investigation_workflow()

        aw3_dtf = fake_dt.strftime("%Y-%m-%d-%H-%M-%S")

        # Check file WAS created
        with open_cloud_file(os.path.normpath(f'{bucket_name}/{sub_folder}/AoU_DRCV_GEN_{aw3_dtf}.csv')) as csv_file:
            csv_reader = csv.DictReader(csv_file)
            rows = list(csv_reader)
            self.assertEqual(1, len(rows))
            self.assertEqual("True", rows[0]['blocklisted'])

        self.clear_table_after_test('genomic_aw3_raw')
        self.clear_table_after_test('genomic_job_run')

    def test_aw3_array_blocklist_populated(self):
        block_research_reason = 'Sample Swap'

        self.job_run_dao.insert(GenomicJobRun(jobId=GenomicJob.AW1_MANIFEST,
                                              startTime=clock.CLOCK.now(),
                                              runStatus=GenomicSubProcessStatus.COMPLETED,
                                              runResult=GenomicSubProcessResult.SUCCESS))

        self._create_fake_datasets_for_gc_tests(3, arr_override=True,
                                                array_participants=range(1, 4),
                                                recon_gc_man_id=1,
                                                genome_center='jh',
                                                genomic_workflow_state=GenomicWorkflowState.AW1,
                                                sample_source="Whole Blood",
                                                ai_an='N',
                                                block_research=1,
                                                block_research_reason=block_research_reason)

        bucket_name = _FAKE_GENOMIC_CENTER_BUCKET_BAYLOR

        create_ingestion_test_file(
            'RDR_AoU_GEN_TestDataManifest.csv',
            bucket_name,
            folder=config.getSetting(config.GENOMIC_AW2_SUBFOLDERS[1])
        )

        self._update_test_sample_ids()

        self._create_stored_samples([
            (1, 1001),
            (2, 1002)
        ])

        genomic_pipeline.ingest_genomic_centers_metrics_files()  # run_id = 2
        # Test sequencing file (required for GEM)
        sequencing_test_files = (
            f'test_data_folder/10001_R01C01.vcf.gz',
            f'test_data_folder/10001_R01C01.vcf.gz.tbi',
            f'test_data_folder/10001_R01C01.vcf.gz.md5sum',
            f'test_data_folder/10001_R01C01_Red.idat',
            f'test_data_folder/10001_R01C01_Grn.idat',
            f'test_data_folder/10001_R01C01_Red.idat.md5sum',
            f'test_data_folder/10001_R01C01_Grn.idat.md5sum',
            f'test_data_folder/10002_R01C02.vcf.gz',
            f'test_data_folder/10002_R01C02.vcf.gz.tbi',
            f'test_data_folder/10002_R01C02.vcf.gz.md5sum',
            f'test_data_folder/10002_R01C02_Red.idat',
            f'test_data_folder/10002_R01C02_Grn.idat',
            f'test_data_folder/10002_R01C02_Red.idat.md5sum',
            f'test_data_folder/10002_R01C02_Grn.idat.md5sum',
        )

        fake_dt = datetime.datetime(2020, 8, 3, 0, 0, 0, 0)
        with clock.FakeClock(fake_dt):
            for f in sequencing_test_files:
                # Set file type
                if "idat" in f.lower():
                    file_type = f.split('/')[-1].split("_")[-1]
                else:
                    file_type = '.'.join(f.split('.')[1:])

                test_file_dict = {
                    'file_path': f'{bucket_name}/{f}',
                    'gc_site_id': 'jh',
                    'bucket_name': bucket_name,
                    'file_prefix': f'Genotyping_sample_raw_data',
                    'file_name': f,
                    'file_type': file_type,
                    'identifier_type': 'chipwellbarcode',
                    'identifier_value': "_".join(f.split('/')[1].split('_')[0:2]).split('.')[0],
                }

                self.data_generator.create_database_gc_data_file_record(**test_file_dict)

        # finally run the AW3 manifest workflow
        fake_dt = datetime.datetime(2020, 8, 3, 0, 0, 0, 0)

        with clock.FakeClock(fake_dt):
            genomic_pipeline.aw3_array_manifest_workflow()  # run_id = 4

        aw3_dtf = fake_dt.strftime("%Y-%m-%d-%H-%M-%S")

        bucket_name = config.getSetting(config.DRC_BROAD_BUCKET_NAME)
        sub_folder = config.GENOMIC_AW3_ARRAY_SUBFOLDER

        with open_cloud_file(os.path.normpath(f'{bucket_name}/{sub_folder}/AoU_DRCV_GEN_{aw3_dtf}.csv')) as csv_file:
            csv_reader = csv.DictReader(csv_file)
            rows = list(csv_reader)

            self.assertTrue(all(obj['blocklisted'] == 'True' and obj['blocklisted'] is not None for obj in rows))
            self.assertTrue(all(obj['blocklisted_reason'] == block_research_reason and obj['blocklisted_reason'] is not
                                None for obj in rows))

        # Test run record is success
        run_obj = self.job_run_dao.get(4)
        self.assertEqual(GenomicSubProcessResult.SUCCESS, run_obj.runResult)

        self.clear_table_after_test('genomic_aw3_raw')
        self.clear_table_after_test('genomic_job_run')

    def test_aw3_array_manifest_with_max_num(self):
        stored_samples = [
            (1, 1001),
            (2, 1002),
            (3, 1003),
            (4, 1004),
            (5, 1005),
            (6, 1006),
            (7, 1007),
        ]

        self.job_run_dao.insert(GenomicJobRun(jobId=GenomicJob.AW1_MANIFEST,
                                              startTime=clock.CLOCK.now(),
                                              runStatus=GenomicSubProcessStatus.COMPLETED,
                                              runResult=GenomicSubProcessResult.SUCCESS))

        self._create_fake_datasets_for_gc_tests(len(stored_samples),
                                                arr_override=True,
                                                array_participants=range(1, len(stored_samples)+1),
                                                recon_gc_man_id=1,
                                                genome_center='jh',
                                                genomic_workflow_state=GenomicWorkflowState.AW1
                                                )

        bucket_name = _FAKE_GENOMIC_CENTER_BUCKET_BAYLOR

        create_ingestion_test_file(
            'RDR_AoU_GEN_TestDataManifest.csv',
            bucket_name,
            folder=config.getSetting(config.GENOMIC_AW2_SUBFOLDERS[1])
        )

        self._update_test_sample_ids()
        self._create_stored_samples(stored_samples)

        genomic_pipeline.ingest_genomic_centers_metrics_files()  # run_id = 2

        sequencing_test_files = []
        for sample in stored_samples:
            sequencing_test_files.append(
                (f'test_data_folder/1000{sample[0]}_R01C0{sample[0]}.vcf.gz',
                 f'test_data_folder/1000{sample[0]}_R01C0{sample[0]}.vcf.gz.tbi',
                 f'test_data_folder/1000{sample[0]}_R01C0{sample[0]}.vcf.gz.md5sum',
                 f'test_data_folder/1000{sample[0]}_R01C0{sample[0]}_Red.idat',
                 f'test_data_folder/1000{sample[0]}_R01C0{sample[0]}_Grn.idat',
                 f'test_data_folder/1000{sample[0]}_R01C0{sample[0]}_Red.idat.md5sum',
                 f'test_data_folder/1000{sample[0]}_R01C0{sample[0]}_Grn.idat.md5sum',
                 )
            )

        sequencing_test_files = [file for file in chain.from_iterable(sequencing_test_files)]

        self.assertEqual(len(sequencing_test_files), len(stored_samples) * 7)

        fake_dt = datetime.datetime(2020, 8, 3, 0, 0, 0, 0)
        with clock.FakeClock(fake_dt):
            for f in sequencing_test_files:
                # Set file type
                if "idat" in f.lower():
                    file_type = f.split('/')[-1].split("_")[-1]
                else:
                    file_type = '.'.join(f.split('.')[1:])

                test_file_dict = {
                    'file_path': f'{bucket_name}/{f}',
                    'gc_site_id': 'jh',
                    'bucket_name': bucket_name,
                    'file_prefix': f'Genotyping_sample_raw_data',
                    'file_name': f,
                    'file_type': file_type,
                    'identifier_type': 'chipwellbarcode',
                    'identifier_value': "_".join(f.split('/')[1].split('_')[0:2]).split('.')[0],
                }

                self.data_generator.create_database_gc_data_file_record(**test_file_dict)

        config.override_setting(config.GENOMIC_MAX_NUM_GENERATE, [3])

        with clock.FakeClock(fake_dt):
            genomic_pipeline.aw3_array_manifest_workflow()  # run_id = 4

        manifest_records = self.manifest_file_dao.get_all()
        self.assertEqual(len(manifest_records), 3)
        for i, manifest in enumerate(manifest_records):
            self.assertTrue(f'_{i+1}.csv' in manifest.fileName)
            self.assertIsNotNone(manifest.recordCount)
            self.assertIsNotNone(manifest.fileName)
            self.assertIsNotNone(manifest.filePath)

        member = self.member_dao.get(2)
        self.assertEqual(GenomicWorkflowState.GEM_READY, member.genomicWorkflowState)

        # Test the manifest file contents
        expected_aw3_array_columns = (
            "chipwellbarcode",
            "biobank_id",
            "sample_id",
            "biobankidsampleid",
            "sex_at_birth",
            "site_id",
            "red_idat_path",
            "red_idat_md5_path",
            "green_idat_path",
            "green_idat_md5_path",
            "vcf_path",
            "vcf_index_path",
            "vcf_md5_path",
            "callrate",
            "sex_concordance",
            "contamination",
            "processing_status",
            "research_id",
            "sample_source",
            "pipeline_id",
            "ai_an",
            "blocklisted",
            "blocklisted_reason"
        )

        bucket_name = config.getSetting(config.DRC_BROAD_BUCKET_NAME)
        bucket_files = [file for file in list_blobs(bucket_name) if file.name.lower().endswith(".csv")]
        # 7 rows / 3 max_num + 1 for remainder
        self.assertEqual(len(bucket_files), 3)

        num_rows = 0
        for file in bucket_files:
            with open_cloud_file(os.path.normpath(f'{bucket_name}/{file.name}')) as csv_file:
                csv_reader = csv.DictReader(csv_file)
                missing_cols = len(set(expected_aw3_array_columns)) - len(set(csv_reader.fieldnames))
                self.assertEqual(0, missing_cols)
                rows = list(csv_reader)
                num_rows += len(rows)

        self.assertEqual(num_rows, len(stored_samples))

        run_obj = self.job_run_dao.get(4)
        self.assertEqual(GenomicSubProcessResult.SUCCESS, run_obj.runResult)

        self.clear_table_after_test('genomic_aw3_raw')
        self.clear_table_after_test('genomic_job_run')
        config.override_setting(config.GENOMIC_MAX_NUM_GENERATE, [4000])

    def test_aw3_array_manifest_validation(self):
        stored_samples = [
            (1, 1001),
            (2, 1002),
            (3, 1003),
            (4, 1004),
            (5, 1005),
            (6, 1006),
        ]

        self.data_generator.create_database_genomic_job_run(
            jobId=GenomicJob.AW3_ARRAY_WORKFLOW,
            startTime=clock.CLOCK.now(),
            runResult=GenomicSubProcessResult.SUCCESS
        )

        self._create_fake_datasets_for_gc_tests(
            len(stored_samples),
            arr_override=True,
            array_participants=range(1, len(stored_samples) + 1),
            recon_gc_man_id=1,
            genome_center='jh',
            genomic_workflow_state=GenomicWorkflowState.AW1
        )

        bucket_name = _FAKE_GENOMIC_CENTER_BUCKET_BAYLOR

        create_ingestion_test_file(
            'RDR_AoU_GEN_TestDataManifest.csv',
            bucket_name,
            folder=config.getSetting(config.GENOMIC_AW2_SUBFOLDERS[1])
        )

        self._update_test_sample_ids()
        self._create_stored_samples(stored_samples)

        genomic_pipeline.ingest_genomic_centers_metrics_files()  # run_id = 2

        sequencing_test_files = []
        for sample in stored_samples:
            sequencing_test_files.append(
                (f'test_data_folder/1000{sample[0]}_R01C0{sample[0]}.vcf.gz',
                 f'test_data_folder/1000{sample[0]}_R01C0{sample[0]}.vcf.gz.tbi',
                 f'test_data_folder/1000{sample[0]}_R01C0{sample[0]}.vcf.gz.md5sum',
                 f'test_data_folder/1000{sample[0]}_R01C0{sample[0]}_Red.idat',
                 f'test_data_folder/1000{sample[0]}_R01C0{sample[0]}_Grn.idat',
                 f'test_data_folder/1000{sample[0]}_R01C0{sample[0]}_Red.idat.md5sum',
                 f'test_data_folder/1000{sample[0]}_R01C0{sample[0]}_Grn.idat.md5sum',
                 )
            )

        sequencing_test_files = [file for file in chain.from_iterable(sequencing_test_files)]

        fake_dt = datetime.datetime(2020, 8, 3, 0, 0, 0, 0)
        with clock.FakeClock(fake_dt):
            for f in sequencing_test_files:
                # Set file type
                if "idat" in f.lower():
                    file_type = f.split('/')[-1].split("_")[-1]
                else:
                    file_type = '.'.join(f.split('.')[1:])
                test_file_dict = {
                    'file_path': f'{bucket_name}/{f}',
                    'gc_site_id': 'jh',
                    'bucket_name': bucket_name,
                    'file_prefix': f'Genotyping_sample_raw_data',
                    'file_name': f,
                    'file_type': file_type,
                    'identifier_type': 'chipwellbarcode',
                    'identifier_value': "_".join(f.split('/')[1].split('_')[0:2]).split('.')[0],
                }

                self.data_generator.create_database_gc_data_file_record(**test_file_dict)

        incident_name = GenomicIncidentCode.MANIFEST_GENERATE_DATA_VALIDATION_FAILED.name
        should_be_incident_count = 0

        current_members = self.member_dao.get_members_with_non_null_sample_ids()

        first_sample_id = current_members[0].sampleId
        last_sample_id = None
        last_id = None

        for i, _ in enumerate(current_members):
            if (i + 1) == len(current_members):
                member = self.member_dao.get(current_members[i].id)
                last_id = i + 1
                last_sample_id = member.sampleId
                member.sampleId = first_sample_id
                self.member_dao.update(member)

        with clock.FakeClock(fake_dt):
            genomic_pipeline.aw3_array_manifest_workflow()  # run_id = 3

        should_be_incident_count += 1
        run_obj = self.job_run_dao.get(3)
        self.assertEqual(GenomicSubProcessResult.ERROR, run_obj.runResult)

        incident = self.incident_dao.get_by_message(
            f"AW3_ARRAY_WORKFLOW: Sample IDs ['{first_sample_id}'] are not distinct"
        )

        self.assertIsNotNone(incident)
        self.assertEqual(
            incident.code,
            incident_name
        )

        member = self.member_dao.get(last_id)
        member.sampleId = last_sample_id
        self.member_dao.update(member)

        current_metrics = self.metrics_dao.get_all()

        bad_data_path = None
        last_id = None

        for i, _ in enumerate(current_metrics):
            if (i + 1) == len(current_metrics):
                metric = self.metrics_dao.get(current_metrics[i].id)
                last_id = i + 1
                edited_path = metric.idatRedPath.split('gs://')
                edited_path = edited_path[1]
                bad_data_path = edited_path
                metric.idatRedPath = edited_path
                self.metrics_dao.upsert(metric)

        with clock.FakeClock(fake_dt):
            genomic_pipeline.aw3_array_manifest_workflow()

        should_be_incident_count += 1
        run_obj = self.job_run_dao.get(4)
        self.assertEqual(GenomicSubProcessResult.ERROR, run_obj.runResult)

        incident = self.incident_dao.get_by_message(
            f'AW3_ARRAY_WORKFLOW: Path {bad_data_path} is invalid formatting'
        )

        self.assertIsNotNone(incident)
        self.assertEqual(
            incident.code,
            incident_name
        )

        no_bucket_path = 'gs://test_data_folder/10006_R01C06_Red.idat'
        update_metric = self.metrics_dao.get(last_id)
        update_metric.idatRedPath = no_bucket_path
        self.metrics_dao.upsert(update_metric)

        with clock.FakeClock(fake_dt):
            genomic_pipeline.aw3_array_manifest_workflow()

        should_be_incident_count += 1
        run_obj = self.job_run_dao.get(5)
        self.assertEqual(GenomicSubProcessResult.ERROR, run_obj.runResult)

        incident = self.incident_dao.get_by_message(
            f'AW3_ARRAY_WORKFLOW: Path {no_bucket_path} is invalid formatting'
        )

        self.assertIsNotNone(incident)
        self.assertEqual(
            incident.code,
            incident_name
        )

        updated_member = self.member_dao.get(3)
        updated_member.sexAtBirth = 'A'
        self.member_dao.update(updated_member)

        with clock.FakeClock(fake_dt):
            genomic_pipeline.aw3_array_manifest_workflow()

        should_be_incident_count += 1
        run_obj = self.job_run_dao.get(6)
        self.assertEqual(GenomicSubProcessResult.ERROR, run_obj.runResult)

        incident = self.incident_dao.get_by_message(
            'AW3_ARRAY_WORKFLOW: Invalid Sex at Birth values'
        )

        self.assertIsNotNone(incident)
        self.assertEqual(
            incident.code,
            incident_name
        )

        all_incidents = [incident for incident in self.incident_dao.get_all() if incident.code ==
                         incident_name]

        self.assertEqual(len(all_incidents), should_be_incident_count)
        self.assertTrue(all(i for i in all_incidents if i.slack_notification == 1 and i.slack_notification_date is
                            not None))

        self.clear_table_after_test('genomic_aw3_raw')
        self.clear_table_after_test('genomic_job_run')

    @mock.patch('rdr_service.genomic.genomic_job_controller.GenomicJobController.execute_cloud_task')
    def test_aw3_wgs_manifest_generation(self, cloud_task):
        # Need GC Manifest for source query : run_id = 1
        self.job_run_dao.insert(GenomicJobRun(jobId=GenomicJob.AW1_MANIFEST,
                                              startTime=clock.CLOCK.now(),
                                              runStatus=GenomicSubProcessStatus.COMPLETED,
                                              runResult=GenomicSubProcessResult.SUCCESS))

        self._create_fake_datasets_for_gc_tests(3, arr_override=False,
                                                recon_gc_man_id=1,
                                                genome_center='rdr',
                                                genomic_workflow_state=GenomicWorkflowState.AW1,
                                                sample_source="Whole Blood",
                                                ai_an='N')

        bucket_name = _FAKE_GENOMIC_CENTER_BUCKET_A

        create_ingestion_test_file(
            'RDR_AoU_SEQ_TestDataManifest.csv',
            bucket_name,
            folder=config.getSetting(config.GENOMIC_AW2_SUBFOLDERS[0])
        )

        self._update_test_sample_ids()
        self._create_stored_samples([(2, 1002)])

        # Create corresponding array genomic_set_members
        for i in range(1, 4):
            self.data_generator.create_database_genomic_set_member(
                participantId=i,
                genomicSetId=1,
                biobankId=i,
                gcManifestParentSampleId=1000+i,
                genomeType="aou_array",
                aw3ManifestJobRunID=1,
                ai_an='N'
            )

        genomic_pipeline.ingest_genomic_centers_metrics_files()  # run_id = 2

        # Test sequencing file (required for AW3 WGS)
        sequencing_test_files = (
            f'test_data_folder/RDR_2_1002_10002_1.hard-filtered.vcf.gz',
            f'test_data_folder/RDR_2_1002_10002_1.hard-filtered.vcf.gz.tbi',
            f'test_data_folder/RDR_2_1002_10002_1.hard-filtered.vcf.gz.md5sum',
            f'test_data_folder/RDR_2_1002_10002_1.cram',
            f'test_data_folder/RDR_2_1002_10002_1.cram.md5sum',
            f'test_data_folder/RDR_2_1002_10002_1.cram.crai',
            f'test_data_folder/RDR_2_1002_10002_1.hard-filtered.gvcf.gz',
            f'test_data_folder/RDR_2_1002_10002_1.hard-filtered.gvcf.gz.md5sum',
        )
        test_date = datetime.datetime(2021, 7, 12, 0, 0, 0, 0)

        # create test records in GenomicGcDataFile
        with clock.FakeClock(test_date):
            for f in sequencing_test_files:
                if "cram" in f:
                    file_prefix = "CRAMs_CRAIs"
                else:
                    file_prefix = "SS_VCF_CLINICAL"

                test_file_dict = {
                    'file_path': f'{bucket_name}/{f}',
                    'gc_site_id': 'rdr',
                    'bucket_name': bucket_name,
                    'file_prefix': f'Wgs_sample_raw_data/{file_prefix}',
                    'file_name': f,
                    'file_type': '.'.join(f.split('.')[1:]),
                    'identifier_type': 'sample_id',
                    'identifier_value': '1002',
                }

                self.data_generator.create_database_gc_data_file_record(**test_file_dict)

        # finally run the AW3 manifest workflow
        fake_dt = datetime.datetime(2020, 8, 3, 0, 0, 0, 0)

        with clock.FakeClock(fake_dt):
            genomic_pipeline.aw3_wgs_manifest_workflow()  # run_id = 3

        manifest_records = self.manifest_file_dao.get_all()
        self.assertEqual(len(manifest_records), 1)
        self.assertEqual(manifest_records[0].recordCount, 1)
        self.assertIsNotNone(manifest_records[0].fileName)
        self.assertIsNotNone(manifest_records[0].filePath)

        self.assertTrue(cloud_task.called)
        cloud_task_args = cloud_task.call_args.args[0]
        self.assertEqual(cloud_task_args['field'], 'aw3ManifestFileId')

        member_ids = cloud_task_args['member_ids']
        self.assertIsNotNone(member_ids)
        self.assertTrue(len(set(member_ids)) == len(member_ids))

        aw3_dtf = fake_dt.strftime("%Y-%m-%d-%H-%M-%S")

        # Test member was updated
        member = self.member_dao.get(2)
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
            "vcf_hf_md5_path",
            "cram_path",
            "cram_md5_path",
            "crai_path",
            "gvcf_path",
            "gvcf_md5_path",
            "contamination",
            "sex_concordance",
            "processing_status",
            "mean_coverage",
            "research_id",
            "sample_source",
            "mapped_reads_pct",
            "sex_ploidy",
            "ai_an",
            "blocklisted",
            "blocklisted_reason"
        )

        bucket_name = config.getSetting(config.DRC_BROAD_BUCKET_NAME)
        sub_folder = config.GENOMIC_AW3_WGS_SUBFOLDER

        with open_cloud_file(os.path.normpath(f'{bucket_name}/{sub_folder}/AoU_DRCV_SEQ_{aw3_dtf}.csv')) as csv_file:
            csv_reader = csv.DictReader(csv_file)
            self.assertEqual(len(set(expected_aw3_columns)), len(set(csv_reader.fieldnames)))

            rows = list(csv_reader)
            self.assertEqual(1, len(rows))

            row = rows[0]
            metric = self.metrics_dao.get(1)
            received = [val for val in metric if 'Received' in val[0] and val[1] == 1]
            paths = [val for val in metric if 'Path' in val[0] and val[1] is not None]

            self.assertEqual(len(sequencing_test_files), len(received))
            self.assertEqual(len(sequencing_test_files), len(paths))

            self.assertEqual(f'{get_biobank_id_prefix()}{member.biobankId}',
                             row['biobank_id'])
            self.assertEqual(f'{get_biobank_id_prefix()}{member.biobankId}_{member.sampleId}',
                             row['biobankidsampleid'])
            self.assertEqual(member.sexAtBirth, row['sex_at_birth'])
            self.assertEqual(member.gcSiteId, row['site_id'])
            self.assertEqual(1000002, int(row['research_id']))

            self.assertEqual('Whole Blood', row['sample_source'])
            self.assertEqual('88.8888888', row['mapped_reads_pct'])
            self.assertEqual('XY', row['sex_ploidy'])
            self.assertEqual('False', row['ai_an'])

            self.assertEqual(metric.hfVcfPath, row["vcf_hf_path"])
            self.assertEqual(metric.hfVcfTbiPath, row["vcf_hf_index_path"])
            self.assertEqual(metric.cramPath, row["cram_path"])
            self.assertEqual(metric.cramMd5Path, row["cram_md5_path"])
            self.assertEqual(metric.craiPath, row["crai_path"])

            # Test GC metrics columns
            self.assertEqual(metric.contamination, row['contamination'])
            self.assertEqual(metric.sexConcordance, row['sex_concordance'])
            self.assertEqual(metric.processingStatus, row['processing_status'])
            self.assertEqual(metric.meanCoverage, row['mean_coverage'])

            # Test AW3 loaded into raw table
            aw3_dao = GenomicAW3RawDao()
            raw_records = aw3_dao.get_all()
            raw_records.sort(key=lambda x: x.biobank_id)

            # Check rows in file against records in raw table
            self.assertEqual(len(rows), len(raw_records))

            for file_row in rows:
                for field in file_row.keys():
                    self.assertEqual(file_row[field], getattr(raw_records[0], field.lower()))

                self.assertEqual("aou_wgs", raw_records[0].genome_type)

        # Test run record is success
        run_obj = self.job_run_dao.get(4)

        self.assertEqual(GenomicSubProcessResult.SUCCESS, run_obj.runResult)

        # Set up 'investigation' test
        investigation_member = member
        investigation_member.genomeType = 'aou_wgs_investigation'
        investigation_member.blockResearch = 1
        self.member_dao.update(investigation_member)

        fake_dt = datetime.datetime(2020, 8, 4, 0, 0, 0, 0)
        with clock.FakeClock(fake_dt):
            genomic_pipeline.aw3_wgs_investigation_workflow()

        aw3_dtf = fake_dt.strftime("%Y-%m-%d-%H-%M-%S")

        # Check file WAS created
        with open_cloud_file(os.path.normpath(f'{bucket_name}/{sub_folder}/AoU_DRCV_SEQ_{aw3_dtf}.csv')) as csv_file:
            csv_reader = csv.DictReader(csv_file)
            rows = list(csv_reader)
            self.assertEqual(1, len(rows))
            self.assertEqual("True", rows[0]['blocklisted'])

        self.clear_table_after_test('genomic_aw3_raw')
        self.clear_table_after_test('genomic_job_run')

    def test_aw3_wgs_blocklist_populated(self):
        block_research_reason = 'Sample Swap'

        self.job_run_dao.insert(GenomicJobRun(jobId=GenomicJob.AW1_MANIFEST,
                                              startTime=clock.CLOCK.now(),
                                              runStatus=GenomicSubProcessStatus.COMPLETED,
                                              runResult=GenomicSubProcessResult.SUCCESS))

        self._create_fake_datasets_for_gc_tests(3, arr_override=False,
                                                recon_gc_man_id=1,
                                                genome_center='rdr',
                                                genomic_workflow_state=GenomicWorkflowState.AW1,
                                                sample_source="Whole Blood",
                                                ai_an='N',
                                                block_research=1,
                                                block_research_reason=block_research_reason)

        bucket_name = _FAKE_GENOMIC_CENTER_BUCKET_A

        create_ingestion_test_file(
            'RDR_AoU_SEQ_TestDataManifest.csv',
            bucket_name,
            folder=config.getSetting(config.GENOMIC_AW2_SUBFOLDERS[0])
        )

        self._update_test_sample_ids()
        self._create_stored_samples([(2, 1002)])

        # Create corresponding array genomic_set_members
        for i in range(1, 4):
            self.data_generator.create_database_genomic_set_member(
                participantId=i,
                genomicSetId=1,
                biobankId=i,
                gcManifestParentSampleId=1000+i,
                genomeType="aou_array",
                aw3ManifestJobRunID=1,
                ai_an='N'
            )

        genomic_pipeline.ingest_genomic_centers_metrics_files()  # run_id = 2

        # Test sequencing file (required for AW3 WGS)
        sequencing_test_files = (
            f'test_data_folder/RDR_2_1002_10002_1.hard-filtered.vcf.gz',
            f'test_data_folder/RDR_2_1002_10002_1.hard-filtered.vcf.gz.tbi',
            f'test_data_folder/RDR_2_1002_10002_1.hard-filtered.vcf.gz.md5sum',
            f'test_data_folder/RDR_2_1002_10002_1.cram',
            f'test_data_folder/RDR_2_1002_10002_1.cram.md5sum',
            f'test_data_folder/RDR_2_1002_10002_1.cram.crai',
            f'test_data_folder/RDR_2_1002_10002_1.hard-filtered.gvcf.gz',
            f'test_data_folder/RDR_2_1002_10002_1.hard-filtered.gvcf.gz.md5sum',
        )
        test_date = datetime.datetime(2021, 7, 12, 0, 0, 0, 0)

        # create test records in GenomicGcDataFile
        with clock.FakeClock(test_date):
            for f in sequencing_test_files:
                if "cram" in f:
                    file_prefix = "CRAMs_CRAIs"
                else:
                    file_prefix = "SS_VCF_CLINICAL"

                test_file_dict = {
                    'file_path': f'{bucket_name}/{f}',
                    'gc_site_id': 'rdr',
                    'bucket_name': bucket_name,
                    'file_prefix': f'Wgs_sample_raw_data/{file_prefix}',
                    'file_name': f,
                    'file_type': '.'.join(f.split('.')[1:]),
                    'identifier_type': 'sample_id',
                    'identifier_value': '1002',
                }

                self.data_generator.create_database_gc_data_file_record(**test_file_dict)

        # finally run the AW3 manifest workflow
        fake_dt = datetime.datetime(2020, 8, 3, 0, 0, 0, 0)

        with clock.FakeClock(fake_dt):
            genomic_pipeline.aw3_wgs_manifest_workflow()  # run_id = 3

        aw3_dtf = fake_dt.strftime("%Y-%m-%d-%H-%M-%S")

        bucket_name = config.getSetting(config.DRC_BROAD_BUCKET_NAME)
        sub_folder = config.GENOMIC_AW3_WGS_SUBFOLDER

        with open_cloud_file(os.path.normpath(f'{bucket_name}/{sub_folder}/AoU_DRCV_SEQ_{aw3_dtf}.csv')) as csv_file:
            csv_reader = csv.DictReader(csv_file)
            rows = list(csv_reader)
            self.assertEqual(1, len(rows))

            row = rows[0]

            self.assertTrue(obj['blocklisted'] == 'True' and obj['blocklisted'] is not None for obj in row)
            self.assertTrue(obj['blocklisted_reason'] == block_research_reason and obj['blocklisted_reason']
                            is not None for obj in row)

        # Test run record is success
        run_obj = self.job_run_dao.get(4)
        self.assertEqual(GenomicSubProcessResult.SUCCESS, run_obj.runResult)

        self.clear_table_after_test('genomic_aw3_raw')
        self.clear_table_after_test('genomic_job_run')

    def test_aw3_wgs_manifest_validation(self):
        stored_samples = [
            (2, 1002),
            (3, 1003),
            (4, 1004),
            (5, 1005),
            (6, 1006),
        ]

        self.data_generator.create_database_genomic_job_run(
            jobId=GenomicJob.AW3_WGS_WORKFLOW,
            startTime=clock.CLOCK.now(),
            runResult=GenomicSubProcessResult.SUCCESS
        )

        self._create_fake_datasets_for_gc_tests(
            len(stored_samples) + 1,
            arr_override=False,
            recon_gc_man_id=1,
            genome_center='rdr',
            genomic_workflow_state=GenomicWorkflowState.AW1
        )

        bucket_name = _FAKE_GENOMIC_CENTER_BUCKET_A

        create_ingestion_test_file(
            'RDR_AoU_SEQ_TestDataManifest.csv',
            bucket_name,
            folder=config.getSetting(config.GENOMIC_AW2_SUBFOLDERS[0])
        )

        self._update_test_sample_ids()
        self._create_stored_samples(stored_samples)

        for i in range(1, 8):
            self.data_generator.create_database_genomic_set_member(
                participantId=i,
                genomicSetId=1,
                biobankId=i,
                gcManifestParentSampleId=1000 + i,
                genomeType="aou_array",
                aw3ManifestJobRunID=1,
            )

        genomic_pipeline.ingest_genomic_centers_metrics_files()  # run_id = 2

        sequencing_test_files = []
        for sample in stored_samples:
            sequencing_test_files.append(
                (f'test_data_folder/RDR_{sample[0]}_100{sample[0]}_1000{sample[0]}_1.hard-filtered.vcf.gz',
                 f'test_data_folder/RDR_{sample[0]}_100{sample[0]}_1000{sample[0]}_1.hard-filtered.vcf.gz.tbi',
                 f'test_data_folder/RDR_{sample[0]}_100{sample[0]}_1000{sample[0]}_1.hard-filtered.vcf.gz.md5sum',
                 f'test_data_folder/RDR_{sample[0]}_100{sample[0]}_1000{sample[0]}_1.vcf.gz',
                 f'test_data_folder/RDR_{sample[0]}_100{sample[0]}_1000{sample[0]}_1.vcf.gz.tbi',
                 f'test_data_folder/RDR_{sample[0]}_100{sample[0]}_1000{sample[0]}_1.vcf.gz.md5sum',
                 f'test_data_folder/RDR_{sample[0]}_100{sample[0]}_1000{sample[0]}_1.cram',
                 f'test_data_folder/RDR_{sample[0]}_100{sample[0]}_1000{sample[0]}_1.cram.md5sum',
                 f'test_data_folder/RDR_{sample[0]}_100{sample[0]}_1000{sample[0]}_1.cram.crai',
                 f'test_data_folder/RDR_{sample[0]}_100{sample[0]}_1000{sample[0]}_1.hard-filtered.gvcf.gz',
                 f'test_data_folder/RDR_{sample[0]}_100{sample[0]}_1000{sample[0]}_1.hard-filtered.gvcf.gz.md5sum',)
            )

        sequencing_test_files = [file for file in chain.from_iterable(sequencing_test_files)]

        test_date = datetime.datetime(2021, 7, 12, 0, 0, 0, 0)
        with clock.FakeClock(test_date):
            for f in sequencing_test_files:
                if "cram" in f:
                    file_prefix = "CRAMs_CRAIs"
                else:
                    file_prefix = "SS_VCF_CLINICAL"

                test_file_dict = {
                    'file_path': f'{bucket_name}/{f}',
                    'gc_site_id': 'rdr',
                    'bucket_name': bucket_name,
                    'file_prefix': f'Wgs_sample_raw_data/{file_prefix}',
                    'file_name': f,
                    'file_type': '.'.join(f.split('.')[1:]),
                    'identifier_type': 'sample_id',
                    'identifier_value': f.split('_')[4],
                }

                self.data_generator.create_database_gc_data_file_record(**test_file_dict)


        incident_name = GenomicIncidentCode.MANIFEST_GENERATE_DATA_VALIDATION_FAILED.name
        should_be_incident_count = 0

        current_members = self.member_dao.get_all()
        current_members = [m for m in current_members if m.genomeType == 'aou_wgs']

        first_sample_id = current_members[0].sampleId
        last_sample_id = None
        last_id = None

        for i, _ in enumerate(current_members):
            if (i + 1) == len(current_members):
                member = self.member_dao.get(current_members[i].id)
                last_id = i + 1
                last_sample_id = member.sampleId
                member.sampleId = first_sample_id
                self.member_dao.update(member)

        fake_dt = datetime.datetime(2020, 8, 3, 0, 0, 0, 0)
        with clock.FakeClock(fake_dt):
            genomic_pipeline.aw3_wgs_manifest_workflow()  # run_id = 3

        # clear aw3 raw records so query finds source data
        with self.member_dao.session() as session:
            session.query(GenomicAW3Raw).delete()

        # still is success with same sample_ids becuase of distinct on query
        run_obj = self.job_run_dao.get(3)
        self.assertEqual(GenomicSubProcessResult.SUCCESS, run_obj.runResult)

        member = self.member_dao.get(last_id)
        member.sampleId = last_sample_id
        self.member_dao.update(member)

        current_metrics = self.metrics_dao.get_all()

        bad_data_path = None
        last_id = None

        for i, _ in enumerate(current_metrics):
            if (i + 1) == len(current_metrics):
                metric = self.metrics_dao.get(current_metrics[i].id)
                last_id = i + 1
                edited_path = metric.hfVcfTbiPath.split('gs://')
                edited_path = edited_path[1]
                bad_data_path = edited_path
                metric.hfVcfTbiPath = edited_path
                self.metrics_dao.upsert(metric)

        with clock.FakeClock(fake_dt):
            genomic_pipeline.aw3_wgs_manifest_workflow()

        should_be_incident_count += 1
        run_obj = self.job_run_dao.get(5)
        self.assertEqual(GenomicSubProcessResult.ERROR, run_obj.runResult)

        incident = self.incident_dao.get_by_message(
            f'AW3_WGS_WORKFLOW: Path {bad_data_path} is invalid formatting'
        )

        self.assertIsNotNone(incident)
        self.assertEqual(
            incident.code,
            incident_name
        )

        no_bucket_path = 'gs://test_data_folder/RDR_6_1006_10006_1.vcf.gz.md5sum'
        update_metric = self.metrics_dao.get(last_id)
        update_metric.hfVcfTbiPath = no_bucket_path
        self.metrics_dao.upsert(update_metric)

        with clock.FakeClock(fake_dt):
            genomic_pipeline.aw3_wgs_manifest_workflow()

        should_be_incident_count += 1
        run_obj = self.job_run_dao.get(6)
        self.assertEqual(GenomicSubProcessResult.ERROR, run_obj.runResult)

        incident = self.incident_dao.get_by_message(
            f'AW3_WGS_WORKFLOW: Path {no_bucket_path} is invalid formatting'
        )

        self.assertIsNotNone(incident)
        self.assertEqual(
            incident.code,
            incident_name
        )

        updated_member = self.member_dao.get(3)
        updated_member.sexAtBirth = 'A'
        self.member_dao.update(updated_member)

        with clock.FakeClock(fake_dt):
            genomic_pipeline.aw3_wgs_manifest_workflow()

        should_be_incident_count += 1
        run_obj = self.job_run_dao.get(7)
        self.assertEqual(GenomicSubProcessResult.ERROR, run_obj.runResult)

        incident = self.incident_dao.get_by_message(
            'AW3_WGS_WORKFLOW: Invalid Sex at Birth values'
        )

        self.assertIsNotNone(incident)
        self.assertEqual(
            incident.code,
            incident_name
        )

        all_incidents = [incident for incident in self.incident_dao.get_all() if incident.code ==
                         incident_name]

        self.assertEqual(len(all_incidents), should_be_incident_count)
        self.assertTrue(all(i for i in all_incidents if i.slack_notification == 1 and i.slack_notification_date is
                            not None))

        self.clear_table_after_test('genomic_aw3_raw')
        self.clear_table_after_test('genomic_job_run')

    def test_aw3_wgs_manifest_with_max_num(self):
        stored_samples = [
            (2, 1002),
            (3, 1003),
            (4, 1004),
            (5, 1005),
            (6, 1006),
        ]

        self.job_run_dao.insert(GenomicJobRun(jobId=GenomicJob.AW1_MANIFEST,
                                              startTime=clock.CLOCK.now(),
                                              runStatus=GenomicSubProcessStatus.COMPLETED,
                                              runResult=GenomicSubProcessResult.SUCCESS))

        self._create_fake_datasets_for_gc_tests(len(stored_samples)+1,
                                                arr_override=False,
                                                recon_gc_man_id=1,
                                                genome_center='rdr',
                                                genomic_workflow_state=GenomicWorkflowState.AW1,
                                                ai_an='N')

        bucket_name = _FAKE_GENOMIC_CENTER_BUCKET_A

        create_ingestion_test_file(
            'RDR_AoU_SEQ_TestDataManifest.csv',
            bucket_name,
            folder=config.getSetting(config.GENOMIC_AW2_SUBFOLDERS[0])
        )

        self._update_test_sample_ids()
        self._create_stored_samples(stored_samples)

        for i in range(1, 8):
            self.data_generator.create_database_genomic_set_member(
                participantId=i,
                genomicSetId=1,
                biobankId=i,
                gcManifestParentSampleId=1000 + i,
                genomeType="aou_array",
                aw3ManifestJobRunID=1,
            )

        genomic_pipeline.ingest_genomic_centers_metrics_files()  # run_id = 2

        sequencing_test_files = []
        for sample in stored_samples:
            sequencing_test_files.append(
                (f'test_data_folder/RDR_{sample[0]}_100{sample[0]}_1000{sample[0]}_1.hard-filtered.vcf.gz',
                 f'test_data_folder/RDR_{sample[0]}_100{sample[0]}_1000{sample[0]}_1.hard-filtered.vcf.gz.tbi',
                 f'test_data_folder/RDR_{sample[0]}_100{sample[0]}_1000{sample[0]}_1.hard-filtered.vcf.gz.md5sum',
                 f'test_data_folder/RDR_{sample[0]}_100{sample[0]}_1000{sample[0]}_1.vcf.gz',
                 f'test_data_folder/RDR_{sample[0]}_100{sample[0]}_1000{sample[0]}_1.vcf.gz.tbi',
                 f'test_data_folder/RDR_{sample[0]}_100{sample[0]}_1000{sample[0]}_1.vcf.gz.md5sum',
                 f'test_data_folder/RDR_{sample[0]}_100{sample[0]}_1000{sample[0]}_1.cram',
                 f'test_data_folder/RDR_{sample[0]}_100{sample[0]}_1000{sample[0]}_1.cram.md5sum',
                 f'test_data_folder/RDR_{sample[0]}_100{sample[0]}_1000{sample[0]}_1.cram.crai',
                 f'test_data_folder/RDR_{sample[0]}_100{sample[0]}_1000{sample[0]}_1.hard-filtered.gvcf.gz',
                 f'test_data_folder/RDR_{sample[0]}_100{sample[0]}_1000{sample[0]}_1.hard-filtered.gvcf.gz.md5sum',)
            )

        sequencing_test_files = [file for file in chain.from_iterable(sequencing_test_files)]

        self.assertEqual(len(sequencing_test_files), len(stored_samples) * 11)

        test_date = datetime.datetime(2021, 7, 12, 0, 0, 0, 0)

        # create test records in GenomicGcDataFile
        with clock.FakeClock(test_date):
            for f in sequencing_test_files:
                if "cram" in f:
                    file_prefix = "CRAMs_CRAIs"
                else:
                    file_prefix = "SS_VCF_CLINICAL"

                test_file_dict = {
                    'file_path': f'{bucket_name}/{f}',
                    'gc_site_id': 'rdr',
                    'bucket_name': bucket_name,
                    'file_prefix': f'Wgs_sample_raw_data/{file_prefix}',
                    'file_name': f,
                    'file_type': '.'.join(f.split('.')[1:]),
                    'identifier_type': 'sample_id',
                    'identifier_value': f.split('_')[4],
                }

                self.data_generator.create_database_gc_data_file_record(**test_file_dict)

        fake_dt = datetime.datetime(2020, 8, 3, 0, 0, 0, 0)

        config.override_setting(config.GENOMIC_MAX_NUM_GENERATE, [2])

        with clock.FakeClock(fake_dt):
            genomic_pipeline.aw3_wgs_manifest_workflow()  # run_id = 3

        manifest_records = self.manifest_file_dao.get_all()
        self.assertEqual(len(manifest_records), 3)
        for i, manifest in enumerate(manifest_records):
            self.assertTrue(f'_{i + 1}.csv' in manifest.fileName)
            self.assertIsNotNone(manifest.recordCount)
            self.assertIsNotNone(manifest.fileName)
            self.assertIsNotNone(manifest.filePath)

        # Test member was updated
        member = self.member_dao.get(2)
        self.assertEqual(GenomicWorkflowState.CVL_READY, member.genomicWorkflowState)

        # Test the manifest file contents
        expected_aw3_wgs_columns = (
            "biobank_id",
            "sample_id",
            "biobankidsampleid",
            "sex_at_birth",
            "site_id",
            "vcf_hf_path",
            "vcf_hf_index_path",
            "vcf_hf_md5_path",
            "cram_path",
            "cram_md5_path",
            "crai_path",
            "gvcf_path",
            "gvcf_md5_path",
            "contamination",
            "sex_concordance",
            "processing_status",
            "mean_coverage",
            "research_id",
            "sample_source",
            "mapped_reads_pct",
            "sex_ploidy",
            "ai_an",
            "blocklisted",
            "blocklisted_reason"
        )

        bucket_name = config.getSetting(config.DRC_BROAD_BUCKET_NAME)
        bucket_files = [file for file in list_blobs(bucket_name) if file.name.lower().endswith(".csv")]
        # 5 rows / 2 max_num + 1 for remainder
        self.assertEqual(len(bucket_files), 3)

        num_rows = 0
        for file in bucket_files:
            with open_cloud_file(os.path.normpath(f'{bucket_name}/{file.name}')) as csv_file:
                csv_reader = csv.DictReader(csv_file)
                missing_cols = len(set(expected_aw3_wgs_columns)) - len(set(csv_reader.fieldnames))
                self.assertEqual(0, missing_cols)
                rows = list(csv_reader)
                num_rows += len(rows)

        self.assertEqual(num_rows, len(stored_samples))

        run_obj = self.job_run_dao.get(4)
        self.assertEqual(GenomicSubProcessResult.SUCCESS, run_obj.runResult)

        self.clear_table_after_test('genomic_aw3_raw')
        self.clear_table_after_test('genomic_job_run')
        config.override_setting(config.GENOMIC_MAX_NUM_GENERATE, [4000])

    def test_aw3_no_records(self):
        genomic_pipeline.aw3_wgs_manifest_workflow()  # run_id = 1

        # Test run record result is success if no records
        run_obj = self.job_run_dao.get(1)

        self.assertEqual(GenomicSubProcessResult.NO_FILES, run_obj.runResult)
        self.clear_table_after_test('genomic_aw3_raw')
        self.clear_table_after_test('genomic_job_run')


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

        for i, member in enumerate(self.member_dao.get_all()):
            record = GenomicGCValidationMetrics()
            record.id = i + 1
            record.genomicSetMemberId = member.id
            self.metrics_dao.upsert(record)

            # Change sample 2 to aou_array_investigation
            if member.id == 2:
                member.genomeType = "aou_array_investigation"
                self.member_dao.update(member)


        # Set up test AW4 manifest
        bucket_name = config.getSetting(config.DRC_BROAD_BUCKET_NAME)
        sub_folder = config.getSetting(config.DRC_BROAD_AW4_SUBFOLDERS[0])
        file_name = 'AoU_DRCB_GEN_2020-07-11-00-00-00.csv'

        create_ingestion_test_file(file_name,
                                         bucket_name,
                                         folder=sub_folder,
                                         include_timestamp=False
                                         )

        # Set up file/JSON
        task_data = {
            "job": GenomicJob.AW4_ARRAY_WORKFLOW,
            "bucket": bucket_name,
            "subfolder": sub_folder,
            "file_data": {
                "create_feedback_record": False,
                "upload_date": "2020-11-20 00:00:00",
                "manifest_type": GenomicManifestTypes.AW4_ARRAY,
                "file_path": f"{bucket_name}/{sub_folder}/{file_name}"
            }
        }

        # Call pipeline function
        genomic_pipeline.execute_genomic_manifest_file_pipeline(task_data)

        # Test AW4 manifest updated fields
        for member in self.member_dao.get_all():
            metrics = self.metrics_dao.get_metrics_by_member_id(member.id)

            self.assertIsNotNone(metrics.drcSexConcordance)
            self.assertIsNotNone(metrics.drcCallRate)

            self.assertIsNone(metrics.drcContamination)
            self.assertIsNone(metrics.drcMeanCoverage)
            self.assertIsNone(metrics.drcFpConcordance)

            if member.id in (1, 2):
                self.assertEqual(3, member.aw4ManifestJobRunID)
                self.assertEqual('0.99689185', metrics.drcCallRate)
            if member.id == 1:
                self.assertEqual('TRUE', metrics.drcSexConcordance)
                self.assertEqual(GenomicQcStatus.PASS, member.qcStatus)
                self.assertEqual('PASS', member.qcStatusStr)
            if member.id == 2:
                self.assertEqual('FALSE', metrics.drcSexConcordance)
                self.assertEqual(GenomicQcStatus.FAIL, member.qcStatus)
                self.assertEqual('FAIL', member.qcStatusStr)

        # Test Files Processed
        file_record = self.file_processed_dao.get(1)
        self.assertEqual(3, file_record.runId)
        self.assertEqual(f'{bucket_name}/{sub_folder}/{file_name}',
                         file_record.filePath)
        self.assertEqual(file_name, file_record.fileName)

        # Test AW4 Raw table
        genomic_pipeline.load_awn_manifest_into_raw_table(f"{bucket_name}/{sub_folder}/{file_name}", "aw4")

        aw4_dao = GenomicAW4RawDao()
        raw_records = aw4_dao.get_all()
        raw_records.sort(key=lambda x: x.biobank_id)

        with open_cloud_file(os.path.normpath(f"{bucket_name}/{sub_folder}/{file_name}")) as csv_file:
            csv_reader = csv.DictReader(csv_file)
            file_rows = list(csv_reader)

            # Check rows in file against records in raw table
            for file_row in file_rows:
                i = int(file_row['biobank_id'])-1
                for field in file_row.keys():
                    self.assertEqual(file_row[field], getattr(raw_records[i], field.lower()))
                expected_genome_type = "aou_array"
                if i == 1:
                    expected_genome_type += "_investigation"

                self.assertEqual(expected_genome_type, raw_records[i].genome_type)

        # Test the job result
        run_obj = self.job_run_dao.get(2)
        self.assertEqual(GenomicSubProcessResult.SUCCESS, run_obj.runResult)

        self.clear_table_after_test('genomic_aw4_raw')

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

        for i, member in enumerate(self.member_dao.get_all()):
            record = GenomicGCValidationMetrics()
            record.id = i + 1
            record.genomicSetMemberId = member.id
            self.metrics_dao.upsert(record)

            # Change sample 2 to aou_array_investigation
            if member.id == 2:
                member.genomeType = "aou_wgs_investigation"
                self.member_dao.update(member)

        # Set up test AW4 manifest
        bucket_name = config.getSetting(config.DRC_BROAD_BUCKET_NAME)
        sub_folder = config.getSetting(config.DRC_BROAD_AW4_SUBFOLDERS[1])
        file_name = 'AoU_DRCB_SEQ_2020-07-11-00-00-00.csv'

        create_ingestion_test_file(file_name,
                                         bucket_name,
                                         folder=sub_folder,
                                         include_timestamp=False
                                         )

        # Set up file/JSON
        task_data = {
            "job": GenomicJob.AW4_WGS_WORKFLOW,
            "bucket": bucket_name,
            "subfolder": sub_folder,
            "file_data": {
                "create_feedback_record": False,
                "upload_date": "2020-11-20 00:00:00",
                "manifest_type": GenomicManifestTypes.AW4_WGS,
                "file_path": f"{bucket_name}/{sub_folder}/{file_name}"
            }
        }

        # Call pipeline function
        genomic_pipeline.execute_genomic_manifest_file_pipeline(task_data)

        # Test AW4 manifest updated fields
        for member in self.member_dao.get_all():
            metrics = self.metrics_dao.get_metrics_by_member_id(member.id)

            self.assertIsNone(metrics.drcCallRate)

            self.assertIsNotNone(metrics.drcSexConcordance)
            self.assertIsNotNone(metrics.drcContamination)
            self.assertIsNotNone(metrics.drcMeanCoverage)
            self.assertIsNotNone(metrics.drcFpConcordance)

            if member.id in (1, 2):
                self.assertEqual(3, member.aw4ManifestJobRunID)
                self.assertEqual('0', metrics.drcContamination)
                self.assertEqual('63.2800', metrics.drcMeanCoverage)
            if member.id == 1:
                self.assertEqual(GenomicQcStatus.PASS, member.qcStatus)
                self.assertEqual('PASS', member.qcStatusStr)
                self.assertEqual('TRUE', metrics.drcFpConcordance)
            if member.id == 2:
                self.assertEqual(GenomicQcStatus.FAIL, member.qcStatus)
                self.assertEqual('FAIL', member.qcStatusStr)
                self.assertEqual('FALSE', metrics.drcFpConcordance)

        # Test Files Processed
        file_record = self.file_processed_dao.get(1)
        self.assertEqual(3, file_record.runId)
        self.assertEqual(f'{bucket_name}/{sub_folder}/{file_name}',
                         file_record.filePath)
        self.assertEqual(file_name, file_record.fileName)

        # Test AW4 Raw table
        genomic_pipeline.load_awn_manifest_into_raw_table(f"{bucket_name}/{sub_folder}/{file_name}", "aw4")

        aw4_dao = GenomicAW4RawDao()
        raw_records = aw4_dao.get_all()
        raw_records.sort(key=lambda x: x.biobank_id)

        with open_cloud_file(os.path.normpath(f"{bucket_name}/{sub_folder}/{file_name}")) as csv_file:
            csv_reader = csv.DictReader(csv_file)
            file_rows = list(csv_reader)

            # Check rows in file against records in raw table
            for file_row in file_rows:
                i = int(file_row['biobank_id'])-1
                for field in file_row.keys():
                    self.assertEqual(file_row[field], getattr(raw_records[i], field.lower()))

                expected_genome_type = "aou_wgs"
                if i == 1:
                    expected_genome_type += "_investigation"

                self.assertEqual(expected_genome_type, raw_records[i].genome_type)

        # Test the job result
        run_obj = self.job_run_dao.get(2)
        self.assertEqual(GenomicSubProcessResult.SUCCESS, run_obj.runResult)

        self.clear_table_after_test('genomic_aw4_raw')

    def test_sub_folder_same_file_names(self):
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

        bucket_name = config.getSetting(config.DRC_BROAD_BUCKET_NAME)
        sub_folder = config.getSetting(config.DRC_BROAD_AW4_SUBFOLDERS[0])
        file_name = 'AoU_DRCB_GEN_2020-07-11-00-00-00.csv'

        create_ingestion_test_file(file_name,
                                         bucket_name,
                                         folder=sub_folder,
                                         include_timestamp=False
                                         )

        create_ingestion_test_file(file_name,
                                         bucket_name,
                                         folder='AW5_array_manifest',
                                         include_timestamp=False
                                         )

        # Set up file/JSON
        task_data = {
            "job": GenomicJob.AW4_ARRAY_WORKFLOW,
            "bucket": bucket_name,
            "subfolder": sub_folder,
            "file_data": {
                "create_feedback_record": False,
                "upload_date": "2020-11-20 00:00:00",
                "manifest_type": GenomicManifestTypes.AW4_ARRAY,
                "file_path": f"{bucket_name}/{sub_folder}/{file_name}"
            }
        }

        # Call pipeline function
        genomic_pipeline.execute_genomic_manifest_file_pipeline(task_data)

        # Test Files Processed
        file_record = self.file_processed_dao.get(1)
        self.assertEqual(3, file_record.runId)
        self.assertEqual(f'{bucket_name}/{sub_folder}/{file_name}',
                         file_record.filePath)
        self.assertEqual(file_name, file_record.fileName)

        self.assertNotEqual(f'{bucket_name}/AW5_array_manifest/{file_name}',
                            file_record.filePath)

        all_files = self.file_processed_dao.get_all()
        self.assertEqual(1, len(all_files))

        # Test the job result
        run_obj = self.job_run_dao.get(2)
        self.assertEqual(GenomicSubProcessResult.SUCCESS, run_obj.runResult)

    def test_gem_metrics_ingest(self):
        # create fake genomic set members
        self._create_fake_datasets_for_gc_tests(3, arr_override=True,
                                                array_participants=(1, 2, 3),
                                                genome_center='JH',
                                                genomic_workflow_state=GenomicWorkflowState.GEM_RPT_READY)
        self._update_test_sample_ids()

        # Create fake file
        # Set up test A2 manifest
        bucket_name = config.getSetting(config.GENOMIC_GEM_BUCKET_NAME)

        create_ingestion_test_file(
            'AoU_GEM_metrics_aggregate_2020-08-28-10-43-21.csv',
            bucket_name,
            include_timestamp=False
        )
        # Run Workflow
        genomic_pipeline.gem_metrics_ingest()  # run_id 1

        members = self.member_dao.get_all()

        # Test metrics were ingested
        for member in members:
            self.assertEqual(1, member.colorMetricsJobRunID)
            self.assertEqual("['ancestry','cilantro','lactose','earwax','bittertaste']",
                             member.gemMetricsAvailableResults)

            if member.id in (1, 2):
                self.assertEqual('yes', member.gemMetricsAncestryLoopResponse)
                self.assertEqual(datetime.datetime(2020, 8, 21, 10, 10, 10), member.gemMetricsResultsReleasedAt)

            else:
                self.assertEqual('later', member.gemMetricsAncestryLoopResponse)
                self.assertEqual(datetime.datetime(2020, 8, 21, 10, 12, 10), member.gemMetricsResultsReleasedAt)

        # Test the job result
        run_obj = self.job_run_dao.get(1)
        self.assertEqual(GenomicSubProcessResult.SUCCESS, run_obj.runResult)

    def test_insert_genomic_manifest_file_record(self):

        # create test file
        bucket_name = _FAKE_GENOMIC_CENTER_BUCKET_A
        sub_folder = config.getSetting(config.GENOMIC_AW2_SUBFOLDERS[1])
        file_name = 'RDR_AoU_GEN_TestDataManifest_11192019.csv'
        create_ingestion_test_file('RDR_AoU_GEN_TestDataManifest.csv',
                                         bucket_name,
                                         folder=sub_folder)

        # Set up file/JSON
        task_data = {
            "job": GenomicJob.AW1_MANIFEST,
            "bucket": bucket_name,
            "file_data": {
                "create_feedback_record": True,
                "upload_date": "2020-11-20 00:00:00",
                "manifest_type": GenomicManifestTypes.AW1,
                "file_path": f"{bucket_name}/{sub_folder}/{file_name}"
            }
        }

        # Call pipeline function
        genomic_pipeline.execute_genomic_manifest_file_pipeline(task_data)
        manifest_record = self.manifest_file_dao.get(1)
        feedback_record = self.manifest_feedback_dao.get(1)

        # Test data was inserted correctly
        # manifest_file
        self.assertEqual(f"{bucket_name}/{sub_folder}/{file_name}", manifest_record.filePath)
        self.assertEqual(GenomicManifestTypes.AW1, manifest_record.manifestTypeId)
        self.assertEqual(0, manifest_record.recordCount)
        self.assertEqual(bucket_name, manifest_record.bucketName)
        self.assertEqual(f"{bucket_name}/{sub_folder}/{file_name}", manifest_record.filePath)
        self.assertEqual(file_name, manifest_record.fileName)
        # manifest_feedback
        self.assertEqual(1, feedback_record.inputManifestFileId)

    @mock.patch('rdr_service.genomic.genomic_job_controller.GenomicJobController.execute_cloud_task')
    def test_aw2f_manifest_generation_e2e(self, cloud_task):
        # Create test genomic members
        self._create_fake_datasets_for_gc_tests(4, arr_override=True,
                                                array_participants=range(1, 5),
                                                genomic_workflow_state=GenomicWorkflowState.AW0)

        # Set Up AW1 File
        # create test file
        bucket_name = _FAKE_GENOMIC_CENTER_BUCKET_A
        sub_folder = _FAKE_GENOTYPING_FOLDER  # config.getSetting(config.GENOMIC_AW2_SUBFOLDERS[1])

        # Setup Test file
        gc_manifest_file = open_genomic_set_file("Genomic-GC-Manifest-Workflow-Test-4.csv")

        gc_manifest_filename = "RDR_AoU_GEN_PKG-1908-218051.csv"
        test_date = datetime.datetime(2020, 11, 20, 0, 0, 0, 0)
        pytz.timezone('US/Central').localize(test_date)

        with clock.FakeClock(test_date):
            write_cloud_csv(
                gc_manifest_filename,
                gc_manifest_file,
                bucket=bucket_name,
                folder=sub_folder,
            )

        # Set up file/JSON
        task_data = {
            "job": GenomicJob.AW1_MANIFEST,
            "bucket": bucket_name,
            "file_data": {
                "create_feedback_record": True,
                "upload_date": test_date,
                "manifest_type": GenomicManifestTypes.AW1,
                "file_path": f"{bucket_name}/{sub_folder}/{gc_manifest_filename}"
            }
        }

        # Call pipeline function
        genomic_pipeline.execute_genomic_manifest_file_pipeline(task_data)  # job_id 1 & 2

        # Set up AW2 File
        aw2_bucket_name = _FAKE_GENOMIC_CENTER_BUCKET_A
        aw2_subfolder = config.getSetting(config.GENOMIC_AW2_SUBFOLDERS[1])

        create_ingestion_test_file(
            'RDR_AoU_GEN_TestDataManifest_3.csv',
            aw2_bucket_name,
            folder=aw2_subfolder
        )

        self._update_test_sample_ids()

        self._create_stored_samples([
            (1, 1001),
            (2, 1002),
            (3, 1003),
            (4, 1004),
        ])

        genomic_pipeline.ingest_genomic_centers_metrics_files()  # run_id = 3

        # Set up test for ignored gc_metrics records
        metrics_record_2 = self.metrics_dao.get(2)

        new_record = deepcopy(metrics_record_2)
        metrics_record_2.ignoreFlag = 1

        new_record.id = 5
        new_record.contamination = '0.1346'

        with self.metrics_dao.session() as session:
            session.add(new_record)
            session.merge(metrics_record_2)

        # run AW2F workflow before 60 days
        dt_40d = test_date + datetime.timedelta(days=40)
        with clock.FakeClock(dt_40d):
            genomic_pipeline.scan_and_complete_feedback_records()

        # Should NOT call cloud task since no records
        self.assertFalse(cloud_task.called)

        # run the AW2F manifest workflow
        dt_60d = test_date + datetime.timedelta(days=60)

        with clock.FakeClock(dt_60d):
            genomic_pipeline.scan_and_complete_feedback_records()  # run_id = 4 & 5

        # Should call cloud task since there are records
        self.assertTrue(cloud_task.called)
        cloud_task_args = cloud_task.call_args.args[0]
        req_keys = ['member_ids', 'is_job_run', 'field', 'value']
        self.assertTrue(set(cloud_task_args.keys()) == set(req_keys))

        member_ids = cloud_task_args['member_ids']
        self.assertIsNotNone(member_ids)
        self.assertTrue(len(set(member_ids)) == len(member_ids))

        # Test manifest feedback record was updated
        manifest_feedback_record = self.manifest_feedback_dao.get(1)

        self.assertEqual(1, manifest_feedback_record.inputManifestFileId)  # id = 1 is the AW1
        self.assertEqual(2, manifest_feedback_record.feedbackManifestFileId)  # id = 2 is the AW2F

        # Test the manifest file contents
        expected_aw2f_columns = (
            "PACKAGE_ID",
            "BIOBANKID_SAMPLEID",
            "BOX_STORAGEUNIT_ID",
            "BOX_ID/PLATE_ID",
            "WELL_POSITION",
            "SAMPLE_ID",
            "PARENT_SAMPLE_ID",
            "COLLECTION_TUBEID",
            "MATRIX_ID",
            "COLLECTION_DATE",
            "BIOBANK_ID",
            "SEX_AT_BIRTH",
            "AGE",
            "NY_STATE_(Y/N)",
            "SAMPLE_TYPE",
            "TREATMENTS",
            "QUANTITY_(ul)",
            "TOTAL_CONCENTRATION_(ng/uL)",
            "TOTAL_DNA(ng)",
            "VISIT_DESCRIPTION",
            "SAMPLE_SOURCE",
            "STUDY",
            "TRACKING_NUMBER",
            "CONTACT",
            "EMAIL",
            "STUDY_PI",
            "TEST_NAME",
            "FAILURE_MODE",
            "FAILURE_MODE_DESC",
            "PROCESSING_STATUS",
            "CONTAMINATION",
            "CONTAMINATION_CATEGORY",
            "CONSENT_FOR_ROR",
        )

        bucket_name = config.getSetting(config.BIOBANK_SAMPLES_BUCKET_NAME)
        sub_folder = config.BIOBANK_AW2F_SUBFOLDER
        gc_manifest_filename = gc_manifest_filename.replace('.csv', '')

        with open_cloud_file(os.path.normpath(
                f'{bucket_name}/{sub_folder}/{gc_manifest_filename}_contamination_1.csv')) as csv_file:
            csv_reader = csv.DictReader(csv_file)
            missing_cols = len(set(expected_aw2f_columns)) - len(set(csv_reader.fieldnames))
            self.assertEqual(0, missing_cols)

            rows = list(csv_reader)

            self.assertEqual(2, len(rows))

            # Test the data in the files is correct
            for r in rows:
                if r['BIOBANK_ID'] == '1':
                    self.assertEqual("PKG-1908-218051", r["PACKAGE_ID"])
                    self.assertEqual("Z1_1001", r["BIOBANKID_SAMPLEID"])
                    self.assertEqual("SU-0026388097", r["BOX_STORAGEUNIT_ID"])
                    self.assertEqual("BX-00299188", r["BOX_ID/PLATE_ID"])
                    self.assertEqual("A01", r["WELL_POSITION"])
                    self.assertEqual("1001", r["SAMPLE_ID"])
                    self.assertEqual("19206003547", r["PARENT_SAMPLE_ID"])
                    self.assertEqual("1", r["COLLECTION_TUBE_ID"])
                    self.assertEqual("1194523886", r["MATRIX_ID"])
                    self.assertEqual("", r["COLLECTION_DATE"])
                    self.assertEqual("1", r["BIOBANK_ID"])
                    self.assertEqual("F", r["SEX_AT_BIRTH"])
                    self.assertEqual("", r["AGE"])
                    self.assertEqual("Y", r["NY_STATE_(Y/N)"])
                    self.assertEqual("DNA", r["SAMPLE_TYPE"])
                    self.assertEqual("TE", r["TREATMENTS"])
                    self.assertEqual("40", r["QUANTITY_(uL)"])
                    self.assertEqual("60", r["TOTAL_CONCENTRATION_(ng/uL)"])
                    self.assertEqual("2400", r["TOTAL_DNA(ng)"])
                    self.assertEqual("All", r["VISIT_DESCRIPTION"])
                    self.assertEqual("Other", r["SAMPLE_SOURCE"])
                    self.assertEqual("PMI Coriell Samples Only", r["STUDY"])
                    self.assertEqual("475523957339", r["TRACKING_NUMBER"])
                    self.assertEqual("Samantha Wirkus", r["CONTACT"])
                    self.assertEqual("Wirkus.Samantha@mayo.edu", r["EMAIL"])
                    self.assertEqual("Josh Denny", r["STUDY_PI"])
                    self.assertEqual("aou_array", r["TEST_NAME"])
                    self.assertEqual("", r["FAILURE_MODE"])
                    self.assertEqual("", r["FAILURE_MODE_DESC"])
                    self.assertEqual("extract wgs", r['CONTAMINATION_CATEGORY'])
                if r['BIOBANK_ID'] == '2':
                    self.assertEqual("extract both", r['CONTAMINATION_CATEGORY'])
                    self.assertEqual('0.1346', r['CONTAMINATION'])
        # Test run record is success
        run_obj = self.job_run_dao.get(5)

        self.assertEqual(GenomicSubProcessResult.SUCCESS, run_obj.runResult)

        for mid in range(1, 3):
            member = self.member_dao.get(mid)
            member.aw2fManifestJobRunID = 4
            self.member_dao.update(member)

        # Continue test for AW2F remainder
        # AW2 data
        new_aw2 = create_ingestion_test_file(
            'RDR_AoU_GEN_TestDataManifest_4.csv',
            aw2_bucket_name,
            folder=aw2_subfolder
        )

        # Ingest AW2 for samples 3 & 4
        # Set up file/JSON
        task_data = {
            "job": GenomicJob.METRICS_INGESTION,
            "bucket": aw2_bucket_name,
            "file_data": {
                "create_feedback_record": False,
                "upload_date": clock.CLOCK.now(),
                "manifest_type": GenomicManifestTypes.AW2,
                "file_path": f"{bucket_name}/{aw2_subfolder}/{new_aw2}"
            }
        }

        # Call pipeline function
        genomic_pipeline.execute_genomic_manifest_file_pipeline(task_data)  # job_run_id 5 & 6

        # Generate remainder AW2F
        genomic_pipeline.send_remainder_contamination_manifests()  # job_run_id 7

        manifest_2_filepath = f'{bucket_name}/{sub_folder}/{gc_manifest_filename}_contamination_2.csv'
        with open_cloud_file(os.path.normpath(manifest_2_filepath)) as csv_file:
            csv_reader = csv.DictReader(csv_file)
            rows = list(csv_reader)
            rows.sort(key=lambda x: x['BIOBANK_ID'])

        # Test remainder AW2F contains correct records
        self.assertEqual(2, len(rows))
        self.assertEqual('3', rows[0]['BIOBANK_ID'])
        self.assertEqual('4', rows[1]['BIOBANK_ID'])

    def test_contamination_calculation_with_another_sample_viable(self):
        file_ingester = GenomicFileIngester(job_id=GenomicJob.METRICS_INGESTION)
        participant = self.data_generator.create_database_participant_summary().participant
        contaminated_sample = self.data_generator.create_database_biobank_stored_sample(
            biobankId=participant.biobankId,
            test='1ED04'
        )
        # create a viable sample
        self.data_generator.create_database_biobank_stored_sample(biobankId=participant.biobankId, test='1ED10')

        contamination_category = file_ingester.calculate_contamination_category(
            contaminated_sample.biobankStoredSampleId,
            0.09,
            GenomicSetMember(participantId=participant.participantId, biobankId=participant.biobankId)
        )
        self.assertNotEqual(GenomicContaminationCategory.TERMINAL_NO_EXTRACT, contamination_category)

        self.session.query(GenomicSampleContamination).filter(
            GenomicSampleContamination.sampleId == contaminated_sample.biobankStoredSampleId
        ).one()  # There should be a contamination record for the sample

    def test_contamination_calculation_with_another_contaminated_sample(self):
        file_ingester = GenomicFileIngester(job_id=GenomicJob.METRICS_INGESTION)
        participant = self.data_generator.create_database_participant_summary().participant
        contaminated_sample = self.data_generator.create_database_biobank_stored_sample(
            biobankId=participant.biobankId,
            test='1ED04'
        )
        other_contaminated_sample = self.data_generator.create_database_biobank_stored_sample(
            biobankId=participant.biobankId,
            test='1ED10'
        )
        self.session.add(GenomicSampleContamination(
            sampleId=other_contaminated_sample.biobankStoredSampleId,
            failedInJob=GenomicJob.METRICS_INGESTION
        ))
        self.session.commit()

        contamination_category = file_ingester.calculate_contamination_category(
            contaminated_sample.biobankStoredSampleId,
            0.09,
            GenomicSetMember(participantId=participant.participantId, biobankId=participant.biobankId)
        )
        self.assertEqual(GenomicContaminationCategory.TERMINAL_NO_EXTRACT, contamination_category)

        self.session.query(GenomicSampleContamination).filter(
            GenomicSampleContamination.sampleId == contaminated_sample.biobankStoredSampleId
        ).one()  # There should be a contamination record for the sample

    def test_contamination_calculation_with_no_other_sample(self):
        file_ingester = GenomicFileIngester(job_id=GenomicJob.METRICS_INGESTION)
        participant = self.data_generator.create_database_participant_summary().participant
        contaminated_sample = self.data_generator.create_database_biobank_stored_sample(
            biobankId=participant.biobankId,
            test='1ED04'
        )

        contamination_category = file_ingester.calculate_contamination_category(
            contaminated_sample.biobankStoredSampleId,
            0.09,
            GenomicSetMember(participantId=participant.participantId, biobankId=participant.biobankId)
        )
        self.assertEqual(GenomicContaminationCategory.TERMINAL_NO_EXTRACT, contamination_category)

        self.session.query(GenomicSampleContamination).filter(
            GenomicSampleContamination.sampleId == contaminated_sample.biobankStoredSampleId
        ).one()  # There should be a contamination record for the sample

    def _create_stored_samples(self, stored_sample_data):
        for biobank_id, stored_sample_id in stored_sample_data:
            # Create the participant and summary if needed
            participant = self.session.query(Participant).filter(
                Participant.biobankId == biobank_id
            ).one_or_none()
            if participant is None:
                self.data_generator.create_database_participant(biobankId=biobank_id)
                # self.data_generator.create_database_participant_summary(participant=participant)

            self.data_generator.create_database_biobank_stored_sample(
                biobankId=biobank_id,
                biobankStoredSampleId=stored_sample_id,
                test='1SAL2'
            )

    def test_ingest_manifest_creates_incident_then_resolved(self):
        bucket_name = _FAKE_GENOMIC_CENTER_BUCKET_A
        test_date = datetime.datetime(2020, 10, 13, 0, 0, 0, 0)
        pytz.timezone('US/Central').localize(test_date)
        gc_manifest_filename = "RDR_AoU_GEN_PKG-1908-218051.csv"

        self._create_fake_datasets_for_gc_tests(3,
                                                arr_override=True,
                                                array_participants=range(1, 4),
                                                genomic_workflow_state=GenomicWorkflowState.AW0
                                                )

        self._insert_control_sample_genomic_set_member(sample_id=30003, genome_type="aou_array")

        # Setup Test file
        gc_manifest_file = open_genomic_set_file("Genomic-GC-Manifest-Workflow-Test-Extra-Field.csv")

        with clock.FakeClock(test_date):
            write_cloud_csv(
                gc_manifest_filename,
                gc_manifest_file,
                bucket=bucket_name,
                folder=_FAKE_GENOTYPING_FOLDER,
            )

        file_name = _FAKE_GENOTYPING_FOLDER + '/' + gc_manifest_filename

        # Set up file/JSON
        task_data = {
            "job": GenomicJob.AW1_MANIFEST,
            "bucket": bucket_name,
            "file_data": {
                "create_feedback_record": True,
                "upload_date": "2020-10-13 00:00:00",
                "manifest_type": GenomicManifestTypes.AW1,
                "file_path": f"{bucket_name}/{file_name}"
            }
        }

        # Call pipeline function
        genomic_pipeline.execute_genomic_manifest_file_pipeline(task_data)  # job_id 1 & 2

        gc_manifest_file = open_genomic_set_file("Genomic-GC-Manifest-Workflow-Test-6.csv")

        all_incidents = self.incident_dao.get_all()
        correct_incident = list(filter(lambda x: gc_manifest_filename in x.message, all_incidents))
        correct_incident = correct_incident[0]

        self.assertEqual(correct_incident.status, GenomicIncidentStatus.OPEN.name)

        self.assertEqual(GenomicSubProcessResult.ERROR, self.job_run_dao.get(2).runResult)

        with clock.FakeClock(test_date):
            write_cloud_csv(
                gc_manifest_filename,
                gc_manifest_file,
                bucket=bucket_name,
                folder=_FAKE_GENOTYPING_FOLDER,
            )

        genomic_pipeline.execute_genomic_manifest_file_pipeline(task_data)

        all_incidents = self.incident_dao.get_all()
        correct_incident = list(filter(lambda x: gc_manifest_filename in x.message, all_incidents))
        correct_incident = correct_incident[0]

        self.assertEqual(correct_incident.status, GenomicIncidentStatus.RESOLVED.name)

        self.assertEqual(GenomicSubProcessResult.SUCCESS, self.job_run_dao.get(6).runResult)

    def test_aw1_load_manifest_to_raw_table(self):
        # Set up test AW1 manifest

        # Setup Test file
        aw1_manifest_file = open_genomic_set_file("Genomic-GC-Manifest-Workflow-Test-3.csv")
        aw1_manifest_filename = "RDR_AoU_GEN_PKG-1908-218051.csv"

        write_cloud_csv(
            aw1_manifest_filename,
            aw1_manifest_file,
            bucket=_FAKE_GENOMIC_CENTER_BUCKET_A,
            folder=_FAKE_GENOTYPING_FOLDER,
        )

        test_file_path = f"{_FAKE_GENOMIC_CENTER_BUCKET_A}/{_FAKE_GENOTYPING_FOLDER}/{aw1_manifest_filename}"
        # Run load job
        genomic_pipeline.load_awn_manifest_into_raw_table(test_file_path, "aw1")

        # Expected columns in table
        expected_columns = [
            "package_id",
            "biobankid_sample_id",
            "box_storageunit_id",
            "box_id_plate_id",
            "well_position",
            "sample_id",
            "parent_sample_id",
            "collection_tube_id",
            "matrix_id",
            "collection_date",
            "biobank_id",
            "sex_at_birth",
            "age",
            "ny_state",
            "sample_type",
            "treatments",
            "quantity",
            "total_concentration",
            "total_dna",
            "visit_description",
            "sample_source",
            "study",
            "tracking_number",
            "contact",
            "email",
            "study_pi",
            "site_name",
            "test_name",
            "failure_mode",
            "failure_mode_desc",
        ]

        aw1_raw_records = self.aw1_raw_dao.get_all()
        aw1_raw_records.sort(key=lambda x: x.id)

        for record in aw1_raw_records:
            if record.test_name:
                self.assertIsNotNone(record.genome_type)
                self.assertEqual(record.test_name, record.genome_type)

        # compare rows in DB to rows in manifest
        for i, aw1_file_row in enumerate(aw1_manifest_file.split("\n")):
            if i == 0 or aw1_file_row == "":
                # skip header row and trailing empty rows
                continue

            for j, aw1_file_column in enumerate(aw1_file_row.split(',')):
                aw1_file_column = aw1_file_column.strip('"')
                self.assertEqual(aw1_file_column, getattr(aw1_raw_records[i-1], expected_columns[j]))

    def test_get_latest_raw_file(self):
        aw1_manifest_file = open_genomic_set_file("Genomic-GC-Manifest-Workflow-Test-5.csv")
        for num in range(3):
            if num == 2:
                g_type = 'SEQ'
            else:
                g_type = 'GEN'
            aw1_manifest_filename = f"RDR_AoU_{g_type}_PKG-1908-218051_v{num}.csv"
            write_cloud_csv(
                aw1_manifest_filename,
                aw1_manifest_file,
                bucket=_FAKE_GENOMIC_CENTER_BUCKET_A,
                folder=_FAKE_GENOTYPING_FOLDER,
            )
            test_file_path = f"{_FAKE_GENOMIC_CENTER_BUCKET_A}/{_FAKE_GENOTYPING_FOLDER}/{aw1_manifest_filename}"
            genomic_pipeline.load_awn_manifest_into_raw_table(test_file_path, "aw1")
            time.sleep(5)

        biobank_id = '2'
        genome_file_type = 'GEN'

        all_raw_records = self.aw1_raw_dao.get_all()
        filtered_records = [rec for rec in all_raw_records
                            if rec.biobank_id == biobank_id
                            and genome_file_type in rec.file_path]
        sorted_records = sorted(filtered_records, key=lambda record: record.created, reverse=True)
        sorted_record = sorted_records[0]

        dao_record = self.aw1_raw_dao.get_raw_record_from_identifier_genome_type(
            identifier=int(biobank_id),
            genome_type='aou_array'
        )
        self.assertEqual(sorted_record.id, dao_record.id)
        self.assertEqual(sorted_record.file_path, dao_record.file_path)

    def test_aw2_array_load_manifest_to_raw_table(self):
        # Set up test AW2 manifest
        test_manifest = 'RDR_AoU_GEN_TestDataManifest.csv'

        gen_set = self.data_generator.create_database_genomic_set(
            genomicSetName=".",
            genomicSetCriteria=".",
            genomicSetVersion=1
        )

        for i in range(1, 9):
            self.data_generator.create_database_genomic_set_member(
                participantId=i,
                genomicSetId=gen_set.id,
                biobankId=i,
                sampleId=1000 + i,
                genomeType="aou_array",
            )

        # Setup Test file
        test_file_name = create_ingestion_test_file(test_manifest,
                                                          _FAKE_GENOMIC_CENTER_BUCKET_A,
                                                          folder=_FAKE_BUCKET_FOLDER)

        test_file_path = f"{_FAKE_GENOMIC_CENTER_BUCKET_A}/{_FAKE_BUCKET_FOLDER}/{test_file_name}"

        # Run load job
        genomic_pipeline.load_awn_manifest_into_raw_table(test_file_path, "aw2")

        aw2_raw_records = self.aw2_raw_dao.get_all()

        for i, record in enumerate(aw2_raw_records):
            member = self.member_dao.get(i + 1)
            self.assertIsNotNone(record.genome_type)
            self.assertEqual(record.genome_type, member.genomeType)

        index = 0
        with open(data_path(test_manifest)) as f:
            csv_reader = csv.DictReader(f)
            for row in csv_reader:
                self.assertEqual(row["Biobank ID"], aw2_raw_records[index].biobank_id)
                self.assertEqual(row["Sample ID"], aw2_raw_records[index].sample_id)
                self.assertEqual(row["Biobankid Sampleid"], aw2_raw_records[index].biobankidsampleid)
                self.assertEqual(row["LIMS ID"], aw2_raw_records[index].lims_id)
                self.assertEqual(row["Chipwellbarcode"], aw2_raw_records[index].chipwellbarcode)
                self.assertEqual(row["Call Rate"], aw2_raw_records[index].call_rate)
                self.assertEqual(row["Sex Concordance"], aw2_raw_records[index].sex_concordance)
                self.assertEqual(row["Contamination"], aw2_raw_records[index].contamination)
                self.assertEqual(row["Sample Source"], aw2_raw_records[index].sample_source)
                self.assertEqual(row["Processing Status"], aw2_raw_records[index].processing_status)
                self.assertEqual(row["Notes"], aw2_raw_records[index].notes)
                self.assertEqual(row["Pipeline ID"], aw2_raw_records[index].pipeline_id)
                self.assertEqual(row["Genome Type"], aw2_raw_records[index].genome_type)
                index += 1

        self.assertEqual(index, len(aw2_raw_records))
        self.aw2_raw_dao.truncate()

    def test_aw2_wgs_load_manifest_to_raw_table(self):
        # Set up test AW2 manifest
        test_manifest = 'RDR_AoU_SEQ_TestDataManifest.csv'

        gen_set = self.data_generator.create_database_genomic_set(
            genomicSetName=".",
            genomicSetCriteria=".",
            genomicSetVersion=1
        )

        for i in range(1, 6):
            self.data_generator.create_database_genomic_set_member(
                participantId=i,
                genomicSetId=gen_set.id,
                biobankId=i,
                sampleId=1001 + i,
                genomeType="aou_wgs",
            )

        # Setup Test file
        test_file_name = create_ingestion_test_file(test_manifest,
                                                          _FAKE_GENOMIC_CENTER_BUCKET_A,
                                                          folder=_FAKE_BUCKET_FOLDER)

        test_file_path = f"{_FAKE_GENOMIC_CENTER_BUCKET_A}/{_FAKE_BUCKET_FOLDER}/{test_file_name}"

        # Run load job
        genomic_pipeline.load_awn_manifest_into_raw_table(test_file_path, "aw2")

        aw2_raw_records = self.aw2_raw_dao.get_all()

        for i, record in enumerate(aw2_raw_records):
            member = self.member_dao.get(i + 1)
            self.assertIsNotNone(record.genome_type)
            self.assertEqual(record.genome_type, member.genomeType)

        index = 0
        with open(data_path(test_manifest)) as f:
            csv_reader = csv.DictReader(f)
            for row in csv_reader:
                self.assertEqual(row["Biobank ID"], aw2_raw_records[index].biobank_id)
                self.assertEqual(row["Sample ID"], aw2_raw_records[index].sample_id)
                self.assertEqual(row["BiobankidSampleid"], aw2_raw_records[index].biobankidsampleid)
                self.assertEqual(row["LIMS ID"], aw2_raw_records[index].lims_id)
                self.assertEqual(row["Mean Coverage"], aw2_raw_records[index].mean_coverage)
                self.assertEqual(row["Genome Coverage"], aw2_raw_records[index].genome_coverage)
                self.assertEqual(row["AoU HDR Coverage"], aw2_raw_records[index].aouhdr_coverage)
                self.assertEqual(row["Sex Concordance"], aw2_raw_records[index].sex_concordance)
                self.assertEqual(row["Contamination"], aw2_raw_records[index].contamination)
                self.assertEqual(row["Sex Ploidy"], aw2_raw_records[index].sex_ploidy)
                self.assertEqual(row["Aligned Q30 Bases"], aw2_raw_records[index].aligned_q30_bases)
                self.assertEqual(row["Array Concordance"], aw2_raw_records[index].array_concordance)
                self.assertEqual(row["Processing Status"], aw2_raw_records[index].processing_status)
                self.assertEqual(row["Notes"], aw2_raw_records[index].notes)
                self.assertEqual(row["Sample Source"], aw2_raw_records[index].sample_source)
                self.assertEqual(row["Mapped Reads pct"], aw2_raw_records[index].mapped_reads_pct)
                self.assertEqual(row["Genome Type"], aw2_raw_records[index].genome_type)
                index += 1

        self.assertEqual(index, len(aw2_raw_records))
        self.aw2_raw_dao.truncate()

    def test_aw1_genomic_incident_inserted(self):
        # Setup Test file
        gc_manifest_file = open_genomic_set_file("Genomic-GC-Manifest-Workflow-Test-6.csv")
        gc_manifest_filename = "RDR_AoU_GEN_PKG-1908-218051.csv"

        write_cloud_csv(
            gc_manifest_filename,
            gc_manifest_file,
            bucket=_FAKE_GENOMIC_CENTER_BUCKET_A,
            folder=_FAKE_GENOTYPING_FOLDER,
        )

        file_name = _FAKE_GENOTYPING_FOLDER + '/' + gc_manifest_filename

        # Set up file/JSON
        task_data = {
            "job": GenomicJob.AW1_MANIFEST,
            "bucket": _FAKE_GENOMIC_CENTER_BUCKET_A,
            "file_data": {
                "create_feedback_record": True,
                "upload_date": "2020-10-13 00:00:00",
                "manifest_type": GenomicManifestTypes.AW1,
                "file_path": f"{_FAKE_GENOMIC_CENTER_BUCKET_A}/{file_name}"
            }
        }

        # Call pipeline function
        genomic_pipeline.execute_genomic_manifest_file_pipeline(task_data)  # job_id 1 & 2

        incident_dao = GenomicIncidentDao()

        incidents = incident_dao.get_all()

        self.assertEqual(2, len(incidents))

        self.assertEqual("1", incidents[0].biobank_id)
        self.assertEqual("1001", incidents[0].sample_id)
        self.assertEqual("1", incidents[0].collection_tube_id)
        self.assertEqual(2, incidents[0].source_job_run_id)
        self.assertEqual(1, incidents[0].source_file_processed_id)
        self.assertEqual("UNABLE_TO_FIND_MEMBER", incidents[0].code)

        self.assertEqual("2", incidents[1].biobank_id)
        self.assertEqual("1002", incidents[1].sample_id)
        self.assertEqual("100002", incidents[1].collection_tube_id)
        self.assertEqual(2, incidents[1].source_job_run_id)
        self.assertEqual(1, incidents[1].source_file_processed_id)
        self.assertEqual("UNABLE_TO_FIND_MEMBER", incidents[1].code)

    def test_ingest_genomic_incident_extra_fields(self):
        # Setup Test file
        gc_manifest_file = open_genomic_set_file("Genomic-GC-Manifest-Workflow-Test-Extra-Field.csv")
        gc_manifest_filename = "RDR_AoU_GEN_PKG-1908-218051.csv"

        write_cloud_csv(
            gc_manifest_filename,
            gc_manifest_file,
            bucket=_FAKE_GENOMIC_CENTER_BUCKET_A,
            folder=_FAKE_GENOTYPING_FOLDER,
        )

        file_name = _FAKE_GENOTYPING_FOLDER + '/' + gc_manifest_filename

        # Set up file/JSON
        task_data = {
            "job": GenomicJob.AW1_MANIFEST,
            "bucket": _FAKE_GENOMIC_CENTER_BUCKET_A,
            "file_data": {
                "create_feedback_record": True,
                "upload_date": "2020-10-13 00:00:00",
                "manifest_type": GenomicManifestTypes.AW1,
                "file_path": f"{_FAKE_GENOMIC_CENTER_BUCKET_A}/{file_name}"
            }
        }

        # Call pipeline function
        genomic_pipeline.execute_genomic_manifest_file_pipeline(task_data)  # job_id 1 & 2

        incident_dao = GenomicIncidentDao()
        incidents = incident_dao.get_all()

        self.assertTrue(any(obj.code == 'FILE_VALIDATION_FAILED_STRUCTURE' for obj in incidents))
        self.assertTrue(any('Extra fields: extrafield' in obj.message for obj in incidents))

    def test_aw2_genomic_incident_inserted(self):
        # set up test file
        test_file = 'RDR_AoU_GEN_TestDataManifest.csv'
        subfolder = config.getSetting(config.GENOMIC_AW2_SUBFOLDERS[1])
        test_file_name = create_ingestion_test_file(
            test_file,
            _FAKE_GENOMIC_CENTER_BUCKET_A,
            folder=subfolder,
            include_sub_num=True
        )

        # run the GC Metrics Ingestion workflow via cloud task
        # Set up file/JSON
        task_data = {
            "job": GenomicJob.METRICS_INGESTION,
            "bucket": _FAKE_GENOMIC_CENTER_BUCKET_A,
            "file_data": {
                "create_feedback_record": False,
                "upload_date": "2020-10-13 00:00:00",
                "manifest_type": GenomicManifestTypes.AW2,
                "file_path": f"{_FAKE_GENOMIC_CENTER_BUCKET_A}/{subfolder}/{test_file_name}"
            }
        }

        # Execute from cloud task
        genomic_pipeline.execute_genomic_manifest_file_pipeline(task_data)

        incident_dao = GenomicIncidentDao()
        incidents = incident_dao.get_all()

        for i, incident in enumerate(incidents):
            message = f'{GenomicJob.METRICS_INGESTION.name}: Cannot find genomic set member for bid, sample_id: ' \
                      f'T{i+1}, ' \
                      f'100{i+1}'
            self.assertIsNotNone(incident.message)
            self.assertEqual(message, incident.message)
            self.assertEqual(0, incident.slack_notification)
            self.assertIsNone(incident.slack_notification_date)

        self.assertEqual(8, len(incidents))
        self.assertEqual("1", incidents[0].biobank_id)
        self.assertEqual("1001", incidents[0].sample_id)
        self.assertEqual(2, incidents[0].source_job_run_id)
        self.assertEqual(1, incidents[0].source_file_processed_id)
        self.assertEqual("UNABLE_TO_FIND_MEMBER", incidents[0].code)

        self.assertEqual("2", incidents[1].biobank_id)
        self.assertEqual("1002", incidents[1].sample_id)
        self.assertEqual(2, incidents[1].source_job_run_id)
        self.assertEqual(1, incidents[1].source_file_processed_id)
        self.assertEqual("UNABLE_TO_FIND_MEMBER", incidents[1].code)

    def test_aw2_array_pipeline_id_validation(self):
        # Setup Test file
        bucket_name = _FAKE_GENOMIC_CENTER_BUCKET_A
        subfolder = config.getSetting(config.GENOMIC_AW2_SUBFOLDERS[1])
        test_file = 'RDR_AoU_GEN_TestDataManifestFailedPipelineID.csv'
        test_date = datetime.datetime(2020, 10, 13, 0, 0, 0, 0)
        pytz.timezone('US/Central').localize(test_date)

        job_id = GenomicJob.METRICS_INGESTION

        with clock.FakeClock(test_date):
            test_file_name = create_ingestion_test_file(test_file, bucket_name,
                                                              folder=subfolder,
                                                              include_sub_num=True)

        self._create_fake_datasets_for_gc_tests(2, arr_override=True,
                                                array_participants=(1, 2),
                                                genomic_workflow_state=GenomicWorkflowState.AW1)

        self._update_test_sample_ids()

        # run the GC Metrics Ingestion workflow via cloud task
        # Set up file/JSON
        task_data = {
            "job": job_id,
            "bucket": bucket_name,
            "file_data": {
                "create_feedback_record": False,
                "upload_date": test_date.isoformat(),
                "manifest_type": GenomicManifestTypes.AW2,
                "file_path": f"{bucket_name}/{subfolder}/{test_file_name}"
            }
        }

        self._create_stored_samples([
            (1, 1001),
            (2, 1002)
        ])

        # Execute from cloud task
        genomic_pipeline.execute_genomic_manifest_file_pipeline(task_data)

        current_run = list(filter(lambda x: x.jobId == job_id, self.job_run_dao.get_all()))[0]
        self.assertEqual(current_run.runResult, GenomicSubProcessResult.ERROR)

        related_incident = list(filter(lambda x: 'FILE_VALIDATION_FAILED_VALUES' in x.code, self.incident_dao.get_all()))[0]
        self.assertEqual(related_incident.message, 'METRICS_INGESTION: Value for Pipeline ID is invalid: invalid')
        self.assertEqual(related_incident.slack_notification, 1)
        self.assertIsNotNone(related_incident.slack_notification_date)

    def test_aw1_genomic_missing_header_cleaned_inserted(self):
        # Setup Test file
        gc_manifest_file = open_genomic_set_file("Genomic-GC-Manifest-Workflow-Missing-Header.csv")
        gc_manifest_filename = "RDR_AoU_GEN_PKG-1908-218051.csv"

        write_cloud_csv(
            gc_manifest_filename,
            gc_manifest_file,
            bucket=_FAKE_GENOMIC_CENTER_BUCKET_A,
            folder=_FAKE_GENOTYPING_FOLDER,
        )

        file_name = _FAKE_GENOTYPING_FOLDER + '/' + gc_manifest_filename

        # Set up file/JSON
        task_data = {
            "job": GenomicJob.AW1_MANIFEST,
            "bucket": _FAKE_GENOMIC_CENTER_BUCKET_A,
            "file_data": {
                "create_feedback_record": True,
                "upload_date": "2020-10-13 00:00:00",
                "manifest_type": GenomicManifestTypes.AW1,
                "file_path": f"{_FAKE_GENOMIC_CENTER_BUCKET_A}/{file_name}"
            }
        }

        # Call pipeline function
        genomic_pipeline.execute_genomic_manifest_file_pipeline(task_data)

        # Test the data was ingested OK
        files_processed = self.file_processed_dao.get_all()
        self.assertEqual(files_processed[0].fileName, gc_manifest_filename)
        self.assertEqual(files_processed[0].fileResult, GenomicSubProcessResult.SUCCESS)
        # Check record count for manifest record
        manifest_record = self.manifest_file_dao.get(1)
        self.assertEqual(file_name.split('/')[1], manifest_record.fileName)
        self.assertEqual(GenomicSubProcessResult.SUCCESS, self.job_run_dao.get(2).runResult)

    def test_feedback_records_reconciled(self):

        aw1_manifest_file = open_genomic_set_file("Genomic-GC-Manifest-Workflow-Test-3.csv")
        aw1_manifest_filename = "RDR_AoU_GEN_PKG-1908-218051.csv"
        write_cloud_csv(
            aw1_manifest_filename,
            aw1_manifest_file,
            bucket=_FAKE_GENOMIC_CENTER_BUCKET_A,
            folder=_FAKE_GENOTYPING_FOLDER,
        )
        test_file_path = f"{_FAKE_GENOMIC_CENTER_BUCKET_A}/{_FAKE_GENOTYPING_FOLDER}/{aw1_manifest_filename}"
        genomic_pipeline.load_awn_manifest_into_raw_table(test_file_path, "aw1")

        raw_records = self.aw1_raw_dao.get_all()
        raw_records = [obj for obj in raw_records if obj.sample_id != ""]

        gen_set = self.data_generator.create_database_genomic_set(
            genomicSetName=".",
            genomicSetCriteria=".",
            genomicSetVersion=1
        )

        gen_job_run = self.data_generator.create_database_genomic_job_run(
            jobId=GenomicJob.AW1_MANIFEST,
            startTime=clock.CLOCK.now(),
            runResult=GenomicSubProcessResult.SUCCESS
        )

        gen_processed_file = self.data_generator.create_database_genomic_file_processed(
            runId=gen_job_run.id,
            startTime=clock.CLOCK.now(),
            filePath=test_file_path,
            bucketName=_FAKE_GENOMIC_CENTER_BUCKET_A,
            fileName=aw1_manifest_filename,
        )

        manifest = self.data_generator.create_database_genomic_manifest_file(
            manifestTypeId=2,
            filePath=test_file_path
        )

        self.data_generator.create_database_genomic_manifest_feedback(
            inputManifestFileId=manifest.id,
            feedbackRecordCount=2
        )

        for raw in raw_records:
            participant = self.data_generator.create_database_participant()
            gen_member = self.data_generator.create_database_genomic_set_member(
                genomicSetId=gen_set.id,
                biobankId="100153482",
                sampleId=raw.sample_id,
                genomeType="aou_array",
                participantId=participant.participantId
            )

            self.data_generator.create_database_genomic_gc_validation_metrics(
                genomicSetMemberId=gen_member.id,
                genomicFileProcessedId=gen_processed_file.id,
                contamination=0.002
            )

        current_records = self.manifest_feedback_dao.get_feedback_reconcile_records()

        self.assertTrue(len(current_records))
        current_record = current_records[0]
        self.assertNotEqual(current_record.raw_feedback_count, current_record.feedbackRecordCount)
        self.assertGreater(current_record.raw_feedback_count, current_record.feedbackRecordCount)

        genomic_pipeline.feedback_record_reconciliation()

        updated_records = self.manifest_feedback_dao.get_feedback_reconcile_records(test_file_path)
        self.assertTrue(len(updated_records))
        updated_record = updated_records[0]
        self.assertEqual(current_record.feedback_id, updated_record.feedback_id)
        self.assertEqual(current_record.raw_feedback_count, updated_record.feedbackRecordCount)

    def test_resolve_gc_missing_files(self):

        bucket_name = _FAKE_GENOMIC_CENTER_BUCKET_BAYLOR

        gen_set = self.data_generator.create_database_genomic_set(
            genomicSetName=".",
            genomicSetCriteria=".",
            genomicSetVersion=1
        )

        gen_job_run = self.data_generator.create_database_genomic_job_run(
            jobId=GenomicJob.AW1_MANIFEST,
            startTime=clock.CLOCK.now(),
            runResult=GenomicSubProcessResult.SUCCESS
        )

        gen_processed_file = self.data_generator.create_database_genomic_file_processed(
            runId=gen_job_run.id,
            startTime=clock.CLOCK.now(),
            filePath='/test_file_path',
            bucketName='test_bucket',
            fileName='test_file_name',
        )

        array_metrics = []
        wgs_metrics = []
        wgs_member_sample_ids = []

        for num in range(2):
            array_member = self.data_generator.create_database_genomic_set_member(
                genomicSetId=gen_set.id,
                biobankId=f"1001534{num}",
                sampleId=f"21042005280{num}",
                genomeType="aou_array",
                genomicWorkflowState=GenomicWorkflowState.AW1
            )

            array_metric = self.data_generator.create_database_genomic_gc_validation_metrics(
                genomicSetMemberId=array_member.id,
                genomicFileProcessedId=gen_processed_file.id,
                chipwellbarcode=f"1000{num}_R01C0{num}"
            )
            array_metrics.append(array_metric)

            wgs_member = self.data_generator.create_database_genomic_set_member(
                genomicSetId=gen_set.id,
                biobankId=f"1001534{num}",
                sampleId=f"21042005290{num}",
                genomeType="aou_wgs",
                genomicWorkflowState=GenomicWorkflowState.AW1
            )
            wgs_member_sample_ids.append(f"21042005290{num}")

            wgs_metric = self.data_generator.create_database_genomic_gc_validation_metrics(
                genomicSetMemberId=wgs_member.id,
                genomicFileProcessedId=gen_processed_file.id
            )
            wgs_metrics.append(wgs_metric)

        for i, attr in enumerate(array_file_types_attributes[0:2]):
            file_name = f"1000{i}_R01C0{i}_{attr['file_type']}"
            self.data_generator.create_database_gc_data_file_record(
                file_path=f'{bucket_name}/test_data_folder/{file_name}',
                gc_site_id='rdr',
                bucket_name=bucket_name,
                file_name=file_name,
                file_type=attr['file_type'],
                identifier_type='chipwellbarcode',
                identifier_value=f'1000{i}_R01C0{i}'
            )

            self.data_generator.create_database_gc_data_missing_file(
                gc_validation_metric_id=array_metrics[i].id,
                run_id=gen_job_run.id,
                gc_site_id='rdr',
                file_type=attr['file_type']
            )

        for i, attr in enumerate(wgs_file_types_attributes[0:2]):
            file_name = f"1000{i}_R01C0{i}.{attr['file_type']}"
            self.data_generator.create_database_gc_data_file_record(
                file_path=f'{bucket_name}/test_data_folder/{file_name}',
                gc_site_id='rdr',
                bucket_name=bucket_name,
                file_name=file_name,
                file_type=attr['file_type'],
                identifier_type='sample_id',
                identifier_value=wgs_member_sample_ids[i]
            )

            self.data_generator.create_database_gc_data_missing_file(
                gc_validation_metric_id=wgs_metrics[i].id,
                run_id=gen_job_run.id,
                gc_site_id='rdr',
                file_type=attr['file_type']
            )

        all_data_files = self.data_file_dao.get_all()
        all_missing_files = self.missing_file_dao.get_all()

        self.assertEqual(len(all_data_files), 4)
        self.assertEqual(len(all_missing_files), 4)

        need_resolve_files = self.missing_file_dao.get_files_to_resolve()

        self.assertEqual(len(need_resolve_files), 4)

        array = [file for file in need_resolve_files if file.identifier_type == 'chipwellbarcode']
        wgs = [file for file in need_resolve_files if file.identifier_type == 'sample_id']

        self.assertEqual(len(need_resolve_files) / 2, len(array))
        self.assertEqual(len(need_resolve_files) / 2, len(wgs))

        genomic_pipeline.genomic_missing_files_resolve()

        need_resolve_files = self.missing_file_dao.get_files_to_resolve()

        self.assertEqual(len(need_resolve_files), 0)

        all_missing_files = self.missing_file_dao.get_all()

        self.assertTrue(all(
            file for file in all_missing_files
            if file.resolved == 1 and file.resolved_date is not None
        ))

        # Test file not in GC Data File not returned by get_files_to_resolve()
        self.data_generator.create_database_gc_data_missing_file(
            gc_validation_metric_id=1,
            run_id=gen_job_run.id,
            gc_site_id='rdr',
            file_type='vcf.gz.tbi',
        )
        need_resolve_files = self.missing_file_dao.get_files_to_resolve()

        self.assertEqual(len(need_resolve_files), 0)

    def test_missing_files_resolved_clean_up(self):

        gen_set = self.data_generator.create_database_genomic_set(
            genomicSetName=".",
            genomicSetCriteria=".",
            genomicSetVersion=1
        )

        gen_member = self.data_generator.create_database_genomic_set_member(
            genomicSetId=gen_set.id,
            biobankId="100153482",
            sampleId="21042005280",
            genomeType="aou_array",
            genomicWorkflowState=GenomicWorkflowState.AW1
        )

        gen_job_run = self.data_generator.create_database_genomic_job_run(
            jobId=GenomicJob.AW1_MANIFEST,
            startTime=clock.CLOCK.now(),
            runResult=GenomicSubProcessResult.SUCCESS
        )

        gen_processed_file = self.data_generator.create_database_genomic_file_processed(
            runId=gen_job_run.id,
            startTime=clock.CLOCK.now(),
            filePath='/test_file_path',
            bucketName='test_bucket',
            fileName='test_file_name',
        )

        gen_metric = self.data_generator.create_database_genomic_gc_validation_metrics(
            genomicSetMemberId=gen_member.id,
            genomicFileProcessedId=gen_processed_file.id
        )

        today = datetime.datetime.today()
        date_one = today - datetime.timedelta(5)
        date_two = today - datetime.timedelta(2)

        for num in range(12):
            self.data_generator.create_database_gc_data_missing_file(
                resolved=1 if num > 3 else 0,
                resolved_date=date_one if num % 2 == 0 else date_two,
                gc_validation_metric_id=gen_metric.id,
                run_id=gen_job_run.id,
                gc_site_id='rdr',
                file_type=random.choice(array_file_types_attributes)['file_type']
            )

        current_missing_files = self.missing_file_dao.get_all()
        self.assertEqual(len(current_missing_files), 12)

        delete_eligible = [file for file in current_missing_files if file.resolved == 1 and file.resolved_date is not
                           None]

        self.assertEqual(len(delete_eligible), 8)

        will_be_deleted = [file for file in delete_eligible if file.resolved_date.date() == date_one.date()]

        self.assertEqual(len(will_be_deleted), 4)

        genomic_pipeline.genomic_missing_files_clean_up(num_days=4)

        current_missing_files = self.missing_file_dao.get_all()

        self.assertEqual(len(current_missing_files), 8)

        for file in will_be_deleted:
            self.assertIsNone(self.missing_file_dao.get(file.id))

    def test_update_members_state_resolved_data_files(self):
        # create test genomic set
        self.data_generator.create_database_genomic_set(
            genomicSetName='test',
            genomicSetCriteria='.',
            genomicSetVersion=1
        )

        # Create test members
        for i in range(1, 4):
            for genome_type in ("aou_array", "aou_wgs"):
                self.data_generator.create_database_genomic_set_member(
                    participantId=i,
                    genomicSetId=1,
                    biobankId=i,
                    collectionTubeId=10 + i,
                    sampleId=100 + i,
                    gcManifestParentSampleId=1000 + i,
                    genomeType=genome_type,
                    genomicWorkflowState=GenomicWorkflowState.GC_DATA_FILES_MISSING
                )
        members = [(i.id, i.genomeType) for i in self.member_dao.get_all()]

        # Create test metrics for members with data files
        for member in members:
            if member[1] == "aou_array":
                self.data_generator.create_database_genomic_gc_validation_metrics(
                    genomicSetMemberId=member[0],
                    idatRedReceived=1,
                    idatRedPath="test/path",
                    idatGreenReceived=1,
                    idatGreenPath="test/path",
                    idatRedMd5Received=1,
                    idatRedMd5Path="test/path",
                    idatGreenMd5Received=0 if member[0] == 5 else 1,  # one still missing a file
                    idatGreenMd5Path="test/path",
                    vcfReceived=1,
                    vcfPath="test/path",
                    vcfMd5Received=1,
                    vcfMd5Path="test/path",
                    vcfTbiReceived=1,
                    vcfTbiPath="test/path",
                )

            if member[1] == "aou_wgs":
                self.data_generator.create_database_genomic_gc_validation_metrics(
                    genomicSetMemberId=member[0],
                    hfVcfReceived=1,
                    hfVcfPath="test/path",
                    hfVcfTbiReceived=1,
                    hfVcfTbiPath="test/path",
                    hfVcfMd5Received=0 if member[0] == 6 else 1,  # one still missing a file,
                    hfVcfMd5Path="test/path",
                    cramReceived=1,
                    cramPath="test/path",
                    cramMd5Received=1,
                    cramMd5Path="test/path",
                    craiReceived=1,
                    craiPath="test/path",
                )

        # run update_members_state_resolved_data_files pipeline
        genomic_pipeline.update_members_state_resolved_data_files()

        # Test all members are in correct state
        members = self.member_dao.get_all()
        for member in members:
            if member.id in (5, 6):
                self.assertEqual(GenomicWorkflowState.GC_DATA_FILES_MISSING, member.genomicWorkflowState)
            elif member.genomeType == "aou_array":
                self.assertEqual(GenomicWorkflowState.GEM_READY, member.genomicWorkflowState)
            else:
                self.assertEqual(GenomicWorkflowState.CVL_READY, member.genomicWorkflowState)

    # DA-2934 Reconciliation process deprecated
    # def test_reconcile_gc_data_file_to_table(self):
    #     # Create files in bucket
    #     array_prefix = "Genotyping_sample_raw_data"
    #
    #     array_test_files_jh = (
    #         f'{array_prefix}/10001_R01C01.vcf.gz',
    #         f'{array_prefix}/10001_R01C01.vcf.gz.tbi',
    #         f'{array_prefix}/10001_R01C01.vcf.gz.md5sum',
    #         f'{array_prefix}/10001_R01C01_Red.idat',
    #         f'{array_prefix}/10001_R01C01_Grn.idat',
    #         f'{array_prefix}/10001_R01C01_Red.idat.md5sum',
    #         f'{array_prefix}/10002_R01C02.vcf.gz',
    #         f'{array_prefix}/10002_R01C02.vcf.gz.tbi',
    #         f'{array_prefix}/10002_R01C02.vcf.gz.md5sum',
    #         f'{array_prefix}/10002_R01C02_Red.idat',
    #         f'{array_prefix}/10002_R01C02_Grn.idat',
    #         f'{array_prefix}/10002_R01C02_Red.idat.md5sum',
    #         f'{array_prefix}/10002_R01C02_Grn.idat.md5sum',
    #     )
    #     for file in array_test_files_jh:
    #         write_cloud_csv(
    #             file,
    #             "atgcatgc",
    #             bucket=_FAKE_GENOMIC_CENTER_BUCKET_BAYLOR,
    #         )
    #
    #     # insert file record into the the gc_data_file_table
    #     self.data_generator.create_database_gc_data_file_record(
    #         file_path=f"{_FAKE_GENOMIC_CENTER_BUCKET_BAYLOR}/{array_test_files_jh[0]}",
    #         gc_site_id="jh",
    #         bucket_name=_FAKE_GENOMIC_CENTER_BUCKET_BAYLOR,
    #         file_prefix=array_prefix,
    #         file_name=array_test_files_jh[0],
    #         file_type="vcf.gz",
    #         identifier_type="chipwellbarcode",
    #         identifier_value="10001_R01C01",
    #     )
    #
    #     nonprod_dict = {
    #         "fake_genomic_center_bucket-baylor": ["Genotyping_sample_raw_data", "Wgs_sample_raw_data"],
    #     }
    #
    #     config.override_setting(config.DATA_BUCKET_SUBFOLDERS_PROD, nonprod_dict)
    #
    #     genomic_pipeline.reconcile_gc_data_file_to_table()
    #
    #     # Test files inserted into genomic_gc_data_file
    #     gc_data_files = self.data_file_dao.get_all()
    #
    #     self.assertEqual(13, len(gc_data_files))
    #     for file in gc_data_files:
    #         self.assertEqual('jh', file.gc_site_id)
    #         self.assertEqual(array_prefix, file.file_prefix)
    #
    #     runs = self.job_run_dao.get_all()
    #     self.assertEqual(GenomicSubProcessResult.SUCCESS, runs[0].runResult)

    # Disabling this job until further notice
    # def test_reconcile_raw_to_aw1_ingested(self):
    #     # Raw table needs resetting for this test when running full suite
    #     self.aw1_raw_dao.truncate()
    #
    #     # create genomic set
    #     self.data_generator.create_database_genomic_set(
    #         genomicSetName='test',
    #         genomicSetCriteria='.',
    #         genomicSetVersion=1
    #     )
    #
    #     # create genomic set members
    #     for i in range(1, 6):
    #         self.data_generator.create_database_genomic_set_member(
    #             participantId=i,
    #             genomicSetId=1,
    #             biobankId=i,
    #             collectionTubeId=100,
    #             genomeType="aou_array",
    #             genomicWorkflowState=GenomicWorkflowState.AW0,
    #             genomicWorkflowStateStr="AW0",
    #         )
    #
    #     # create control parent sample
    #     self.data_generator.create_database_genomic_set_member(
    #         genomicSetId=1,
    #         biobankId='HG-1005',
    #         collectionTubeId=100,
    #         genomeType="aou_array",
    #         genomicWorkflowState=GenomicWorkflowState.CONTROL_SAMPLE
    #     )
    #
    #     # Set up replated samples test
    #     self.data_generator.create_database_genomic_set_member(
    #         participantId=6,
    #         genomicSetId=1,
    #         biobankId=6,
    #         collectionTubeId=1006,
    #         sampleId=10006,
    #         genomeType="aou_array",
    #         genomicWorkflowState=GenomicWorkflowState.AW2,
    #         genomicWorkflowStateStr="AW2",
    #     )
    #
    #     # The replate-request
    #     self.data_generator.create_database_genomic_set_member(
    #         participantId=6,
    #         genomicSetId=1,
    #         biobankId=6,
    #         collectionTubeId=1006,
    #         genomeType="aou_array",
    #         genomicWorkflowState=GenomicWorkflowState.EXTRACT_REQUESTED,
    #         genomicWorkflowStateStr="EXTRACT_REQUESTED",
    #         replatedMemberId=7,
    #     )
    #
    #     # Set up test AW1
    #     aw1_manifest_file = open_genomic_set_file("Genomic-GC-Manifest-Workflow-Test-7.csv")
    #     aw1_manifest_filename = "RDR_AoU_GEN_PKG-1908-218051.csv"
    #
    #     write_cloud_csv(
    #         aw1_manifest_filename,
    #         aw1_manifest_file,
    #         bucket=_FAKE_GENOMIC_CENTER_BUCKET_A,
    #         folder=_FAKE_GENOTYPING_FOLDER,
    #     )
    #     test_file_path = f"{_FAKE_GENOMIC_CENTER_BUCKET_A}/{_FAKE_GENOTYPING_FOLDER}/{aw1_manifest_filename}"
    #     self.data_generator.create_database_genomic_job_run(
    #         jobId=GenomicJob.AW1_MANIFEST,
    #         startTime=clock.CLOCK.now()
    #     )
    #     self.data_generator.create_database_genomic_file_processed(
    #         runId=1,
    #         startTime=clock.CLOCK.now(),
    #         filePath=test_file_path,
    #         bucketName=_FAKE_GENOMIC_CENTER_BUCKET_A,
    #         fileName=aw1_manifest_filename,
    #     )
    #
    #     # Run load job
    #     genomic_pipeline.load_awn_manifest_into_raw_table(test_file_path, "aw1")
    #
    #     genomic_pipeline.reconcile_raw_to_aw1_ingested()
    #
    #     member1 = self.member_dao.get(1)
    #     self.assertEqual('1', member1.biobankId)
    #     self.assertEqual('1001', member1.sampleId)
    #     self.assertEqual('1', member1.collectionTubeId)
    #     self.assertEqual('jh', member1.gcSiteId)
    #     self.assertEqual(GenomicWorkflowState.AW1, member1.genomicWorkflowState)
    #     self.assertEqual(1, member1.aw1FileProcessedId)
    #
    #     member_cntrl = self.member_dao.get(6)
    #     self.assertEqual('HG-1005', member_cntrl.biobankId)
    #     self.assertEqual('1005', member_cntrl.sampleId)

    def test_reconcile_raw_to_aw2_ingested(self):
        # Basic Setup
        self.data_generator.create_database_genomic_job_run(
            jobId=GenomicJob.AW1_MANIFEST,
            startTime=clock.CLOCK.now()
        )
        # create genomic set
        self.data_generator.create_database_genomic_set(
            genomicSetName='test',
            genomicSetCriteria='.',
            genomicSetVersion=1
        )
        # insert set members
        stored_samples = []
        for i in range(1, 7):
            self.data_generator.create_database_genomic_set_member(
                participantId=i,
                genomicSetId=1,
                biobankId=i,
                collectionTubeId=100+i,
                sampleId=1000+i,
                genomeType="aou_array",
                genomicWorkflowState=GenomicWorkflowState.EXTRACT_REQUESTED if i == 6 else GenomicWorkflowState.AW1,
                genomicWorkflowStateStr="EXTRACT_REQUESTED" if i == 6 else "AW1",
                replatedMemberId=1 if i == 6 else None,
            )
            ss = (i, 100+i)
            stored_samples.append(ss)

        self._create_stored_samples(stored_samples)

        # Setup Test file
        test_file_name = create_ingestion_test_file('RDR_AoU_SEQ_TestDataManifest.csv',
                                                          _FAKE_GENOMIC_CENTER_BUCKET_A,
                                                          folder=_FAKE_BUCKET_FOLDER)

        test_file_path = f"{_FAKE_GENOMIC_CENTER_BUCKET_A}/{_FAKE_BUCKET_FOLDER}/{test_file_name}"

        self.data_generator.create_database_genomic_file_processed(
            runId=1,
            startTime=clock.CLOCK.now(),
            filePath=test_file_path,
            bucketName=_FAKE_GENOMIC_CENTER_BUCKET_A,
            fileName=test_file_name,
        )

        # Run load job
        genomic_pipeline.load_awn_manifest_into_raw_table(test_file_path, "aw2")

        self.data_generator.create_database_genomic_file_processed(
            runId=1,
            startTime=clock.CLOCK.now(),
            filePath=test_file_path,
            bucketName=_FAKE_GENOMIC_CENTER_BUCKET_A,
            fileName=test_file_name,
        )

        genomic_pipeline.load_awn_manifest_into_raw_table(test_file_path, "aw2")

        genomic_pipeline.reconcile_raw_to_aw2_ingested()

        metrics = self.metrics_dao.get_all()
        self.assertEqual(4, len(metrics))

    def test_raw_aw1_delete_from_filepath(self):
        # Raw table needs resetting for this test when running full suite
        self.aw1_raw_dao.truncate()

        # Setup Test file
        gc_manifest_file = open_genomic_set_file("Genomic-GC-Manifest-Workflow-Test-Extra-Field.csv")
        gc_manifest_filename = "RDR_AoU_GEN_PKG-1908-218051.csv"

        write_cloud_csv(
            gc_manifest_filename,
            gc_manifest_file,
            bucket=_FAKE_GENOMIC_CENTER_BUCKET_A,
            folder=_FAKE_GENOTYPING_FOLDER,
        )

        file_name = _FAKE_GENOTYPING_FOLDER + '/' + gc_manifest_filename

        # Set up file/JSON
        task_data = {
            "job": GenomicJob.AW1_MANIFEST,
            "bucket": _FAKE_GENOMIC_CENTER_BUCKET_A,
            "file_data": {
                "create_feedback_record": True,
                "upload_date": "2020-10-13 00:00:00",
                "manifest_type": GenomicManifestTypes.AW1,
                "file_path": f"{_FAKE_GENOMIC_CENTER_BUCKET_A}/{file_name}"
            }
        }
        # Run load job
        genomic_pipeline.load_awn_manifest_into_raw_table(f"{_FAKE_GENOMIC_CENTER_BUCKET_A}/{file_name}", "aw1")

        # Call pipeline function
        genomic_pipeline.execute_genomic_manifest_file_pipeline(task_data)  # job_id 1 & 2

        records = self.aw1_raw_dao.get_all()
        self.assertEqual(0, len(records))

    def test_raw_aw2_delete_from_filepath(self):
        # Raw table needs resetting for this test when running full suite
        self.aw2_raw_dao.truncate()

        # Setup Test file
        manifest_filename = "RDR_AoU_SEQ_TestBadStructureDataManifest.csv"
        manifest_file = open_genomic_set_file(manifest_filename)

        write_cloud_csv(
            manifest_filename,
            manifest_file,
            bucket=_FAKE_GENOMIC_CENTER_BUCKET_A,
            folder=_FAKE_GENOTYPING_FOLDER,
        )

        file_name = _FAKE_GENOTYPING_FOLDER + '/' + manifest_filename

        # Set up file/JSON
        task_data = {
            "job": GenomicJob.METRICS_INGESTION,
            "bucket": _FAKE_GENOMIC_CENTER_BUCKET_A,
            "file_data": {
                "create_feedback_record": True,
                "upload_date": "2020-10-13 00:00:00",
                "manifest_type": GenomicManifestTypes.AW1,
                "file_path": f"{_FAKE_GENOMIC_CENTER_BUCKET_A}/{file_name}"
            }
        }
        # Run load job
        genomic_pipeline.load_awn_manifest_into_raw_table(f"{_FAKE_GENOMIC_CENTER_BUCKET_A}/{file_name}", "aw2")

        # Call pipeline function
        genomic_pipeline.execute_genomic_manifest_file_pipeline(task_data)  # job_id 1 & 2

        records = self.aw2_raw_dao.get_all()
        self.assertEqual(0, len(records))

    def test_reconcile_informing_loop(self):
        event_dao = UserEventMetricsDao()
        event_dao.truncate()  # for test suite

        for pid in range(8):
            self.data_generator.create_database_participant(participantId=1+pid, biobankId=1+pid)
        # Set up initial job run ID
        self.data_generator.create_database_genomic_job_run(
            jobId=GenomicJob.METRICS_FILE_INGEST,
            startTime=clock.CLOCK.now()
        )

        # Set up ingested metrics data
        events = ['gem.informing_loop.started',
                  'gem.informing_loop.screen8_no',
                  'gem.informing_loop.screen8_yes',
                  'hdr.informing_loop.started',
                  'gem.informing_loop.screen3']

        for p in range(4):
            for i in range(len(events)):
                self.data_generator.create_database_genomic_user_event_metrics(
                    created=clock.CLOCK.now(),
                    modified=clock.CLOCK.now(),
                    participant_id=p+1,
                    created_at=datetime.datetime(2021, 12, 29, 00) + datetime.timedelta(hours=i),
                    event_name=events[i],
                    run_id=1,
                    ignore_flag=0,
                    reconcile_job_run_id=1 if p == 3 and i in [0, 2] else None  # For edge case
                )

        # Insert last event for pid 5 (test for no IL response)
        self.data_generator.create_database_genomic_user_event_metrics(
            created=clock.CLOCK.now(),
            modified=clock.CLOCK.now(),
            participant_id=5,
            created_at=datetime.datetime(2021, 12, 29, 00),
            event_name='gem.informing_loop.started',
            run_id=1,
            ignore_flag=0,
        )

        # Insert last event for pid 7 (test for maybe_later response)
        self.data_generator.create_database_genomic_user_event_metrics(
            created=clock.CLOCK.now(),
            modified=clock.CLOCK.now(),
            participant_id=7,
            created_at=datetime.datetime(2021, 12, 29, 00),
            event_name="gem.informing_loop.screen8_maybe_later",
            run_id=1,
            ignore_flag=0,
        )

        # Set up informing loop from message broker records
        decisions = [None, 'no', 'yes']
        for p in range(4):
            for i in range(3):
                self.data_generator.create_database_genomic_informing_loop(
                    message_record_id=i,
                    event_type='informing_loop_started' if i == 0 else 'informing_loop_decision',
                    module_type='gem',
                    participant_id=p+1,
                    decision_value=decisions[i],
                    event_authored_time=datetime.datetime(2021, 12, 29, 00) + datetime.timedelta(hours=i)
                )

        # Test for only started event
        self.data_generator.create_database_genomic_user_event_metrics(
            created=clock.CLOCK.now(),
            modified=clock.CLOCK.now(),
            participant_id=6,
            created_at=datetime.datetime(2021, 12, 29, 00),
            event_name='gem.informing_loop.started',
            run_id=1,
            ignore_flag=0,
        )
        self.data_generator.create_database_genomic_informing_loop(
            message_record_id=100,
            event_type='informing_loop_started',
            module_type='gem',
            participant_id=6,
            decision_value=None,
            event_authored_time=datetime.datetime(2021, 12, 29, 00)
        )

        self.data_generator.create_database_genomic_informing_loop(
            message_record_id=100,
            event_type='informing_loop_decision',
            module_type='gem',
            participant_id=7,
            decision_value='maybe_later',
            event_authored_time=datetime.datetime(2021, 12, 29, 00)
        )

        # Run reconcile job
        genomic_pipeline.reconcile_informing_loop_responses()

        # Test no incident created for "started" event mismatch
        incidents = self.incident_dao.get_all()
        self.assertEqual(0, len(incidents))

        # Test data ingested correctly
        pid_list = [1, 2, 3, 6, 7]
        event_list = ['gem.informing_loop.screen8_no',
                      'gem.informing_loop.screen8_yes',
                      'gem.informing_loop.screen8_maybe_later']

        updated_events = event_dao.get_all_event_objects_for_pid_list(
            pid_list,
            module='gem',
            event_list=event_list
        )

        for event in updated_events:
            self.assertEqual(2, event.reconcile_job_run_id)

        old_event = event_dao.get(1)

        old_event.created = old_event.created - datetime.timedelta(days=8)
        with event_dao.session() as session:
            session.merge(old_event)

    def test_investigation_aw2_ingestion(self):
        self._create_fake_datasets_for_gc_tests(3,
                                                genome_type='aou_array',
                                                array_participants=range(1, 4),
                                                genomic_workflow_state=GenomicWorkflowState.AW0
                                                )

        # self._update_test_sample_ids()

        # Create data from AW1 investigation
        for m in self.member_dao.get_all():
            m.collectionTubeId = f'replated_{m.collectionTubeId}'
            self.member_dao.update(m)

        # Setup Array AW1 Test file
        gc_manifest_file = open_genomic_set_file("AW1-array-investigation-test.csv")
        gc_manifest_filename = "RDR_AoU_GEN_PKG-1908-218051.csv"

        test_date = datetime.datetime(2020, 10, 13, 0, 0, 0, 0)
        pytz.timezone('US/Central').localize(test_date)

        bucket_name = _FAKE_GENOMIC_CENTER_BUCKET_A

        with clock.FakeClock(test_date):
            write_cloud_csv(
                gc_manifest_filename,
                gc_manifest_file,
                bucket=bucket_name,
                folder=_FAKE_GENOTYPING_FOLDER,
            )

        # Get   subfolder, and filename from argument
        file_name = _FAKE_GENOTYPING_FOLDER + '/' + gc_manifest_filename

        # Set up file/JSON
        task_data = {
            "job": GenomicJob.AW1_MANIFEST,
            "bucket": bucket_name,
            "file_data": {
                "create_feedback_record": True,
                "upload_date": "2020-10-13 00:00:00",
                "manifest_type": GenomicManifestTypes.AW1,
                "file_path": f"{bucket_name}/{file_name}"
            }
        }

        # Call pipeline function
        genomic_pipeline.execute_genomic_manifest_file_pipeline(task_data)

        # Setup AW2 investigation file
        aw2_manifest_file = open_genomic_set_file("RDR_AoU_GEN_TestDataManifestInvestigation.csv")

        aw2_manifest_filename = "RDR_AoU_GEN_TestDataManifest_11192019_1.csv"

        test_date = datetime.datetime(2020, 10, 13, 0, 0, 0, 0)
        pytz.timezone('US/Central').localize(test_date)

        subfolder = config.getSetting(config.GENOMIC_AW2_SUBFOLDERS[1])
        with clock.FakeClock(test_date):
            write_cloud_csv(
                aw2_manifest_filename,
                aw2_manifest_file,
                bucket=_FAKE_GENOMIC_CENTER_BUCKET_A,
                folder=subfolder,
            )

        # Get bucket, subfolder, and filename from argument
        bucket_name = _FAKE_GENOMIC_CENTER_BUCKET_A
        file_name = subfolder + '/' + aw2_manifest_filename

        # Set up file/JSON
        task_data_aw2 = {
            "job": GenomicJob.METRICS_INGESTION,
            "bucket": bucket_name,
            "file_data": {
                "create_feedback_record": True,
                "upload_date": "2020-10-13 00:00:00",
                "manifest_type": GenomicManifestTypes.AW2,
                "file_path": f"{bucket_name}/{file_name}"
            }
        }

        # Call pipeline function
        genomic_pipeline.execute_genomic_manifest_file_pipeline(task_data_aw2)

        # verify AW2 data
        metrics = self.metrics_dao.get_all()
        self.assertEqual(2, len(metrics))
        for metric in metrics:
            self.assertIn(metric.genomicSetMemberId, [4, 5])

        members = self.member_dao.get_members_from_member_ids([4, 5])
        for member in members:
            self.assertEqual(GenomicWorkflowState.AW2, member.genomicWorkflowState)
            self.assertEqual(GenomicWorkflowState.AW2.name, member.genomicWorkflowStateStr)

    def test_gc_metrics_array_data(self):

        # Create the fake ingested data
        self._create_fake_datasets_for_gc_tests(2, arr_override=True, array_participants=[1, 2],
                                                genome_center='rdr',
                                                genomic_workflow_state=GenomicWorkflowState.AW1)
        bucket_name = _FAKE_GENOMIC_CENTER_BUCKET_RDR
        create_ingestion_test_file('RDR_AoU_GEN_TestDataManifest.csv',
                                   bucket_name,
                                   folder=config.getSetting(config.GENOMIC_AW2_SUBFOLDERS[1]))

        self._update_test_sample_ids()

        self._create_stored_samples([
            (1, 1001),
            (2, 1002),
        ])

        genomic_pipeline.ingest_genomic_centers_metrics_files()  # run_id = 1

        gc_record = self.metrics_dao.get(1)

        # Test the gc_metrics were populated at ingestion
        self.assertEqual(1, gc_record.vcfReceived)
        self.assertEqual(1, gc_record.vcfTbiReceived)
        self.assertEqual(1, gc_record.vcfMd5Received)
        self.assertEqual(1, gc_record.idatRedReceived)
        self.assertEqual(1, gc_record.idatGreenReceived)
        self.assertEqual(1, gc_record.idatRedMd5Received)
        self.assertEqual(1, gc_record.idatGreenMd5Received)

        self.assertEqual(f"gs://{bucket_name}/Genotyping_sample_raw_data/10001_R01C01.vcf.gz", gc_record.vcfPath)
        self.assertEqual(f"gs://{bucket_name}/Genotyping_sample_raw_data/10001_R01C01.vcf.gz.tbi", gc_record.vcfTbiPath)
        self.assertEqual(f"gs://{bucket_name}/Genotyping_sample_raw_data/10001_R01C01.vcf.gz.md5sum", gc_record.vcfMd5Path)
        self.assertEqual(f"gs://{bucket_name}/Genotyping_sample_raw_data/10001_R01C01_Red.idat", gc_record.idatRedPath)
        self.assertEqual(f"gs://{bucket_name}/Genotyping_sample_raw_data/10001_R01C01_Grn.idat", gc_record.idatGreenPath)
        self.assertEqual(f"gs://{bucket_name}/Genotyping_sample_raw_data/10001_R01C01_Red.idat.md5sum", gc_record.idatRedMd5Path)
        self.assertEqual(f"gs://{bucket_name}/Genotyping_sample_raw_data/10001_R01C01_Grn.idat.md5sum",
                         gc_record.idatGreenMd5Path)

        gc_record = self.metrics_dao.get(2)

        # Test the gc_metrics were populated at ingestion
        self.assertEqual(1, gc_record.vcfReceived)
        self.assertEqual(1, gc_record.vcfTbiReceived)
        self.assertEqual(1, gc_record.vcfMd5Received)
        self.assertEqual(1, gc_record.idatRedReceived)
        self.assertEqual(1, gc_record.idatGreenReceived)
        self.assertEqual(1, gc_record.idatRedMd5Received)
        self.assertEqual(1, gc_record.idatGreenMd5Received)

        # Test member updated with job ID
        member = self.member_dao.get(1)
        self.assertEqual(GenomicWorkflowState.GEM_READY, member.genomicWorkflowState)
        self.assertEqual('GEM_READY', member.genomicWorkflowStateStr)

        # Test member updated with job ID
        member = self.member_dao.get(2)
        self.assertEqual(GenomicWorkflowState.GEM_READY, member.genomicWorkflowState)
        self.assertEqual('GEM_READY', member.genomicWorkflowStateStr)

    def test_gc_metrics_wgs_data(self):

        # Create the fake ingested data
        self._create_fake_datasets_for_gc_tests(2, genome_center='rdr', genomic_workflow_state=GenomicWorkflowState.AW1)
        bucket_name = _FAKE_GENOMIC_CENTER_BUCKET_RDR
        create_ingestion_test_file('RDR_AoU_SEQ_TestDataManifest.csv',
                                   bucket_name,
                                   folder=config.getSetting(config.GENOMIC_AW2_SUBFOLDERS[0]))

        self._update_test_sample_ids()

        self._create_stored_samples([
            (2, 1002)
        ])

        genomic_pipeline.ingest_genomic_centers_metrics_files()  # run_id = 1

        # Test the reconciliation process
        sequencing_test_files = (
            'Wgs_sample_raw_data/SS_VCF_clinical/RDR_T2_1002_10002_1.hard-filtered.vcf.gz',
            'Wgs_sample_raw_data/SS_VCF_clinical/RDR_T2_1002_10002_1.hard-filtered.vcf.gz.tbi',
            'Wgs_sample_raw_data/SS_VCF_clinical/RDR_T2_1002_10002_1.hard-filtered.vcf.gz.md5sum',
            'Wgs_sample_raw_data/CRAMs_CRAIs/RDR_T2_1002_10002_1.cram',
            'Wgs_sample_raw_data/CRAMs_CRAIs/RDR_T2_1002_10002_1.cram.md5sum',
        )

        test_date = datetime.datetime(2021, 7, 12, 0, 0, 0, 0)

        # create test records in GenomicGcDataFile
        with clock.FakeClock(test_date):
            for f in sequencing_test_files:
                if "cram" in f:
                    file_prefix = "CRAMs_CRAIs"
                else:
                    file_prefix = "SS_VCF_CLINICAL"

                test_file_dict = {
                    'file_path': f'{bucket_name}/{f}',
                    'gc_site_id': 'rdr',
                    'bucket_name': bucket_name,
                    'file_prefix': f'Wgs_sample_raw_data/{file_prefix}',
                    'file_name': f,
                    'file_type': '.'.join(f.split('.')[1:]),
                    'identifier_type': 'sample_id',
                    'identifier_value': '1002',
                }

                self.data_generator.create_database_gc_data_file_record(**test_file_dict)

        gc_record = self.metrics_dao.get(1)

        # Test the gc_metrics were updated with reconciliation data
        self.assertEqual(1, gc_record.hfVcfReceived)
        self.assertEqual(1, gc_record.hfVcfTbiReceived)
        self.assertEqual(1, gc_record.hfVcfMd5Received)
        self.assertEqual(0, gc_record.rawVcfReceived)
        self.assertEqual(0, gc_record.rawVcfTbiReceived)
        self.assertEqual(0, gc_record.rawVcfMd5Received)
        self.assertEqual(1, gc_record.cramReceived)
        self.assertEqual(1, gc_record.cramMd5Received)
        self.assertEqual(1, gc_record.craiReceived)

        self.assertEqual(f"gs://{bucket_name}/{sequencing_test_files[0]}", gc_record.hfVcfPath)
        self.assertEqual(f"gs://{bucket_name}/{sequencing_test_files[1]}", gc_record.hfVcfTbiPath)
        self.assertEqual(f"gs://{bucket_name}/{sequencing_test_files[2]}", gc_record.hfVcfMd5Path)
        self.assertEqual(f"gs://{bucket_name}/{sequencing_test_files[3]}", gc_record.cramPath)
        self.assertEqual(f"gs://{bucket_name}/{sequencing_test_files[4]}", gc_record.cramMd5Path)

        # Test member updated with job ID and state
        member = self.member_dao.get(2)
        self.assertEqual(GenomicWorkflowState.CVL_READY, member.genomicWorkflowState)
        self.assertEqual('CVL_READY', member.genomicWorkflowStateStr)

    @mock.patch('rdr_service.genomic.genomic_job_controller.GenomicJobController.execute_cloud_task')
    def test_aw3_array_manifest_generation_missing_files(self, cloud_task):
        # Need GC Manifest for source query : run_id = 1
        self.job_run_dao.insert(GenomicJobRun(jobId=GenomicJob.AW1_MANIFEST,
                                              startTime=clock.CLOCK.now(),
                                              runStatus=GenomicSubProcessStatus.COMPLETED,
                                              runResult=GenomicSubProcessResult.SUCCESS))

        self._create_fake_datasets_for_gc_tests(3, arr_override=True,
                                                array_participants=range(1, 4),
                                                recon_gc_man_id=1,
                                                genome_center='jh',
                                                genomic_workflow_state=GenomicWorkflowState.AW1,
                                                sample_source="Whole Blood",
                                                ai_an='N')

        bucket_name = _FAKE_GENOMIC_CENTER_BUCKET_RDR

        create_ingestion_test_file('RDR_AoU_GEN_TestDataManifest.csv',
                                         bucket_name,
                                         folder=config.getSetting(config.GENOMIC_AW2_SUBFOLDERS[1]))

        self._update_test_sample_ids()

        self._create_stored_samples([
            (1, 1001),
            (2, 1002)
        ])

        genomic_pipeline.ingest_genomic_centers_metrics_files()  # run_id = 2

        # Test sequencing file (required for GEM)
        sequencing_test_files = (
            f'test_data_folder/10001_R01C01.vcf.gz',
            f'test_data_folder/10001_R01C01.vcf.gz.tbi',
            f'test_data_folder/10001_R01C01.vcf.gz.md5sum',
            f'test_data_folder/10001_R01C01_Red.idat',
            f'test_data_folder/10001_R01C01_Grn.idat',
            f'test_data_folder/10001_R01C01_Red.idat.md5sum',
            f'test_data_folder/10001_R01C01_Grn.idat.md5sum',
            f'test_data_folder/10002_R01C02.vcf.gz',
            f'test_data_folder/10002_R01C02.vcf.gz.tbi',
            f'test_data_folder/10002_R01C02.vcf.gz.md5sum',
            # f'test_data_folder/10002_R01C02_Red.idat',
            f'test_data_folder/10002_R01C02_Grn.idat',
            f'test_data_folder/10002_R01C02_Red.idat.md5sum',
            f'test_data_folder/10002_R01C02_Grn.idat.md5sum',
        )

        fake_dt = datetime.datetime(2020, 8, 3, 0, 0, 0, 0)
        with clock.FakeClock(fake_dt):
            for f in sequencing_test_files:
                # Set file type
                if "idat" in f.lower():
                    file_type = f.split('/')[-1].split("_")[-1]
                else:
                    file_type = '.'.join(f.split('.')[1:])

                test_file_dict = {
                    'file_path': f'{bucket_name}/{f}',
                    'gc_site_id': 'jh',
                    'bucket_name': bucket_name,
                    'file_prefix': f'Genotyping_sample_raw_data',
                    'file_name': f,
                    'file_type': file_type,
                    'identifier_type': 'chipwellbarcode',
                    'identifier_value': "_".join(f.split('/')[1].split('_')[0:2]).split('.')[0],
                }

                self.data_generator.create_database_gc_data_file_record(**test_file_dict)

        # finally run the AW3 manifest workflow
        fake_dt = datetime.datetime(2020, 8, 3, 0, 0, 0, 0)

        with clock.FakeClock(fake_dt):
            genomic_pipeline.aw3_array_manifest_workflow()  # run_id = 3

        manifest_records = self.manifest_file_dao.get_all()
        self.assertEqual(len(manifest_records), 1)
        self.assertEqual(manifest_records[0].recordCount, 1)
        self.assertIsNotNone(manifest_records[0].fileName)
        self.assertIsNotNone(manifest_records[0].filePath)

        self.assertTrue(cloud_task.called)
        cloud_task_args = cloud_task.call_args.args[0]
        self.assertEqual(cloud_task_args['field'], 'aw3ManifestFileId')

        member_ids = cloud_task_args['member_ids']
        self.assertIsNotNone(member_ids)
        self.assertTrue(len(set(member_ids)) == len(member_ids))

        aw3_dtf = fake_dt.strftime("%Y-%m-%d-%H-%M-%S")

        # Test member was updated
        member = self.member_dao.get(1)
        self.assertEqual(GenomicWorkflowState.GEM_READY, member.genomicWorkflowState)

        bucket_name = config.getSetting(config.DRC_BROAD_BUCKET_NAME)
        sub_folder = config.GENOMIC_AW3_ARRAY_SUBFOLDER

        with open_cloud_file(os.path.normpath(f'{bucket_name}/{sub_folder}/AoU_DRCV_GEN_{aw3_dtf}.csv')) as csv_file:
            csv_reader = csv.DictReader(csv_file)

            rows = list(csv_reader)

            self.assertEqual(1, len(rows))
            self.assertEqual(f'{get_biobank_id_prefix()}{member.biobankId}', rows[0]['biobank_id'])
            self.assertEqual(member.sampleId, rows[0]['sample_id'])
            self.assertEqual(f'{get_biobank_id_prefix()}{member.biobankId}_{member.sampleId}',
                             rows[0]['biobankidsampleid'])
            self.assertEqual(member.sexAtBirth, rows[0]['sex_at_birth'])
            self.assertEqual(member.gcSiteId, rows[0]['site_id'])
            self.assertEqual(1000001, int(rows[0]['research_id']))
            self.assertEqual('Whole Blood', rows[0]['sample_source'])
            self.assertEqual('cidr_egt_1', rows[0]['pipeline_id'])
            self.assertEqual('False', rows[0]['ai_an'])

            # Test File Paths
            metric = self.metrics_dao.get(1)
            self.assertEqual(metric.idatRedPath, rows[0]['red_idat_path'])
            self.assertEqual(metric.idatRedMd5Path, rows[0]['red_idat_md5_path'])
            self.assertEqual(metric.idatGreenPath, rows[0]['green_idat_path'])
            self.assertEqual(metric.idatGreenMd5Path, rows[0]['green_idat_md5_path'])
            self.assertEqual(metric.vcfPath, rows[0]['vcf_path'])
            self.assertEqual(metric.vcfTbiPath, rows[0]['vcf_index_path'])
            self.assertEqual(metric.vcfMd5Path, rows[0]['vcf_md5_path'])

            # Test processing GC metrics columns
            self.assertEqual(metric.callRate, rows[0]['callrate'])
            self.assertEqual(metric.sexConcordance, rows[0]['sex_concordance'])
            self.assertEqual(metric.contamination, rows[0]['contamination'])
            self.assertEqual(metric.processingStatus, rows[0]['processing_status'])

            self.assertEqual(metric.pipelineId, rows[0]['pipeline_id'])

            # Test AW3 loaded into raw table
            aw3_dao = GenomicAW3RawDao()
            raw_records = aw3_dao.get_all()
            raw_records.sort(key=lambda x: x.biobank_id)

            # Check rows in file against records in raw table
            self.assertEqual(len(rows), len(raw_records))

            for file_row in rows:
                i = int(file_row['biobank_id'][1:])-1
                for field in file_row.keys():
                    self.assertEqual(file_row[field], getattr(raw_records[i], field.lower()))

                self.assertEqual("aou_array", raw_records[i].genome_type)

        # Test run record is success
        run_obj = self.job_run_dao.get(4)
        self.assertEqual(GenomicSubProcessResult.SUCCESS, run_obj.runResult)

        # Set up 'investigation' test
        investigation_member = member
        investigation_member.genomeType = 'aou_array_investigation'
        investigation_member.blockResearch = 1
        self.member_dao.update(investigation_member)

        fake_dt = datetime.datetime(2020, 8, 4, 0, 0, 0, 0)
        with clock.FakeClock(fake_dt):
            genomic_pipeline.aw3_array_investigation_workflow()

        aw3_dtf = fake_dt.strftime("%Y-%m-%d-%H-%M-%S")

        # Check file WAS created
        with open_cloud_file(os.path.normpath(f'{bucket_name}/{sub_folder}/AoU_DRCV_GEN_{aw3_dtf}.csv')) as csv_file:
            csv_reader = csv.DictReader(csv_file)
            rows = list(csv_reader)
            self.assertEqual(1, len(rows))
            self.assertEqual("True", rows[0]['blocklisted'])

        self.clear_table_after_test('genomic_aw3_raw')
        self.clear_table_after_test('genomic_job_run')

    @mock.patch('rdr_service.genomic.genomic_job_controller.GenomicJobController.execute_cloud_task')
    def test_aw3_wgs_manifest_generation_missing_files(self, cloud_task):
        # Need GC Manifest for source query : run_id = 1
        self.job_run_dao.insert(GenomicJobRun(jobId=GenomicJob.AW1_MANIFEST,
                                              startTime=clock.CLOCK.now(),
                                              runStatus=GenomicSubProcessStatus.COMPLETED,
                                              runResult=GenomicSubProcessResult.SUCCESS))

        self._create_fake_datasets_for_gc_tests(3, arr_override=False,
                                                recon_gc_man_id=1,
                                                genome_center='rdr',
                                                genomic_workflow_state=GenomicWorkflowState.AW1,
                                                sample_source="Whole Blood",
                                                ai_an='N')

        bucket_name = _FAKE_GENOMIC_CENTER_BUCKET_A

        create_ingestion_test_file(
            'RDR_AoU_SEQ_TestDataManifest.csv',
            bucket_name,
            folder=config.getSetting(config.GENOMIC_AW2_SUBFOLDERS[0])
        )

        self._update_test_sample_ids()
        self._create_stored_samples([(3, 1003)])
        self._create_stored_samples([(2, 1002)])

        # Create corresponding array genomic_set_members
        for i in range(1, 4):
            self.data_generator.create_database_genomic_set_member(
                participantId=i,
                genomicSetId=1,
                biobankId=i,
                gcManifestParentSampleId=1000+i,
                genomeType="aou_array",
                aw3ManifestJobRunID=1,
                ai_an='N'
            )

        genomic_pipeline.ingest_genomic_centers_metrics_files()  # run_id = 2

        # Test sequencing file (required for AW3 WGS)
        sequencing_test_files = (
            f'test_data_folder/RDR_2_1002_10002_1.hard-filtered.vcf.gz',
            f'test_data_folder/RDR_2_1002_10002_1.hard-filtered.vcf.gz.tbi',
            f'test_data_folder/RDR_2_1002_10002_1.hard-filtered.vcf.gz.md5sum',
            f'test_data_folder/RDR_2_1002_10002_1.cram',
            f'test_data_folder/RDR_2_1002_10002_1.cram.md5sum',
            f'test_data_folder/RDR_2_1002_10002_1.cram.crai',
            f'test_data_folder/RDR_2_1002_10002_1.hard-filtered.gvcf.gz',
            f'test_data_folder/RDR_2_1002_10002_1.hard-filtered.gvcf.gz.md5sum',
            f'test_data_folder/RDR_3_1003_10003_1.hard-filtered.vcf.gz',
            f'test_data_folder/RDR_3_1003_10003_1.hard-filtered.vcf.gz.tbi',
            f'test_data_folder/RDR_3_1003_10003_1.hard-filtered.vcf.gz.md5sum',
            f'test_data_folder/RDR_3_1003_10003_1.cram',
            f'test_data_folder/RDR_3_1003_10003_1.cram.md5sum',
            f'test_data_folder/RDR_3_1003_10003_1.cram.crai',
            #f'test_data_folder/RDR_3_1003_10003_1.hard-filtered.gvcf.gz',
            f'test_data_folder/RDR_3_1003_10003_1.hard-filtered.gvcf.gz.md5sum',
        )
        test_date = datetime.datetime(2021, 7, 12, 0, 0, 0, 0)

        # create test records in GenomicGcDataFile
        with clock.FakeClock(test_date):
            for f in sequencing_test_files:
                if "cram" in f:
                    file_prefix = "CRAMs_CRAIs"
                else:
                    file_prefix = "SS_VCF_CLINICAL"

                test_file_dict = {
                    'file_path': f'{bucket_name}/{f}',
                    'gc_site_id': 'rdr',
                    'bucket_name': bucket_name,
                    'file_prefix': f'Wgs_sample_raw_data/{file_prefix}',
                    'file_name': f,
                    'file_type': '.'.join(f.split('.')[1:]),
                    'identifier_type': 'sample_id',
                    'identifier_value': f.split('_')[4],
                }

                self.data_generator.create_database_gc_data_file_record(**test_file_dict)

        # finally run the AW3 manifest workflow
        fake_dt = datetime.datetime(2020, 8, 3, 0, 0, 0, 0)

        with clock.FakeClock(fake_dt):
            genomic_pipeline.aw3_wgs_manifest_workflow()  # run_id = 3

        manifest_records = self.manifest_file_dao.get_all()
        self.assertEqual(len(manifest_records), 1)
        self.assertEqual(manifest_records[0].recordCount, 1)
        self.assertIsNotNone(manifest_records[0].fileName)
        self.assertIsNotNone(manifest_records[0].filePath)

        self.assertTrue(cloud_task.called)
        cloud_task_args = cloud_task.call_args.args[0]
        self.assertEqual(cloud_task_args['field'], 'aw3ManifestFileId')

        member_ids = cloud_task_args['member_ids']
        self.assertIsNotNone(member_ids)
        self.assertTrue(len(set(member_ids)) == len(member_ids))

        aw3_dtf = fake_dt.strftime("%Y-%m-%d-%H-%M-%S")

        # Test member was updated
        member = self.member_dao.get(2)
        self.assertEqual(GenomicWorkflowState.CVL_READY, member.genomicWorkflowState)

        bucket_name = config.getSetting(config.DRC_BROAD_BUCKET_NAME)
        sub_folder = config.GENOMIC_AW3_WGS_SUBFOLDER

        with open_cloud_file(os.path.normpath(f'{bucket_name}/{sub_folder}/AoU_DRCV_SEQ_{aw3_dtf}.csv')) as csv_file:
            csv_reader = csv.DictReader(csv_file)

            rows = list(csv_reader)
            self.assertEqual(1, len(rows))

            row = rows[0]
            metric = self.metrics_dao.get(1)

            self.assertEqual(f'{get_biobank_id_prefix()}{member.biobankId}',
                             row['biobank_id'])
            self.assertEqual(f'{get_biobank_id_prefix()}{member.biobankId}_{member.sampleId}',
                             row['biobankidsampleid'])
            self.assertEqual(member.sexAtBirth, row['sex_at_birth'])
            self.assertEqual(member.gcSiteId, row['site_id'])
            self.assertEqual(1000002, int(row['research_id']))

            self.assertEqual('Whole Blood', row['sample_source'])
            self.assertEqual('88.8888888', row['mapped_reads_pct'])
            self.assertEqual('XY', row['sex_ploidy'])
            self.assertEqual('False', row['ai_an'])

            self.assertEqual(metric.hfVcfPath, row["vcf_hf_path"])
            self.assertEqual(metric.hfVcfTbiPath, row["vcf_hf_index_path"])
            self.assertEqual(metric.cramPath, row["cram_path"])
            self.assertEqual(metric.cramMd5Path, row["cram_md5_path"])
            self.assertEqual(metric.craiPath, row["crai_path"])

            # Test GC metrics columns
            self.assertEqual(metric.contamination, row['contamination'])
            self.assertEqual(metric.sexConcordance, row['sex_concordance'])
            self.assertEqual(metric.processingStatus, row['processing_status'])
            self.assertEqual(metric.meanCoverage, row['mean_coverage'])

            # Test AW3 loaded into raw table
            aw3_dao = GenomicAW3RawDao()
            raw_records = aw3_dao.get_all()
            raw_records.sort(key=lambda x: x.biobank_id)

            # Check rows in file against records in raw table
            self.assertEqual(len(rows), len(raw_records))

            for file_row in rows:
                for field in file_row.keys():
                    self.assertEqual(file_row[field], getattr(raw_records[0], field.lower()))

                self.assertEqual("aou_wgs", raw_records[0].genome_type)

        # Test run record is success
        run_obj = self.job_run_dao.get(4)

        self.assertEqual(GenomicSubProcessResult.SUCCESS, run_obj.runResult)

        # Set up 'investigation' test
        investigation_member = member
        investigation_member.genomeType = 'aou_wgs_investigation'
        investigation_member.blockResearch = 1
        self.member_dao.update(investigation_member)

        fake_dt = datetime.datetime(2020, 8, 4, 0, 0, 0, 0)
        with clock.FakeClock(fake_dt):
            genomic_pipeline.aw3_wgs_investigation_workflow()

        aw3_dtf = fake_dt.strftime("%Y-%m-%d-%H-%M-%S")

        # Check file WAS created
        with open_cloud_file(os.path.normpath(f'{bucket_name}/{sub_folder}/AoU_DRCV_SEQ_{aw3_dtf}.csv')) as csv_file:
            csv_reader = csv.DictReader(csv_file)
            rows = list(csv_reader)
            self.assertEqual(1, len(rows))
            self.assertEqual("True", rows[0]['blocklisted'])

        self.clear_table_after_test('genomic_aw3_raw')
        self.clear_table_after_test('genomic_job_run')

    @mock.patch('rdr_service.services.email_service.EmailService.send_email')
    def test_aw3_ready_missing_data_files_report(self, email_mock):
        self.job_run_dao.insert(GenomicJobRun(jobId=GenomicJob.AW1_MANIFEST,
                                              startTime=clock.CLOCK.now(),
                                              runStatus=GenomicSubProcessStatus.COMPLETED,
                                              runResult=GenomicSubProcessResult.SUCCESS))

        self._create_fake_datasets_for_gc_tests(3, arr_override=False,
                                                recon_gc_man_id=1,
                                                genome_center='rdr',
                                                genomic_workflow_state=GenomicWorkflowState.AW1,
                                                sample_source="Whole Blood",
                                                ai_an='N')

        bucket_name = _FAKE_GENOMIC_CENTER_BUCKET_A

        create_ingestion_test_file(
            'RDR_AoU_SEQ_TestDataManifest.csv',
            bucket_name,
            folder=config.getSetting(config.GENOMIC_AW2_SUBFOLDERS[0])
        )

        self._update_test_sample_ids()
        self._create_stored_samples([(3, 1003)])
        self._create_stored_samples([(2, 1002)])

        # Create corresponding array genomic_set_members
        for i in range(1, 4):
            self.data_generator.create_database_genomic_set_member(
                participantId=i,
                genomicSetId=1,
                biobankId=i,
                gcManifestParentSampleId=1000+i,
                genomeType="aou_array",
                aw3ManifestJobRunID=1,
                ai_an='N'
            )

        genomic_pipeline.ingest_genomic_centers_metrics_files()  # run_id = 2

        # Test sequencing file (required for AW3 WGS)
        sequencing_test_files = (
            f'test_data_folder/RDR_2_1002_10002_1.hard-filtered.vcf.gz',
            f'test_data_folder/RDR_2_1002_10002_1.hard-filtered.vcf.gz.tbi',
            f'test_data_folder/RDR_2_1002_10002_1.hard-filtered.vcf.gz.md5sum',
            f'test_data_folder/RDR_2_1002_10002_1.cram',
            f'test_data_folder/RDR_2_1002_10002_1.cram.md5sum',
            f'test_data_folder/RDR_2_1002_10002_1.cram.crai',
            f'test_data_folder/RDR_2_1002_10002_1.hard-filtered.gvcf.gz',
            f'test_data_folder/RDR_2_1002_10002_1.hard-filtered.gvcf.gz.md5sum',
            f'test_data_folder/RDR_3_1003_10003_1.hard-filtered.vcf.gz',
            f'test_data_folder/RDR_3_1003_10003_1.hard-filtered.vcf.gz.tbi',
            f'test_data_folder/RDR_3_1003_10003_1.hard-filtered.vcf.gz.md5sum',
            f'test_data_folder/RDR_3_1003_10003_1.cram',
            f'test_data_folder/RDR_3_1003_10003_1.cram.md5sum',
            f'test_data_folder/RDR_3_1003_10003_1.cram.crai',
            #f'test_data_folder/RDR_3_1003_10003_1.hard-filtered.gvcf.gz',
            f'test_data_folder/RDR_3_1003_10003_1.hard-filtered.gvcf.gz.md5sum',
        )
        test_date = datetime.datetime(2021, 7, 12, 0, 0, 0, 0)

        # create test records in GenomicGcDataFile
        with clock.FakeClock(test_date):
            for f in sequencing_test_files:
                if "cram" in f:
                    file_prefix = "CRAMs_CRAIs"
                else:
                    file_prefix = "SS_VCF_CLINICAL"

                test_file_dict = {
                    'file_path': f'{bucket_name}/{f}',
                    'gc_site_id': 'rdr',
                    'bucket_name': bucket_name,
                    'file_prefix': f'Wgs_sample_raw_data/{file_prefix}',
                    'file_name': f,
                    'file_type': '.'.join(f.split('.')[1:]),
                    'identifier_type': 'sample_id',
                    'identifier_value': f.split('_')[4],
                }

                self.data_generator.create_database_gc_data_file_record(**test_file_dict)

        config.override_setting(config.RDR_GENOMICS_NOTIFICATION_EMAIL, 'email@test.com')
        with GenomicJobController(GenomicJob.AW3_MISSING_DATA_FILE_REPORT) as controller:
            controller.check_aw3_ready_missing_files()

        # mock checks
        self.assertEqual(email_mock.call_count, 1)

