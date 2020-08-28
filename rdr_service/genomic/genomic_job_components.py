"""
Component Classes for Genomic Jobs
Components are assembled by the JobController for a particular Genomic Job
"""

import csv
import logging
import re
import pytz
from collections import deque, namedtuple
from copy import deepcopy
import sqlalchemy

from rdr_service.genomic.genomic_state_handler import GenomicStateHandler

from rdr_service import clock
from rdr_service.model.participant_summary import ParticipantSummary
from rdr_service.services.jira_utils import JiraTicketHandler
from rdr_service.api_util import (
    open_cloud_file,
    copy_cloud_file,
    delete_cloud_file,
    list_blobs
)
from rdr_service.model.genomics import (
    GenomicSet,
    GenomicSetMember,
    GenomicGCValidationMetrics,
)
from rdr_service.participant_enums import (
    GenomicSubProcessResult,
    WithdrawalStatus,
    QuestionnaireStatus,
    SampleStatus,
    GenomicSetStatus,
    GenomicManifestTypes,
    GenomicJob,
    GenomicSubProcessStatus,
    Race,
    GenomicValidationFlag,
    GenomicSetMemberStatus,
    SuspensionStatus,
    GenomicWorkflowState,
    ParticipantCohort)
from rdr_service.dao.genomics_dao import (
    GenomicGCValidationMetricsDao,
    GenomicSetMemberDao,
    GenomicFileProcessedDao,
    GenomicSetDao,
    GenomicJobRunDao
)
from rdr_service.dao.biobank_stored_sample_dao import BiobankStoredSampleDao
from rdr_service.dao.site_dao import SiteDao
from rdr_service.dao.participant_summary_dao import ParticipantSummaryDao
from rdr_service.genomic.genomic_biobank_manifest_handler import (
    create_and_upload_genomic_biobank_manifest_file,
    update_package_id_from_manifest_result_file,
    _get_genomic_set_id_from_filename
)
from rdr_service.genomic.validation import (
    GENOMIC_VALID_AGE,
)
from rdr_service.offline.sql_exporter import SqlExporter
from rdr_service.config import (
    getSetting,
    GENOMIC_CVL_RECONCILIATION_REPORT_SUBFOLDER,
    CVL_W1_MANIFEST_SUBFOLDER,
    CVL_W3_MANIFEST_SUBFOLDER,
    GENOMIC_GEM_A1_MANIFEST_SUBFOLDER,
    GENOMIC_GEM_A3_MANIFEST_SUBFOLDER,
    GENOME_TYPE_ARRAY,
    GENOME_TYPE_WGS,
    GAE_PROJECT,
    GENOMIC_AW3_ARRAY_SUBFOLDER,
    GENOMIC_AW3_WGS_SUBFOLDER,
)
from rdr_service.code_constants import COHORT_1_REVIEW_CONSENT_YES_CODE


class GenomicFileIngester:
    """
    This class ingests a file from a source GC bucket into the destination table
    """

    def __init__(self, job_id=None,
                 job_run_id=None,
                 bucket=None,
                 archive_folder=None,
                 sub_folder=None,
                 _controller=None):

        self.controller = _controller
        self.job_id = job_id
        self.job_run_id = job_run_id
        self.file_obj = None
        self.file_queue = deque()

        self.bucket_name = bucket
        self.archive_folder_name = archive_folder
        self.sub_folder_name = sub_folder

        # Sub Components
        self.file_validator = GenomicFileValidator(job_id=self.job_id)
        self.file_mover = GenomicFileMover(archive_folder=self.archive_folder_name)
        self.metrics_dao = GenomicGCValidationMetricsDao()
        self.file_processed_dao = GenomicFileProcessedDao()
        self.member_dao = GenomicSetMemberDao()
        self.job_run_dao = GenomicJobRunDao()

    def generate_file_processing_queue(self):
        """
        Creates the list of files to be ingested in this run.
        Ordering is currently arbitrary;
        """
        files = self._get_uningested_file_names_from_bucket()
        if files == GenomicSubProcessResult.NO_FILES:
            return files
        else:
            for file_name in files:
                file_path = "/" + self.bucket_name + "/" + file_name
                new_file_record = self.file_processed_dao.insert_file_record(
                    self.job_run_id,
                    file_path,
                    self.bucket_name,
                    file_name.split('/')[-1])

                self.file_queue.append(new_file_record)

    def _get_uningested_file_names_from_bucket(self):
        """
        Searches the bucket for un-processed files.
        :return: list of filenames or NO_FILES result code
        """
        # Setup date
        timezone = pytz.timezone('Etc/Greenwich')
        date_limit_obj = timezone.localize(self.controller.last_run_time)

        # Look for new files with valid filenames
        bucket = '/' + self.bucket_name
        files = list_blobs(bucket, prefix=self.sub_folder_name)

        files = [s.name for s in files
                 if s.updated > date_limit_obj
                 and self.file_validator.validate_filename(s.name.lower())]

        if not files:
            logging.info('No files in cloud bucket {}'.format(self.bucket_name))
            return GenomicSubProcessResult.NO_FILES
        return files

    def generate_file_queue_and_do_ingestion(self):
        """
        Main method of the ingestor component,
        generates a queue and processes each file
        :return: result code
        """
        file_queue_result = self.generate_file_processing_queue()

        if file_queue_result == GenomicSubProcessResult.NO_FILES:
            logging.info('No files to process.')
            return file_queue_result
        else:
            logging.info('Processing files in queue.')
            results = []
            while len(self.file_queue) > 0:
                try:
                    ingestion_result = self._ingest_genomic_file(
                        self.file_queue[0])
                    file_ingested = self.file_queue.popleft()
                    results.append(ingestion_result == GenomicSubProcessResult.SUCCESS)
                    logging.info(f'Ingestion attempt for {file_ingested.fileName}: {ingestion_result}')

                    self.file_processed_dao.update_file_record(
                        file_ingested.id,
                        GenomicSubProcessStatus.COMPLETED,
                        ingestion_result
                    )

                except IndexError:
                    logging.info('No files left in file queue.')

            return GenomicSubProcessResult.SUCCESS if all(results) \
                else GenomicSubProcessResult.ERROR

    def _ingest_genomic_file(self, file_obj):
        """
        Reads a file object from bucket and inserts into DB
        :param: file_obj: A genomic file object
        :return: A GenomicSubProcessResultCode
        """
        self.file_obj = file_obj
        data_to_ingest = self._retrieve_data_from_path(self.file_obj.filePath)

        if data_to_ingest == GenomicSubProcessResult.ERROR:
            return GenomicSubProcessResult.ERROR
        elif data_to_ingest:
            logging.info(f'Ingesting data from {self.file_obj.fileName}')
            logging.info("Validating file.")
            self.file_validator.valid_schema = None
            validation_result = self.file_validator.validate_ingestion_file(
                self.file_obj.fileName, data_to_ingest)

            if validation_result != GenomicSubProcessResult.SUCCESS:
                return validation_result

            if self.job_id in [GenomicJob.AW1_MANIFEST, GenomicJob.AW1F_MANIFEST]:
                gc_site_id = self._get_site_from_aw1()
                return self._ingest_gc_manifest(data_to_ingest, gc_site_id)

            if self.job_id == GenomicJob.METRICS_INGESTION:
                return self._process_gc_metrics_data_for_insert(data_to_ingest)

            if self.job_id == GenomicJob.GEM_A2_MANIFEST:
                return self._ingest_gem_a2_manifest(data_to_ingest)

            if self.job_id == GenomicJob.W2_INGEST:
                return self._ingest_cvl_w2_manifest(data_to_ingest)

            if self.job_id in (GenomicJob.AW4_ARRAY_WORKFLOW, GenomicJob.AW4_WGS_WORKFLOW):
                return self._ingest_aw4_manifest(data_to_ingest)

        else:
            logging.info("No data to ingest.")
            return GenomicSubProcessResult.NO_FILES
        return GenomicSubProcessResult.ERROR

    def _ingest_bb_return_manifest(self):
        """
        Processes the Biobank return manifest file data
        Uses genomic_biobank_manifest_handler functions.
        :return: Result Code
        """
        try:
            genomic_set_id = _get_genomic_set_id_from_filename(self.file_obj.fileName)
            with open_cloud_file(self.file_obj.filePath) as csv_file:
                update_package_id_from_manifest_result_file(genomic_set_id, csv_file)
            return GenomicSubProcessResult.SUCCESS
        except RuntimeError:
            return GenomicSubProcessResult.ERROR

    def _ingest_gc_manifest(self, data, _site):
        """
        Updates the GenomicSetMember with GC Manifest data
        :param data:
        :param _site: gc_site ID
        :return: result code
        """
        gc_manifest_column_mappings = {
            'packageId': 'packageid',
            'sampleId': 'sampleid',
            'gcManifestBoxStorageUnitId': 'boxstorageunitid',
            'gcManifestBoxPlateId': 'boxid/plateid',
            'gcManifestWellPosition': 'wellposition',
            'gcManifestParentSampleId': 'parentsampleid',
            'collectionTubeId': 'collectiontubeid',
            'gcManifestMatrixId': 'matrixid',
            'gcManifestTreatments': 'treatments',
            'gcManifestQuantity_ul': 'quantity(ul)',
            'gcManifestTotalConcentration_ng_per_ul': 'totalconcentration(ng/ul)',
            'gcManifestTotalDNA_ng': 'totaldna(ng)',
            'gcManifestVisitDescription': 'visitdescription',
            'gcManifestSampleSource': 'samplesource',
            'gcManifestStudy': 'study',
            'gcManifestTrackingNumber': 'trackingnumber',
            'gcManifestContact': 'contact',
            'gcManifestEmail': 'email',
            'gcManifestStudyPI': 'studypi',
            'gcManifestTestName': 'testname',
            'gcManifestFailureMode': 'failuremode',
            'gcManifestFailureDescription': 'failuremodedesc',
        }
        try:
            for row in data['rows']:
                row_copy = dict(zip([key.lower().replace(' ', '').replace('_', '')
                                     for key in row], row.values()))
                collection_tube_id = row_copy['collectiontubeid']
                genome_type = row_copy['testname']
                member = self.member_dao.get_member_from_collection_tube(collection_tube_id, genome_type)

                if member is None:
                    # Fix for invalid values
                    try:
                        parent_sample_id = int(row_copy['parentsampleid'])

                    except ValueError:
                        parent_sample_id = 0

                    # Check if Programmatic control
                    if self._check_if_control_sample(parent_sample_id) is not None:
                        logging.warning(f'Control sample found: {parent_sample_id}')
                        # TODO: Ignoring control samples for now
                        # RDR may need to do something with them in the future

                    else:
                        logging.warning(f'Invalid collection tube ID: {collection_tube_id}'
                                        f' or genome_type: {genome_type}')
                    continue

                member.gcSiteId = _site

                if member.validationStatus != GenomicSetMemberStatus.VALID:
                    logging.warning(f'Invalidated member found BID: {member.biobankId}')

                for key in gc_manifest_column_mappings.keys():
                    try:
                        member.__setattr__(key, row_copy[gc_manifest_column_mappings[key]])
                    except KeyError:
                        member.__setattr__(key, None)

                member.reconcileGCManifestJobRunId = self.job_run_id

                # Update genomic state for failures
                _signal = "aw1-reconciled"

                if member.gcManifestFailureMode is not None and \
                    member.gcManifestFailureMode != '':
                    _signal = 'aw1-failed'

                member.genomicWorkflowState = GenomicStateHandler.get_new_state(
                    member.genomicWorkflowState,
                    signal=_signal)

                self.member_dao.update(member)
            return GenomicSubProcessResult.SUCCESS
        except RuntimeError:
            return GenomicSubProcessResult.ERROR

    def _ingest_gem_a2_manifest(self, file_data):
        """
        Processes the GEM A2 manifest file data
        Updates GenomicSetMember object with gem_pass field.
        :return: Result Code
        """
        try:
            for row in file_data['rows']:
                sample_id = row['sample_id']
                member = self.member_dao.get_member_from_sample_id_with_state(sample_id,
                                                                              GENOME_TYPE_ARRAY,
                                                                              GenomicWorkflowState.A1)
                if member is None:
                    logging.warning(f'Invalid sample ID: {sample_id}')
                    continue
                member.gemPass = row['success']

                member.gemA2ManifestJobRunId = self.job_run_id
                member.gemDateOfImport = row['date_of_import']

                _signal = 'a2-gem-pass' if member.gemPass.lower() == 'y' else 'a2-gem-fail'

                member.genomicWorkflowState = GenomicStateHandler.get_new_state(
                    member.genomicWorkflowState,
                    signal=_signal)

                self.member_dao.update(member)

            return GenomicSubProcessResult.SUCCESS
        except (RuntimeError, KeyError):
            return GenomicSubProcessResult.ERROR

    def _ingest_aw4_manifest(self, file_data):
        """
        Processes the AW4 manifest file data
        :param file_data:
        :return:
        """
        try:
            for row in file_data['rows']:
                sample_id = row['sample_id']
                genome_type = GENOME_TYPE_ARRAY if self.job_id == GenomicJob.AW4_ARRAY_WORKFLOW else GENOME_TYPE_WGS

                member = self.member_dao.get_member_from_aw3_sample(sample_id,
                                                                    genome_type)
                if member is None:
                    logging.warning(f'Invalid sample ID: {sample_id}')
                    continue

                member.aw4ManifestJobRunID = self.job_run_id

                self.member_dao.update(member)

            return GenomicSubProcessResult.SUCCESS

        except (RuntimeError, KeyError):
            return GenomicSubProcessResult.ERROR

    def _retrieve_data_from_path(self, path):
        """
        Retrieves the last genomic data file from a bucket
        :param path: The source file to ingest
        :return: CSV data as a dicitonary
        """
        try:
            filename = path.split('/')[2]
            logging.info(
                'Opening CSV file from queue {}: {}.'
                .format(path.split('/')[1], filename)
            )
            data_to_ingest = {'rows': []}
            with open_cloud_file(path) as csv_file:
                csv_reader = csv.DictReader(csv_file, delimiter=",")
                data_to_ingest['fieldnames'] = csv_reader.fieldnames
                for row in csv_reader:
                    data_to_ingest['rows'].append(row)
            return data_to_ingest

        except FileNotFoundError:
            logging.error(f"File path '{path}' not found")
            return GenomicSubProcessResult.ERROR

    def _process_gc_metrics_data_for_insert(self, data_to_ingest):
        """ Since input files vary in column names,
        this standardizes the field-names before passing to the bulk inserter
        :param data_to_ingest: stream of data in dict format
        :return result code
        """
        gc_metrics_batch = []

        # iterate over each row from CSV and
        # add to insert batch gc metrics record
        for row in data_to_ingest['rows']:
            # change all key names to lower
            row_copy = dict(zip([key.lower().replace(' ', '').replace('_', '')
                                 for key in row],
                                row.values()))
            row_copy['file_id'] = self.file_obj.id
            sample_id = row_copy['sampleid']

            # TODO: limit call rate to 10 characters for now until clarification from GCs
            try:
                row_copy['callrate'] = row_copy['callrate'][:10]
            except KeyError:
                pass

            genome_type = self.file_validator.genome_type
            member = self.member_dao.get_member_from_sample_id_with_state(int(sample_id),
                                                                          genome_type,
                                                                          GenomicWorkflowState.AW1)
            if member is not None:
                self.member_dao.update_member_state(member, GenomicWorkflowState.AW2)
                row_copy['member_id'] = member.id
                gc_metrics_batch.append(row_copy)
            else:
                logging.warning(f'Sample ID {sample_id} has no corresponding Genomic Set Member.')

        return self.metrics_dao.insert_gc_validation_metrics_batch(gc_metrics_batch)

    def _ingest_cvl_w2_manifest(self, file_data):
        """
        Processes the CVL W2 manifest file data
        :return: Result Code
        """
        try:
            for row in file_data['rows']:
                # change all key names to lower
                row_copy = dict(zip([key.lower().replace(' ', '').replace('_', '')
                                     for key in row],
                                    row.values()))

                biobank_id = row_copy['biobankid']
                member = self.member_dao.get_member_from_biobank_id(biobank_id, GENOME_TYPE_WGS)

                if member is None:
                    logging.warning(f'Invalid Biobank ID: {biobank_id}')
                    continue

                member.genomeType = row_copy['testname']
                member.cvlW2ManifestJobRunID = self.job_run_id

                member.genomicWorkflowState = GenomicStateHandler.get_new_state(
                    member.genomicWorkflowState,
                    signal='w2-ingestion-success')

                self.member_dao.update(member)

            return GenomicSubProcessResult.SUCCESS

        except (RuntimeError, KeyError):
            return GenomicSubProcessResult.ERROR

    def _get_site_from_aw1(self):
        """
        Returns the Genomic Center's site ID from the AW1 filename
        :return: GC site ID string
        """
        return self.file_obj.fileName.split('/')[-1].split("_")[0].lower()

    def _check_if_control_sample(self, sample_id):
        """
        Checks a sample against the list of control samples
        In genomic_set_member
        :param sample_id:
        :return: True if sample exists, else false.
        """

        return self.member_dao.get_control_sample(sample_id)


class GenomicFileValidator:
    """
    This class validates the Genomic Centers files
    """
    GENOME_TYPE_MAPPINGS = {
        'gen': GENOME_TYPE_ARRAY,
        'seq': GENOME_TYPE_WGS,
    }

    def __init__(self, filename=None, data=None, schema=None, job_id=None):
        self.filename = filename
        self.data_to_validate = data
        self.valid_schema = schema
        self.job_id = job_id
        self.genome_type = None

        self.GC_METRICS_SCHEMAS = {
            'seq': (
                "biobankid",
                "sampleid",
                "biobankidsampleid",
                "limsid",
                "meancoverage",
                "genomecoverage",
                "aouhdrcoverage",
                "contamination",
                "sexconcordance",
                "sexploidy",
                "alignedq30bases",
                "arrayconcordance",
                "processingstatus",
                "notes",
            ),
            'gen': (
                "biobankid",
                "sampleid",
                "biobankidsampleid",
                "limsid",
                "chipwellbarcode",
                "callrate",
                "sexconcordance",
                "processingstatus",
                "notes",
            ),
        }
        self.VALID_GENOME_CENTERS = ('uw', 'bam', 'bcm', 'bi', 'jh', 'rdr')
        self.VALID_CVL_FACILITIES = ('rdr', 'color', 'uw', 'baylor')

        self.GC_MANIFEST_SCHEMA = (
            "packageid",
            "biobankidsampleid",
            "boxstorageunitid",
            "boxid/plateid",
            "wellposition",
            "sampleid",
            "parentsampleid",
            "collectiontubeid",
            "matrixid",
            "collectiondate",
            "biobankid",
            "sexatbirth",
            "age",
            "nystate(y/n)",
            "sampletype",
            "treatments",
            "quantity(ul)",
            "totalconcentration(ng/ul)",
            "totaldna(ng)",
            "visitdescription",
            "samplesource",
            "study",
            "trackingnumber",
            "contact",
            "email",
            "studypi",
            "testname",
            "failuremode",
            "failuremodedesc"
        )

        self.GEM_A2_SCHEMA = (
            "biobankid",
            "sampleid",
            "success",
            "dateofimport",
        )

        self.CVL_W2_SCHEMA = (
            "genomicsetname",
            "biobankid",
            "sexatbirth",
            "nyflag",
            "siteid",
            "secondaryvalidation",
            "datesubmitted",
            "testname",
        )

        self.AW4_ARRAY_SCHEMA = (
            "biobankid",
            "sampleid",
            "sexatbirth",
            "siteid",
            "redidatpath",
            "redidatmd5path",
            "greenidatpath",
            "greenidatmd5path",
            "vcfpath",
            "vcfindexpath",
            "researchid"
        )

        self.AW4_WGS_SCHEMA = (
            "biobankid",
            "sampleid",
            "sexatbirth",
            "siteid",
            "vcfhfpath",
            "vcfhfmd5path",
            "vcfhfindexpath",
            "vcfrawpath",
            "vcfrawmd5path",
            "vcfrawindexpath",
            "crampath",
            "crammd5path",
            "craipath",
            "researchid"
        )

    def validate_ingestion_file(self, filename, data_to_validate):
        """
        Procedure to validate an ingestion file
        :param filename:
        :param data_to_validate:
        :return: result code
        """
        self.filename = filename
        if not self.validate_filename(filename):
            return GenomicSubProcessResult.INVALID_FILE_NAME

        struct_valid_result = self._check_file_structure_valid(
            data_to_validate['fieldnames'])

        if struct_valid_result == GenomicSubProcessResult.INVALID_FILE_NAME:
            return GenomicSubProcessResult.INVALID_FILE_NAME

        if not struct_valid_result:
            logging.info("file structure of {} not valid.".format(filename))
            return GenomicSubProcessResult.INVALID_FILE_STRUCTURE

        return GenomicSubProcessResult.SUCCESS

    def validate_filename(self, filename):
        """
        Applies a naming rule to an arbitrary filename
        Naming rules are defined as local functions and
        Mapped to a Genomic Job ID in naming_rules dict.
        :param filename: passed to each name rule as 'fn'
        :return: boolean
        """

        # Naming Rule Definitions
        def bb_result_name_rule(fn):
            """Biobank to DRC Result name rule"""
            filename_components = [x.lower() for x in fn.split('/')[-1].split("-")]
            return (
                filename_components[0] == 'genomic' and
                filename_components[1] == 'manifest' and
                filename_components[2] in ('aou_array', 'aou_wgs')
            )

        def gc_validation_metrics_name_rule(fn):
            """GC metrics file name rule"""
            filename_components = [x.lower() for x in fn.split('/')[-1].split("_")]
            return (
                filename_components[0] in self.VALID_GENOME_CENTERS and
                filename_components[1] == 'aou' and
                filename_components[2] in self.GC_METRICS_SCHEMAS.keys()
            )

        def bb_to_gc_manifest_name_rule(fn):
            """Biobank to GCs manifest name rule"""
            filename_components = [x.lower() for x in fn.split('/')[-1].split("_")]
            return (
                len(filename_components) == 4 and
                filename_components[0] in self.VALID_GENOME_CENTERS and
                filename_components[1] == 'aou' and
                filename_components[2] in ('seq', 'gen') and
                re.search(r"pkg-[0-9]{4}-[0-9]{5,}\.csv$",
                          filename_components[3]) is not None
            )

        def aw1f_manifest_name_rule(fn):
            """Biobank to GCs Failure (AW1F) manifest name rule"""
            filename_components = [x.lower() for x in fn.split('/')[-1].split("_")]
            return (
                len(filename_components) == 5 and
                filename_components[0] in self.VALID_GENOME_CENTERS and
                filename_components[1] == 'aou' and
                filename_components[2] in ('seq', 'gen') and
                re.search(r"pkg-[0-9]{4}-[0-9]{5,}$",
                          filename_components[3]) is not None and
                filename_components[4] == 'failure.csv'
            )

        def cvl_w2_manifest_name_rule(fn):
            """
            CVL W2 (secondary validation) manifest name rule
            UW_AoU_CVL_RequestValidation_Date.csv
            """
            filename_components = [x.lower() for x in fn.split('/')[-1].split("_")]
            return (
                len(filename_components) == 5 and
                filename_components[0] in self.VALID_CVL_FACILITIES and
                filename_components[1] == 'aou' and
                filename_components[2] == 'cvl' and
                filename_components[3] == 'requestvalidation'
            )

        def gem_a2_manifest_name_rule(fn):
            """GEM A2 manifest name rule: i.e. AoU_GEM_A2_manifest_2020-07-11-00-00-00.csv"""
            filename_components = [x.lower() for x in fn.split('/')[-1].split("_")]
            return (
                len(filename_components) == 5 and
                filename_components[0] == 'aou' and
                filename_components[1] == 'gem' and
                filename_components[2] == 'a2'
            )

        def aw4_arr_manifest_name_rule(fn):
            """DRC Broad AW4 Array manifest name rule: i.e. AoU_DRCB_GEN_2020-07-11-00-00-00.csv"""
            filename_components = [x.lower() for x in fn.split('/')[-1].split("_")]
            return (
                filename_components[0] == 'aou' and
                filename_components[1] == 'drcb' and
                filename_components[2] == 'gen'
            )

        def aw4_wgs_manifest_name_rule(fn):
            """DRC Broad AW4 WGS manifest name rule: i.e. AoU_DRCB_SEQ_2020-07-11-00-00-00.csv"""
            filename_components = [x.lower() for x in fn.split('/')[-1].split("_")]
            return (
                filename_components[0] == 'aou' and
                filename_components[1] == 'drcb' and
                filename_components[2] == 'seq'
            )

        name_rules = {
            GenomicJob.BB_RETURN_MANIFEST: bb_result_name_rule,
            GenomicJob.METRICS_INGESTION: gc_validation_metrics_name_rule,
            GenomicJob.AW1_MANIFEST: bb_to_gc_manifest_name_rule,
            GenomicJob.AW1F_MANIFEST: aw1f_manifest_name_rule,
            GenomicJob.GEM_A2_MANIFEST: gem_a2_manifest_name_rule,
            GenomicJob.W2_INGEST: cvl_w2_manifest_name_rule,
            GenomicJob.AW4_ARRAY_WORKFLOW: aw4_arr_manifest_name_rule,
            GenomicJob.AW4_WGS_WORKFLOW: aw4_wgs_manifest_name_rule,
        }

        return name_rules[self.job_id](filename)

    def _check_file_structure_valid(self, fields):
        """
        Validates the structure of the CSV against a defined set of columns.
        :param fields: the data from the CSV file; dictionary per row.
        :return: boolean; True if valid structure, False if not.
        """
        if not self.valid_schema:
            self.valid_schema = self._set_schema(self.filename)

        if self.valid_schema == GenomicSubProcessResult.INVALID_FILE_NAME:
            return GenomicSubProcessResult.INVALID_FILE_NAME

        cases = tuple([field.lower().replace('\ufeff', '').replace(' ', '').replace('_', '')
                       for field in fields])
        return cases == self.valid_schema

    def _set_schema(self, filename):
        """Since the schemas are different for WGS and Array metrics files,
        this parses the filename to return which schema
        to use for validation of the CSV columns
        :param filename: filename of the csv to validate in string format.
        :return: schema_to_validate,
            (tuple from the CSV_SCHEMA or result code of INVALID_FILE_NAME).
        """
        try:
            if self.job_id == GenomicJob.METRICS_INGESTION:
                file_type = filename.lower().split("_")[2]
                self.genome_type = self.GENOME_TYPE_MAPPINGS[file_type]
                return self.GC_METRICS_SCHEMAS[file_type]
            if self.job_id == GenomicJob.AW1_MANIFEST:
                return self.GC_MANIFEST_SCHEMA
            if self.job_id == GenomicJob.GEM_A2_MANIFEST:
                return self.GEM_A2_SCHEMA
            if self.job_id == GenomicJob.AW1F_MANIFEST:
                return self.GC_MANIFEST_SCHEMA  # AW1F and AW1 use same schema

            if self.job_id == GenomicJob.W2_INGEST:
                return self.CVL_W2_SCHEMA

            if self.job_id == GenomicJob.AW4_ARRAY_WORKFLOW:
                return self.AW4_ARRAY_SCHEMA

            if self.job_id == GenomicJob.AW4_WGS_WORKFLOW:
                return self.AW4_WGS_SCHEMA

        except (IndexError, KeyError):
            return GenomicSubProcessResult.INVALID_FILE_NAME


class GenomicFileMover:
    """
    This utility class moves files in the bucket by copying into an archive folder
    and deleting the old instance.
    """

    def __init__(self, archive_folder=None):
        self.archive_folder = archive_folder

    def archive_file(self, file_obj=None, file_path=None):
        """
        This method moves a file to an archive
        by copy and delete
        :param file_obj: a genomic_file_processed object to move
        :return:
        """
        source_path = file_obj.filePath if file_obj else file_path
        file_name = source_path.split('/')[-1]
        archive_path = source_path.replace(file_name,
                                           f"{self.archive_folder}/"
                                           f"{file_name}")
        try:
            copy_cloud_file(source_path, archive_path)
            delete_cloud_file(source_path)
        except FileNotFoundError:
            logging.error(f"No file found at '{file_obj.filePath}'")


class GenomicReconciler:
    """ This component handles reconciliation between genomic datasets """
    def __init__(self, run_id, job_id, archive_folder=None, file_mover=None, bucket_name=None):

        self.run_id = run_id
        self.job_id = job_id
        self.bucket_name = bucket_name
        self.archive_folder = archive_folder
        self.cvl_file_name = None

        # Dao components
        self.member_dao = GenomicSetMemberDao()
        self.metrics_dao = GenomicGCValidationMetricsDao()
        self.file_dao = GenomicFileProcessedDao()

        # Other components
        self.file_mover = file_mover

        # Data files and names will be different
        # file types are defined as
        # (field_for_received_flag, filename suffix, field_for_gcs_path)
        self.genotyping_file_types = (('idatRedReceived', "_red.idat", "idatRedPath"),
                                      ('idatGreenReceived', "_grn.idat", "idatGreenPath"),
                                      ('idatRedMd5Received', "_red.idat.md5sum", "idatRedMd5Path"),
                                      ('idatGreenMd5Received', "_grn.idat.md5sum", "idatGreenMd5Path"),
                                      ('vcfReceived', ".vcf.gz", "vcfPath"),
                                      ('vcfTbiReceived', ".vcf.gz.tbi", "vcfTbiPath"),
                                      ('vcfMd5Received', ".vcf.gz.md5sum", "vcfMd5Path"))

        self.sequencing_file_types = (("hfVcfReceived", ".hard-filtered.vcf.gz", "hfVcfPath"),
                                      ("hfVcfTbiReceived", ".hard-filtered.vcf.gz.tbi", "hfVcfTbiPath"),
                                      ("hfVcfMd5Received", ".hard-filtered.vcf.md5sum", "hfVcfMd5Path"),
                                      ("rawVcfReceived", ".vcf.gz", "rawVcfPath"),
                                      ("rawVcfTbiReceived", ".vcf.gz.tbi", "rawVcfTbiPath"),
                                      ("rawVcfMd5Received", ".vcf.md5sum", "rawVcfMd5Path"),
                                      ("cramReceived", ".cram", "cramPath"),
                                      ("cramMd5Received", ".cram.md5sum", "cramMd5Path"),
                                      ("craiReceived", ".crai", "craiPath"))

    def reconcile_metrics_to_manifest(self):
        """ The main method for the metrics vs. manifest reconciliation """
        try:

            unreconciled_members = self.member_dao.get_null_field_members('reconcileMetricsBBManifestJobRunId')
            results = []
            for member in unreconciled_members:
                results.append(
                    self.member_dao.update_member_job_run_id(
                        member, self.run_id, 'reconcileMetricsBBManifestJobRunId')
                )
            return GenomicSubProcessResult.SUCCESS \
                if GenomicSubProcessResult.ERROR not in results \
                else GenomicSubProcessResult.ERROR
        except RuntimeError:
            return GenomicSubProcessResult.ERROR

    def reconcile_metrics_to_genotyping_data(self):
        """ The main method for the AW2 manifest vs. array data reconciliation
        :return: result code
        """
        metrics = self.metrics_dao.get_with_missing_gen_files()

        total_missing_data = []

        # Iterate over metrics, searching the bucket for filenames
        for metric in metrics:
            member = self.member_dao.get(metric.genomicSetMemberId)

            file = self.file_dao.get(metric.genomicFileProcessedId)
            missing_data_files = []

            for file_type in self.genotyping_file_types:
                if not getattr(metric, file_type[0]):
                    filename = f"{metric.chipwellbarcode}{file_type[1]}"
                    file_exists = self._get_full_filename(file.bucketName, filename)

                    if file_exists != 0:
                        setattr(metric, file_type[0], 1)
                        setattr(metric, file_type[2], f'gs://{file.bucketName}/{file_exists}')

                    if not file_exists:
                        setattr(metric, file_type[0], file_exists)
                        missing_data_files.append(filename)

            self.metrics_dao.update(metric)

            next_state = GenomicStateHandler.get_new_state(member.genomicWorkflowState, signal='gem-ready')

            # Update state for missing files
            if len(missing_data_files) > 0:
                next_state = GenomicStateHandler.get_new_state(member.genomicWorkflowState, signal='missing')

                total_missing_data.append((file.fileName, missing_data_files))

            # Update Job Run ID on member
            self.member_dao.update_member_job_run_id(member, self.run_id, 'reconcileMetricsSequencingJobRunId')
            self.member_dao.update_member_state(member, next_state)

        # Make a roc ticket for missing data files
        if len(total_missing_data) > 0:
            alert = GenomicAlertHandler()

            summary = '[Genomic System Alert] Missing AW2 Array Manifest Files'
            description = "The following AW2 manifests are missing data files."
            description += f"\nGenomic Job Run ID: {self.run_id}"

            for f in total_missing_data:

                description += self._compile_missing_data_alert(f[0], f[1])
            alert.make_genomic_alert(summary, description)

        return GenomicSubProcessResult.SUCCESS

    def reconcile_metrics_to_sequencing_data(self):
        """ The main method for the AW2 manifest vs. sequencing data reconciliation
        :return: result code
        """
        metrics = self.metrics_dao.get_with_missing_seq_files()

        # TODO: Update filnames when clarified
        external_ids = "LocalID_InternalRevisionNumber"

        total_missing_data = []

        # Iterate over metrics, searching the bucket for filenames
        for metric in metrics:
            member = self.member_dao.get(metric.GenomicGCValidationMetrics.genomicSetMemberId)

            file = self.file_dao.get(metric.GenomicGCValidationMetrics.genomicFileProcessedId)
            gc_prefix = file.fileName.split('_')[0]

            missing_data_files = []
            for file_type in self.sequencing_file_types:

                if not getattr(metric.GenomicGCValidationMetrics, file_type[0]):
                    filename = f"{gc_prefix}_{metric.biobankId}_{metric.sampleId}_{external_ids}{file_type[1]}"
                    file_exists = self._get_full_filename(file.bucketName, filename)

                    if file_exists != 0:
                        setattr(metric.GenomicGCValidationMetrics, file_type[0], 1)
                        setattr(metric.GenomicGCValidationMetrics, file_type[2],
                                f'gs://{file.bucketName}/{file_exists}')

                    if not file_exists:
                        setattr(metric.GenomicGCValidationMetrics, file_type[0], file_exists)
                        missing_data_files.append(filename)

            self.metrics_dao.update(metric.GenomicGCValidationMetrics)

            next_state = GenomicStateHandler.get_new_state(member.genomicWorkflowState, signal='cvl-ready')

            # Handle for missing data files
            if len(missing_data_files) > 0:
                next_state = GenomicStateHandler.get_new_state(member.genomicWorkflowState, signal='missing')

                total_missing_data.append((file.fileName, missing_data_files))

            # Update Member
            self.member_dao.update_member_job_run_id(member, self.run_id, 'reconcileMetricsSequencingJobRunId')
            self.member_dao.update_member_state(member, next_state)

        # Make a roc ticket for missing data files
        if len(total_missing_data) > 0:
            alert = GenomicAlertHandler()

            summary = '[Genomic System Alert] Missing AW2 WGS Manifest Files'
            description = "The following AW2 manifests are missing data files."
            description += f"\nGenomic Job Run ID: {self.run_id}"

            for f in total_missing_data:
                description += self._compile_missing_data_alert(f[0], f[1])
            alert.make_genomic_alert(summary, description)

        return GenomicSubProcessResult.SUCCESS

    def _compile_missing_data_alert(self, _filename, _missing_data):
        """
        Compiles the description to include in a GenomicAlert
        :param _filename:
        :param _missing_data: list of files
        :return: summary, description
        """

        description = f"\n\tManifest File: {_filename}"
        description += f"\n\tMissing Genotype Data: {_missing_data}"

        return description

    def generate_cvl_reconciliation_report(self):
        """
        The main method for the CVL Reconciliation report,
        ouptuts report file to the cvl subfolder and updates
        genomic_set_member
        :return: result code
        """
        members = self.member_dao.get_members_for_cvl_reconciliation()
        if members:
            cvl_subfolder = getSetting(GENOMIC_CVL_RECONCILIATION_REPORT_SUBFOLDER)
            self.cvl_file_name = f"{cvl_subfolder}/cvl_report_{self.run_id}.csv"
            self._write_cvl_report_to_file(members)

            results = []
            for member in members:
                results.append(self.member_dao.update_member_job_run_id(
                    member, job_run_id=self.run_id,
                    field='reconcileCvlJobRunId')
                )

            return GenomicSubProcessResult.SUCCESS \
                if GenomicSubProcessResult.ERROR not in results \
                else GenomicSubProcessResult.ERROR

        return GenomicSubProcessResult.NO_FILES

    def reconcile_gem_report_states(self, _last_run_time=None):
        """
        Scans GEM report states for changes
        :param _last_run_time: the time when the current job last ran
        """

        # Get unconsented members to update (consent > last run time of job_id)
        unconsented_gror_members = self.member_dao.get_unconsented_gror_since_date(_last_run_time)

        # update each member with the new state
        for member in unconsented_gror_members:
            new_state = GenomicStateHandler.get_new_state(member.genomicWorkflowState,
                                                          signal='unconsented')

            if new_state is not None or new_state != member.genomicWorkflowState:
                self.member_dao.update_member_state(member, new_state)

        # Get reconsented members to update (consent > last run time of job_id)
        reconsented_gror_members = self.member_dao.get_reconsented_gror_since_date(_last_run_time)

        # update each member with the new state
        for member in reconsented_gror_members:
            new_state = GenomicStateHandler.get_new_state(member.genomicWorkflowState,
                                                          signal='reconsented')

            if new_state is not None or new_state != member.genomicWorkflowState:
                self.member_dao.update_member_state(member, new_state)

    def _check_genotyping_file_exists(self, bucket_name, filename):
        files = list_blobs('/' + bucket_name)
        filenames = [f.name for f in files if f.name.endswith(filename)]
        return 1 if len(filenames) > 0 else 0

    def _get_full_filename(self, bucket_name, filename):
        files = list_blobs('/' + bucket_name)
        filenames = [f.name for f in files if f.name.lower().endswith(filename.lower())]
        return filenames[0] if len(filenames) > 0 else 0

    def _get_sequence_files(self, bucket_name):
        """
        Checks the bucket for sequencing files based on naming convention
        :param bucket_name:
        :return: file list or result code
        """
        try:
            files = list_blobs('/' + bucket_name)
            # TODO: naming_convention is not yet finalized
            naming_convention = r"^gc_sequencing_t\d*\.txt$"
            files = [s.name for s in files
                     if self.archive_folder not in s.name.lower()
                     if re.search(naming_convention,
                                  s.name.lower())]
            if not files:
                logging.info(
                    f'No valid sequencing files in bucket {bucket_name}'
                )
                return GenomicSubProcessResult.NO_FILES
            return files
        except FileNotFoundError:
            return GenomicSubProcessResult.ERROR

    def _parse_seq_filename(self, filename):
        """
        Takes a sequencing filename and returns the biobank id.
        :param filename:
        :return: biobank_id
        """
        # TODO: naming_convention is not yet finalized
        try:
            # pull biobank ID from filename
            return filename.lower().split('_')[-1].split('.')[0][1:]
        except IndexError:
            return GenomicSubProcessResult.INVALID_FILE_NAME

    def _update_genomic_set_member_seq_reconciliation(self, member, seq_file_name, job_run_id):
        """
        Uses member DAO to update GenomicSetMember object
        with sequencing reconciliation data
        :param member: the GenomicSetMember to update
        :param seq_file_name:
        :param job_run_id:
        :return: query result
        """
        return self.member_dao.update_member_sequencing_file(member,
                                                             job_run_id,
                                                             seq_file_name)

    def _write_cvl_report_to_file(self, members):
        """
        writes data to csv file in bucket
        :param members:
        :return: result code
        """
        try:
            # extract only columns we need
            cvl_columns = ('biobank_id', 'sample_id', 'member_id')
            report_data = ((m.biobankId, m.sampleId, m.id) for m in members)

            # Use SQL exporter
            exporter = SqlExporter(self.bucket_name)
            with exporter.open_cloud_writer(self.cvl_file_name) as writer:
                writer.write_header(cvl_columns)
                writer.write_rows(report_data)
            return GenomicSubProcessResult.SUCCESS
        except RuntimeError:
            return GenomicSubProcessResult.ERROR


class GenomicBiobankSamplesCoupler:
    """This component creates the source data for Cohot 3:
    new genomic set and members from the biobank samples pipeline.
    Class uses the manifest handler to create and upload a manifest"""

    _SEX_AT_BIRTH_CODES = {
        'male': 'M',
        'female': 'F',
        'none_intersex': 'NA'
    }
    _VALIDATION_FLAGS = (GenomicValidationFlag.INVALID_WITHDRAW_STATUS,
                         GenomicValidationFlag.INVALID_SUSPENSION_STATUS,
                         GenomicValidationFlag.INVALID_CONSENT,
                         GenomicValidationFlag.INVALID_AGE,
                         GenomicValidationFlag.INVALID_AIAN,
                         GenomicValidationFlag.INVALID_SEX_AT_BIRTH)

    _ARRAY_GENOME_TYPE = "aou_array"
    _WGS_GENOME_TYPE = "aou_wgs"
    COHORT_1_ID = "C1"
    COHORT_2_ID = "C2"
    COHORT_3_ID = "C3"

    GenomicSampleMeta = namedtuple("GenomicSampleMeta", ["bids",
                                                         "pids",
                                                         "order_ids",
                                                         "site_ids",
                                                         "sample_ids",
                                                         "valid_withdrawal_status",
                                                         "valid_suspension_status",
                                                         "gen_consents",
                                                         "valid_ages",
                                                         "sabs",
                                                         "gror",
                                                         "valid_ai_ans"])

    def __init__(self, run_id):
        self.samples_dao = BiobankStoredSampleDao()
        self.set_dao = GenomicSetDao()
        self.member_dao = GenomicSetMemberDao()
        self.site_dao = SiteDao()
        self.ps_dao = ParticipantSummaryDao()
        self.run_id = run_id

    def create_new_genomic_participants(self, from_date):
        """
        This method determines which samples to enter into the genomic system
        from Cohort 3 (New Participants).
        Validation is handled in the query that retrieves the newly consented
        participants' samples to process.
        :param: from_date : the date from which to lookup new biobank_ids
        :return: result
        """
        samples = self._get_new_biobank_samples(from_date)
        samples_meta = self.GenomicSampleMeta(*samples)

        if len(samples) > 0:
            return self.process_samples_into_manifest(samples_meta, cohort=self.COHORT_3_ID)

        else:
            logging.info(f'New Participant Workflow: No new samples to process.')
            return GenomicSubProcessResult.NO_FILES

    def create_c2_genomic_participants(self, from_date, local=False):
        """
        Creates Cohort 2 Participants in the genomic system using reconsent.
        Validation is handled in the query that retrieves the newly consented
        participants. Only valid participants are currently sent.
        Refactored to first pull valid participants, then pull their samples,
        applying the new business logic of prioritizing
        collection date & blood over saliva.

        :param: from_date : the date from which to lookup new participants
        :return: result
        """

        participants = self._get_new_c2_participants(from_date)

        if len(participants) > 0:
            return self.create_matrix_and_process_samples(participants, cohort=self.COHORT_2_ID, local=local)

        else:
            logging.info(f'Cohort 2 Participant Workflow: No participants to process.')
            return GenomicSubProcessResult.NO_FILES

    def create_c1_genomic_participants(self, from_date, local=False):
        """
        Creates Cohort 1 Participants in the genomic system using reconsent.
        Validation is handled in the query that retrieves the newly consented
        participants. Only valid participants are currently sent.

        :param: from_date : the date from which to lookup new participants
        :return: result
        """

        participants = self._get_new_c1_participants(from_date)

        if len(participants) > 0:
            return self.create_matrix_and_process_samples(participants, cohort=self.COHORT_1_ID, local=local)

        else:
            logging.info(f'Cohort 1 Participant Workflow: No participants to process.')
            return GenomicSubProcessResult.NO_FILES

    def process_samples_into_manifest(self, samples_meta, cohort, local=False):
        """
        Compiles AW0 Manifest from samples list.
        :param samples_meta:
        :param cohort:
        :param local: overrides automatic push to bucket
        :return: job result code
        """

        logging.info(f'{self.__class__.__name__}: Processing new biobank_ids {samples_meta.bids}')
        new_genomic_set = self._create_new_genomic_set()

        # Create genomic set members
        for i, bid in enumerate(samples_meta.bids):
            # Don't write participant to table if no sample
            if samples_meta.sample_ids[i] == 0:
                continue

            logging.info(f'Validating sample: {samples_meta.sample_ids[i]}')
            validation_criteria = (
                samples_meta.valid_withdrawal_status[i],
                samples_meta.valid_suspension_status[i],
                samples_meta.gen_consents[i],
                samples_meta.valid_ages[i],
                samples_meta.valid_ai_ans[i],
                samples_meta.sabs[i] in self._SEX_AT_BIRTH_CODES.values()
            )
            valid_flags = self._calculate_validation_flags(validation_criteria)
            logging.info(f'Creating genomic set members for PID: {samples_meta.pids[i]}')
            new_array_member_obj = GenomicSetMember(
                biobankId=bid,
                genomicSetId=new_genomic_set.id,
                participantId=samples_meta.pids[i],
                nyFlag=self._get_new_york_flag(samples_meta.site_ids[i]),
                sexAtBirth=samples_meta.sabs[i],
                biobankOrderId=samples_meta.order_ids[i],
                collectionTubeId=samples_meta.sample_ids[i],
                validationStatus=(GenomicSetMemberStatus.INVALID if len(valid_flags) > 0
                                  else GenomicSetMemberStatus.VALID),
                validationFlags=valid_flags,
                ai_an='N' if samples_meta.valid_ai_ans[i] else 'Y',
                genomeType=self._ARRAY_GENOME_TYPE,
                genomicWorkflowState=GenomicWorkflowState.AW0_READY
            )
            # Also create a WGS member
            new_wgs_member_obj = deepcopy(new_array_member_obj)
            new_wgs_member_obj.genomeType = self._WGS_GENOME_TYPE

            self.member_dao.insert(new_array_member_obj)
            self.member_dao.insert(new_wgs_member_obj)

        # Create & transfer the Biobank Manifest based on the new genomic set
        try:
            if local:
                return new_genomic_set.id
            else:
                create_and_upload_genomic_biobank_manifest_file(new_genomic_set.id,
                                                                cohort_id=cohort)

            # Handle Genomic States for manifests
            for member in self.member_dao.get_members_from_set_id(new_genomic_set.id):
                new_state = GenomicStateHandler.get_new_state(member.genomicWorkflowState,
                                                              signal='manifest-generated')

                if new_state is not None or new_state != member.genomicWorkflowState:
                    self.member_dao.update_member_state(member, new_state)

            logging.info(f'{self.__class__.__name__}: Genomic set members created ')
            return GenomicSubProcessResult.SUCCESS
        except RuntimeError:
            return GenomicSubProcessResult.ERROR

    def create_matrix_and_process_samples(self, participants, cohort, local):
        """
        Wrapper method for processing participants for C1 and C2 manifests
        :param cohort:
        :param participants:
        :param local:
        :return:
        """
        participant_matrix = self.GenomicSampleMeta(*participants)

        for i, _bid in enumerate(participant_matrix.bids):
            logging.info(f'Retrieving samples for PID: f{participant_matrix.pids[i]}')
            blood_sample_data = self._get_usable_blood_sample(pid=participant_matrix.pids[i],
                                                              bid=_bid)

            saliva_sample_data = self._get_usable_saliva_sample(pid=participant_matrix.pids[i],
                                                                bid=_bid)

            # Determine which sample ID to use
            sample_data = self._determine_best_sample(blood=blood_sample_data,
                                                      saliva=saliva_sample_data)

            # update the sample id, collected site, and biobank order
            if sample_data is not None:
                participant_matrix.sample_ids[i] = sample_data[0]
                participant_matrix.site_ids[i] = sample_data[1]
                participant_matrix.order_ids[i] = sample_data[2]

            else:
                logging.info(f'No valid samples for pid {participant_matrix.pids[i]}.')

        # insert new members and make the manifest
        return self.process_samples_into_manifest(participant_matrix, cohort=cohort, local=local)

    def _get_new_biobank_samples(self, from_date):
        """
        Retrieves BiobankStoredSample objects with `rdr_created`
        after the last run of the new participant workflow job.
        The query filters out participants that do not match the
        genomic validation requirements.
        :param: from_date
        :return: list of tuples (bid, pid, biobank_identifier.value, collected_site_id)
        """

        _new_samples_sql = """
        SELECT DISTINCT
          ss.biobank_id,
          p.participant_id,
          o.biobank_order_id,
          o.collected_site_id,
          ss.biobank_stored_sample_id,
          CASE
            WHEN p.withdrawal_status = :withdrawal_param THEN 1 ELSE 0
          END as valid_withdrawal_status,
          CASE
            WHEN p.suspension_status = :suspension_param THEN 1 ELSE 0
          END as valid_suspension_status,
          CASE
            WHEN ps.consent_for_study_enrollment = :general_consent_param THEN 1 ELSE 0
          END as general_consent_given,
          CASE
            WHEN ps.date_of_birth < DATE_SUB(now(), INTERVAL :dob_param YEAR) THEN 1 ELSE 0
          END AS valid_age,
          CASE
            WHEN c.value = "SexAtBirth_Male" THEN "M"
            WHEN c.value = "SexAtBirth_Female" THEN "F"
            ELSE "NA"
          END as sab,
          CASE
            WHEN ps.consent_for_genomics_ror = 1 THEN 1 ELSE 0
          END AS gror_consent,
          CASE
              WHEN native.participant_id IS NULL THEN 1 ELSE 0
          END AS valid_ai_an
        FROM
            biobank_stored_sample ss
            JOIN participant p ON ss.biobank_id = p.biobank_id
            JOIN biobank_order_identifier oi ON ss.biobank_order_identifier = oi.value
            JOIN biobank_order o ON oi.biobank_order_id = o.biobank_order_id
            JOIN participant_summary ps ON ps.participant_id = p.participant_id
            JOIN code c ON c.code_id = ps.sex_id
            LEFT JOIN (
              SELECT ra.participant_id
              FROM participant_race_answers ra
                  JOIN code cr ON cr.code_id = ra.code_id
                      AND SUBSTRING_INDEX(cr.value, "_", -1) = "AIAN"
            ) native ON native.participant_id = p.participant_id
        WHERE TRUE
            AND (
                    ps.sample_status_1ed04 = :sample_status_param
                    OR
                    ps.sample_status_1sal2 = :sample_status_param
                )
            AND ss.test IN ("1ED04", "1SAL2")
            AND ss.rdr_created > :from_date_param
            AND ps.consent_cohort = :cohort_3_param
        """
        params = {
            "sample_status_param": SampleStatus.RECEIVED.__int__(),
            "dob_param": GENOMIC_VALID_AGE,
            "general_consent_param": QuestionnaireStatus.SUBMITTED.__int__(),
            "ai_param": Race.AMERICAN_INDIAN_OR_ALASKA_NATIVE.__int__(),
            "from_date_param": from_date.strftime("%Y-%m-%d"),
            "withdrawal_param": WithdrawalStatus.NOT_WITHDRAWN.__int__(),
            "suspension_param": SuspensionStatus.NOT_SUSPENDED.__int__(),
            "cohort_3_param": ParticipantCohort.COHORT_3.__int__(),
        }
        with self.samples_dao.session() as session:
            result = session.execute(_new_samples_sql, params).fetchall()
        return list(zip(*result))

    def _get_new_c2_participants(self, from_date):
        """
        Retrieves C2 participants and validation data.
        Broken out so that DNA samples' business logic is handled separately
        :param from_date:
        :return:
        """
        _c2_participant_sql = """
            SELECT DISTINCT
              ps.biobank_id,
              ps.participant_id,
              0 AS biobank_order_id,
              0 AS collected_site_id,
              0 AS biobank_stored_sample_id,
              CASE
                WHEN ps.withdrawal_status = :withdrawal_param THEN 1 ELSE 0
              END as valid_withdrawal_status,
              CASE
                WHEN ps.suspension_status = :suspension_param THEN 1 ELSE 0
              END as valid_suspension_status,
              CASE
                WHEN ps.consent_for_study_enrollment = :general_consent_param THEN 1 ELSE 0
              END as general_consent_given,
              CASE
                WHEN ps.date_of_birth < DATE_SUB(now(), INTERVAL :dob_param YEAR) THEN 1 ELSE 0
              END AS valid_age,
              CASE
                WHEN c.value = "SexAtBirth_Male" THEN "M"
                WHEN c.value = "SexAtBirth_Female" THEN "F"
                ELSE "NA"
              END as sab,
              CASE
                WHEN ps.consent_for_genomics_ror = :general_consent_param THEN 1 ELSE 0
              END AS gror_consent,
              CASE
                  WHEN native.participant_id IS NULL THEN 1 ELSE 0
              END AS valid_ai_an
            FROM
                participant_summary ps    
                JOIN code c ON c.code_id = ps.sex_id
                LEFT JOIN (
                  SELECT ra.participant_id
                  FROM participant_race_answers ra
                      JOIN code cr ON cr.code_id = ra.code_id
                          AND SUBSTRING_INDEX(cr.value, "_", -1) = "AIAN"
                ) native ON native.participant_id = ps.participant_id
                LEFT JOIN genomic_set_member m ON m.participant_id = ps.participant_id
                    AND m.genomic_workflow_state <> :ignore_param
            WHERE TRUE
                AND (
                        ps.sample_status_1ed04 = :sample_status_param
                        OR
                        ps.sample_status_1sal2 = :sample_status_param
                    )
                AND ps.consent_cohort = :cohort_2_param
                AND ps.questionnaire_on_dna_program_authored > :from_date_param
                AND ps.questionnaire_on_dna_program = :general_consent_param
                AND m.id IS NULL
            HAVING TRUE
                # Validations for Cohort 2
                AND valid_ai_an = 1
                AND valid_age = 1
                AND general_consent_given = 1
                AND valid_suspension_status = 1
                AND valid_withdrawal_status = 1
            ORDER BY ps.biobank_id   
        """

        params = {
            "sample_status_param": SampleStatus.RECEIVED.__int__(),
            "dob_param": GENOMIC_VALID_AGE,
            "general_consent_param": QuestionnaireStatus.SUBMITTED.__int__(),
            "ai_param": Race.AMERICAN_INDIAN_OR_ALASKA_NATIVE.__int__(),
            "from_date_param": from_date.strftime("%Y-%m-%d"),
            "withdrawal_param": WithdrawalStatus.NOT_WITHDRAWN.__int__(),
            "suspension_param": SuspensionStatus.NOT_SUSPENDED.__int__(),
            "cohort_2_param": ParticipantCohort.COHORT_2.__int__(),
            "ignore_param": GenomicWorkflowState.IGNORE.__int__(),
        }

        with self.ps_dao.session() as session:
            result = session.execute(_c2_participant_sql, params).fetchall()

        return list([list(r) for r in zip(*result)])

    def _get_new_c1_participants(self, from_date):
        """
        Retrieves C1 participants and validation data.
        :param from_date:
        :return:
        """
        _c1_participant_sql = """
            SELECT DISTINCT
              ps.biobank_id,
              ps.participant_id,
              0 AS biobank_order_id,
              0 AS collected_site_id,
              0 AS biobank_stored_sample_id,
              CASE
                WHEN ps.withdrawal_status = :withdrawal_param THEN 1 ELSE 0
              END as valid_withdrawal_status,
              CASE
                WHEN ps.suspension_status = :suspension_param THEN 1 ELSE 0
              END as valid_suspension_status,
              CASE
                WHEN ps.consent_for_study_enrollment = :general_consent_param THEN 1 ELSE 0
              END as general_consent_given,
              CASE
                WHEN ps.date_of_birth < DATE_SUB(now(), INTERVAL :dob_param YEAR) THEN 1 ELSE 0
              END AS valid_age,
              CASE
                WHEN c.value = "SexAtBirth_Male" THEN "M"
                WHEN c.value = "SexAtBirth_Female" THEN "F"
                ELSE "NA"
              END as sab,
              CASE
                WHEN ps.consent_for_genomics_ror = :general_consent_param THEN 1 ELSE 0
              END AS gror_consent,
              CASE
                  WHEN native.participant_id IS NULL THEN 1 ELSE 0
              END AS valid_ai_an
            FROM
                participant_summary ps    
                JOIN code c ON c.code_id = ps.sex_id
                LEFT JOIN (
                  SELECT ra.participant_id
                  FROM participant_race_answers ra
                      JOIN code cr ON cr.code_id = ra.code_id
                          AND SUBSTRING_INDEX(cr.value, "_", -1) = "AIAN"
                ) native ON native.participant_id = ps.participant_id
                LEFT JOIN genomic_set_member m ON m.participant_id = ps.participant_id
                    AND m.genomic_workflow_state <> :ignore_param
                JOIN questionnaire_response qr
                    ON qr.participant_id = ps.participant_id
                JOIN questionnaire_response_answer qra 
                    ON qra.questionnaire_response_id = qr.questionnaire_response_id
                JOIN code recon ON recon.code_id = qra.value_code_id
                    AND recon.value = :c1_reconsent_param
            WHERE TRUE
                AND (
                        ps.sample_status_1ed04 = :sample_status_param
                        OR
                        ps.sample_status_1sal2 = :sample_status_param
                    )
                AND ps.consent_cohort = :cohort_1_param
                AND qr.authored > :from_date_param                
                AND m.id IS NULL
            HAVING TRUE
                # Validations for Cohort 1
                AND valid_ai_an = 1
                AND valid_age = 1
                AND general_consent_given = 1
                AND valid_suspension_status = 1
                AND valid_withdrawal_status = 1
            ORDER BY ps.biobank_id   
        """

        params = {
            "sample_status_param": SampleStatus.RECEIVED.__int__(),
            "dob_param": GENOMIC_VALID_AGE,
            "general_consent_param": QuestionnaireStatus.SUBMITTED.__int__(),
            "ai_param": Race.AMERICAN_INDIAN_OR_ALASKA_NATIVE.__int__(),
            "from_date_param": from_date.strftime("%Y-%m-%d"),
            "withdrawal_param": WithdrawalStatus.NOT_WITHDRAWN.__int__(),
            "suspension_param": SuspensionStatus.NOT_SUSPENDED.__int__(),
            "cohort_1_param": ParticipantCohort.COHORT_1.__int__(),
            "c1_reconsent_param": COHORT_1_REVIEW_CONSENT_YES_CODE,
            "ignore_param": GenomicWorkflowState.IGNORE.__int__(),
        }

        with self.ps_dao.session() as session:
            result = session.execute(_c1_participant_sql, params).fetchall()

        return list([list(r) for r in zip(*result)])

    def _get_usable_blood_sample(self, pid, bid):
        """
        Select 1ED04 based on max collected date and 1ED04
        :param pid: participant_id
        :param bid: biobank_id
        :return: tuple(blood_collected date, blood sample, blood site, blood order)
        """
        _samples_sql = """
            # Max 1ED04 Sample
            SELECT ed04.collected AS blood_collected
                , ssed.biobank_stored_sample_id AS blood_sample
                , oed.collected_site_id AS blood_site
                , oed.biobank_order_id AS blood_order
            FROM biobank_stored_sample ssed
                JOIN biobank_order_identifier edid ON edid.value = ssed.biobank_order_identifier
                JOIN biobank_order oed ON oed.biobank_order_id = edid.biobank_order_id
                JOIN biobank_ordered_sample ed04 ON oed.biobank_order_id = ed04.order_id
                    AND ed04.test = "1ED04"
            WHERE TRUE
                and ssed.biobank_id = :bid_param
                and ssed.test = "1ED04"
                and ssed.status < 13
                and ed04.collected = (
                    SELECT MAX(os.collected)
                    FROM biobank_ordered_sample os
                        JOIN biobank_order o ON o.biobank_order_id = os.order_id
                    WHERE os.test = "1ED04"
                        AND o.participant_id = :pid_param
                    GROUP BY o.participant_id
                )              
            """

        params = {
            "pid_param": pid,
            "bid_param": bid,
        }

        with self.samples_dao.session() as session:
            result = session.execute(_samples_sql, params).first()

        return result

    def _get_usable_saliva_sample(self, pid, bid):
        """
        Select 1SAL2 based on max collected date
        :param pid: participant_id
        :param bid: biobank_id
        :return: tuple(saliva date, saliva sample, saliva site, saliva order)
        """
        _samples_sql = """
            # Max 1SAL2 Sample
            select sal2.collected AS saliva_collected
                , sssal.biobank_stored_sample_id AS saliva_sample
                , osal.collected_site_id AS saliva_site
                , osal.biobank_order_id AS saliva_order
            FROM biobank_order osal
                JOIN biobank_order_identifier salid ON osal.biobank_order_id = salid.biobank_order_id
                JOIN biobank_ordered_sample sal2 ON osal.biobank_order_id = sal2.order_id
                    AND sal2.test = "1SAL2"
                JOIN biobank_stored_sample sssal ON salid.value = sssal.biobank_order_identifier
            WHERE TRUE
                and sssal.biobank_id = :bid_param
                and sssal.status < 13
                and sssal.test = "1SAL2"
                and sal2.collected = (
                    SELECT MAX(os.collected)
                    FROM biobank_ordered_sample os
                        JOIN biobank_order o ON o.biobank_order_id = os.order_id
                    WHERE os.test = "1SAL2"
                            AND o.participant_id = :pid_param
                        GROUP BY o.participant_id
                    )            
            """

        params = {
            "pid_param": pid,
            "bid_param": bid,
        }

        with self.samples_dao.session() as session:
            result = session.execute(_samples_sql, params).first()

        return result

    def _determine_best_sample(self, blood, saliva):
        """
        Determines which sample to use based on:
        latest collection date, Blood over Saliva
        :param blood:
        :param saliva:
        :return: tuple of sample to use (sample_id to use, collected_site_ID, biobank_order_id)
        """
        if blood is None:
            if saliva is None:
                # If both None, return None
                return None

            # if only blood is none, return saliva
            return saliva[1:]

        # if only saliva is None, return blood
        if saliva is None:
            return blood[1:]

        # if both samples, return latest or blood if same date
        if blood[0] >= saliva[0]:
            return blood[1:]

        else:
            return saliva[1:]

    def _create_new_genomic_set(self):
        """Inserts a new genomic set for this run"""
        attributes = {
            'genomicSetName': f'new_participant_workflow_{self.run_id}',
            'genomicSetCriteria': '.',
            'genomicSetVersion': 1,
            'genomicSetStatus': GenomicSetStatus.VALID,
        }
        new_set_obj = GenomicSet(**attributes)
        return self.set_dao.insert(new_set_obj)

    def _create_new_set_member(self, **kwargs):
        """Inserts new GenomicSetMember object"""
        new_member_obj = GenomicSetMember(**kwargs)
        return self.member_dao.insert(new_member_obj)

    def _get_new_york_flag(self, collected_site_id):
        """
        Looks up whether a collected site's state is NY
        :param collected_site_id: the id of the site
        :return: int (1 or 0 for NY or Not)
        """
        return int(self.site_dao.get(collected_site_id).state == 'NY')

    def _calculate_validation_flags(self, validation_criteria):
        """
        Determines validation and flags for genomic sample
        :param validation_criteria:
        :return: list of validation flags
        """
        # Process validation flags for inserting into genomic_set_member
        flags = [flag for (passing, flag) in
                 zip(validation_criteria, self._VALIDATION_FLAGS)
                 if not passing]
        return flags


class ManifestDefinitionProvider:
    """
    Helper class to produce the definitions for each manifest
    """
    # Metadata for the various manifests
    ManifestDef = namedtuple('ManifestDef', ["job_run_field",
                                             "source_data",
                                             "destination_bucket",
                                             "output_filename",
                                             "columns",
                                             "signal"])

    PROCESSING_STATUS_PASS = 'pass'
    DEFAULT_SIGNAL = 'manifest-generated'

    def __init__(self, job_run_id=None, bucket_name=None,):
        # Attributes
        self.job_run_id = job_run_id
        self.bucket_name = bucket_name

        self.MANIFEST_DEFINITIONS = dict()
        self._setup_manifest_definitions()

    def _setup_manifest_definitions(self):
        """
        Creates the manifest definitions to use when generating the manifest
        based on manifest type
        """
        now_formatted = clock.CLOCK.now().strftime("%Y-%m-%d-%H-%M-%S")
        # Set each Manifest Definition as an instance of ManifestDef()
        # DRC Broad CVL WGS Manifest
        self.MANIFEST_DEFINITIONS[GenomicManifestTypes.CVL_W1] = self.ManifestDef(
            job_run_field='cvlW1ManifestJobRunId',
            source_data=self._get_source_data_query(GenomicManifestTypes.CVL_W1),
            destination_bucket=f'{self.bucket_name}',
            output_filename=f'{CVL_W1_MANIFEST_SUBFOLDER}/AoU_CVL_Manifest_{now_formatted}.csv',
            columns=self._get_manifest_columns(GenomicManifestTypes.CVL_W1),
            signal=self.DEFAULT_SIGNAL,
        )

        # Color Array A1 Manifest
        self.MANIFEST_DEFINITIONS[GenomicManifestTypes.GEM_A1] = self.ManifestDef(
            job_run_field='gemA1ManifestJobRunId',
            source_data=self._get_source_data_query(GenomicManifestTypes.GEM_A1),
            destination_bucket=f'{self.bucket_name}',
            output_filename=f'{GENOMIC_GEM_A1_MANIFEST_SUBFOLDER}/AoU_GEM_A1_manifest_{now_formatted}.csv',
            columns=self._get_manifest_columns(GenomicManifestTypes.GEM_A1),
            signal=self.DEFAULT_SIGNAL,
        )

        # Color A3 Manifest
        self.MANIFEST_DEFINITIONS[GenomicManifestTypes.GEM_A3] = self.ManifestDef(
            job_run_field='gemA3ManifestJobRunId',
            source_data=self._get_source_data_query(GenomicManifestTypes.GEM_A3),
            destination_bucket=f'{self.bucket_name}',
            output_filename=f'{GENOMIC_GEM_A3_MANIFEST_SUBFOLDER}/AoU_GEM_WD_{now_formatted}.csv',
            columns=self._get_manifest_columns(GenomicManifestTypes.GEM_A3),
            signal=self.DEFAULT_SIGNAL,
        )

        # DRC to CVL W3 Manifest
        self.MANIFEST_DEFINITIONS[GenomicManifestTypes.CVL_W3] = self.ManifestDef(
            job_run_field='cvlW3ManifestJobRunID',
            source_data=self._get_source_data_query(GenomicManifestTypes.CVL_W3),
            destination_bucket=f'{self.bucket_name}',
            output_filename=f'{CVL_W3_MANIFEST_SUBFOLDER}/AoU_CVL_W1_{now_formatted}.csv',
            columns=self._get_manifest_columns(GenomicManifestTypes.CVL_W3),
            signal=self.DEFAULT_SIGNAL,
        )

        # DRC to Broad AW3 Array Manifest
        self.MANIFEST_DEFINITIONS[GenomicManifestTypes.AW3_ARRAY] = self.ManifestDef(
            job_run_field='aw3ManifestJobRunID',
            source_data=self._get_source_data_query(GenomicManifestTypes.AW3_ARRAY),
            destination_bucket=f'{self.bucket_name}',
            output_filename=f'{GENOMIC_AW3_ARRAY_SUBFOLDER}/AoU_DRCV_GEN_{now_formatted}.csv',
            columns=self._get_manifest_columns(GenomicManifestTypes.AW3_ARRAY),
            signal="bypass",
        )

        # DRC to Broad AW3 WGS Manifest
        self.MANIFEST_DEFINITIONS[GenomicManifestTypes.AW3_WGS] = self.ManifestDef(
            job_run_field='aw3ManifestJobRunID',
            source_data=self._get_source_data_query(GenomicManifestTypes.AW3_WGS),
            destination_bucket=f'{self.bucket_name}',
            output_filename=f'{GENOMIC_AW3_WGS_SUBFOLDER}/AoU_DRCV_SEQ_{now_formatted}.csv',
            columns=self._get_manifest_columns(GenomicManifestTypes.AW3_WGS),
            signal="bypass",
        )

    def _get_source_data_query(self, manifest_type):
        """
        Returns the query to use for manifest's source data
        :param manifest_type:
        :return: query object
        """
        query_sql = ""

        # AW3 Array Manifest
        if manifest_type == GenomicManifestTypes.AW3_ARRAY:
            query_sql = (
                sqlalchemy.select(
                    [
                        GenomicGCValidationMetrics.chipwellbarcode,
                        GenomicSetMember.biobankId,
                        GenomicSetMember.sampleId,
                        GenomicSetMember.sexAtBirth,
                        GenomicSetMember.gcSiteId,
                        GenomicGCValidationMetrics.idatRedPath,
                        GenomicGCValidationMetrics.idatRedMd5Path,
                        GenomicGCValidationMetrics.idatGreenPath,
                        GenomicGCValidationMetrics.idatGreenMd5Path,
                        GenomicGCValidationMetrics.vcfPath,
                        GenomicGCValidationMetrics.vcfTbiPath,
                        GenomicGCValidationMetrics.vcfMd5Path,
                        sqlalchemy.bindparam('research_id', None),
                    ]
                ).select_from(
                    sqlalchemy.join(
                        sqlalchemy.join(ParticipantSummary,
                                        GenomicSetMember,
                                        GenomicSetMember.participantId == ParticipantSummary.participantId),
                        GenomicGCValidationMetrics,
                        GenomicGCValidationMetrics.genomicSetMemberId == GenomicSetMember.id
                    )
                ).where(
                    (GenomicGCValidationMetrics.processingStatus == self.PROCESSING_STATUS_PASS) &
                    (GenomicSetMember.genomicWorkflowState != GenomicWorkflowState.IGNORE) &
                    (GenomicSetMember.genomeType == GENOME_TYPE_ARRAY) &
                    (ParticipantSummary.withdrawalStatus == WithdrawalStatus.NOT_WITHDRAWN) &
                    (ParticipantSummary.suspensionStatus == SuspensionStatus.NOT_SUSPENDED) &
                    (GenomicGCValidationMetrics.idatRedReceived == 1) &
                    (GenomicGCValidationMetrics.idatGreenReceived == 1) &
                    (GenomicGCValidationMetrics.idatRedMd5Received == 1) &
                    (GenomicGCValidationMetrics.idatGreenMd5Received == 1) &
                    (GenomicGCValidationMetrics.vcfReceived == 1) &
                    (GenomicGCValidationMetrics.vcfMd5Received == 1) &
                    (GenomicSetMember.aw3ManifestJobRunID == None)
                )
            )

        # AW3 WGS Manifest
        if manifest_type == GenomicManifestTypes.AW3_WGS:
            query_sql = (
                sqlalchemy.select(
                    [
                        GenomicSetMember.biobankId,
                        GenomicSetMember.sampleId,
                        (GenomicSetMember.biobankId + '_' + GenomicSetMember.sampleId),
                        GenomicSetMember.sexAtBirth,
                        GenomicSetMember.gcSiteId,
                        GenomicGCValidationMetrics.hfVcfPath,
                        GenomicGCValidationMetrics.hfVcfTbiPath,
                        #GenomicGCValidationMetrics.hfVcfMd5Path,
                        GenomicGCValidationMetrics.rawVcfPath,
                        GenomicGCValidationMetrics.rawVcfTbiPath,
                        #GenomicGCValidationMetrics.rawVcfMd5Path,
                        GenomicGCValidationMetrics.cramPath,
                        GenomicGCValidationMetrics.cramMd5Path,
                        GenomicGCValidationMetrics.craiPath,
                        sqlalchemy.bindparam('research_id', None),
                        GenomicSetMember.sampleId,
                    ]
                ).select_from(
                    sqlalchemy.join(
                        sqlalchemy.join(ParticipantSummary,
                                        GenomicSetMember,
                                        GenomicSetMember.participantId == ParticipantSummary.participantId),
                        GenomicGCValidationMetrics,
                        GenomicGCValidationMetrics.genomicSetMemberId == GenomicSetMember.id
                    )
                ).where(
                    (GenomicGCValidationMetrics.processingStatus == self.PROCESSING_STATUS_PASS) &
                    (GenomicSetMember.genomicWorkflowState != GenomicWorkflowState.IGNORE) &
                    (GenomicSetMember.genomeType == GENOME_TYPE_WGS) &
                    (ParticipantSummary.withdrawalStatus == WithdrawalStatus.NOT_WITHDRAWN) &
                    (ParticipantSummary.suspensionStatus == SuspensionStatus.NOT_SUSPENDED) &
                    (GenomicSetMember.aw3ManifestJobRunID == None) &
                    (GenomicGCValidationMetrics.hfVcfReceived == 1) &
                    (GenomicGCValidationMetrics.hfVcfTbiReceived == 1) &
                    (GenomicGCValidationMetrics.hfVcfMd5Received == 1) &
                    (GenomicGCValidationMetrics.rawVcfReceived == 1) &
                    (GenomicGCValidationMetrics.rawVcfTbiReceived == 1) &
                    (GenomicGCValidationMetrics.rawVcfMd5Received == 1) &
                    (GenomicGCValidationMetrics.cramReceived == 1) &
                    (GenomicGCValidationMetrics.cramMd5Received == 1) &
                    (GenomicGCValidationMetrics.craiReceived == 1)
                )
            )

        # CVL W1 Manifest
        if manifest_type == GenomicManifestTypes.CVL_W1:
            query_sql = (
                sqlalchemy.select(
                    [
                        GenomicSet.genomicSetName,
                        GenomicSetMember.biobankId,
                        GenomicSetMember.sampleId,
                        GenomicSetMember.sexAtBirth,
                        GenomicSetMember.nyFlag,
                        GenomicGCValidationMetrics.siteId,
                        sqlalchemy.bindparam('secondary_validation', None),
                        sqlalchemy.bindparam('date_submitted', None),
                        sqlalchemy.bindparam('test_name', 'aou_wgs'),
                    ]
                ).select_from(
                    sqlalchemy.join(
                        sqlalchemy.join(GenomicSet, GenomicSetMember, GenomicSetMember.genomicSetId == GenomicSet.id),
                        GenomicGCValidationMetrics,
                        GenomicGCValidationMetrics.genomicSetMemberId == GenomicSetMember.id
                    )
                ).where(
                    (GenomicGCValidationMetrics.processingStatus == 'pass') &
                    (GenomicSetMember.genomicWorkflowState == GenomicWorkflowState.CVL_READY) &
                    (GenomicSetMember.genomicWorkflowState != GenomicWorkflowState.IGNORE) &
                    (GenomicSetMember.genomeType == "aou_wgs")
                )
            )

        # CVL W3 Manifest
        if manifest_type == GenomicManifestTypes.CVL_W3:
            query_sql = (
                sqlalchemy.select(
                    [
                        sqlalchemy.bindparam('value', ''),
                        GenomicSetMember.sampleId,
                        GenomicSetMember.biobankId,
                        GenomicSetMember.collectionTubeId.label("collection_tubeid"),
                        GenomicSetMember.sexAtBirth,
                        sqlalchemy.bindparam('genome_type', 'aou_wgs'),
                        GenomicSetMember.nyFlag,
                        sqlalchemy.bindparam('request_id', ''),
                        sqlalchemy.bindparam('package_id', ''),
                        GenomicSetMember.ai_an,
                        GenomicSetMember.gcSiteId.label('site_id'),
                    ]
                ).select_from(
                    sqlalchemy.join(
                        GenomicSetMember,
                        ParticipantSummary,
                        GenomicSetMember.participantId == ParticipantSummary.participantId
                    )
                ).where(
                    (GenomicSetMember.genomicWorkflowState == GenomicWorkflowState.W2) &
                    (GenomicSetMember.genomicWorkflowState != GenomicWorkflowState.IGNORE) &
                    (GenomicSetMember.genomeType == "aou_cvl") &
                    (ParticipantSummary.consentForGenomicsROR == QuestionnaireStatus.SUBMITTED)
                )
            )

        # Color GEM A1 Manifest
        if manifest_type == GenomicManifestTypes.GEM_A1:
            query_sql = (
                sqlalchemy.select(
                    [
                        GenomicSetMember.biobankId,
                        GenomicSetMember.sampleId,
                        GenomicSetMember.sexAtBirth,
                        sqlalchemy.func.IF(ParticipantSummary.consentForGenomicsROR == QuestionnaireStatus.SUBMITTED,
                                           sqlalchemy.sql.expression.literal("yes"),
                                           sqlalchemy.sql.expression.literal("no")),
                        ParticipantSummary.consentForGenomicsRORAuthored,
                        GenomicGCValidationMetrics.chipwellbarcode,
                        GenomicSetMember.gcSiteId,
                    ]
                ).select_from(
                    sqlalchemy.join(
                        sqlalchemy.join(ParticipantSummary,
                                        GenomicSetMember,
                                        GenomicSetMember.participantId == ParticipantSummary.participantId),
                        GenomicGCValidationMetrics,
                        GenomicGCValidationMetrics.genomicSetMemberId == GenomicSetMember.id
                    )
                ).where(
                    (GenomicGCValidationMetrics.processingStatus == 'pass') &
                    (GenomicSetMember.genomicWorkflowState == GenomicWorkflowState.GEM_READY) &
                    (GenomicSetMember.genomicWorkflowState != GenomicWorkflowState.IGNORE) &
                    (GenomicSetMember.genomeType == "aou_array") &
                    (ParticipantSummary.withdrawalStatus == WithdrawalStatus.NOT_WITHDRAWN) &
                    (ParticipantSummary.suspensionStatus == SuspensionStatus.NOT_SUSPENDED) &
                    (ParticipantSummary.consentForGenomicsROR == QuestionnaireStatus.SUBMITTED)
                )
            )

        # Color GEM A3 Manifest
        # Those with A1 and not A3 or updated consents since sent A3
        if manifest_type == GenomicManifestTypes.GEM_A3:
            query_sql = (
                sqlalchemy.select(
                    [
                        GenomicSetMember.biobankId,
                        GenomicSetMember.sampleId,
                    ]
                ).select_from(
                    sqlalchemy.join(ParticipantSummary,
                                    GenomicSetMember,
                                    GenomicSetMember.participantId == ParticipantSummary.participantId)
                ).where(
                    (GenomicSetMember.genomicWorkflowState == GenomicWorkflowState.GEM_RPT_PENDING_DELETE) &
                    (GenomicSetMember.genomicWorkflowState != GenomicWorkflowState.IGNORE) &
                    (GenomicSetMember.genomeType == "aou_array")
                )
            )

        return query_sql

    def _get_manifest_columns(self, manifest_type):
        """
        Defines the columns of each manifest-type
        :param manifest_type:
        :return: column tuple
        """
        columns = tuple()
        if manifest_type == GenomicManifestTypes.CVL_W1:
            columns = (
                "genomic_set_name",
                "biobank_id",
                "sample_id",
                "sex_at_birth",
                "ny_flag",
                "site_id",
                "secondary_validation",
                "date_submitted",
                "test_name",
            )
        elif manifest_type == GenomicManifestTypes.GEM_A1:
            columns = (
                'biobank_id',
                'sample_id',
                "sex_at_birth",
                "consent_for_ror",
                "date_of_consent_for_ror",
                "chipwellbarcode",
                "genome_center",
            )
        elif manifest_type == GenomicManifestTypes.GEM_A3:
            columns = (
                'biobank_id',
                'sample_id',
            )

        elif manifest_type == GenomicManifestTypes.CVL_W3:
            columns = (
                "value",
                "sample_id",
                "biobank_id",
                "collection_tubeid",
                "sex_at_birth",
                "genome_type",
                "ny_flag",
                "request_id",
                "package_id",
                "ai_an",
                "site_id",
            )

        elif manifest_type == GenomicManifestTypes.AW3_ARRAY:
            columns = (
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
                "vcf_md5_path",
                "research_id",
            )

        elif manifest_type == GenomicManifestTypes.AW3_WGS:
            columns = (
                "biobank_id",
                "sample_id",
                "biobankidsampleid",
                "sex_at_birth",
                "site_id",
                "vcf_hf_path",
                "vcf_hf_index_path",
                "vcf_raw_path",
                "vcf_raw_index_path",
                "cram_path",
                "cram_md5_path",
                "crai_path",
                "research_id"
            )

        return columns

    def get_def(self, manifest_type):
        return self.MANIFEST_DEFINITIONS[manifest_type]


class ManifestCompiler:
    """
    This component compiles Genomic manifests
    based on definitions provided by ManifestDefinitionProvider
    """
    def __init__(self, run_id, bucket_name=None):
        self.run_id = run_id

        self.bucket_name = bucket_name
        self.output_file_name = None
        self.manifest_def = None

        self.def_provider = ManifestDefinitionProvider(
            job_run_id=run_id, bucket_name=bucket_name
        )

        # Dao components
        self.member_dao = GenomicSetMemberDao()
        self.metrics_dao = GenomicGCValidationMetricsDao()

    def generate_and_transfer_manifest(self, manifest_type, genome_type):
        """
        Main execution method for ManifestCompiler
        :return: result code
        """
        self.manifest_def = self.def_provider.get_def(manifest_type)

        source_data = self._pull_source_data()
        if source_data:
            self.output_file_name = self.manifest_def.output_filename

            logging.info(
                f'Preparing manifest of type {manifest_type}...'
                f'{self.manifest_def.destination_bucket}/{self.manifest_def.output_filename}'
            )

            self._write_and_upload_manifest(source_data)

            results = []
            for row in source_data:
                member = self.member_dao.get_member_from_sample_id(row.sample_id, genome_type)
                results.append(
                    self.member_dao.update_member_job_run_id(
                        member,
                        job_run_id=self.run_id,
                        field=self.manifest_def.job_run_field
                    )
                )

                # Handle Genomic States for manifests
                if self.manifest_def.signal != "bypass":
                    new_state = GenomicStateHandler.get_new_state(member.genomicWorkflowState,
                                                                  signal=self.manifest_def.signal)

                    if new_state is not None or new_state != member.genomicWorkflowState:
                        self.member_dao.update_member_state(member, new_state)

            return GenomicSubProcessResult.SUCCESS \
                if GenomicSubProcessResult.ERROR not in results \
                else GenomicSubProcessResult.ERROR
        logging.info(f'No records found for manifest type: {manifest_type}.')
        return GenomicSubProcessResult.NO_FILES

    def _pull_source_data(self):
        """
        Runs the source data query
        :return: result set
        """
        with self.member_dao.session() as session:
            return session.execute(self.manifest_def.source_data).fetchall()

    def _write_and_upload_manifest(self, source_data):
        """
        writes data to csv file in bucket
        :return: result code
        """
        try:
            # Use SQL exporter
            exporter = SqlExporter(self.bucket_name)
            with exporter.open_cloud_writer(self.manifest_def.output_filename) as writer:
                writer.write_header(self.manifest_def.columns)
                writer.write_rows(source_data)
            return GenomicSubProcessResult.SUCCESS
        except RuntimeError:
            return GenomicSubProcessResult.ERROR


class GenomicAlertHandler:
    """
    Creates a jira ROC ticket using Jira utils
    """
    ROC_BOARD_ID = "ROC"

    def __init__(self):
        self._jira_handler = JiraTicketHandler()

    def make_genomic_alert(self, summary: str, description: str):
        """
        Wraps create_ticket with genomic specifics
        Get's the board ID and adds ticket to sprint
        :param summary: the 'title' of the ticket
        :param description: the 'body' of the ticket
        """
        if GAE_PROJECT in ["all-of-us-rdr-prod", "all-of-us-rdr-stable"]:
            ticket = self._jira_handler.create_ticket(summary, description,
                                                      board_id=self.ROC_BOARD_ID)

            active_sprint = self._jira_handler.get_active_sprint(
                self._jira_handler.get_board_by_id(self.ROC_BOARD_ID))

            self._jira_handler.add_ticket_to_sprint(ticket, active_sprint)

        else:
            logging.info('Suppressing alert for missing files')
            return
