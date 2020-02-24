"""
Component Classes for Genomic Jobs
Components are assembled by the JobController for a particular Genomic Job
"""

import csv
import logging
import re
from collections import deque, namedtuple

from rdr_service.api_util import (
    open_cloud_file,
    copy_cloud_file,
    delete_cloud_file,
    list_blobs
)
from rdr_service.model.genomics import GenomicSet, GenomicSetMember
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
    GenomicSetMemberStatus)
from rdr_service.dao.genomics_dao import (
    GenomicGCValidationMetricsDao,
    GenomicSetMemberDao,
    GenomicFileProcessedDao,
    GenomicSetDao,
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
    GENOMIC_VALID_CONSENT_CUTOFF,
)
from rdr_service.offline.sql_exporter import SqlExporter
from rdr_service.config import (
    getSetting,
    GENOMIC_CVL_RECONCILIATION_REPORT_SUBFOLDER,
    GENOMIC_CVL_MANIFEST_SUBFOLDER,
)


class GenomicFileIngester:
    """
    This class ingests a file from a source GC bucket into the destination table
    """

    def __init__(self, job_id=None,
                 job_run_id=None,
                 bucket=None,
                 archive_folder=None,
                 sub_folder=None):

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

        # TODO: Part of downstream tickets to clarify these
        self.file_name_conventions = {
            GenomicJob.METRICS_INGESTION: 'datamanifest',
            GenomicJob.BB_RETURN_MANIFEST: 'manifest-result',
            GenomicJob.BB_GC_MANIFEST: 'gc-manifest',
        }

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
        bucket = '/' + self.bucket_name
        files = list_blobs(bucket, prefix=self.sub_folder_name)
        files = [s.name for s in files
                 if self.archive_folder_name not in s.name.lower()
                 if self.file_validator.validate_filename(s.name.lower())]
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

                    self.file_mover.archive_file(file_ingested)

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

        if self.job_id == GenomicJob.BB_RETURN_MANIFEST:
            logging.info("Ingesting Manifest Result Files...")
            return self._ingest_bb_return_manifest()

        if self.job_id == GenomicJob.BB_GC_MANIFEST:
            logging.info("Ingesting GC Manifest...")
            data_to_ingest = self._retrieve_data_from_path(self.file_obj.filePath)
            if data_to_ingest == GenomicSubProcessResult.ERROR:
                return GenomicSubProcessResult.ERROR
            elif data_to_ingest:
                logging.info(f'Ingesting GC manifest data from {self.file_obj.fileName}')
                return self._ingest_gc_manifest(data_to_ingest)
            else:
                logging.info("No data to ingest.")
                return GenomicSubProcessResult.NO_FILES

        if self.job_id == GenomicJob.METRICS_INGESTION:
            data_to_ingest = self._retrieve_data_from_path(self.file_obj.filePath)
            if data_to_ingest == GenomicSubProcessResult.ERROR:
                return GenomicSubProcessResult.ERROR
            elif data_to_ingest:
                logging.info("Data to ingest from {}".format(self.file_obj.fileName))
                logging.info("Validating GC metrics file.")
                self.file_validator.valid_schema = None
                validation_result = self.file_validator.validate_ingestion_file(
                    self.file_obj.fileName, data_to_ingest)
                if validation_result != GenomicSubProcessResult.SUCCESS:
                    return validation_result
                return self._process_gc_metrics_data_for_insert(data_to_ingest)
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

    def _ingest_gc_manifest(self, data):
        """
        Updates the GenomicSetMember with GC Manifest data (just manifest)
        :param data:
        :return: result code
        """
        try:
            for row in data['rows']:
                sample_id = row['Biobankid Sampleid'].split('_')[-1]
                member = self.member_dao.get_member_from_sample_id(sample_id)
                self.member_dao.update_member_job_run_id(member,
                                                         self.job_run_id,
                                                         'reconcileGCManifestJobRunId')
            return GenomicSubProcessResult.SUCCESS
        except RuntimeError:
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
            row_copy = row.copy()
            for key in row.keys():
                val = row_copy.pop(key)
                key_lower = key.lower()
                row_copy[key_lower] = val

            # row_copy['member_id'] = 1
            row_copy['file_id'] = self.file_obj.id
            row_copy['biobank id'] = row_copy['biobank id'].replace('T', '')

            obj_to_insert = row_copy
            gc_metrics_batch.append(obj_to_insert)

        return self.metrics_dao.insert_gc_validation_metrics_batch(gc_metrics_batch)


class GenomicFileValidator:
    """
    This class validates the Genomic Centers files
    """

    def __init__(self, filename=None, data=None, schema=None, job_id=None):
        self.filename = filename
        self.data_to_validate = data
        self.valid_schema = schema
        self.job_id = job_id

        self.GC_CSV_SCHEMAS = {
            'seq': (
                "biobank id",
                "biobankidsampleid",
                "lims id",
                "mean coverage",
                "genome coverage",
                "contamination",
                "sex concordance",
                "aligned q20 bases",
                "processing status",
                "notes",
                "consent for ror",
                "withdrawn_status",
                "site_id"
            ),
            'gen': (
                "biobank id",
                "biobankidsampleid",
                "lims id",
                "call rate",
                "sex concordance",
                "contamination",
                "processing status",
                "notes",
                "site_id"
            ),
        }
        self.VALID_GENOME_CENTERS = ('uw', 'bam', 'bi', 'rdr')
        self.VALID_CVL_FACILITIES = ('color', 'uw', 'baylor')

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
            filename_components = [x.lower() for x in fn.split("_")]
            return (
                len(filename_components) == 5 and
                filename_components[0] in self.VALID_GENOME_CENTERS and
                filename_components[1] == 'aou' and
                filename_components[2] in self.GC_CSV_SCHEMAS.keys() and
                re.search(r"[0-1][0-9][0-3][0-9]20[1-9][0-9]\.csv",
                          filename_components[4]) is not None
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

        def cvl_sec_val_manifest_name_rule(fn):
            """CVL secondary validation manifest name rule"""
            filename_components = [x.lower() for x in fn.split('/')[-1].split("_")]
            return (
                len(filename_components) == 4 and
                filename_components[0] in self.VALID_CVL_FACILITIES and
                filename_components[1] == 'aou' and
                filename_components[2] == 'cvl' and
                re.search(r"pkg-[0-9]{4}-[0-9]{5,}\.csv$",
                          filename_components[3]) is not None
            )

        name_rules = {
            GenomicJob.BB_RETURN_MANIFEST: bb_result_name_rule,
            GenomicJob.METRICS_INGESTION: gc_validation_metrics_name_rule,
            GenomicJob.BB_GC_MANIFEST: bb_to_gc_manifest_name_rule,
            GenomicJob.CVL_SEC_VAL_MAN: cvl_sec_val_manifest_name_rule,
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

        return tuple(
            [field.lower() for field in fields]
        ) == self.valid_schema

    def _set_schema(self, filename):
        """Since the schemas are different for WGS and Array metrics files,
        this parses the filename to return which schema
        to use for validation of the CSV columns
        :param filename: filename of the csv to validate in string format.
        :return: schema_to_validate,
            (tuple from the CSV_SCHEMA or result code of INVALID_FILE_NAME).
        """
        try:
            file_type = filename.lower().split("_")[2]
            return self.GC_CSV_SCHEMAS[file_type]
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
    def __init__(self, run_id, archive_folder=None, file_mover=None, bucket_name=None):

        self.run_id = run_id

        self.bucket_name = bucket_name
        self.archive_folder = archive_folder
        self.cvl_file_name = None

        # Dao components
        self.member_dao = GenomicSetMemberDao()
        self.metrics_dao = GenomicGCValidationMetricsDao()

        # Other components
        self.file_mover = file_mover

    def reconcile_metrics_to_manifest(self):
        """ The main method for the metrics vs. manifest reconciliation """
        try:
            unreconciled_metrics = self.metrics_dao.get_null_set_members()
            results = []
            for metric in unreconciled_metrics:
                member = self.member_dao.get_member_from_sample_id(metric.sampleId)
                results.append(
                    self.metrics_dao.update_metric_set_member_id(
                        metric, member.id)
                )
                results.append(
                    self.member_dao.update_member_job_run_id(
                        member, self.run_id, 'reconcileMetricsBBManifestJobRunId')
                )
            return GenomicSubProcessResult.SUCCESS \
                if GenomicSubProcessResult.ERROR not in results \
                else GenomicSubProcessResult.ERROR
        except RuntimeError:
            return GenomicSubProcessResult.ERROR

    def reconcile_metrics_to_sequencing(self, bucket_name):
        """ The main method for the metrics vs. sequencing reconciliation
        :param bucket_name: the bucket to look for sequencin files
        :return: result code
        """
        file_list = self._get_sequence_files(bucket_name)

        if file_list == GenomicSubProcessResult.NO_FILES:
            logging.info('No sequencing files to reconcile.')
            return file_list
        else:
            # iterate over seq file list and update metrics
            results = []
            for seq_file_name in file_list:
                logging.info(f'Reconciling Sequencing File: {seq_file_name}')
                seq_sample_id = self._parse_seq_filename(
                    seq_file_name)
                if seq_sample_id == GenomicSubProcessResult.INVALID_FILE_NAME:
                    logging.info(f'Filename unable to be parsed: f{seq_file_name}')
                    return seq_sample_id
                else:
                    member = self.member_dao.get_member_from_sample_id(seq_sample_id)
                    if member:
                        # Updates the relevant fields for reconciliation
                        results.append(
                            self._update_genomic_set_member_seq_reconciliation(member,
                                                                               seq_file_name,
                                                                               self.run_id))
                        # Archive the file
                        seq_file_path = "/" + bucket_name + "/" + seq_file_name
                        self.file_mover.archive_file(file_path=seq_file_path)
            return GenomicSubProcessResult.SUCCESS \
                if GenomicSubProcessResult.ERROR not in results \
                else GenomicSubProcessResult.ERROR

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

    def _get_sequence_metrics_by_biobank_id(self, biobank_id):
        """
        Calls the metrics DAO
        :param biobank_id:
        :return: list of GenomicGCValidationMetrics
        objects with null sequencing_file_name
        """
        return self.metrics_dao.get_metrics_by_biobank_id(biobank_id)

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
            with exporter.open_writer(self.cvl_file_name) as writer:
                writer.write_header(cvl_columns)
                writer.write_rows(report_data)
            return GenomicSubProcessResult.SUCCESS
        except RuntimeError:
            return GenomicSubProcessResult.ERROR


class GenomicBiobankSamplesCoupler:
    """This component creates new genomic set
    and members from the biobank samples pipeline,
    then calls the manifest handler to create and upload a manifest"""

    _SEX_AT_BIRTH_CODES = {
        'male': 'M',
        'female': 'F'
    }
    _VALIDATION_FLAGS = (GenomicValidationFlag.INVALID_WITHDRAW_STATUS,
                         GenomicValidationFlag.INVALID_CONSENT,
                         GenomicValidationFlag.INVALID_AGE,
                         GenomicValidationFlag.INVALID_AIAN,
                         GenomicValidationFlag.INVALID_SEX_AT_BIRTH)

    def __init__(self, run_id):
        self.samples_dao = BiobankStoredSampleDao()
        self.set_dao = GenomicSetDao()
        self.member_dao = GenomicSetMemberDao()
        self.site_dao = SiteDao()
        self.ps_dao = ParticipantSummaryDao()
        self.run_id = run_id

    def create_new_genomic_participants(self, from_date):
        """This method is the main execution method for this class
        It determines which biobankIDs to process and then executes subprocesses
        Validation is handled in the query that retrieves the new Biobank IDs to process.
        :param: from_date : the date from which to lookup new biobank_ids
        :return: result
        """
        samples = self._get_new_biobank_samples(from_date)
        if len(samples) > 0:
            # Get the genomic data to insert into GenomicSetMember as multi-dim tuple
            GenomicSampleMeta = namedtuple("GenomicSampleMeta", ["bids",
                                                                 "pids",
                                                                 "order_ids",
                                                                 "site_ids",
                                                                 "sample_ids",
                                                                 "not_withdrawn",
                                                                 "gen_consents",
                                                                 "valid_ages",
                                                                 "sabs",
                                                                 "gror",
                                                                 "valid_ai_ans"])
            samples_meta = GenomicSampleMeta(*samples)
            logging.info(f'{self.__class__.__name__}: Processing new biobank_ids {samples_meta.bids}')
            new_genomic_set = self._create_new_genomic_set()
            # Create genomic set members
            for i, bid in enumerate(samples_meta.bids):
                logging.info(f'Validating sample: {samples_meta.sample_ids}')
                validation_criteria = (
                    samples_meta.not_withdrawn[i],
                    samples_meta.gen_consents[i],
                    samples_meta.valid_ages[i],
                    samples_meta.valid_ai_ans[i],
                    samples_meta.sabs[i] in self._SEX_AT_BIRTH_CODES.values()
                )
                valid_flags = self._calculate_validation_flags(validation_criteria)
                logging.info(f'Creating genomic set member for PID: {samples_meta.pids[i]}')
                self._create_new_set_member(
                    biobankId=bid,
                    genomicSetId=new_genomic_set.id,
                    participantId=samples_meta.pids[i],
                    nyFlag=self._get_new_york_flag(samples_meta.site_ids[i]),
                    sexAtBirth=samples_meta.sabs[i],
                    biobankOrderId=samples_meta.order_ids[i],
                    sampleId=samples_meta.sample_ids[i],
                    validationStatus=(GenomicSetMemberStatus.INVALID if len(valid_flags) > 0
                                      else GenomicSetMemberStatus.VALID),
                    ai_an='N' if samples_meta.valid_ai_ans[i] else 'Y'
                )
            # Create & transfer the Biobank Manifest based on the new genomic set
            try:
                create_and_upload_genomic_biobank_manifest_file(new_genomic_set.id)
                logging.info(f'{self.__class__.__name__}: Genomic set members created ')
                return GenomicSubProcessResult.SUCCESS
            except RuntimeError:
                return GenomicSubProcessResult.ERROR
        else:
            logging.info(f'New Participant Workflow: No new biobank_ids to process.')
            return GenomicSubProcessResult.NO_FILES

    def _get_new_biobank_samples(self, from_date):
        """
        Retrieves BiobankStoredSample objects with `rdr_created`
        after the last run of the new participant workflow job.
        The query filters out participants that do not match the
        genomic validation requirements.
        :param: from_date
        :return: list of tuples (bid, pid, biobank_identifier.value, collected_site_id)
        """
        # TODO: add Genomic RoR Consent when that Code is added
        _new_samples_sql = """
        SELECT DISTINCT
          ss.biobank_id,
          p.participant_id,
          o.biobank_order_id,
          o.collected_site_id,
          ss.biobank_stored_sample_id,
          CASE
            WHEN p.withdrawal_status = :withdrawal_param THEN 1 ELSE 0
          END as not_withdrawn, 
          CASE
            WHEN ps.consent_for_study_enrollment = :general_consent_param THEN 1 ELSE 0
          END as general_consent_given,
          CASE
            WHEN ps.date_of_birth < DATE_SUB(now(), INTERVAL :dob_param*365 DAY) THEN 1 ELSE 0
          END AS valid_age,
          CASE
            WHEN c.value = "SexAtBirth_Male" THEN "M"
            WHEN c.value = "SexAtBirth_Female" THEN "F"
            ELSE "NA"
          END as sab, 
          CASE
            WHEN TRUE THEN 0 ELSE 0
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
            AND ps.consent_for_study_enrollment_time > :consent_cutoff_param
        """
        params = {
            "sample_status_param": SampleStatus.RECEIVED.__int__(),
            "dob_param": GENOMIC_VALID_AGE,
            "general_consent_param": QuestionnaireStatus.SUBMITTED.__int__(),
            "consent_cutoff_param": GENOMIC_VALID_CONSENT_CUTOFF.strftime("%Y-%m-%d"),
            "ai_param": Race.AMERICAN_INDIAN_OR_ALASKA_NATIVE.__int__(),
            "from_date_param": from_date.strftime("%Y-%m-%d"),
            "withdrawal_param": WithdrawalStatus.NOT_WITHDRAWN.__int__(),
        }
        with self.samples_dao.session() as session:
            result = session.execute(_new_samples_sql, params).fetchall()
        return list(zip(*result))

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
                                             "columns"])

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
        # Set each Manifest Definition as an instance of ManifestDef()
        # DRC Broad CVL WGS Manifest
        self.MANIFEST_DEFINITIONS[GenomicManifestTypes.DRC_CVL_WGS] = self.ManifestDef(
            job_run_field='cvlManifestWgsJobRunId',
            source_data=self._get_source_data_query(GenomicManifestTypes.DRC_CVL_WGS),
            destination_bucket=f'{self.bucket_name}',
            output_filename=f'{getSetting(GENOMIC_CVL_MANIFEST_SUBFOLDER)}/cvl_wgs_manifest_{self.job_run_id}.csv',
            columns=self._get_manifest_columns(GenomicManifestTypes.DRC_CVL_WGS),
        )

        # Color Array CVL Manifest
        self.MANIFEST_DEFINITIONS[GenomicManifestTypes.DRC_CVL_ARR] = self.ManifestDef(
            job_run_field='cvlManifestArrJobRunId',
            source_data=self._get_source_data_query(GenomicManifestTypes.DRC_CVL_ARR),
            destination_bucket=f'{self.bucket_name}',
            output_filename=f'{getSetting(GENOMIC_CVL_MANIFEST_SUBFOLDER)}/cvl_arr_manifest_{self.job_run_id}.csv',
            columns=self._get_manifest_columns(GenomicManifestTypes.DRC_CVL_ARR),
        )

    def _get_source_data_query(self, manifest_type):
        """
        Returns the query to use for manifest's source data
        :param manifest_type:
        :return: query object
        """
        query_sql = ""

        # DRC Broad CVL WGS Manifest
        if manifest_type == GenomicManifestTypes.DRC_CVL_WGS:
            query_sql = """
                SELECT s.genomic_set_name
                    , m.biobank_id
                    , m.sample_id
                    , m.sex_at_birth
                    , m.ny_flag
                    , gcv.site_id
                    , NULL as secondary_validation
                FROM genomic_set_member m
                    JOIN genomic_set s
                        ON s.id = m.genomic_set_id
                    JOIN genomic_gc_validation_metrics gcv
                        ON gcv.genomic_set_member_id = m.id
                WHERE gcv.processing_status = "pass"
                    AND m.reconcile_cvl_job_run_id IS NOT NULL
                    AND m.cvl_manifest_wgs_job_run_id IS NULL
                    AND m.genome_type = "aou_wgs"                    
            """

        # Color Array CVL Manifest
        if manifest_type == GenomicManifestTypes.DRC_CVL_ARR:
            query_sql = """
                SELECT s.genomic_set_name
                    , m.biobank_id
                    , m.sample_id
                    , m.sex_at_birth
                    , m.ny_flag
                    , gcv.site_id
                    , NULL as secondary_validation
                FROM genomic_set_member m
                    JOIN genomic_set s
                        ON s.id = m.genomic_set_id
                    JOIN genomic_gc_validation_metrics gcv
                        ON gcv.genomic_set_member_id = m.id
                WHERE gcv.processing_status = "pass"
                    AND m.reconcile_cvl_job_run_id IS NOT NULL
                    AND m.cvl_manifest_wgs_job_run_id IS NULL
                    AND m.genome_type = "aou_array"                    
            """
        return query_sql

    def _get_manifest_columns(self, manifest_type):
        """
        Defines the columns of each manifest-type
        :param manifest_type:
        :return: column tuple
        """
        columns = tuple()
        if manifest_type in [GenomicManifestTypes.DRC_CVL_WGS,
                             GenomicManifestTypes.DRC_CVL_ARR]:
            columns = (
                "genomic_set_name",
                "biobank_id",
                "sample_id",
                "sex_at_birth",
                "ny_flag",
                "site_id",
                "secondary_validation",
            )
        return columns

    def get_def(self, manifest_type):
        return self.MANIFEST_DEFINITIONS[manifest_type]


class ManifestCompiler:
    """
    This component compiles Genomic manifests
    based on definitions provided by ManifestDefinitionProvider
    TODO: All manifests should be updated to using this pattern, currently only CVL
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

    def generate_and_transfer_manifest(self, manifest_type):
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
                member = self.member_dao.get_member_from_sample_id(row.sample_id)
                results.append(
                    self.member_dao.update_member_job_run_id(
                        member,
                        job_run_id=self.run_id,
                        field=self.manifest_def.job_run_field
                    )
                )
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
            with exporter.open_writer(self.manifest_def.output_filename) as writer:
                writer.write_header(self.manifest_def.columns)
                writer.write_rows(source_data)
            return GenomicSubProcessResult.SUCCESS
        except RuntimeError:
            return GenomicSubProcessResult.ERROR
