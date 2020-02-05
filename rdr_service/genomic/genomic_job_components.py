"""
Component Classes for Genomic Jobs
Components are assembled by the JobController for a particular Genomic Job
"""

import csv
import logging
import re
import datetime
from collections import deque, namedtuple

from sqlalchemy import or_

from rdr_service.api_util import (
    open_cloud_file,
    copy_cloud_file,
    delete_cloud_file,
    list_blobs
)
from rdr_service.model.biobank_stored_sample import BiobankStoredSample
from rdr_service.model.biobank_order import BiobankOrder, BiobankOrderIdentifier
from rdr_service.model.participant import Participant
from rdr_service.model.participant_summary import ParticipantSummary
from rdr_service.model.genomics import GenomicSet, GenomicSetMember
from rdr_service.model.code import Code
from rdr_service.participant_enums import (
    GenomicSubProcessResult,
    WithdrawalStatus,
    QuestionnaireStatus,
    SampleStatus,
    GenomicSetStatus,
    GenomicManifestTypes,
)
from rdr_service.dao.genomics_dao import (
    GenomicGCValidationMetricsDao,
    GenomicSetMemberDao,
    GenomicFileProcessedDao,
    GenomicSetDao,
)
from rdr_service.dao.biobank_stored_sample_dao import BiobankStoredSampleDao
from rdr_service.dao.site_dao import SiteDao
from rdr_service.dao.participant_summary_dao import ParticipantSummaryDao
from rdr_service.genomic.genomic_biobank_manifest_handler import create_and_upload_genomic_biobank_manifest_file
from rdr_service.genomic.validation import (
    GENOMIC_VALID_AGE,
    GENOMIC_VALID_CONSENT_CUTOFF,
)
from rdr_service.offline.sql_exporter import SqlExporter
from rdr_service.config import (
    getSetting,
    GENOMIC_CVL_RECONCILIATION_REPORT_SUBFOLDER,
    GENOMIC_CVL_MANIFEST_SUBFOLDER,
    DNA_SAMPLE_TEST_CODES,
)

class GenomicFileIngester:
    """
    This class ingests a file from a source GC bucket into the destination table
    """

    def __init__(self):

        self.file_obj = None
        self.file_queue = deque()

        # Sub Components
        self.file_validator = None
        self.dao = GenomicGCValidationMetricsDao()
        self.file_processed_dao = GenomicFileProcessedDao()

    def generate_file_processing_queue(self, bucket_name, archive_folder_name, job_run_id):
        """
        Creates the list of files to be ingested in this run.
        Ordering is currently arbitrary;
        """
        files = self._get_uningested_file_names_from_bucket(bucket_name, archive_folder_name)
        if files == GenomicSubProcessResult.NO_FILES:
            return files
        else:
            for file_name in files:
                file_path = "/" + bucket_name + "/" + file_name
                new_file_record = self._create_file_record(job_run_id,
                                                           file_path,
                                                           bucket_name,
                                                           file_name)
                self.file_queue.append(new_file_record)

    def _get_uningested_file_names_from_bucket(self,
                                               bucket_name,
                                               archive_folder_name):
        """
        Searches the bucket for un-processed files.
        :param bucket_name:
        :return: list of filenames or NO_FILES result code
        """
        files = list_blobs('/' + bucket_name)
        files = [s.name for s in files
                 if archive_folder_name not in s.name.lower()
                 if 'datamanifest' in s.name.lower()]
        if not files:
            logging.info('No files in cloud bucket {}'.format(bucket_name))
            return GenomicSubProcessResult.NO_FILES
        return files

    def _create_file_record(self, run_id, path, bucket_name, file_name):
        return self.file_processed_dao.insert_file_record(run_id, path,
                                                   bucket_name, file_name)

    def _get_file_queue_for_run(self, run_id):
        return self.file_processed_dao.get_files_for_run(run_id)

    def ingest_gc_validation_metrics_file(self, file_obj):
        """
        Process to ingest the cell line data from
        the GC bucket and write to the database
        :param: file_obj: A genomic file object
        :return: A GenomicSubProcessResultCode
        """
        self.file_obj = file_obj
        self.file_validator = GenomicFileValidator()

        data_to_ingest = self._retrieve_data_from_path(self.file_obj.filePath)

        if data_to_ingest == GenomicSubProcessResult.ERROR:
            return GenomicSubProcessResult.ERROR
        elif data_to_ingest:
            # Validate the
            validation_result = self.file_validator.validate_ingestion_file(
                self.file_obj.fileName, data_to_ingest)

            if validation_result != GenomicSubProcessResult.SUCCESS:
                return validation_result

            logging.info("Data to ingest from {}".format(self.file_obj.fileName))
            return self._process_gc_metrics_data_for_insert(data_to_ingest)

        else:
            logging.info("No data to ingest.")
            return GenomicSubProcessResult.NO_FILES

    def update_file_processed(self, file_id, status, result):
        """Updates the genomic_file_processed record """
        self.file_processed_dao.update_file_record(file_id, status, result)

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

        return self.dao.insert_gc_validation_metrics_batch(gc_metrics_batch)


class GenomicFileValidator:
    """
    This class validates the Genomic Centers files
    """

    def __init__(self, filename=None, data=None, schema=None):
        self.filename = filename
        self.data_to_validate = data
        self.valid_schema = schema

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

    def validate_ingestion_file(self, filename, data_to_validate):
        """
        Procedure to validate an ingestion file
        :param filename:
        :param data_to_validate:
        :return: result code
        """
        self.filename = filename
        if not self._check_filename_valid(filename):
            return GenomicSubProcessResult.INVALID_FILE_NAME

        struct_valid_result = self._check_file_structure_valid(
            data_to_validate['fieldnames'])

        if struct_valid_result == GenomicSubProcessResult.INVALID_FILE_NAME:
            return GenomicSubProcessResult.INVALID_FILE_NAME

        if not struct_valid_result:
            logging.info("file structure of {} not valid.".format(filename))
            return GenomicSubProcessResult.INVALID_FILE_STRUCTURE

        return GenomicSubProcessResult.SUCCESS

    def _check_filename_valid(self, filename):
        # TODO: revisit this once naming convention is finalized for other jobs
        filename_components = filename.split('_')
        return (
            len(filename_components) == 5 and
            filename_components[1].lower() == 'aou' and
            filename_components[2].lower() in self.GC_CSV_SCHEMAS.keys() and
            re.search(r"[0-1][0-9][0-3][0-9]20[1-9][0-9]\.csv",
                      filename_components[4]) is not None
        )

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
        file_name = file_obj.fileName if file_obj else file_path.split('/')[-1]
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
                        member, self.run_id, 'reconcileManifestJobRunId')
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
                                                                 "sample_ids"])
            samples_meta = GenomicSampleMeta(*samples)
            logging.info(f'{self.__class__.__name__}: Processing new biobank_ids {samples_meta.bids}')
            new_genomic_set = self._create_new_genomic_set()
            # Create genomic set members
            for i, bid in enumerate(samples_meta.bids):
                # Validate sex at birth
                sab_code = self._get_sex_at_birth(samples_meta.pids[i])
                if sab_code not in self._SEX_AT_BIRTH_CODES.values():
                    continue
                logging.info(f'Creating genomic set member for PID: {samples_meta.pids[i]}')
                self._create_new_set_member(
                    biobankId=bid,
                    genomicSetId=new_genomic_set.id,
                    participantId=samples_meta.pids[i],
                    nyFlag=self._get_new_york_flag(samples_meta.site_ids[i]),
                    sexAtBirth=sab_code,
                    biobankOrderId=samples_meta.order_ids[i],
                    sampleId=samples_meta.sample_ids[i],
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
        with self.samples_dao.session() as session:
            result = session.query(BiobankStoredSample.biobankId,
                                   Participant.participantId,
                                   BiobankOrder.biobankOrderId,
                                   BiobankOrder.collectedSiteId,
                                   BiobankStoredSample.biobankStoredSampleId).filter(
                BiobankStoredSample.biobankId == Participant.biobankId,
                BiobankOrder.biobankOrderId == BiobankOrderIdentifier.biobankOrderId,
                BiobankStoredSample.biobankOrderIdentifier == BiobankOrderIdentifier.value,
                Participant.withdrawalStatus == WithdrawalStatus.NOT_WITHDRAWN,
                ParticipantSummary.participantId == Participant.participantId,
                ParticipantSummary.consentForStudyEnrollment == QuestionnaireStatus.SUBMITTED,
                ParticipantSummary.consentForStudyEnrollmentTime > GENOMIC_VALID_CONSENT_CUTOFF,
                ParticipantSummary.dateOfBirth < (
                    datetime.datetime.now() - datetime.timedelta(days=GENOMIC_VALID_AGE*365)
                ), or_(
                    ParticipantSummary.sampleStatus1ED04 == SampleStatus.RECEIVED,
                    ParticipantSummary.sampleStatus1SAL2 == SampleStatus.RECEIVED
                ),
                BiobankStoredSample.test.in_(("1ED04", "1SAL2")),
                BiobankStoredSample.rdrCreated > from_date).all()
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

    def _get_sex_at_birth(self, participant_id):
        """
        Looks up participant's sex at birth based on code.vale
        :param participant_id: the id of the participant
        :return: 'M', 'F', or 'NA'
        """
        # Assumes code.values like 'SexAtBirth_Male' and 'SexAtBirth_Female'
        with self.ps_dao.session() as session:
            result = session.query(Code.value)\
                          .filter(Code.codeId == ParticipantSummary.sexId,
                                  ParticipantSummary.participantId == participant_id)\
                          .first()
        return self._SEX_AT_BIRTH_CODES.get(
                 result[0].lower().split('_')[-1], 'NA') if result else 'NA'


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
