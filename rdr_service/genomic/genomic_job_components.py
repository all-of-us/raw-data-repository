"""
Component Classes for Genomic Jobs
Components are assembled by the JobController for a particular Genomic Job
"""
from collections import deque, namedtuple

import csv
import logging
import re

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
from rdr_service.participant_enums import GenomicSubProcessResult
from rdr_service.dao.genomics_dao import (
    GenomicGCValidationMetricsDao,
    GenomicSetMemberDao,
    GenomicFileProcessedDao,
    GenomicSetDao,
)
from rdr_service.dao.biobank_stored_sample_dao import BiobankStoredSampleDao
from rdr_service.dao.site_dao import SiteDao
from rdr_service.dao.participant_summary_dao import ParticipantSummaryDao
from rdr_service.genomic.genomic_biobank_menifest_handler import create_and_upload_genomic_biobank_manifest_file

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
    def __init__(self, run_id, archive_folder=None, file_mover=None):

        self.run_id = run_id

        self.bucket_name = None
        self.archive_folder = archive_folder

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
                member = self._lookup_member(metric.biobankId)
                results.append(
                    self.metrics_dao.update_manifest_reconciled(
                        metric, member.id, self.run_id)
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
                seq_biobank_id = self._parse_seq_filename(
                    seq_file_name)
                if seq_biobank_id == GenomicSubProcessResult.INVALID_FILE_NAME:
                    logging.info(f'Filename unable to be parsed: f{seq_file_name}')
                    return seq_biobank_id
                else:
                    metric_obj = self._get_sequence_metrics_to_reconcile_for_biobank_id(
                        seq_biobank_id)
                    if metric_obj:
                        # Updates the relevant fields for reconciliation
                        # sequence files for non-existent GC metrics are ignored
                        results.append(
                            self._update_gc_metrics(metric_obj, seq_file_name,
                                                    self.run_id)
                        )
                        # Archive the file
                        seq_file_path = "/" + bucket_name + "/" + seq_file_name
                        self.file_mover.archive_file(file_path=seq_file_path)
            return GenomicSubProcessResult.SUCCESS \
                if GenomicSubProcessResult.ERROR not in results \
                else GenomicSubProcessResult.ERROR

    def _lookup_member(self, biobank_id):
        """
        Calls the DAO to query for a genomic set member from a biobank ID
        :param biobank_id:
        :return: GenomicSetMember object
        """
        return self.member_dao.get_id_with_biobank_id(biobank_id)

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

    def _get_sequence_metrics_to_reconcile_for_biobank_id(self, biobank_id):
        """
        Calls the metrics DAO
        :param biobank_id:
        :return: list of GenomicGCValidationMetrics
        objects with null sequencing_file_name
        """
        return self.metrics_dao.get_metrics_to_reconcile_seq(biobank_id)

    def _update_gc_metrics(self, metric_obj, seq_file_name, job_run_id):
        """
        Uses metrics DAO to update GenomicGCValidationMetrics object
        with sequencing reconciliation data
        :param metric_obj: the object to update
        :param seq_file_name:
        :param job_run_id:
        :return: query result
        """
        return self.metrics_dao.update_metrics_with_seq_file(metric_obj,
                                                             seq_file_name,
                                                             job_run_id)


class GenomicBiobankSamplesCoupler:
    """This component creates new genomic set
    and members from the biobank samples pipeline"""

    def __init__(self, run_id):
        self.samples_dao = BiobankStoredSampleDao()
        self.set_dao = GenomicSetDao()
        self.member_dao = GenomicSetMemberDao()
        self.site_dao = SiteDao()
        self.ps_dao = ParticipantSummaryDao()
        self.run_id = run_id

        # Components
        self.manifest_coupler = GenomicManifestCoupler()

    def create_new_genomic_participants(self, from_date):
        """This method is the main execution method for this class
        It determines which biobankIDs to process and then executes subprocesses
        :param: from_date : the date from which to lookup new biobank_ids
        :return: result
        """
        # bid, pid, identifier, site_id
        GenomicSampleMeta = namedtuple("GenomicSampleMeta", "bid, pid, identifier, site_id")
        sample_meta = GenomicSampleMeta(*self._get_new_biobank_samples(from_date))
        print(sample_meta)
        if len(sample_meta) > 0:
            logging.info(f'Processing new biobank_ids {sample_meta[0]}')
            new_genomic_set = self._create_new_genomic_set()
            # Create genomic set members
            for i, bid in enumerate(sample_meta[0]):
                # TODO: add biobankOrderId, client, etc. attributes
                self._create_new_set_member(
                    biobankId=bid,
                    genomicSetId=new_genomic_set.id,
                    participantId=sample_meta[1][i],
                    nyFlag=self._get_new_york_flag(sample_meta[3][i]),
                    sexAtBirth=self._get_sex_at_birth(sample_meta[1][i]),
                )

            # Manifest
            try:
                create_and_upload_genomic_biobank_manifest_file(new_genomic_set.id)
                return GenomicSubProcessResult.SUCCESS
            except RuntimeError:
                return GenomicSubProcessResult.ERROR

        else:
            logging.info(f'New Participant Workflow: No new biobank_ids to process.')
            return GenomicSubProcessResult.NO_FILES

    def _get_new_biobank_samples(self, from_date):
        """
        This method retrieves BiobankStoredSample objects with nightlyReportDate
        after the last run of the new participant workflow job.
        :param: from_date
        :return: list of tuples (bid, pid, biobank_identifier.value, collected_site_id)
        """
        with self.samples_dao.session() as session:
            result = session.query(BiobankStoredSample.biobankId,
                                   Participant.participantId,
                                   BiobankOrder.biobankOrderId,
                                   BiobankOrder.collectedSiteId).filter(
                BiobankStoredSample.biobankId == Participant.biobankId,
                BiobankOrder.biobankOrderId == BiobankOrderIdentifier.biobankOrderId,
                BiobankStoredSample.biobankOrderIdentifier == BiobankOrderIdentifier.value,
                BiobankStoredSample.nightlyReportDate > from_date).distinct()
        return list(zip(*result))

    def _create_new_genomic_set(self):
        """Inserts a new genomic set for this run"""
        attributes = {
            'genomicSetName': f'new_participant_workflow_{self.run_id}',
            'genomicSetCriteria': '.',
            'genomicSetVersion': 1,
        }
        new_set_obj = GenomicSet(**attributes)
        return self.set_dao.insert(new_set_obj)

    def _create_new_set_member(self, **kwargs):
        """Inserts new GenomicSetMember objects"""
        new_member_obj = GenomicSetMember(**kwargs)
        return self.member_dao.insert(new_member_obj)

    def _get_new_york_flag(self, collected_site_id):
        """
        Looks up whether a collected site's state is NY
        :param collected_site_id: the id of the site
        :return: int
        """
        return int(self.site_dao.get(collected_site_id).state == 'NY')

    def _get_sex_at_birth(self, participant_id):
        """
        Looks up participant's sex at birth
        :param participant_id: the id of the participant
        :return: 'M', 'F', or 'NA'
        """
        # TODO: Implement when details received DA-1352
        # with self.ps_dao.session() as session:
        #     result = session.query(Code.value)\
        #                   .filter(Code.codeId == ParticipantSummary.sexId,
        #                           ParticipantSummary.participantId == participant_id)\
        #                   .first()
        # if result and result in _SEX_AT_BIRTH_CODES:
        #     print(result)
        #     return result
        # else:
        #     return 'NA'
        return 'F'

class GenomicManifestCoupler:
    """This component uses the existing manifest
    handler to create a genomic manifest"""

    def __init__(self):
        pass

    def create_manifest(self):
        pass
