"""
Component Classes for Genomic Jobs
Components are assembled by the JobController for a particular Genomic Job
"""

import csv
import logging

from rdr_service.api_util import open_cloud_file
from rdr_service.model.genomics import GenomicSubProcessResult
from rdr_service.dao.genomics_dao import GenomicGCValidationMetricsDao
from rdr_service.genomic.genomic_set_file_handler import (
    FileNotFoundError
)


class GenomicFileIngester:
    """
    This class ingests a file from a source GC bucket into the destination table
    """

    def __init__(self):

        self.file_obj = None

        # Sub Components
        self.file_validator = None
        self.dao = GenomicGCValidationMetricsDao()

    def ingest_gc_validation_metrics_file(self,
                                          file_obj,
                                          genomic_set_member_id=None):
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

    def _retrieve_data_from_path(self, path, archive_folder=None):
        """
        Retrieves the last genomic data file from a bucket
        :param path: The source file to ingest
        :param archive_folder: subfolder in GC bucket to move processed files
        :return: (csv filename, csv file data as a DictReader)
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
        :return cleaned data
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

            row_copy['member_id'] = 1
            row_copy['file_id'] = self.file_obj.id
            row_copy['biobank id'] = row_copy['biobank id'].replace('T', '')

            obj_to_insert = row_copy
            gc_metrics_batch.append(obj_to_insert)

        return self.dao.insert_gc_validation_metrics_batch(gc_metrics_batch)


class GenomicFileValidator:
    """
    This module validates the Genomic Centers files
    Validates data structure against a schema, or validates the data.
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
        :return: validation_result: Enum(GenomicSubProcessResult)
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
        # TODO: once naming convention is finalized
        return True

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

