#! /bin/env python
#
# Tool for the Single Point of Truth
#
import argparse
import logging
import json
import sys
import uuid
import itertools
import re

import sqlalchemy
from dateutil.parser import parse
from google.cloud import bigquery
from google.cloud.exceptions import NotFound
from sqlalchemy.orm import aliased
from sqlalchemy import func, or_
from sqlalchemy import case

from rdr_service.dao import database_factory
from rdr_service.clock import Clock
from rdr_service.model.code import Code
from rdr_service.model.genomics import GenomicSetMember, GenomicGCValidationMetrics
from rdr_service.model.participant import Participant
from rdr_service.model.participant_summary import ParticipantSummary
from rdr_service.model.questionnaire import QuestionnaireQuestion
from rdr_service.model.questionnaire_response import QuestionnaireResponse, QuestionnaireResponseAnswer
from rdr_service.participant_enums import QuestionnaireResponseClassificationType
from rdr_service.services.system_utils import setup_logging, setup_i18n
from rdr_service.spot.value_normalizer import ValueNormalizer
from rdr_service.tools.tool_libs import GCPProcessContext
from rdr_service.tools.tool_libs.tool_base import ToolBase
from rdr_service.spot.initialization import default_rdr_ods_tables, default_rdr_ods_table_data

_logger = logging.getLogger("rdr_logger")

# Tool_cmd and tool_desc name are required.
# Remember to add/update bash completion in 'tool_lib/tools.bash'
tool_cmd = "spot"
tool_desc = "tool for loading spot ODS and exporting to data mart."


def validate_args(arg_string):
    """
    Decorator to validate an argument
    :param arg_string: string representation of argument attribute
    :return:
    """
    _logger.info('Validating args')
    def wrapper(meth):

        def inner(obj):
            if arg_string == "cutoff_date":
                if obj.args.cutoff_date is None:
                    _logger.error("--cutoff-date required for operation")
                    return 1
                obj.args.cutoff_date = parse(obj.args.cutoff_date)
            if arg_string == "ods_table":
                valid_ods_tables = []
                if meth.__name__ == 'purge_duplicate_values':
                    valid_ods_tables.append("sample_data_element")

                if obj.args.ods_table not in valid_ods_tables:
                    _logger.error("--ods-table required for operation")
                    _logger.error(f"--ods-table required to be one of {valid_ods_tables}")
                    return 1

            return meth(obj)

        return inner
    return wrapper


class SpotTool(ToolBase):
    client = None
    export_timestamp = None
    loadable_ods_tables = [
        "data_element",
        "data_element_registry",
        "export_schema",
        "export_schema_data_element"
    ]
    default_datasets = ("rdr_ods", "genomic_research_mart")

    def run(self):
        if self.args.project == "all-of-us-rdr-prod":
            _logger.warning("You are about to run this operation on prod.")
            response = input("Continue? (y/n)> ")
            if response != "y":
                _logger.info("Aborted.")
                return 0
        self.client = bigquery.Client(project=self.args.project)
        process_map = {
            "LOAD_ODS_TABLE": self.load_ods_table_from_file,
            "TRANSFER_RDR_SAMPLE_DATA_TO_ODS": self.load_ods_sample_data_element,
            "TRANSFER_RDR_PARTICIPANT_SURVEY_DATA_TO_ODS": self.load_ods_participant_survey_data_element,
            "TRANSFER_RDR_PARTICIPANT_CONSENT_DATA_TO_ODS": self.load_ods_participant_consent_data_element,
            "EXPORT_ODS_TO_DATAMART": self.export_ods_data_to_datamart,
            "INITIALIZE_SYSTEM": self.initialize_system,
            "SEED_ODS_DATA": self.seed_ods_data,
            "PURGE_DUPLICATE_VALUES": self.purge_duplicate_values,
        }

        return process_map[self.args.process]()

    @staticmethod
    def create_uuid_for_row(row):
        """
        No auto-increment columns in BQ, so this column creates a UUID for the row.
        The smaller
        :param row: dict containing row data
        :return:
        """
        row.update({'id': uuid.uuid4().hex})
        return row

    def initialize_system(self):
        for dataset_name in self.default_datasets:
            dataset_id = f"{self.args.project}.{dataset_name}"
            try:
                self.client.get_dataset(dataset_id)  # Make an API request.
                _logger.warning(f"{dataset_id} already exists. Skipping initialization.")
                continue
            except NotFound:
                _logger.info(f"Initializing {dataset_name}")

            init_method_name = f"initialize_{dataset_name}"

            getattr(self, init_method_name)(dataset_id)

        if self.args.seed_data:
            _logger.info("Seeding data elements and export schemata...")
            if not self.args.dryrun:
                self.seed_ods_data()

        return 0

    def extract_schema(self, table_id):
        """
        Utility to get the BQ schema in a copy/pasteable format
        :param table_id:
        :return:
        """
        table = self.client.get_table(table_id)  # API Request

        # View table properties for copy/paste
        for a in table.schema:
            print(f'bigquery.{a},')

    def initialize_rdr_ods(self, dataset_id):
        # Create the rdr_ods dataset
        _logger.info(f'Creating dataset...')
        if not self.args.dryrun:
            self.create_bq_dataset(dataset_id)

            # Create the rdr_ods tables
            _logger.info(f'Creating tables...')
            for table in default_rdr_ods_tables:
                self.create_bq_table(table_id=f"{dataset_id}.{table['table_name']}",
                                     fields=table['fields'])

    def initialize_genomic_research_mart(self, dataset_id):
        if not self.args.dryrun:
            self.create_bq_dataset(dataset_id)

    def create_bq_dataset(self, dataset_id):

        # Construct a full Dataset object to send to the API.
        dataset = bigquery.Dataset(dataset_id)

        # Set location
        dataset.location = "US"

        # Send the dataset to the API for creation, with an explicit timeout.
        dataset = self.client.create_dataset(dataset, timeout=30)  # Make an API request.
        _logger.info(f"Created dataset {self.client.project}.{dataset.dataset_id}")
        return dataset

    def create_bq_table(self, table_id, fields):
        table = bigquery.Table(table_id, schema=fields)
        table = self.client.create_table(table)  # Make an API request.
        _logger.info(
            f"Created table {table.project}.{table.dataset_id}.{table.table_id}"
        )

    def seed_ods_data(self):
        """
        Loads ODS default data elements and schemata
        """
        for table in default_rdr_ods_table_data:
            _logger.info(f"Seeding: {table['table_name']}")
            self.args.ods_table = table['table_name']
            self.args.data_file = table['data']
            self.load_ods_table_from_file()

    def load_ods_table_from_file(self):
        """
        Wrapper function for populating ODS tables with data.
        Handles validating optional tool args and
        preps data from JSON file to call load_data_to_bq_table.
        Table must be registered in loadable_ods_tables
        """
        if self.args.ods_table not in self.loadable_ods_tables:
            _logger.error(f"ODS Table must be in {self.loadable_ods_tables}")
            return 1

        if not self.args.data_file:
            _logger.error("--data-file must be supplied.")
            return 1

        try:
            with open(self.args.data_file, "r") as f:
                data = json.load(f)

                if self.args.ods_table == "data_element" and self.args.new_ids:
                    data = map(self.create_uuid_for_row, data)

                table_id = f"{self.gcp_env.project}.rdr_ods.{self.args.ods_table}"
                return self.load_data_to_bq_table(table_id, data)

        except FileNotFoundError:
            _logger.error(f'File {self.args.data_file} was not found.')
            return 1
        except json.decoder.JSONDecodeError as e:
            _logger.error(f"Error decoding json file: {e}")
            return 1

    def load_data_to_bq_table(self, table_id, rows_to_insert, skip_invalid=False, buffer_size=10000):
        """
        Inserts rows into a BQ table
        :param buffer_size:
        :param skip_invalid:
            If True: Insert all valid rows.
            If False, fail entire request if any rows are invalid
        :param table_id: qualified table ID to insert data into
        :param rows_to_insert: sequence of JSON serializable data to insert
        """
        # Loading by chunks of 10k
        buffer = []
        chunk = 1
        for row in rows_to_insert:
            buffer.append(row)
            if len(buffer) % buffer_size == 0:

                errors = self.client.insert_rows_json(table_id, buffer,
                                                      skip_invalid_rows=skip_invalid)  # Make an API request.
                if not errors:
                    _logger.info(f"New rows have been added (chunk {chunk}).")
                else:
                    _logger.error(f"Encountered errors while inserting rows in chunk {chunk}: {errors}")
                    return 1

                buffer = []
                chunk += 1

        errors = self.client.insert_rows_json(table_id, buffer,
                                              skip_invalid_rows=skip_invalid)  # Make an API request.
        if not errors:
            _logger.info(f"New rows have been added (chunk {chunk}).")
            return 0
        else:
            _logger.error(f"Encountered errors while inserting rows in chunk {chunk}: {errors}")
            return 1

    def run_bq_query_job(self, query_string, job_config=None):
        """
        Executes a BQ job
        :param query_string: SQL String to execute in job
        :param job_config: JobConfig object
        :return: results of the query job
        """
        query_job = self.client.query(query_string, job_config=job_config)
        return query_job.result()

    def call_export_genomic_research_procedure(self, schema):
        # Query parameters are not supported for pivot columns.
        # String formatting the query is vulnerable to injection
        # Using a stored procedure:  rdr_ods.test_export_genomic_research_procedure
        # EXECUTE IMMEDIATE provides security contrainsts to prevent injection:
        # https://cloud.google.com/bigquery/docs/multi-statement-queries#security_constraints
        _logger.info("Exporting Data to intermediate table...")
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("cutoff_date", "TIMESTAMP", self.args.cutoff_date),
            ]
        )
        if schema == "genomic_research_array":
            return self.run_bq_query_job(
                "CALL rdr_ods.export_genomic_research_array_procedure(@cutoff_date);",
                job_config=job_config
            )
        if schema == "genomic_research_wgs":
            return self.run_bq_query_job(
                "CALL rdr_ods.export_genomic_research_wgs_procedure(@cutoff_date);",
                job_config=job_config
            )
        _logger.error("ERROR: Invalid export schema")
        sys.exit()

    @validate_args(arg_string="cutoff_date")
    def load_ods_sample_data_element(self):
        """
        Main function for extracting RDR genomic data, pivoting the data,
        and loading it into rdr_ods.sample_data_element
        """
        self.gcp_env.activate_sql_proxy()

        # get data elements from registry
        registered_member_data_elements = list(self.get_data_elements_from_registry("sample_data_element",
                                                                                    "rdr_genomic"))

        # Get all newly modified members (with aw4)
        modified_members = self.get_modified_member_data(registered_member_data_elements,
                                                         last_update_date=self.args.cutoff_date)

        # Pivot registered
        pivoted_data = self.pivot_member_data(registered_member_data_elements, modified_members)

        # Load the pivoted data to BQ table
        table_id = f"{self.args.project}.rdr_ods.sample_data_element"

        self.load_data_to_bq_table(table_id, pivoted_data)

        return 0

    @validate_args(arg_string="cutoff_date")
    def load_ods_participant_survey_data_element(self):
        """
        Main function for extracting RDR survey data
        and loading it into rdr_ods.participant_survey_data_element
        """
        self.gcp_env.activate_sql_proxy()

        # get data elements from registry
        survey_data_elements = list(self.get_data_elements_from_registry("participant_survey_data_element",
                                                                         "rdr_survey"))

        # Get all newly modified members (with aw4)
        survey_data = self.get_new_survey_data(survey_data_elements,
                                               last_update_date=self.args.cutoff_date)

        _logger.info("building survey data element records...")
        rows = self.build_participant_data_element_row(survey_data)

        # Load the rows to BQ table
        table_id = f"{self.args.project}.rdr_ods.participant_survey_data_element"

        self.load_data_to_bq_table(table_id, rows)

        return 0

    def load_ods_participant_consent_data_element(self):
        """
        Main function for extracting RDR consent/withdrawal data
        and loading it into rdr_ods.participant_consent_data_element
        TODO: For deadline's sake, values are hardcoded. Should increase scaleability in next phase
        """
        self.gcp_env.activate_sql_proxy()

        consent_targets = ['ParticipantSummary.consentForStudyEnrollment', 'Participant.withdrawalStatus']

        consent_regs = list(self.get_consent_data_element_registry(source_targets=consent_targets))

        # Primary consent and WD status are currently special cases
        # In a future phase, I'll break this out into its own table
        participant_consent_data = self.get_participant_consent_data(last_update_date=self.args.cutoff_date,
                                                                     data_elements=consent_regs)

        _logger.info("building consent data element records...")
        rows = self.build_participant_data_element_row(participant_consent_data)

        table_id = f"{self.args.project}.rdr_ods.participant_consent_data_element"

        self.load_data_to_bq_table(table_id, rows)

        return 0

    @validate_args(arg_string="ods_table")
    def purge_duplicate_values(self):
        """
        Removes duplicate de-value pairs, retaining the latest
        from the ODS table supplied by --ods-table.
        currently only sample_data_element required
        :return:
        """
        # calls stored proc with ods table to purge
        if self.args.ods_table == "sample_data_element":
            self.call_purge_duplicate_sample_values_procedure()

        return 0


    @validate_args(arg_string="cutoff_date")
    def export_ods_data_to_datamart(self):
        """
        Main function for extracting ODS data, pivoting the data,
        loading it into an rdr_ods temp table, and then exporting it as
        a snapshot to the genomic_research_datamart
        """
        for schema in ['genomic_research_array', 'genomic_research_wgs']:

            export_schema = self.get_export_schema(schema)

            # Pivot data from sample_data_element into BQ temp table
            self.call_export_genomic_research_procedure(schema)

            # load records into snapshot table in rdr_genomic_research_export dataset
            destination = self.get_destination_table(export_schema)

            self.load_datamart_snapshot(destination)

            _logger.info(f"Data export to {destination} complete.")

        return 0

    def get_data_elements_from_registry(self, target_table, source_system):
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("target_table", "STRING", target_table),
                bigquery.ScalarQueryParameter("source_system", "STRING", source_system),
            ]
        )
        return self.run_bq_query_job(f"""
        SELECT reg.data_element_id
          , reg.target_table
          , de.source_system
          , de.source_target
          , reg.normalization_rule
        FROM `rdr_ods.data_element_registry` reg
          INNER JOIN `rdr_ods.data_element` de ON de.id = reg.data_element_id
        where true
          and reg.target_table = @target_table
          and reg.active_flag = true
          and de.source_system = @source_system
        """, job_config=job_config)

    def get_consent_data_element_registry(self, source_targets):
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ArrayQueryParameter("source_targets", "STRING", source_targets),
            ]
        )
        return self.run_bq_query_job(f"""
                SELECT reg.data_element_id
                  , reg.target_table
                  , de.source_system
                  , de.source_target
                FROM `rdr_ods.data_element_registry` reg
                  INNER JOIN `rdr_ods.data_element` de ON de.id = reg.data_element_id
                where true
                  and reg.active_flag = true
                  and de.source_system = "rdr_consent"
                  and de.source_target IN UNNEST(@source_targets)
                """, job_config=job_config)

    @staticmethod
    def get_participant_consent_data(last_update_date, data_elements):
        session = database_factory.get_database().make_session()
        # TODO: Refactor queries and DE metadata to be more dynamic
        # Get data element metadata for union below
        withdrawal_de = filter(lambda de: de['source_target'] == "Participant.withdrawalStatus",
                               data_elements).__next__()

        primary_de = filter(lambda de: de['source_target'] == 'ParticipantSummary.consentForStudyEnrollment',
                            data_elements).__next__()

        query_participant = session.query(
            Participant.participantId,
            Participant.researchId,
            Participant.withdrawalStatus.label("value_string"),
            Participant.withdrawalAuthored.label("authored_timestamp"),
            func.now().label('created_timestamp'),
            sqlalchemy.literal(withdrawal_de.data_element_id).label("data_element_id")
        ).select_from(
            Participant
        ).filter(
            Participant.withdrawalAuthored >= last_update_date,
            # QuestionnaireResponse.participantId.in_() TODO: Add param
        )
        query_summary = session.query(
                ParticipantSummary.participantId,
                Participant.researchId,
                ParticipantSummary.consentForStudyEnrollment.label("value_string"),
                ParticipantSummary.consentForStudyEnrollmentAuthored.label("authored_timestamp"),
                func.now().label('created_timestamp'),
                sqlalchemy.literal(primary_de.data_element_id).label("data_element_id")
            ).select_from(
                ParticipantSummary
            ).join(
                Participant,
                Participant.participantId == ParticipantSummary.participantId
            ).filter(
                ParticipantSummary.consentForStudyEnrollmentAuthored >= last_update_date,
            )
        set_a = set(query_summary.all())
        set_b = set(query_participant.all())
        return set_a.union(set_b)

    @staticmethod
    def get_modified_member_data(data_elements, last_update_date):
        """
        Retrieves genomic modified data since last_update_date
        Builds selection columns dynamically based on data_elements
        :param data_elements: sample_data_elements list
        :param last_update_date:
        :return: query results
        """
        rdr_attributes = [
            GenomicSetMember.participantId,
            GenomicSetMember.sampleId,
            Participant.researchId,
            func.now().label('created_timestamp'),
        ]

        for data_element in data_elements:
            rdr_attributes.append(eval(data_element.source_target))

        session = database_factory.get_database().make_session()

        query = session.query(
            *rdr_attributes
        ).join(
            Participant,
            Participant.participantId == GenomicSetMember.participantId
        ).join(
            ParticipantSummary,
            Participant.participantId == ParticipantSummary.participantId
        ).join(
            GenomicGCValidationMetrics,
            GenomicGCValidationMetrics.genomicSetMemberId == GenomicSetMember.id
        ).filter(
            GenomicSetMember.aw4ManifestJobRunID.isnot(None),
            GenomicSetMember.ignoreFlag == 0,
            GenomicGCValidationMetrics.ignoreFlag == 0,
            or_(
                GenomicSetMember.modified > last_update_date,
                GenomicGCValidationMetrics.modified > last_update_date,
                ),
            # GenomicSetMember.participantId.in_(),  # TODO: Add param
            GenomicSetMember.genomeType.in_(["aou_wgs", "aou_array"])
        )
        return query.all()

    @staticmethod
    def get_new_survey_data(data_elements, last_update_date):
        """
        Retrieves questionnaire response answers since last_update_date
        Builds filters and selection cases dynamically based on data_elements
        :param data_elements: survey_data_elements list
        :param last_update_date:
        :return: query results
        """
        # Code table aliases
        question_code = aliased(Code)
        answer_code = aliased(Code)

        # Attribute list
        rdr_attributes = [
            QuestionnaireResponse.participantId,
            Participant.researchId,
            question_code.value.label('question_code'),
            answer_code.value.label('value_string'),
            QuestionnaireResponse.authored.label("authored_timestamp"),
            func.now().label('created_timestamp'),
        ]
        # Get DE IDs from data element and add as case
        case_statements = []
        for de in data_elements:
            case_statements.append((question_code.value == de.source_target, de.data_element_id))

        rdr_attributes.append(
            case(
                case_statements, else_=None
            ).label('data_element_id')
        )

        session = database_factory.get_database().make_session()

        query = session.query(
            *rdr_attributes
        ).select_from(
            QuestionnaireResponseAnswer
        ).join(
            QuestionnaireQuestion,
            QuestionnaireQuestion.questionnaireQuestionId == QuestionnaireResponseAnswer.questionId
        ).join(
            QuestionnaireResponse,
            QuestionnaireResponseAnswer.questionnaireResponseId == QuestionnaireResponse.questionnaireResponseId
        ).join(
            question_code,
            question_code.codeId == QuestionnaireQuestion.codeId
        ).join(
            answer_code,
            answer_code.codeId == QuestionnaireResponseAnswer.valueCodeId
        ).join(
            Participant,
            Participant.participantId == QuestionnaireResponse.participantId
        ).filter(
            QuestionnaireResponse.authored >= last_update_date,
            # QuestionnaireResponse.participantId.in_() TODO: Add param
            QuestionnaireResponse.classificationType == QuestionnaireResponseClassificationType.COMPLETE,
            question_code.value.in_([de.source_target for de in data_elements])
        )
        return query.all()

    @staticmethod
    def build_participant_data_element_row(data):
        """
        Generator function to build json-serializable
        participant_survey|consent_data_element record
        :param data:
        :return dictionary representation of record to insert
        """

        for row in data:
            new_row = {
                'participant_id': row.participantId,
                'research_id': row.researchId,
                'data_element_id': row.data_element_id,
                'value_string': str(row.value_string),
                'created_timestamp': row.created_timestamp.isoformat(),
                'authored_timestamp': row.authored_timestamp.isoformat()
            }

            yield new_row

    @staticmethod
    def pivot_member_data(data_elements, data):
        """
        Builds json-serializable row for insert into sample_data_element
        takes cartesian product of data_elements and row data
        starmap applies build_new_row to each element in product
        starmap is filtered to remove rows where DE value is None
        :param data_elements:
        :param data:
        :return:
        """
        _logger.info("pivoting member data...")

        def build_new_row(old_row, data_element):
            """
            applies normalization rules to old_row
            builds new row with data_element metadata.
            :param old_row:
            :param data_element:
            :return: new_row dict
            """
            attribute_name = data_element.source_target.split(".")[-1]
            de_val = getattr(old_row, attribute_name)

            if de_val is not None:
                de_val = str(de_val)

                # Apply normalization rules
                rules = data_element.normalization_rule
                if rules:
                    normalizer = ValueNormalizer()
                    for rule in rules:
                        de_val = normalizer.rule_map[rule](de_val)

                new_row = {
                    'participant_id': old_row.participantId,
                    'research_id': old_row.researchId,
                    'sample_id': old_row.sampleId,
                    'data_element_id': data_element.data_element_id,
                    'value_string': de_val,
                    'created_timestamp': old_row.created_timestamp.isoformat()
                }

                return new_row

        product = itertools.product(data, data_elements)
        return filter(lambda x: x is not None, itertools.starmap(build_new_row, product))

    def get_export_schema(self, schema):
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("schema", "STRING", schema),
            ]
        )
        return list(self.run_bq_query_job(
            """
            select schema_name
              , destination_mart
              , destination_target_table
            from `rdr_ods.export_schema`
            where schema_name = @schema
            """, job_config=job_config
        ))[0]

    def get_export_data_element_names(self):
        rows = self.run_bq_query_job(
            """
            select display_name
            from `rdr_ods.export_schema_data_element`
            where schema_name = "genomic_research"
              and active_flag is true
            ;
            """
        )
        return [name.display_name for name in rows]

    def load_datamart_snapshot(self, destination):
        _logger.info(f"Loading data to {destination}...")
        return self.run_bq_query_job(
            f"CALL rdr_ods.create_export_snapshot('{destination}');"
        )

    def call_purge_duplicate_sample_values_procedure(self):
        confirm = input(f"Are you sure you want to purge duplicates for {self.args.ods_table}? \n(y/n)>")
        if confirm.lower() == "y":
            _logger.info(f"Calling stored procedure...")
            return self.run_bq_query_job(
                f"CALL rdr_ods.purge_duplicate_sample_values();"
            )
        else:
            _logger.info("Aborting.")
            return 1

    @staticmethod
    def get_destination_table(export_schema):
        clock = Clock()
        # Todo: set up name rules for different export templates
        snapshot_id = clock.now().strftime("%Y%m%d%H%M%S")

        tablename_template = export_schema.destination_target_table

        name = f"{export_schema.destination_mart}."

        name += re.sub("%.*?%", snapshot_id, tablename_template)

        return name


def run():
    # Set global debug value and setup application logging.
    setup_logging(
        _logger, tool_cmd, "--debug" in sys.argv, "{0}.log".format(tool_cmd) if "--log-file" in sys.argv else None
    )
    setup_i18n()
    tool_processes = [
        "LOAD_ODS_TABLE",
        "TRANSFER_RDR_SAMPLE_DATA_TO_ODS",
        "TRANSFER_RDR_PARTICIPANT_SURVEY_DATA_TO_ODS",
        "TRANSFER_RDR_PARTICIPANT_CONSENT_DATA_TO_ODS",
        "EXPORT_ODS_TO_DATAMART",
        "INITIALIZE_SYSTEM",
        "SEED_ODS_DATA",
        "PURGE_DUPLICATE_VALUES"
    ]
    # Setup program arguments.
    parser = argparse.ArgumentParser(prog=tool_cmd, description=tool_desc)
    parser.add_argument("--project", help="gcp project name", default="localhost")  # noqa
    parser.add_argument("--account", help="pmi-ops account", default=None)  # noqa
    parser.add_argument("--service-account", help="gcp iam service account", default=None)  # noqa
    parser.add_argument("--dryrun", help="use for testing",
                        action="store_true",
                        default=False)  # noqa
    parser.add_argument("--process",
                        help="process to run, choose one: {}".format(tool_processes),
                        choices=tool_processes,
                        default=None,
                        required=True,
                        type=str)  # noqa
    parser.add_argument("--ods-table", help=f"ODS table to load",
                        default=None,
                        required=False,
                        type=str)  # noqa

    parser.add_argument("--cutoff-date", help=f"Date to use as cutoff",
                        default=None,
                        required=False)  # noqa

    parser.add_argument("--data-file", help="json file to load", default=None)  # noqa
    parser.add_argument("--seed-data", help="load ods tables with data on initialization",
                        action="store_true",
                        default=False)  # noqa
    parser.add_argument("--new-ids", help="creates new uuids if loading rdr_ods.data_element",
                        action="store_true",
                        default=False)  # noqa

    args = parser.parse_args()

    with GCPProcessContext(tool_cmd, args.project, args.account, args.service_account) as gcp_env:
        try:
            ods_process = SpotTool(args, gcp_env)
            exit_code = ods_process.run()
        # pylint: disable=broad-except
        except Exception as e:
            _logger.info(f'Error has occured, {e}. For help use "ods --help".')
            exit_code = 1

        return exit_code


# --- Main Program Call ---
if __name__ == "__main__":
    sys.exit(run())
