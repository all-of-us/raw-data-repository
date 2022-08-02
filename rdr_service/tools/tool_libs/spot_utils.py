#! /bin/env python
#
# Template for RDR tool python program.
#
import argparse
import logging
import json
import sys
import datetime
import uuid
import itertools
import re
from google.cloud import bigquery
from sqlalchemy.orm import aliased
from sqlalchemy import func
from sqlalchemy import case

from rdr_service.dao import database_factory
from rdr_service.clock import Clock
from rdr_service.model.code import Code
from rdr_service.model.genomics import GenomicSetMember
from rdr_service.model.participant import Participant
from rdr_service.model.participant_summary import ParticipantSummary
from rdr_service.model.questionnaire import QuestionnaireQuestion
from rdr_service.model.questionnaire_response import QuestionnaireResponse, QuestionnaireResponseAnswer
from rdr_service.participant_enums import QuestionnaireResponseStatus, QuestionnaireResponseClassificationType
from rdr_service.services.system_utils import setup_logging, setup_i18n
from rdr_service.tools.tool_libs import GCPProcessContext
from rdr_service.tools.tool_libs.tool_base import ToolBase
# from rdr_service.spot.data import default

_logger = logging.getLogger("rdr_logger")

# Tool_cmd and tool_desc name are required.
# Remember to add/update bash completion in 'tool_lib/tools.bash'
tool_cmd = "spot"
tool_desc = "tool for loading spot ODS and exporting to data mart."


class SpotTool(ToolBase):
    client = bigquery.Client()
    session = None
    export_timestamp = None
    loadable_ods_tables = [
        "data_element",
        "data_element_registry",
        "export_schema",
        "export_schema_data_element"
    ]

    def run(self):
        self.gcp_env.activate_sql_proxy()

        process_map = {
            "LOAD_ODS_TABLE": self.load_ods_table,
            "LOAD_ODS_EXPORT_SCHEMA_DATA_ELEMENT": None,
            "TRANSFER_RDR_SAMPLE_DATA_TO_ODS": self.load_ods_sample_data_element,
            "TRANSFER_RDR_PARTICIPANT_DATA_TO_ODS": self.load_ods_participant_data_element,
            "EXPORT_ODS_TO_DATAMART": self.export_ods_data_to_datamart
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

    def load_ods_table(self):
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

                if self.args.ods_table == "data_element":
                    data = map(self.create_uuid_for_row, data)

                table_id = f"{self.gcp_env.project}.rdr_ods.{self.args.ods_table}"
                return self.load_data_to_bq_table(table_id, data)

        except FileNotFoundError:
            _logger.error(f'File {self.args.data_file} was not found.')
            return 1
        except json.decoder.JSONDecodeError as e:
            _logger.error(f"Error decoding json file: {e}")
            return 1

    def load_data_to_bq_table(self, table_id, rows_to_insert):
        errors = self.client.insert_rows_json(table_id, rows_to_insert)  # Make an API request.
        if not errors:
            _logger.info("New rows have been added.")
            return 0
        else:
            _logger.error("Encountered errors while inserting rows: {}".format(errors))
            return 1

    def run_bq_query_job(self, query_string, job_config=None):
        query_job = self.client.query(query_string, job_config=job_config)
        return query_job.result()

    def export_sample_data_element_to_temp_table(self):
        # Query parameters are not supported for pivot columns.
        # String formatting the query is vulnerable to injection
        # Using a stored procedure:  rdr_ods.test_export_genomic_research_procedure
        # EXECUTE IMMEDIATE provides security contrainsts to prevent injection:
        # https://cloud.google.com/bigquery/docs/multi-statement-queries#security_constraints
        _logger.info("Exporting Data to intermediate table...")
        return self.run_bq_query_job(
            "CALL rdr_ods.test_export_genomic_research_procedure();"
        )

    def load_ods_sample_data_element(self):
        """
        Main function for extracting RDR genomic data, pivoting the data,
        and loading it into rdr_ods.sample_data_element
        """
        # get data elements from registry
        registered_member_data_elements = list(self.get_data_elements_from_registry("sample_data_element"))

        # Get all newly modified members (with aw4)
        modified_members = self.get_modified_member_data(registered_member_data_elements,
                                                         last_update_date=datetime.datetime(2022, 7, 25, 20))

        # Pivot registered
        pivoted_data = self.pivot_member_data(registered_member_data_elements, modified_members)

        # Load the pivoted data to BQ table
        table_id = "all-of-us-rdr-sandbox.rdr_ods.sample_data_element"

        self.load_data_to_bq_table(table_id, pivoted_data)

        return 0

    def load_ods_participant_data_element(self):
        """
        Main function for extracting RDR survey data
        and loading it into rdr_ods.sample_data_element
        """

        # get data elements from registry
        survey_data_elements = list(self.get_data_elements_from_registry("participant_data_element"))

        # Get all newly modified members (with aw4)
        survey_data = self.get_new_survey_data(survey_data_elements,
                                               last_update_date=datetime.datetime(2022, 8, 1, 0))

        _logger.info("building survey data element records...")
        rows = self.build_survey_data_element_row(survey_data)

        # Load the rows to BQ table
        table_id = "all-of-us-rdr-sandbox.rdr_ods.participant_data_element"

        self.load_data_to_bq_table(table_id, rows)

        return 0

    def export_ods_data_to_datamart(self):
        """
        Main function for extracting ODS data, pivoting the data,
        loading it into an rdr_ods temp table, and then exporting it as
        a snapshot to the genomic_research_datamart
        """
        export_schema = self.get_export_schema()

        # Pivot data from sample_data_element into BQ temp table
        self.export_sample_data_element_to_temp_table()

        # load records into snapshot table in rdr_genomic_research_export dataset
        destination = self.get_destination_table(export_schema)

        self.load_datamart_snapshot(destination)

        _logger.info(f"Data export to {destination} complete.")

        return 0

    def get_data_elements_from_registry(self, target_table):
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("target_table", "STRING", target_table),
            ]
        )
        return self.run_bq_query_job("""
        SELECT reg.data_element_id
          , reg.target_table
          , de.source_system
          , de.source_target
        FROM `all-of-us-rdr-sandbox.rdr_ods.data_element_registry` reg
          INNER JOIN `all-of-us-rdr-sandbox.rdr_ods.data_element` de ON de.id = reg.data_element_id
        where true
          and reg.target_table = @target_table
          and reg.active_flag = true
        """, job_config=job_config)

    def get_modified_member_data(self, data_elements, last_update_date):
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

        self.session = database_factory.get_database().make_session()

        query = self.session.query(
            *rdr_attributes
        ).join(
            Participant,
            Participant.participantId == GenomicSetMember.participantId
        ).join(
            ParticipantSummary,
            Participant.participantId == ParticipantSummary.participantId
        ).filter(
            GenomicSetMember.aw4ManifestJobRunID.isnot(None),
            GenomicSetMember.modified > last_update_date,
            GenomicSetMember.genomeType == "aou_wgs"
        )
        return query.all()

    def get_new_survey_data(self, data_elements, last_update_date):
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
            question_code.value.label('question_code'),
            answer_code.value.label('answer_code'),
            QuestionnaireResponse.authored,
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

        self.session = database_factory.get_database().make_session()

        query = self.session.query(
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
            GenomicSetMember,
            GenomicSetMember.participantId == QuestionnaireResponse.participantId
        ).join(
            question_code,
            question_code.codeId == QuestionnaireQuestion.codeId
        ).join(
            answer_code,
            answer_code.codeId == QuestionnaireResponseAnswer.valueCodeId
        ).join(

        ).filter(
            GenomicSetMember.genomeType == "aou_wgs",
            GenomicSetMember.aw4ManifestJobRunID.isnot(None),
            QuestionnaireResponse.authored >= last_update_date,
            QuestionnaireResponse.status == QuestionnaireResponseStatus.COMPLETED,
            QuestionnaireResponse.classificationType == QuestionnaireResponseClassificationType.COMPLETE,
            question_code.value.in_([de.source_target for de in data_elements])
        )
        return query.all()

    @staticmethod
    def build_survey_data_element_row(survey_data):
        """
        Generator function to build json-serializable survey_data_element record
        :param survey_data:
        :return dictionary representation of record to insert
        """

        for row in survey_data:
            new_row = {
                'participant_id': row.participantId,
                'data_element_id': row.data_element_id,
                'value_string': row.answer_code,
                'created_timestamp': row.created_timestamp.isoformat(),
                'authored_timestamp': row.authored.isoformat()
            }

            yield new_row

    @staticmethod
    def pivot_member_data(data_elements, data):
        _logger.info("pivoting member data...")

        def build_new_row(old_row, data_element):
            attribute_name = data_element.source_target.split(".")[-1]
            new_row = {
                'participant_id': old_row.participantId,
                'research_id': old_row.researchId,
                'sample_id': old_row.sampleId,
                'data_element_id': data_element.data_element_id,
                'value_string': str(getattr(old_row, attribute_name)),
                'created_timestamp': old_row.created_timestamp.isoformat()
            }
            return new_row

        product = itertools.product(data, data_elements)
        return itertools.starmap(build_new_row, product)

    def get_export_schema(self):
        return list(self.run_bq_query_job(
            """
            select schema_name
              , destination_mart
              , destination_target_table
            from `rdr_ods.export_schema`
            where schema_name = "genomic_research"
            """
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
            f"CALL rdr_ods.test_load_snapshot_table('{destination}');"
        )

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
        "TRANSFER_RDR_PARTICIPANT_DATA_TO_ODS",
        "EXPORT_ODS_TO_DATAMART",
    ]
    # Setup program arguments.
    parser = argparse.ArgumentParser(prog=tool_cmd, description=tool_desc)
    parser.add_argument("--project", help="gcp project name", default="localhost")  # noqa
    parser.add_argument("--account", help="pmi-ops account", default=None)  # noqa
    parser.add_argument("--service-account", help="gcp iam service account", default=None)  # noqa
    parser.add_argument("--process",
                        help="process to run, choose one: {}".format(tool_processes),
                        choices=tool_processes,
                        default=None,
                        required=True,
                        type=str)
    parser.add_argument("--ods-table", help=f"ODS table to load",
                        default=None,
                        required=False,
                        type=str)

    parser.add_argument("--data-file", help="json file to load", default=None)  # noqa

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
