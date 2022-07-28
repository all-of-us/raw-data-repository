#! /bin/env python
#
# Template for RDR tool python program.
#
import argparse
import logging
import sys
import datetime
import uuid
import itertools
import re
from google.cloud import bigquery

from rdr_service.dao import database_factory
from rdr_service.clock import Clock
from rdr_service.model.genomics import GenomicSetMember
from rdr_service.model.participant import Participant
from rdr_service.model.participant_summary import ParticipantSummary
from rdr_service.services.system_utils import setup_logging, setup_i18n

from rdr_service.tools.tool_libs import GCPProcessContext
from rdr_service.tools.tool_libs.tool_base import ToolBase

_logger = logging.getLogger("rdr_logger")

# Tool_cmd and tool_desc name are required.
# Remember to add/update bash completion in 'tool_lib/tools.bash'
tool_cmd = "ods"
tool_desc = "proof of concept tool for loading rdr_ods BQ dataset."

TEST_EXPORT_SCHEMA = [{
  "schema_name": "genomic_research",
  "destination_mart": "genomic_research_mart",
  "destination_target_table": "rdr_genomic_research_export_%eid%",
}]

TEST_EXPORT_SCHEMA_DE = [
    {
        "schema_name": "genomic_research",
        "data_element_id": "ede63e9d82ec45aaaa96ccedba082df5",
        "display_name": "drc_qc_status",
        "active_flag": True
    },
    {
        "schema_name": "genomic_research",
        "data_element_id": "fabb52d4336c438aab4c885cbffe21b8",
        "display_name": "primary_consent",
        "active_flag": True
    }
]


class OdsTool(ToolBase):
    client = bigquery.Client()
    session = None
    created_timestamp = None
    export_timestamp = None

    def run(self):
        self.gcp_env.activate_sql_proxy()
        # server_config = self.get_server_config()
        # table_id = "all-of-us-rdr-sandbox.rdr_ods.export_schema_data_element"
        # data = TEST_EXPORT_SCHEMA_DE
        # self.load_data_to_bq_table(table_id, data)  # only for populating test data for now

        # self.load_ods_sample_data_element()

        self.export_ods_data_to_datamart()

        return 0

    @staticmethod
    def create_uuid_for_row(row):
        row.update({'id': uuid.uuid4().hex})
        return row

    def load_data_to_bq_table(self, table_id, rows_to_insert):
        errors = self.client.insert_rows_json(table_id, rows_to_insert)  # Make an API request.
        if not errors:
            print("New rows have been added.")
        else:
            print("Encountered errors while inserting rows: {}".format(errors))

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
        # get data elements from registry
        registered_member_data_elements = list(self.get_sample_data_elements_from_registry())

        # Get all newly modified members (with aw4)
        modified_members = self.get_modified_member_data(registered_member_data_elements,
                                                         last_update_date=datetime.datetime(2022, 7, 25, 20))

        # Pivot registered
        pivoted_data = self.pivot_member_data(registered_member_data_elements, modified_members)

        # Load the pivoted data to BQ table
        table_id = "all-of-us-rdr-sandbox.rdr_ods.sample_data_element"

        self.load_data_to_bq_table(table_id, pivoted_data)

    def export_ods_data_to_datamart(self):
        export_schema = self.get_export_schema()

        # Pivot data from sample_data_element into BQ temp table
        self.export_sample_data_element_to_temp_table()

        # load records into snapshot table in rdr_genomic_research_export dataset
        destination = self.get_destination_table(export_schema)

        self.load_datamart_snapshot(destination)

        _logger.info(f"Data export to {destination} complete.")

    def get_sample_data_elements_from_registry(self):
        return self.run_bq_query_job("""
        SELECT reg.data_element_id
          , reg.target_table
          , de.source_system
          , de.source_target
        FROM `all-of-us-rdr-sandbox.rdr_ods.data_element_registry` reg
          INNER JOIN `all-of-us-rdr-sandbox.rdr_ods.data_element` de ON de.id_1 = reg.data_element_id
        where true
          and reg.target_table = "sample_data_element"
          and reg.active_flag = true
          # and starts_with(de.source_target, "GenomicSetMember")
        """)

    def get_modified_member_data(self, data_elements, last_update_date):
        rdr_attributes = [
            GenomicSetMember.participantId,
            GenomicSetMember.sampleId,
            Participant.researchId,
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

    def pivot_member_data(self, data_elements, data):
        _logger.info("pivoting data...")

        def build_new_row(old_row, data_element):
            attribute_name = data_element.source_target.split(".")[-1]
            new_row = {
                'participant_id': old_row.participantId,
                'research_id': old_row.researchId,
                'sample_id': old_row.sampleId,
                'data_element_id': data_element.data_element_id,
                'value_string': str(getattr(old_row, attribute_name)),
                'created_timestamp': self.created_timestamp
            }
            return new_row

        # set the created_timestamp
        clock = Clock()

        self.created_timestamp = clock.now().isoformat()

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


def get_ods_process_for_run(args, gcp_env):
    process_map = {
        'load_data_elements': OdsTool(args, gcp_env),
        'load_samples': OdsTool(args, gcp_env),
    }
    return process_map.get(args.process)


def run():
    # Set global debug value and setup application logging.
    setup_logging(
        _logger, tool_cmd, "--debug" in sys.argv, "{0}.log".format(tool_cmd) if "--log-file" in sys.argv else None
    )
    setup_i18n()

    # Setup program arguments.
    parser = argparse.ArgumentParser(prog=tool_cmd, description=tool_desc)
    parser.add_argument("--debug", help="enable debug output", default=False, action="store_true")  # noqa
    parser.add_argument("--log-file", help="write output to a log file", default=False, action="store_true")  # noqa
    parser.add_argument("--project", help="gcp project name", default="localhost")  # noqa
    parser.add_argument("--account", help="pmi-ops account", default=None)
    parser.add_argument("--service-account", help="gcp iam service account", default=None)  # noqa

    # subparser = parser.add_subparsers(help='', dest='process')

    # samples_parser = subparser.add_parser("load_samples")
    # data_elements_parser = subparser.add_parser("load_data_elements")
    # manifest.add_argument("--manifest-template", help="which manifest to generate",
    #                       default=None,
    #                       required=True)  # noqa
    # manifest.add_argument("--sample-id-file", help="path to the list of sample_ids to include in manifest. "
    #                                                "Leave blank for End-to-End manifest (pulls all eligible samples)",
    #                           default=None)  # noqa
    # manifest.add_argument("--update-samples",
    #                       help="update the result state and manifest job run id field on completion",
    #                       default=False, required=False, action="store_true")  # noqa
    # manifest.add_argument("--output-manifest-directory", help="local output directory for the generated manifest"
    #                                                    , default=None)  # noqa
    # manifest.add_argument("--output-manifest-filename", help="what to name the output file",
    #                           default=None, required=False)  # noqa
    # manifest.add_argument("--cvl-site-id", help="cvl site to pass to manifest query",
    #                       default=None, required=False)  # noqa

    args = parser.parse_args()

    with GCPProcessContext(tool_cmd, args.project, args.account, args.service_account) as gcp_env:
        try:
            ods_process = get_ods_process_for_run(args, gcp_env)
            exit_code = ods_process.run()
        # pylint: disable=broad-except
        except Exception as e:
            _logger.info(f'Error has occured, {e}. For help use "ods --help".')
            exit_code = 1

        return exit_code


# --- Main Program Call ---
if __name__ == "__main__":
    sys.exit(run())
