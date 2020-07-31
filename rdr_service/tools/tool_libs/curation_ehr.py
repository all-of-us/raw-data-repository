#! /bin/env python
#
# Simple program to query table schemas from Curation EHR BigQuery tables and
# output SQL Create and scheduled query statements for use in PDR BigQuery.
#

import argparse
import json
import logging
import sys

from rdr_service.model.bq_base import BQException
from rdr_service.services.gcp_utils import gcp_bq_command
from rdr_service.services.system_utils import setup_logging, setup_i18n
from rdr_service.tools.tool_libs import GCPProcessContext, GCPEnvConfigObject

_logger = logging.getLogger("rdr_logger")

# Tool_cmd and tool_desc name are required.
# Remember to add/update bash completion in 'tool_lib/tools.bash'
tool_cmd = "curation-ehr"
tool_desc = "put tool help description here"

tables = [
    "_mapping_observation",
    "_mapping_attribute_definition",
    "_mapping_cohort_definition",
    "_mapping_condition_era",
    "_mapping_condition_occurrence",
    "_mapping_device_exposure",
    "_mapping_dose_era",
    "_mapping_drug_era",
    "_mapping_drug_exposure",
    "_mapping_location",
    "_mapping_measurement",
    "_mapping_observation_period",
    "_mapping_person",
    "_mapping_procedure_occurrence",
    "_mapping_specimen",
    "_mapping_visit_occurrence",
    "unioned_ehr_observation",
    "unioned_ehr_condition_era",
    "unioned_ehr_condition_occurrence",
    "unioned_ehr_device_exposure",
    "unioned_ehr_dose_era",
    "unioned_ehr_drug_era",
    "unioned_ehr_drug_exposure",
    "unioned_ehr_fact_relationship",
    "unioned_ehr_measurement",
    "unioned_ehr_measurement_concept_sets",
    "unioned_ehr_measurement_concept_sets_descendants",
    "unioned_ehr_observation_period",
    "unioned_ehr_person",
    "unioned_ehr_procedure_occurrence",
    "unioned_ehr_provider",
    "unioned_ehr_source_to_concept_map",
    "unioned_ehr_specimen",
    "unioned_ehr_visit_occurrence"
]

excluded_fields = {

    "_person": [
        "location_id",
        "provider_id",
        "care_site_id",
        "person_source_value",
        "gender_source_value",
        "race_source_value",
        "ethnicity_source_value"
    ],

    "_visit_occurrence": [
        "provider_id",
        "care_site_id",
        "visit_source_value",
        "admitted_from_source_value",
        "discharge_to_source_value",
    ],

    "_condition_occurrence": [
        "provider_id",
        "condition_source_value",
        "condition_status_source_value",
    ],

    "_drug_exposure": [
        "stop_reason",
        "sig",
        "lot_number",
        "drug_source_value",
        "route_source_value",
        "dose_unit_source_value",
    ],

    "_procedure_occurrence": [
        "procedure_source_value",
        "modifier_source_value",
    ],

    "_device_exposure": [
        "unique_device_id",
        "provider_id",
        "device_source_value",
    ],

    "_measurement": [
        "Measurement_source_value",
        "unit_source_value",
        "value_source_value",
    ],

    "_observation": [
        "observation_source_value",
        "qualifier_source_value",
        "unit_source_value",
        "value_as_string",
    ],

    "_specimen": [
        "specimen_source_value",
        "anatomic_site_source_value",
        "disease_status_source_value",
    ]
}

CREATE_SQL = """
CREATE OR REPLACE TABLE `aou-pdr-data-prod`.curation_data_view.%%TABLE%% AS
    SELECT CURRENT_DATETIME() as created, %%FIELDS%%
    FROM `aou-res-curation-prod`.ehr_ops.%%TABLE%% LIMIT 0;"""

SCHED_SQL = """
INSERT `aou-pdr-data-prod`.curation_data_view.%%TABLE%%
    (created, %%FIELDS%%)
SELECT CURRENT_DATETIME() as created, %%DI_FIELDS%%
FROM `aou-res-curation-prod`.ehr_ops.%%TABLE%%
WHERE NOT EXISTS (
    SELECT %%FIELDS%%
    FROM `aou-pdr-data-prod`.curation_data_view.%%TABLE%%)
"""

class CurationEHRClass(object):
    def __init__(self, args, gcp_env: GCPEnvConfigObject):
        """
        :param args: command line arguments.
        :param gcp_env: gcp environment information, see: gcp_initialize().
        """
        self.args = args
        self.gcp_env = gcp_env


    def get_table_schema(self, project_id, dataset_id, table_id):
        """
        Retrieve the table schema from BigQuery
        :param project_id: project id
        :param dataset_id: dataset id
        :param table_id: table id
        :return: string
        """
        # bq show --schema --format=prettyjson [PROJECT_ID]:[DATASET].[TABLE]
        args = '{0}:{1}.{2}'.format(project_id, dataset_id, table_id)
        pcode, so, se = gcp_bq_command('show', args=args,
                                       command_flags='--schema --format=prettyjson')  # pylint: disable=unused-variable

        if pcode != 0:
            if 'Not found' in so:
                return None
            if 'Authorization error' in so:
                _logger.error('** BigQuery returned an authorization error, please check the following: **')
                _logger.error('   * Service account has correct permissions.')
                _logger.error('   * Timezone and time on computer match PMI account settings.')
                # for more suggestions look at:
                #    https://blog.timekit.io/google-oauth-invalid-grant-nightmare-and-how-to-fix-it-9f4efaf1da35
            raise BQException(se if se else so)

        return so

    def di_field_check(self, table, fields):
        """
        Convert DI fields to nulls for select statement field list.
        :param table: table name
        :param fields: list of field names
        :return: di field list
        """
        di_fields = list()

        for field in fields:
            di_field = False
            for k, v in excluded_fields.items():
                if k in table:
                    if field in v:
                        di_field = True
            di_fields.append(f'null as {field}' if di_field else field)

        return di_fields

    def run(self):
        """
        Main program process
        :return: Exit code value
        """

        for table in tables:
            so = self.get_table_schema('aou-res-curation-prod', 'ehr_ops', table)
            if not so:
                print(f'Error: failed to retrieve {table}\'s schema')
                continue

            schema = json.loads(so)

            fields = list()

            for rec in schema:
                fields.append(rec['name'])

            sql_fields = ', '.join(fields)
            di_fields = ', '.join(self.di_field_check(table, fields))

            print(f'--- Start {table} ---')
            sql = CREATE_SQL.replace('%%TABLE%%', table).replace('%%FIELDS%%', sql_fields)
            print(f'{sql}\n')

            sql = SCHED_SQL.replace('%%TABLE%%', table).replace('%%FIELDS%%', sql_fields).\
                        replace('%%DI_FIELDS%%', di_fields)
            print(f'{sql}\n--- End {table} ---\n')

        return 0


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
    parser.add_argument("--account", help="pmi-ops account", default=None)  # noqa
    parser.add_argument("--service-account", help="gcp iam service account", default=None)  # noqa
    args = parser.parse_args()

    with GCPProcessContext(tool_cmd, args.project, args.account, args.service_account) as gcp_env:
        process = CurationEHRClass(args, gcp_env)
        exit_code = process.run()
        return exit_code


# --- Main Program Call ---
if __name__ == "__main__":
    sys.exit(run())
