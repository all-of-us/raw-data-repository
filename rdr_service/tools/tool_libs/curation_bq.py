import logging
from typing import Union

import google.cloud.bigquery
from google.cloud import bigquery

from rdr_service.tools.tool_libs.tool_base import cli_run, ToolBase
from rdr_service.etl.bq import queries

_logger = logging.getLogger("rdr_logger")

tool_cmd = "curation-bq"
tool_desc = "Run Curation ETL process in BigQuery"


class CurationBQ(ToolBase):
    """
    Loads tables to BQ to run ETL process.
    To use this tool manually create the dataset in BigQuery in the us-central-1 region.
    Then run the tool with --load-data to trigger loading ETL data to the dataset.
    Then run the tool with --run-etl to run the ETL queries and produce the output.
    """
    EXTERNAL_CONNECTION = "all-of-us-rdr-prod.us-central1.bq-rdr-preprod-curation"
    import_tables = {
        'src_clean': 'cdm',
        'src_meas': 'cdm',
        'combined_question_filter': 'cdm',
        'combined_survey_filter': 'cdm',
        'source_to_concept_map': 'cdm',
        'concept': 'voc',
        'concept_relationship': 'voc',
        'site': 'rdr',
        'measurement_to_qualifier': 'rdr',
        'deceased_report': 'rdr'
    }

    etl_process_steps = [
        'src_participant',
        'src_mapped',
        'src_gender',
        'src_race',
        'src_race_2',
        'src_ethnicity',
        'src_ethnicity_2',
        'src_person_location',
        'location',
        'update_location_id',
        'person',
        'tmp_cv_concept_lk',
        'tmp_vcv_concept_lk',
        'src_meas_mapped',
        'tmp_visits_src',
        'visit_occurrence',
        'observation',
        'care_site',
        'measurement',
        'note',
        'tmp_fact_rel_sd',
        'fact_relationship',
        'survey_conduct',
        'death',
        'ehr_consent_temp_table',
        'ehr_consent',
        'wear_consent',
        'participant_id_mapping',
        'finalize',
        'qrai_author',
        'qrai_language',
        'qrai_code',
        'tmp_survey_conduct',
        'survey_conduct',
        'create_empty_tables',
        'pid_rid_mapping'
    ]

    export_tables = [
        'care_site',
        'condition_era',
        'condition_occurrence',
        'consent',
        'cost',
        'death',
        'device_exposure',
        'dose_era',
        'drug_era',
        'drug_exposure',
        'fact_relationship',
        'location',
        'measurement',
        'metadata',
        'note_nlp',
        'observation',
        'observation_period',
        'payer_plan_period',
        'person',
        'pid_rid_mapping',
        'procedure_occurrence',
        'provider',
        'questionnaire_response_additional_info',
        'visit_detail',
        'visit_occurrence',
        'wear_consent'
    ]

    def __init__(self, args, gcp_env=None, tool_name=None, replica=False):
        super().__init__(args, gcp_env, tool_name, replica)
        self.client = bigquery.Client()
        self.dataset_id = f"{args.project}.{args.dataset}"

    def run(self):
        super().run()
        if not self.args.dataset:
            _logger.error("No dataset specified")
            return 1

        if self.args.load_data:
            self.import_tables_to_bq()
        elif self.args.run_etl:
            self.run_etl()
        elif self.args.export:
            self.export()
        else:
            _logger.error("One of --load-data, --run-etl, or --export must be set")

    def run_query(self, sql: str, job_config: Union[None, google.cloud.bigquery.QueryJobConfig]):
        query_job = self.client.query(sql, job_config=job_config)
        result = query_job.result()  # wait for query to finish
        _logger.debug(f"Rows in result: {result.total_rows}")

    def import_table(self, table_name: str, schema: str):
        job_config = bigquery.QueryJobConfig(destination=f"{self.dataset_id}.{table_name}")

        sql = f"""SELECT * FROM EXTERNAL_QUERY("{self.EXTERNAL_CONNECTION}", "SELECT * FROM {schema}.{table_name};");"""
        self.run_query(sql, job_config)

    def import_tables_to_bq(self):
        for table, schema in self.import_tables.items():
            _logger.debug(f"Importing {schema}.{table}")
            self.import_table(table, schema)

    def run_etl(self):
        _logger.debug("Filtering src_clean")
        self.filter_src_clean()
        for step in self.etl_process_steps:
            _logger.debug(f"Running {step}")
            table_name = queries.queries[step]['destination']
            query = queries.queries[step]['query']
            append_to_table = queries.queries[step]['append']
            if not table_name:
                job_config = None
            else:
                if append_to_table:
                    write_disposition = bigquery.job.WriteDisposition.WRITE_APPEND
                else:
                    write_disposition = bigquery.job.WriteDisposition.WRITE_TRUNCATE
                job_config = bigquery.QueryJobConfig(destination=f"{self.dataset_id}.{table_name}",
                                                     write_disposition=write_disposition)
            self.run_query(query.format(dataset_id=self.dataset_id, cutoff=self.args.cutoff), job_config)

    def filter_src_clean(self):
        self.run_query(sql=queries.queries['filter_questions']['query'].format(dataset_id=self.dataset_id),
                       job_config=None)
        self.run_query(sql=queries.queries['filter_surveys']['query'].format(dataset_id=self.dataset_id),
                       job_config=None)

    def export(self):
        client = bigquery.Client()
        dataset_ref = bigquery.DatasetReference(self.args.project, self.args.dataset)
        for table in self.export_tables:
            _logger.info(f"Exporting table {table}")
            table_ref = dataset_ref.table(table)
            bq_table = client.get_table(table_ref)
            _logger.info(f"Table byte size: {bq_table.num_bytes}")
            if bq_table.num_bytes > 900000000:  # Shard if table size close to 1GB
                destination = f"gs://{self.args.destination}/{table}_*.csv"
            else:
                destination = f"gs://{self.args.destination}/{table}.csv"
            extract_job = client.extract_table(
                table_ref,
                destination,
                location="us-central1",
            )
            extract_job.result()


def add_additional_arguments(parser):
    parser.add_argument("--debug", help="enable debug output", default=False, action="store_true")
    parser.add_argument("--log-file", help="write output to a log file", default=False, action="store_true")
    parser.add_argument("--dataset", help="dataset to use for ETL run", required=True)
    parser.add_argument("--load-data", help="Load data to dataset", default=False, action="store_true")
    parser.add_argument("--run-etl", help="Run the ETL process", default=False, action="store_true")
    parser.add_argument("--export", help="Export data to GCS bucket", default=False, action="store_true")
    parser.add_argument("--destination", help="GCS bucket and path to export to")
    parser.add_argument("--cutoff", help="cutoff date used for the run", required=True)


def run():
    cli_run(tool_cmd, tool_desc, CurationBQ, add_additional_arguments)
