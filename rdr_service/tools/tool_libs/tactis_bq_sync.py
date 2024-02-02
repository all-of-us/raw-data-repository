import argparse

from dateutil.parser import parse
from google.cloud import bigquery

from rdr_service.offline.tactis_bq_sync import TactisBQDataSync
from rdr_service.tools.tool_libs.tool_base import cli_run, ToolBase

tool_cmd = 'tactis-bq-sync'
tool_desc = 'Sync Tactis participant data that has been modified since a given date to BigQuery'


class TactisBQSync(ToolBase):

    def __init__(self, args, gcp_env=None, tool_name=None, replica=False):
        super().__init__(args, gcp_env, tool_name, replica)
        self.client = bigquery.Client()
        self.dataset_id = f"{args.project}.{args.dataset}"

    def run(self):
        super(TactisBQSync, self).run()

        dataset = self.args.dataset
        table_name = self.args.table
        since_date = parse(self.args.since)

        data_sync = TactisBQDataSync(
            dataset=dataset,
            table_name=table_name,
            since_date=since_date
        )
        data_sync.sync_data_to_bigquery()


def add_additional_arguments(parser: argparse.ArgumentParser):
    parser.add_argument("--dataset", help="BigQuery dataset where data should be synced", required=True)
    parser.add_argument("--table", help="RDR table containing Tactis data", required=True)
    parser.add_argument(
        '--since',
        help='Participants that have a modified Tactis field since this date string will be synced to BigQuery',
        required=True
    )


def run():
    cli_run(tool_cmd, tool_desc, TactisBQSync, add_additional_arguments)
