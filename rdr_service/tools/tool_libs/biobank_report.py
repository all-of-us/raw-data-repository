import argparse
import csv
from datetime import datetime, timedelta

from rdr_service import config
from rdr_service.model.utils import to_client_participant_id
from rdr_service.offline.biobank_samples_pipeline import get_withdrawal_report_query
from rdr_service.tools.tool_libs.tool_base import cli_run, logger, ToolBase

tool_cmd = 'biobank-report'
tool_desc = 'Generates report for the Biobank'


class BiobankReportTool(ToolBase):
    def run(self):
        super(BiobankReportTool, self).run()

        if self.args.command == 'withdrawal':
            self.generate_withdrawal_report()

    def generate_withdrawal_report(self, start_date: datetime = None, file_path: str = None):
        now = datetime.now()
        if start_date is None:
            two_months_ago = now - timedelta(weeks=8)
            start_date = datetime(two_months_ago.year, two_months_ago.month, 27)
        if file_path is None:
            one_month_ago = now - timedelta(weeks=4)
            file_path = f'report_{one_month_ago.year}-{one_month_ago.month}_withdrawals.csv'

        logger.info(f'Generating withdrawal report for data since {start_date}')
        with self.get_session() as session, open(file_path, 'w') as output_file:
            csv_writer = csv.DictWriter(output_file, [
                'participant_id',
                'biobank_id',
                'withdrawal_time',
                'is_native_american',
                'needs_disposal_ceremony',
                'participant_origin',
                'paired_hpo',
                'paired_org',
                'paired_site',
                'withdrawal_reason_justification',
                'deceased_status'
            ])
            csv_writer.writeheader()

            server_config = self.get_server_config()

            report_query = get_withdrawal_report_query(start_date=start_date)
            result_list = session.execute(report_query)
            for result in result_list:
                csv_writer.writerow({
                    'participant_id': to_client_participant_id(result.participant_id),
                    'biobank_id': f'{server_config[config.BIOBANK_ID_PREFIX][0]}{result.biobank_id}',
                    'withdrawal_time': result.withdrawal_time,
                    'is_native_american': result.is_native_american,
                    'needs_disposal_ceremony': result.needs_disposal_ceremony,
                    'participant_origin': result.participant_origin,
                    'paired_hpo': result.paired_hpo,
                    'paired_org': result.paired_org,
                    'paired_site': result.paired_site,
                    'withdrawal_reason_justification': result.withdrawal_reason_justification,
                    'deceased_status': result.deceased_status
                })

        logger.info(f'SUCCESS: report written to {file_path}')


def add_additional_arguments(parser: argparse.ArgumentParser):
    subparsers = parser.add_subparsers(dest='command', required=True)
    subparsers.add_parser('withdrawal')

def run():
    return cli_run(tool_cmd, tool_desc, BiobankReportTool, add_additional_arguments)
