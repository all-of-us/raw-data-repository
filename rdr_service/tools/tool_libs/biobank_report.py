import argparse
import csv
from datetime import datetime, timedelta

from rdr_service import clock, config
from rdr_service.model.utils import to_client_participant_id
from rdr_service.offline.biobank_samples_pipeline import get_withdrawal_report_query
from rdr_service.participant_enums import DeceasedStatus
from rdr_service.storage import GoogleCloudStorageProvider
from rdr_service.tools.tool_libs.tool_base import cli_run, logger, ToolBase

tool_cmd = 'biobank-report'
tool_desc = 'Generates report for the Biobank'


class BiobankReportTool(ToolBase):
    def run(self):
        super(BiobankReportTool, self).run()

        if self.args.report_type == 'withdrawal':
            if self.args.upload_file and self.args.generate:
                logger.error('Can only generate a report or upload one, not both at the same time.')
            elif self.args.generate:
                self.generate_withdrawal_report()
            elif self.args.upload_file:
                self.upload_withdrawal_report(self.args.upload_file)
            else:
                logger.error('Need to specify to either generate or upload a withdrawal report')

    def generate_withdrawal_report(self, start_date: datetime = None, file_path: str = None):
        now = clock.CLOCK.now()
        if start_date is None:
            two_months_ago = now - timedelta(weeks=8)
            start_date = datetime(two_months_ago.year, two_months_ago.month, 27)
        if file_path is None:
            file_path = self._get_default_withdrawal_name()

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
                deceased_status = result.deceased_status
                if result.deceased_status is None:  # Can be None if the summary doesn't exist
                    deceased_status = DeceasedStatus.UNSET

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
                    'deceased_status': str(deceased_status)
                })

        logger.info(f'SUCCESS: report written to {file_path}')

    def upload_withdrawal_report(self, report_file_path: str):
        # Check that the file's columns are correct
        with open(report_file_path) as withdrawal_report_file:
            csv_reader = csv.reader(withdrawal_report_file)
            report_headers = next(csv_reader)
            if report_headers != [
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
            ]:
                # This makes sure all the expected columns are in the file,
                # but especially to make sure participant_id was removed
                raise Exception('Unexpected set of columns in the file.')

        # Upload the file to Google Storage
        biobank_bucket_name = self.get_server_config()[config.BIOBANK_SAMPLES_BUCKET_NAME][0]
        destination_path = f'{biobank_bucket_name}/reconciliation/{self._get_default_withdrawal_name()}'

        logger.info(f'Uploading {self.args.upload_file} to {destination_path}')
        storage_provider = GoogleCloudStorageProvider()
        storage_provider.upload_from_file(source_file=self.args.upload_file, path=destination_path)

    @classmethod
    def _get_default_withdrawal_name(cls):
        one_month_ago = clock.CLOCK.now() - timedelta(weeks=4)
        return f'report_{one_month_ago.year}-{str(one_month_ago.month).zfill(2)}_withdrawals.csv'


def add_additional_arguments(parser: argparse.ArgumentParser):
    subparsers = parser.add_subparsers(dest='report_type', required=True)

    withdrawal_parser = subparsers.add_parser('withdrawal')
    withdrawal_parser.add_argument(
        '--generate',
        help='Generates the withdrawal report to be audited.',
        action='store_true',
        default=False
    )
    withdrawal_parser.add_argument('--upload-file', help='Upload an audited report to the bucket.', default=None)


def run():
    return cli_run(tool_cmd, tool_desc, BiobankReportTool, add_additional_arguments)
