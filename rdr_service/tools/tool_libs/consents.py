import argparse
from dateutil.parser import parse

from rdr_service.dao.consent_dao import ConsentDao
from rdr_service.model.consent_file import ConsentFile
from rdr_service.tools.tool_libs.tool_base import cli_run, logger, ToolBase

tool_cmd = 'consents'
tool_desc = 'Get reports of consent issues and modify validation records'


class ConsentTool(ToolBase):
    def __init__(self, *args, **kwargs):
        super(ConsentTool, self).__init__(*args, **kwargs)
        self._consent_dao = ConsentDao()

    def run(self):
        super(ConsentTool, self).run()

        min_validation_date = parse(self.args.since) if self.args.since else None
        results_to_report = self._consent_dao.get_files_needing_correction(min_modified_datetime=min_validation_date)

        report_lines = []
        previous_participant_id = None
        for result in results_to_report:
            if previous_participant_id and previous_participant_id != result.participant_id and self.args.verbose:
                report_lines.append('')
            previous_participant_id = result.participant_id

            report_lines.append(self._line_output_for_validation(result, verbose=self.args.verbose))

        logger.info('\n'.join(report_lines))

    @classmethod
    def _line_output_for_validation(cls, result: ConsentFile, verbose: bool):
        if not result.file_exists:
            error_message = 'missing file'
        else:
            errors_with_file = []
            if not result.is_signature_valid:
                errors_with_file.append('invalid signature')
            if not result.is_signing_date_valid:
                extra_info = ''
                if verbose:
                    time_difference = result.signing_date - result.expected_sign_date
                    extra_info = f', diff of {time_difference.days} days'
                errors_with_file.append(
                    f'invalid signing date (expected {result.expected_sign_date} '
                    f'but file has {result.signing_date}{extra_info})'
                )
            if result.other_errors is not None:
                errors_with_file.append(result.other_errors)
            error_message = ', '.join(errors_with_file)
        return f'P{result.participant_id} - {str(result.type).ljust(10)} {error_message}'


def add_additional_arguments(parser: argparse.ArgumentParser):
    subparsers = parser.add_subparsers(dest='command', required=True)

    report_parser = subparsers.add_parser('report-errors')
    report_parser.add_argument(
        '--since',
        help='date or timestamp, if provided the report will only contain more recent validation information'
    )
    report_parser.add_argument(
        '--verbose',
        help='Enable verbose output, useful when auditing flagged files',
        default=False,
        action="store_true"
    )


def run():
    cli_run(tool_cmd, tool_desc, ConsentTool, add_additional_arguments)
