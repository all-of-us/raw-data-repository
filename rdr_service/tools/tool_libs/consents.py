import argparse
from datetime import datetime, timedelta
from dateutil.parser import parse
from io import StringIO

from rdr_service.dao.consent_dao import ConsentDao
from rdr_service.dao.hpo_dao import HPODao
from rdr_service.dao.participant_summary_dao import ParticipantSummaryDao
from rdr_service.model.consent_file import ConsentFile, ConsentSyncStatus, ConsentType
from rdr_service.services.consent.validation import ConsentValidationController
from rdr_service.storage import GoogleCloudStorageProvider
from rdr_service.tools.tool_libs.tool_base import cli_run, logger, ToolBase

tool_cmd = 'consents'
tool_desc = 'Get reports of consent issues and modify validation records'


class ConsentTool(ToolBase):
    def __init__(self, *args, **kwargs):
        super(ConsentTool, self).__init__(*args, **kwargs)
        self._consent_dao = ConsentDao()
        self._storage_provider = GoogleCloudStorageProvider()

    def run(self):
        super(ConsentTool, self).run()

        if self.args.command == 'report-errors':
            self.report_files_for_correction()
        elif self.args.command == 'modify':
            self.modify_file_results()
        elif self.args.command == 'validate':
            self.validate_consents()

    def report_files_for_correction(self):
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

    def modify_file_results(self):
        file = self._consent_dao.get(self.args.id)
        if file is None:
            logger.error('Unable to find validation record')

        logger.info('File info:'.ljust(16) + f'P{file.participant_id}, {file.file_path}')
        self._check_for_update(
            new_value=self.args.type,
            stored_value=file.type,
            parser_func=ConsentType,
            callback=lambda parsed_value: self._log_property_change('type', file.type, parsed_value)
        )
        self._check_for_update(
            new_value=self.args.sync_status,
            stored_value=file.sync_status,
            parser_func=ConsentSyncStatus,
            callback=lambda parsed_value: self._log_property_change('sync_status', file.sync_status, parsed_value)
        )
        confirmation_answer = input('\nMake the changes above (Y/n)? : ')
        if confirmation_answer and confirmation_answer.lower().strip() != 'y':
            logger.info('Aborting update')
        else:
            logger.info('Updating file record')
            self._check_for_update(
                new_value=self.args.type,
                stored_value=file.type,
                parser_func=ConsentType,
                callback=lambda parsed_value: setattr(file, 'type', parsed_value)
            )
            self._check_for_update(
                new_value=self.args.sync_status,
                stored_value=file.sync_status,
                parser_func=ConsentSyncStatus,
                callback=lambda parsed_value: setattr(file, 'sync_status', parsed_value)
            )
            self._consent_dao.batch_update_consent_files([file])

    def validate_consents(self):
        min_date = parse(self.args.min_date)
        max_date = parse(self.args.max_date) if self.args.max_date else None

        controller = ConsentValidationController(
            consent_dao=ConsentDao(),
            participant_summary_dao=ParticipantSummaryDao(),
            hpo_dao=HPODao(),
            storage_provider=GoogleCloudStorageProvider()
        )
        controller.validate_recent_uploads(min_consent_date=min_date, max_consent_date=max_date)

    def _line_output_for_validation(self, file: ConsentFile, verbose: bool):
        output_line = StringIO()
        output_line.write(f'P{file.participant_id} - {str(file.type).ljust(10)} ')

        if not file.file_exists:
            output_line.write('missing file')
        else:
            errors_with_file = []
            if not file.is_signature_valid:
                errors_with_file.append('invalid signature')
            if not file.is_signing_date_valid:
                errors_with_file.append(self._get_date_error_details(file, verbose))
            if file.other_errors is not None:
                errors_with_file.append(file.other_errors)

            output_line.write(', '.join(errors_with_file))
            if verbose:
                output_line.write(f' - {self._get_link(file)}')

        return output_line.getvalue()

    @classmethod
    def _get_date_error_details(cls, file: ConsentFile, verbose: bool = False):
        extra_info = ''
        if verbose:
            time_difference = file.signing_date - file.expected_sign_date
            extra_info = f', diff of {time_difference.days} days'
        return f'invalid signing date (expected {file.expected_sign_date} '\
               f'but file has {file.signing_date}{extra_info})'

    def _get_link(self, file: ConsentFile):
        bucket_name, *name_parts = file.file_path.split('/')
        blob = self._storage_provider.get_blob(
            bucket_name=bucket_name,
            blob_name='/'.join(name_parts)
        )
        return blob.generate_signed_url(datetime.now() + timedelta(hours=2))

    @classmethod
    def _log_property_change(cls, property_name, old_value, new_value):
        logger.info(f'{property_name}:'.ljust(16) + f'{old_value} => {new_value}')

    @classmethod
    def _new_value(cls, entered_value, parser_func=None):
        if isinstance(entered_value, str) and entered_value.lower() == 'none':
            return None
        elif parser_func is not None:
            return parser_func(entered_value)
        else:
            return entered_value

    @classmethod
    def _check_for_update(cls, new_value, stored_value, parser_func=None, callback=None):
        if new_value is not None:
            parsed_value = cls._new_value(entered_value=new_value, parser_func=parser_func)
            if parsed_value != stored_value:
                callback(parsed_value)


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

    modify_parser = subparsers.add_parser('modify')
    modify_parser.add_argument(
        '--id', help='Database id of the record to modify', required=True
    )
    modify_parser.add_argument(
        '--type', help='New consent type value to set'
    )
    modify_parser.add_argument(
        '--sync-status', help='New sync status to set (format: string or int value of the new status'
    )

    modify_parser = subparsers.add_parser('validate')
    modify_parser.add_argument('--min_date', help='Earliest date of the expected consents to validate', required=True)
    modify_parser.add_argument('--max_date', help='Latest date of the expected consents to validate')


def run():
    cli_run(tool_cmd, tool_desc, ConsentTool, add_additional_arguments)
