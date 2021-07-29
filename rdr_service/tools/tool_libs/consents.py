import argparse
import csv
from datetime import datetime, timedelta
from dateutil.parser import parse

from rdr_service import config
from rdr_service.dao.consent_dao import ConsentDao
from rdr_service.dao.hpo_dao import HPODao
from rdr_service.dao.participant_dao import ParticipantHistoryDao
from rdr_service.dao.participant_summary_dao import ParticipantSummaryDao
from rdr_service.model.consent_file import ConsentFile, ConsentSyncStatus, ConsentType
from rdr_service.offline.sync_consent_files import ConsentSyncGuesser
from rdr_service.services.consent.validation import ConsentValidationController, LogResultStrategy, StoreResultStrategy
from rdr_service.storage import GoogleCloudStorageProvider
from rdr_service.tools.tool_libs.tool_base import cli_run, logger, ToolBase

from rdr_service.offline.sync_consent_files import ConsentSyncController
from rdr_service.dao.participant_dao import ParticipantDao

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
        elif self.args.command == 'upload':
            self.upload_records()
        elif self.args.command == 'check-retro-sync':
            self.check_retro_sync()

    def check_retro_sync(self):
        with self.get_session() as session:
            consent_dao = ConsentDao()
            sync_config = self.get_server_config()[config.CONSENT_SYNC_BUCKETS]
            files = consent_dao.get_files_ready_to_sync(org_names=sync_config.keys(), session=session)

            retro_files = [file for file in files if file.file_upload_time < datetime(2021, 6, 1)]
            guesser = ConsentSyncGuesser(session=session, participant_history_dao=ParticipantHistoryDao())
            guesser.check_consents(retro_files)

    def report_files_for_correction(self):
        min_validation_date = parse(self.args.since) if self.args.since else None
        with self.get_session() as session, LogResultStrategy(
            logger=logger,
            verbose=self.args.verbose,
            storage_provider=self._storage_provider
        ) as strategy:
            strategy.add_all(self._consent_dao.get_files_needing_correction(
                session=session,
                min_modified_datetime=min_validation_date
            ))

        input('Press Enter to exit...')  # The Google SA key will need to stay active for links to docs to work

    def modify_file_results(self):
        with self.get_session() as session:
            file = self._consent_dao.get_with_session(session, self.args.id)
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
                self._consent_dao.batch_update_consent_files(session, [file])

    def validate_consents(self):
        sync_controller = ConsentSyncController(
            consent_dao=ConsentDao(),
            participant_dao=ParticipantDao(),
            storage_provider=GoogleCloudStorageProvider()
        )
        sync_controller.sync_ready_files()

        min_date = parse(self.args.min_date)
        max_date = parse(self.args.max_date) if self.args.max_date else None

        controller = ConsentValidationController(
            consent_dao=ConsentDao(),
            participant_summary_dao=ParticipantSummaryDao(),
            hpo_dao=HPODao(),
            storage_provider=GoogleCloudStorageProvider()
        )
        with self.get_session() as session, StoreResultStrategy(
            session=session,
            consent_dao=controller.consent_dao
        ) as store_strategy:
            controller.validate_recent_uploads(
                session,
                store_strategy,
                min_consent_date=min_date,
                max_consent_date=max_date
            )

    def upload_records(self):
        data_to_upload = []
        with open(self.args.file) as input_file:
            input_csv = csv.DictReader(input_file)
            for validation_data in input_csv:
                data_to_upload.append(ConsentFile(**validation_data))

        with self.get_session() as session:
            self._consent_dao.batch_update_consent_files(session, data_to_upload)

    @classmethod
    def _get_date_error_details(cls, file: ConsentFile, verbose: bool = False):
        extra_info = ''
        if verbose and file.signing_date and file.expected_sign_date:
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
        return blob.generate_signed_url(datetime.utcnow() + timedelta(hours=2))

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

    modify_parser = subparsers.add_parser('upload')
    modify_parser.add_argument(
        '--file',
        help='CSV file defining validation record data that should be added to the database',
        required=True
    )

    subparsers.add_parser('check-retro-sync')


def run():
    cli_run(tool_cmd, tool_desc, ConsentTool, add_additional_arguments)
