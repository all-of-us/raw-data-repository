import argparse
import csv
from datetime import datetime, timedelta
import os
from typing import List

import pytz
from dateutil.parser import parse
from google.cloud.storage.blob import Blob
from io import StringIO

from rdr_service.dao.consent_dao import ConsentDao
from rdr_service.dao.participant_summary_dao import ParticipantSummaryDao
from rdr_service.model.consent_file import ConsentFile, ConsentSyncStatus, ConsentType
from rdr_service.model.participant import ParticipantHistory
from rdr_service.model.participant_summary import ParticipantSummary
from rdr_service.services.consent.validation import ConsentValidationController,\
    ReplacementStoringStrategy, StoreResultStrategy
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
        elif self.args.command == 'revalidate':
            self.revalidate()
        elif self.args.command == 'generate-ce':
            self.generate_ce()
        elif self.args.command == 'check-org-change':
            self.check_org_change()
        elif self.args.command == 'validate':
            self.validate()
        elif self.args.command == 'list-bucket':
            self.list_bucket()
        elif self.args.command == 'corrections':
            self.corrections()
        elif self.args.command == 'trigger-sync':
            self.trigger_sync()
        elif self.args.command == 'check-missing':
            self.check_missing()

    def trigger_sync(self):
        with self.get_session() as session:
            controller = ConsentSyncController(
                consent_dao=ConsentDao(),
                participant_dao=ParticipantDao(),
                storage_provider=GoogleCloudStorageProvider(),
                session=session,
                config=self.get_server_config()
            )
            controller.sync_ready_files(session)

    def corrections(self):
        consent_dao = ConsentDao()
        with self.get_session() as session:
            with ReplacementStoringStrategy(
                session=session,
                consent_dao=consent_dao
            ) as strategy:
                validation_controller = ConsentValidationController(
                    consent_dao=consent_dao,
                    participant_summary_dao=ParticipantSummaryDao(),
                    storage_provider=GoogleCloudStorageProvider()
                )
                for participant_id in [

                ]:
                    print(participant_id)
                    validation_controller.generate_new_validations(
                        session=session,
                        participant_id=participant_id,
                        consent_type=ConsentType(self.args.type),
                        output_strategy=strategy
                    )

    def check_missing(self):
        consent_dao = ConsentDao()
        with self.get_session() as session:
            validation_controller = ConsentValidationController(
                consent_dao=consent_dao,
                participant_summary_dao=ParticipantSummaryDao(),
                storage_provider=GoogleCloudStorageProvider()
            )
            validation_controller.check_file_existence(session)

    def list_bucket(self):
        storage_provider = GoogleCloudStorageProvider()
        blobs: List[Blob] = storage_provider.list(
            bucket_name='ptc-uploads-all-of-us-rdr-prod',
            prefix='Participant'
        )
        with open('full_bucket_list_output.csv', 'w') as out_file:
            csv_file = csv.writer(out_file)
            csv_file.writerow([
                'participant_id', 'file_name', 'file_size_bytes', 'file_upload_time'
            ])

            write_count = 0
            for blob in blobs:
                if os.path.basename(blob.name).endswith('pdf'):
                    _, participant_id, file_name = blob.name.split('/')
                    write_count += 1
                    csv_file.writerow([
                        participant_id, file_name, blob.size, blob.time_created
                    ])

                    if write_count % 10000 == 0:
                        print(f'wrote {write_count}')

    def check_org_change(self):
        with self.get_session() as session:
            cutoff_date = datetime(2021, 5, 28)
            modified_participant_ids = list(session.query(
                ParticipantSummary.participantId,
                ParticipantSummary.consentForStudyEnrollmentFirstYesAuthored
            ).filter(ParticipantSummary.lastModified > cutoff_date).all())
            for modified_participant_id, consent_authored in modified_participant_ids:
                print(f'checking {modified_participant_id}')
                history_records: List[ParticipantHistory] = session.query(
                    ParticipantHistory.organizationId,
                    ParticipantHistory.lastModified
                ).filter(
                    ParticipantHistory.participantId == modified_participant_id
                ).all()
                last_org_id_seen = None
                latest_org_change_record = None
                for history_record in history_records:
                    if history_record.organizationId != last_org_id_seen:
                        last_org_id_seen = history_record.organizationId
                        latest_org_change_record = history_record

                if (
                    latest_org_change_record
                    and latest_org_change_record.lastModified > cutoff_date
                    and consent_authored < datetime(2021, 6, 1)
                ):
                    print(f'P{modified_participant_id} changed org on {latest_org_change_record.lastModified}')


    def revalidate(self):
        validation_controller = ConsentValidationController(
            consent_dao=ConsentDao(),
            participant_summary_dao=ParticipantSummaryDao(),
            storage_provider=GoogleCloudStorageProvider()
        )
        with self.get_session() as session:
            for _id in self.args.ids.split(','):
                validation_controller.revalidate_record(session, _id)

    def generate_ce(self):
        validation_controller = ConsentValidationController(
            consent_dao=ConsentDao(),
            participant_summary_dao=ParticipantSummaryDao(),
            storage_provider=GoogleCloudStorageProvider()
        )
        with self.get_session() as session:
            # with LogResultStrategy(
            #     logger=logger,
            #     verbose=True,
            #     storage_provider=GoogleCloudStorageProvider()
            # ) as output_strategy:
            # validation_controller.validate_recent_uploads(session, output_strategy)
            validation_controller.generate_records_for_ce(session,
                                                          min_consent_date=datetime(2021, 6, 24, tzinfo=pytz.utc))

        input('Press Enter to exit...')

    def validate(self):
        validation_controller = ConsentValidationController(
            consent_dao=ConsentDao(),
            participant_summary_dao=ParticipantSummaryDao(),
            storage_provider=GoogleCloudStorageProvider()
        )
        with self.get_session() as session:
            with StoreResultStrategy(
                consent_dao=ConsentDao(),
                session=session
            ) as strategy:
                min_time = datetime(2010, 7, 1, 8)
                validation_controller.validate_recent_uploads(
                    session, strategy, min_consent_date=min_time
                )

    def report_files_for_correction(self):
        min_validation_date = parse(self.args.since) if self.args.since else None
        with self.get_session() as session:
            results_to_report = self._consent_dao.get_files_needing_correction(
                session=session,
                min_modified_datetime=min_validation_date
            )

        report_lines = []
        previous_participant_id = None
        for result in results_to_report:
            if previous_participant_id and previous_participant_id != result.participant_id and self.args.verbose:
                report_lines.append('')
            previous_participant_id = result.participant_id
            report_lines.append(self._line_output_for_validation(result, verbose=self.args.verbose))

        logger.info('\n'.join(report_lines))
        input('Press Enter to exit...')

    def modify_file_results(self):
        with self.get_session() as session:
            for _id in self.args.id.split(','):
                file = self._consent_dao.get_with_session(session, _id)
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
                    callback=lambda parsed_value: self._log_property_change('sync_status',
                                                                            file.sync_status, parsed_value)
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

    def _line_output_for_validation(self, file: ConsentFile, verbose: bool):
        output_line = StringIO()
        if verbose:
            output_line.write(f'{file.id} - ')  # TODO: ljust the id
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
                output_line.write(f'\n{self._get_link(file)}')

        return output_line.getvalue()

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

    revalidate_parser = subparsers.add_parser('revalidate')
    revalidate_parser.add_argument(
        '--ids', help='Database id of the record to modify', required=True
    )

    subparsers.add_parser('generate-ce')
    subparsers.add_parser('check-org-change')
    subparsers.add_parser('validate')
    subparsers.add_parser('list-bucket')
    subparsers.add_parser('trigger-sync')
    subparsers.add_parser('check-missing')

    corrections_parser = subparsers.add_parser('corrections')
    corrections_parser.add_argument('--participant', required=True)
    corrections_parser.add_argument('--type', required=True)


def run():
    cli_run(tool_cmd, tool_desc, ConsentTool, add_additional_arguments)
