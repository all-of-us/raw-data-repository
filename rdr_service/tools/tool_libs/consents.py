import argparse
import csv
from datetime import datetime, timedelta
from dateutil.parser import parse
from itertools import islice
from typing import Collection

import requests
from sqlalchemy import Boolean, Integer

from rdr_service import config
from rdr_service.app_util import BatchManager
from rdr_service.dao.consent_dao import ConsentDao
from rdr_service.dao.hpo_dao import HPODao
from rdr_service.dao.participant_dao import ParticipantHistoryDao
from rdr_service.dao.participant_summary_dao import ParticipantSummaryDao
from rdr_service.resource.tasks import dispatch_rebuild_consent_metrics_tasks
from rdr_service.services.gcp_utils import gcp_make_auth_header
from rdr_service.model.consent_file import ConsentFile, ConsentSyncStatus, ConsentType
from rdr_service.model.participant import Participant
from rdr_service.model.utils import Enum
from rdr_service.offline.sync_consent_files import ConsentSyncGuesser
from rdr_service.services.consent.validation import ConsentValidationController, ReplacementStoringStrategy,\
    LogResultStrategy
from rdr_service.storage import GoogleCloudStorageProvider
from rdr_service.tools.tool_libs.tool_base import cli_run, logger, ToolBase


tool_cmd = 'consents'
tool_desc = 'Get reports of consent issues and modify validation records'


class ConsentTool(ToolBase):
    def __init__(self, *args, **kwargs):
        super(ConsentTool, self).__init__(*args, **kwargs)
        self._consent_dao = ConsentDao()
        self._storage_provider = GoogleCloudStorageProvider()
        self._sever_config = None

    def run_process(self):
        # start a gcp environment without the service account to retrieve the config without permission issues
        with self.initialize_process_context(
            self.tool_cmd,
            self.args.project,
            self.args.account,
            service_account=''
        ) as gcp_env:
            self.gcp_env = gcp_env
            self._sever_config = self.get_server_config()

        super(ConsentTool, self).run_process()

    def run(self):
        super(ConsentTool, self).run()

        # Overwrite the local config consent buckets with what is available from the target environment
        config.override_setting(config.CONSENT_PDF_BUCKET, self._sever_config[config.CONSENT_PDF_BUCKET])

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
        elif self.args.command == 'retro-validation':
            self.retro_validate()
        elif self.args.command == 'files-for-update':
            self.files_for_update()

    def _call_server_for_retro_validation(self, participant_ids):
        if len(participant_ids) > 0:
            print(f'Processing batch that starts with {participant_ids[0]}')

        response = requests.post(
            'https://offline-dot-all-of-us-rdr-prod.appspot.com/offline/ManuallyValidateFiles',
            headers=gcp_make_auth_header(),
            json={
                'ids': participant_ids
            }
        )

        if response.status_code != 200:
            exit(1)  # Exiting program to prevent further calls with a bad key

    def files_for_update(self):
        logger.info('Generating file...')
        storage_provider = GoogleCloudStorageProvider()
        with open('output.csv', 'w') as output_file, self.get_session() as session:
            # Updatable query to get the files to verify and modify
            files: Collection[ConsentFile] = session.query(ConsentFile).join(
                Participant, Participant.participantId == ConsentFile.participant_id
            ).filter(
                ConsentFile.type == ConsentType.EHR,
                ConsentFile.sync_status == ConsentSyncStatus.NEEDS_CORRECTING
            ).order_by(ConsentFile.participant_id, ConsentFile.file_upload_time).limit(500).all()

            fields_to_output = [
                'id',
                'file_path',
                'file',
                'participant_id',
                'type',
                'file_exists',
                'is_signature_valid',
                'is_signing_date_valid',
                'signature_str',
                'expected_sign_date',
                'signing_date',
                'sync_status'
            ]

            csv_writer = csv.DictWriter(output_file, fields_to_output)
            csv_writer.writeheader()
            for file in files:
                output_record = {
                    'file': None
                }
                for field_name in [name for name in fields_to_output if hasattr(ConsentFile, name)]:
                    value = getattr(file, field_name)

                    field_type = getattr(ConsentFile, field_name).expression.type
                    if isinstance(field_type, (Enum, Boolean)):
                        value = int(value)

                    output_record[field_name] = value

                if file.file_path:
                    bucket_name, *name_parts = file.file_path.split('/')
                    blob = storage_provider.get_blob(
                        bucket_name=bucket_name,
                        blob_name='/'.join(name_parts)
                    )
                    output_record['file'] = blob.generate_signed_url(datetime.utcnow() + timedelta(hours=4))

                csv_writer.writerow(output_record)

        input('Press Enter to exit...')  # The Google SA key will need to stay active for links to docs to work

    def retro_validate(self):
        with self.get_session() as session:
            participant_summaries = ConsentDao.get_participants_needing_validation(session=session)

        with BatchManager(batch_size=10, callback=self._call_server_for_retro_validation) as batch_manager:
            for summary in participant_summaries:
                batch_manager.add(summary.participantId)

    def check_retro_sync(self):
        with self.get_session() as session:
            consent_dao = ConsentDao()
            sync_config = self.get_server_config()[config.CONSENT_SYNC_BUCKETS]
            files = consent_dao.get_files_ready_to_sync(org_names=sync_config.keys(), session=session)

            retro_files = [file for file in files if file.file_upload_time < datetime(2021, 6, 1)]
            guesser = ConsentSyncGuesser(session=session, participant_history_dao=ParticipantHistoryDao())
            guesser.check_consents(retro_files, session=session)

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
                self._consent_dao.batch_update_consent_files([file], session)
                session.commit()
                dispatch_rebuild_consent_metrics_tasks([file.id],
                                                       project_id=self.gcp_env.project, build_locally=True)

    def validate_consents(self):
        consent_type = None
        if self.args.type:
            consent_type = ConsentType(self.args.type)

        controller = ConsentValidationController(
            consent_dao=ConsentDao(),
            participant_summary_dao=ParticipantSummaryDao(),
            hpo_dao=HPODao(),
            storage_provider=GoogleCloudStorageProvider()
        )
        with open(self.args.pid_file) as pid_file,\
                self.get_session() as session,\
                ReplacementStoringStrategy(session=session, consent_dao=controller.consent_dao) as store_strategy:
            # Get participant ids from the file in batches
            # (retrieving all their summaries at once, processing them before the next batch)
            participant_lookup_batch_size = 500
            participant_ids = list(islice(pid_file, participant_lookup_batch_size))
            while participant_ids:
                summaries = ParticipantSummaryDao.get_by_ids_with_session(session=session, obj_ids=participant_ids)
                for participant_summary in summaries:
                    controller.validate_participant_consents(
                        summary=participant_summary,
                        output_strategy=store_strategy,
                        types_to_validate=[consent_type]
                    )
                participant_ids = list(islice(pid_file, participant_lookup_batch_size))

    def upload_records(self):
        file_ids_modified = []
        with open(self.args.file) as input_file, self.get_session() as session:
            input_csv = csv.DictReader(input_file)
            for validation_data in input_csv:
                consent_file: ConsentFile = session.query(ConsentFile).filter(
                    ConsentFile.id == validation_data['id']
                ).one()

                for field_name, value in validation_data.items():
                    if not hasattr(ConsentFile, field_name):
                        continue  # Skip any fields that were just for output purpose, like the link for the file
                    if value == '':
                        value = None

                    field_definition = getattr(ConsentFile, field_name)
                    if field_name == 'type':
                        value = ConsentType(int(value))
                    elif isinstance(field_definition.expression.type, Boolean):
                        value = (value == '1')
                    elif isinstance(field_definition.expression.type, Integer):
                        value = int(value)

                    setattr(consent_file, field_name, value)

                file_ids_modified.append(consent_file.id)

        if file_ids_modified:
            dispatch_rebuild_consent_metrics_tasks(file_ids_modified, project_id=self.gcp_env.project)

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
    modify_parser.add_argument('--pid-file', help='File listing the participant ids to validate', required=True)
    modify_parser.add_argument('--type', help='Consent type to validate, defaults to validating all consents.')

    modify_parser = subparsers.add_parser('upload')
    modify_parser.add_argument(
        '--file',
        help='CSV file defining validation record data that should be added to the database',
        required=True
    )

    subparsers.add_parser('check-retro-sync')
    subparsers.add_parser('retro-validation')
    subparsers.add_parser('files-for-update')


def run():
    cli_run(tool_cmd, tool_desc, ConsentTool, add_additional_arguments)
