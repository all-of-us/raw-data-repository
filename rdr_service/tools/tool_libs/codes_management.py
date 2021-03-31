import csv
from datetime import datetime
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload
from oauth2client.service_account import ServiceAccountCredentials
import os

from rdr_service.dao.code_dao import CodeDao
from rdr_service.services.gcp_utils import gcp_get_iam_service_key_info
from rdr_service.model.code import Code
from rdr_service.offline.codebook_importer import CodebookImporter
from rdr_service.services.gcp_config import GCP_INSTANCES
from rdr_service.services.redcap_client import RedcapClient
from rdr_service.tools.tool_libs.tool_base import cli_run, logger, ToolBase

# Tool_cmd and tool_desc name are required.
# Remember to add/update bash completion in 'tool_lib/tools.bash'
tool_cmd = "codes"
tool_desc = "Manage code import/export process. Syncing codes from the provided Redcap project and/or exporting " \
            "codes that the RDR is aware of to the Ops team's Drive folder."

REDCAP_PROJECT_KEYS = 'project_api_keys'
DRIVE_EXPORT_FOLDER_ID = 'drive_export_folder'
EXPORT_SERVICE_ACCOUNT_NAME = 'code_export_service_account'

CODE_EXPORT_NAME_PREFIX = 'codes_'

# data shared between tool classes
_now_string = datetime.now().strftime('%Y-%m-%d_%H%M')
code_export_file_path = f'{CODE_EXPORT_NAME_PREFIX}{_now_string}.csv'
drive_folder_id = ''
exporter_service_account_name = ''


class CodesExportClass(ToolBase):
    @staticmethod
    def trash_previous_exports(credentials, v3_drive_service):
        # I haven't been able to figure out how to use only version 2 or only version 3 of drive.
        # We don't have permissions to outright delete files, but V2 allows us to move files to the trash.
        # However, I can't figure out how to see the files with V2 to be able to delete them. Hence the mix.

        # https://developers.google.com/drive/api/v3/reference/files/list
        response = v3_drive_service.files().list(
            supportsAllDrives=True,
            includeItemsFromAllDrives=True,
            q=f"'{drive_folder_id}' in parents"
        ).execute()
        files = response.get('files', [])

        # v2 lets us 'trash' files without further permissions
        v2_drive_service = build('drive', 'v2', credentials=credentials)
        for file in files:
            if file['name'].startswith(CODE_EXPORT_NAME_PREFIX):
                v2_drive_service.files().trash(
                    supportsAllDrives=True,
                    fileId=file['id']
                ).execute()

    @staticmethod
    def upload_file(drive_service):
        # https://developers.google.com/drive/api/v3/reference/files/create
        file_metadata = {
            'name': code_export_file_path,
            'parents': [drive_folder_id]
        }
        media = MediaFileUpload(code_export_file_path, mimetype='text/csv')
        drive_service.files().create(body=file_metadata, media_body=media, supportsAllDrives=True).execute()

    @staticmethod
    def initialize_process_context(tool_name, project, account, service_account):
        if project == '_all':
            project = 'all-of-us-rdr-prod'

        return ToolBase.initialize_process_context(tool_name, project, account, service_account)

    def run(self):
        # Intentionally not calling super's run
        # since the SA for exporting probably doesn't have SQL permissions
        # and super's run currently tries to activate the sql proxy

        if not self.args.dry_run:
            service_key_info = gcp_get_iam_service_key_info(self.gcp_env.service_key_id)
            credentials = ServiceAccountCredentials.from_json_keyfile_name(service_key_info['key_path'])
            drive_service = build('drive', 'v3', credentials=credentials)

            logger.info(f'Uploading code export for {self.gcp_env.project}')

            self.trash_previous_exports(credentials, drive_service)
            self.upload_file(drive_service)
            os.remove(code_export_file_path)


class CodesSyncClass(ToolBase):
    codes_allowed_for_reuse = []

    def parse_values_from_config(self, redcap_project_name):
        server_config = self.get_server_config()

        # Getting folder ID for export while syncing since export SA might not have permissions to the server config
        if DRIVE_EXPORT_FOLDER_ID not in server_config:
            logger.error('ERROR: Server config file does not list drive export folder id')
            return None
        global drive_folder_id
        drive_folder_id = server_config[DRIVE_EXPORT_FOLDER_ID]

        # And since we're here anyway... let's get the service account that should do the export
        if EXPORT_SERVICE_ACCOUNT_NAME not in server_config:
            logger.error('ERROR: Server config file does not list the export service account')
            return None
        global exporter_service_account_name
        exporter_service_account_name = server_config[EXPORT_SERVICE_ACCOUNT_NAME]

        if self.args.redcap_key:
            return self.args.redcap_key
        elif not self.args.export_only:
            if REDCAP_PROJECT_KEYS not in server_config:
                logger.error('ERROR: Server config file does not list any API keys')
                return None

            keys = server_config[REDCAP_PROJECT_KEYS]
            if redcap_project_name not in keys:
                logger.error(f'ERROR: Project "{redcap_project_name}" not listed with key in server config')
                return None

            return server_config[REDCAP_PROJECT_KEYS][redcap_project_name]
        else:
            return None

    @staticmethod
    def write_export_file(session):
        with open(code_export_file_path, 'w') as output_file:
            code_csv_writer = csv.writer(output_file)
            code_csv_writer.writerow([
                'Code Value',
                'Display',
                'Parent Values',
                'Module Values'
            ])
            codes = session.query(Code).order_by(Code.value).all()
            for code in codes:
                row_data = [code.value, code.display]

                parent_codes = CodeDao.get_parent_codes(code, session)
                if parent_codes:
                    row_data.append('|'.join([parent.value for parent in parent_codes]))

                    module_codes = CodeDao.get_module_codes(code, session)
                    if module_codes:
                        row_data.append('|'.join([module_code.value for module_code in module_codes]))
                code_csv_writer.writerow(row_data)

    def run_process(self):
        if self.args.reuse_codes:
            self.codes_allowed_for_reuse = [code_val.strip() for code_val in self.args.reuse_codes.split(',')]

        if self.args.project == '_all':
            for project in GCP_INSTANCES.keys():
                with self.initialize_process_context(self.tool_cmd, project, self.args.account,
                                                     self.args.service_account) as gcp_env:
                    self.gcp_env = gcp_env
                    self.run(skip_file_export_write=('prod' not in project))
            return 0
        else:
            return super(CodesSyncClass, self).run_process()

    def run(self, skip_file_export_write=False):
        super(CodesSyncClass, self).run()
        exit_code = 0

        with self.get_session() as session:

            # Get the server config to read Redcap API keys
            project_api_key = self.parse_values_from_config(self.args.redcap_project)

            if not self.args.export_only:

                if not self.args.dry_run:
                    logger.info(f'Importing codes for {self.gcp_env.project}')
                if project_api_key is None:
                    logger.error('Unable to find project API key')
                    return 1

                # Get the data-dictionary and process codes
                redcap = RedcapClient()
                dictionary_json = redcap.get_data_dictionary(project_api_key)
                project_json = redcap.get_project_info(project_api_key)

                code_importer = CodebookImporter(project_json, self.args.dry_run, session,
                                                 self.codes_allowed_for_reuse, logger)
                for item_json in dictionary_json:
                    code_importer.import_data_dictionary_item(item_json)

                # Don't save anything if codes were unintentionally reused
                if code_importer.code_reuse_found and not self.args.dry_run:
                    logger.error('The above codes were already in the RDR database. '
                                 'Please verify with the team creating questionnaires in Redcap that this '
                                 'was intentional, and then re-run the tool with the "--reuse-codes" argument '
                                 'to specify that they should be allowed.')
                    exit_code = 1

                # Don't save anything if no module code was found
                if code_importer.survey is None or code_importer.survey.code is None:
                    logger.error('No module code found, canceling import')
                    exit_code = 1

                if code_importer.invalid_codes_found:
                    code_values_str = ", ".join([f'"{value}"' for value in code_importer.invalid_codes_found])
                    logger.error(f'Invalid code values found: {code_values_str}')
                    exit_code = 1

                if code_importer.questions_missing_options:
                    code_values_str = ", ".join([f'"{value}"' for value in code_importer.questions_missing_options])
                    logger.error(f'The following question codes are missing answer options: {code_values_str}')
                    exit_code = 1

            if not self.args.dry_run:
                if exit_code == 1:
                    session.rollback()
                elif not skip_file_export_write:
                    self.write_export_file(session)

        return exit_code


def add_additional_arguments(parser):
    parser.add_argument('--redcap-project', help='Name of Redcap project to sync')
    parser.add_argument('--reuse-codes', default='',
                        help='Codes that have intentionally been reused from another project')
    parser.add_argument('--redcap-key', default=None, help='Redcap API key to use')
    parser.add_argument('--dry-run', action='store_true', help='Only print information, do not save or export codes')
    parser.add_argument('--export-only', action='store_true',
                        help='Only export codes, do not import anything new from Redcap')


def run():
    import_exit_code = cli_run(tool_cmd, tool_desc, CodesSyncClass, add_additional_arguments)
    if import_exit_code == 0:
        return cli_run(tool_cmd, tool_desc, CodesExportClass, add_additional_arguments, defaults={
            'service_account': exporter_service_account_name
        })
