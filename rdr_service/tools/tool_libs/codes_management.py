import requests
from sqlalchemy.orm.session import Session

from rdr_service.clock import CLOCK
from rdr_service.model.code import Code, CodeType
from rdr_service.tools.tool_libs._tool_base import cli_run, logger, ToolBase
from rdr_service.tools.tool_libs.app_engine_manager import AppConfigClass

# Tool_cmd and tool_desc name are required.
# Remember to add/update bash completion in 'tool_lib/tools.bash'
tool_cmd = "codes"
tool_desc = "Manage code import/export process. Syncing codes from the provided Redcap project and/or exporting " \
            "codes that the RDR is aware of to the Ops team's Drive folder."

REDCAP_PROJECT_KEYS = 'project_api_keys'
CODE_SYSTEM = 'http://terminology.pmi-ops.org/CodeSystem/ppi'


class CodesManagementClass(ToolBase):
    module_code = None
    codes_allowed_for_reuse = []
    code_reuse_found = False

    def get_api_key(self, redcap_project_name):
        # The AppConfig class uses the git_project field from args when initializing,
        # looks like it uses it as a root directory for other purposes.
        self.args.git_project = self.gcp_env.git_project

        # Get the server config
        app_config_manager = AppConfigClass(self.args, self.gcp_env)
        server_config = app_config_manager.get_bucket_app_config()

        if REDCAP_PROJECT_KEYS not in server_config:
            logger.error('ERROR: Server config file does not list any API keys')
            return None

        keys = server_config[REDCAP_PROJECT_KEYS]
        if redcap_project_name not in keys:
            logger.error(f'ERROR: Project "{redcap_project_name}" not listed with key in server config')
            return None

        return server_config[REDCAP_PROJECT_KEYS][redcap_project_name]

    def initialize_code(self, session: Session, value, display, parent=None, code_type=None):
        new_code = Code(
            codeType=code_type,
            value=value,
            shortValue=value[:50],
            display=display,
            system=CODE_SYSTEM,
            mapped=True,
            created=CLOCK.now()
        )
        existing_code_with_value = session.query(Code).filter(Code.value == value).one_or_none()
        if existing_code_with_value:
            # Answer codes should automatically be allowed to be reused, anything else needs to be explicitly stated
            if code_type != CodeType.ANSWER and value not in self.codes_allowed_for_reuse:
                # At this point, it's not an answer code and it wasn't explicitly allowed for reuse
                # so set up the tool to stop saving and print out the code.
                # Let the script continue so that any other duplications can be caught.
                self.code_reuse_found = True
                logger.error(f'Code "{value}" is already in use')
            else:
                # Allows for reused codes to be a child (or parent) of other codes being imported
                return existing_code_with_value
        elif self.args.dry_run:
            logger.info(f'Found new "{code_type}" type code, value: {value}')
        elif not self.code_reuse_found and not self.args.dry_run:
            # Associating a code with a parent adds it to the session too,
            # so it should only happen when we intend to save it.
            # (But the parent here could be empty, so we make sure the code gets to the session)
            new_code.parent = parent
            session.add(new_code)

        return new_code

    def import_answer_code(self, session, answer_text, question_code):
        # There may be multiple commas in the display string, we want to split on the first to get the code
        code, display = (part.strip() for part in answer_text.split(',', 1))
        self.initialize_code(session, code, display, question_code, CodeType.ANSWER)

    def import_data_dictionary_item(self, session: Session, code_json):
        code_value = code_json['field_name']
        code_description = code_json['field_label']

        if code_json['field_type'] == 'descriptive':
            # Only catch the first 'descriptive' field we see. That's the module code.
            # Descriptive fields other than the first are considered to be readonly, display text.
            # So we don't want to save codes for them
            if not self.module_code:
                new_code = self.initialize_code(session, code_value, code_description,
                                                self.module_code, CodeType.MODULE)
                self.module_code = new_code
        else:
            new_code = self.initialize_code(session, code_value, code_description,
                                            self.module_code, CodeType.QUESTION)

            answers_string = code_json['select_choices_or_calculations']
            if answers_string:
                for answer_text in answers_string.split('|'):
                    self.import_answer_code(session, answer_text.strip(), new_code)

    @staticmethod
    def retrieve_data_dictionary(api_key):
        # https://precisionmedicineinitiative.atlassian.net/browse/PD-5404
        headers = {
            'User-Agent': 'RDR code sync tool',
            'Accept': None,
            'Connection': None,
        }

        response = requests.post('https://redcap.pmi-ops.org/api/', data={
            'token': api_key,
            'content': 'metadata',
            'format': 'json',
            'returnFormat': 'json'
        }, headers=headers)
        if response.status_code != 200:
            logger.error(f'ERROR: Received status code {response.status_code} from API')

        return response.content

    def run(self):
        super(CodesManagementClass, self).run()

        if hasattr(self.args, 'reuse_codes'):
            self.codes_allowed_for_reuse = [code_val.strip() for code_val in self.args.reuse_codes.split(',')]

        # Get the server config to read Redcap API keys
        project_api_key = self.get_api_key(self.args.redcap_project)
        if project_api_key is None:
            logger.error('Unable to find project API key')
            return 1

        # Get the data-dictionary and process codes
        dictionary_json = self.retrieve_data_dictionary(project_api_key)
        with self.get_session() as session:
            for item_json in dictionary_json:
                self.import_data_dictionary_item(session, item_json)

            # Don't save anything if codes were unintentionally reused
            if self.code_reuse_found and not self.args.dry_run:
                session.rollback()
                logger.error('The above codes were already in the RDR database. '
                             'Please verify with the team creating questionnaires in Redcap that this was intentional, '
                             'and then re-run the tool with the "--reuse-codes" arguement to specify that they should '
                             'be allowed.')
                return 1

        return 0


def add_additional_arguments(parser):
    parser.add_argument('--redcap-project', help='Name of Redcap project to sync')
    parser.add_argument('--reuse-codes', help='Codes that have intentionally been reused from another project')
    parser.add_argument('--dry-run', help='Only print information, do not save or export codes')


def run():
    cli_run(tool_cmd, tool_desc, CodesManagementClass, add_additional_arguments)
