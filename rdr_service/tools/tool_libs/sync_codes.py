import requests
from sqlalchemy.orm.session import Session

from rdr_service.clock import CLOCK
from rdr_service.model.code import Code, CodeType
from rdr_service.tools.tool_libs._tool_base import cli_run, logger, ToolBase
from rdr_service.tools.tool_libs.app_engine_manager import AppConfigClass

# Tool_cmd and tool_desc name are required.
# Remember to add/update bash completion in 'tool_lib/tools.bash'
tool_cmd = "sync-codes"
tool_desc = "Syncs codes from the provided Redcap project"

REDCAP_PROJECT_KEYS = 'project_api_keys'
CODE_SYSTEM = 'http://terminology.pmi-ops.org/CodeSystem/ppi'
META_DATA_FIELD_TYPE = ['text', 'radio', 'dropdown', 'checkbox', 'yesno', 'truefalse']


class SyncCodesClass(ToolBase):
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

    @staticmethod
    def initialize_code(session: Session, value, display, code_type=None):
        new_code = Code(
            codeType=code_type,
            value=value,
            shortValue=value[:50],
            display=display,
            system=CODE_SYSTEM,
            mapped=True,
            created=CLOCK.now()
        )
        session.add(new_code)
        return new_code

    def import_answer_code(self, session: Session, answer_text, question_code):
        # There may be multiple commas in the display string, we want to split on the first to get the code
        code, display = (part.strip() for part in answer_text.split(',', 1))
        answer_code = self.initialize_code(session, code, display, CodeType.ANSWER)
        answer_code.parent = question_code

    def import_data_dictionary_item(self, session: Session, code_json):
        new_code = self.initialize_code(session, code_json['field_name'], code_json['field_label'])

        if code_json['field_type'] == 'descriptive':
            new_code.codeType = CodeType.MODULE
        else:
            new_code.codeType = CodeType.QUESTION
            answers_string = code_json['select_choices_or_calculations']
            if answers_string:
                for answer_text in answers_string.split('|'):
                    self.import_answer_code(session, answer_text.strip(), new_code)

    @staticmethod
    def retrieve_data_dictionary(api_key):
        response = requests.post('https://redcap.pmi-ops.org/api/', data={
            'token': api_key,
            'content': 'metadata',
            'format': 'json',
            'returnFormat': 'json'
        })
        if response.status_code != 200:
            logger.error(f'ERROR: Received status code {response.status_code} from API')

        return response.content

    def run(self):
        super(SyncCodesClass, self).run()

        # Get the server config to read Redcap API keys
        project_api_key = self.get_api_key(self.args.redcap_project)
        if project_api_key is None:
            logger.error('Unable to find project API key')
            return 1
        dictionary_json = self.retrieve_data_dictionary(project_api_key)

        with self.get_session() as session:
            for item_json in dictionary_json:
                self.import_data_dictionary_item(session, item_json)

        return 0


def add_additional_arguments(parser):
    parser.add_argument('--redcap-project', required=True, help='Name of Redcap project to sync')


def run():
    cli_run(tool_cmd, tool_desc, SyncCodesClass)
