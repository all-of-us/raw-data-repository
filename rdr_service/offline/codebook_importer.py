from datetime import datetime
import re

from rdr_service.clock import CLOCK
from rdr_service.model.code import Code, CodeType
from rdr_service.model.survey import Survey, SurveyQuestion, SurveyQuestionType, SurveyQuestionOption

CODE_SYSTEM = 'http://terminology.pmi-ops.org/CodeSystem/ppi'
CODE_TYPES_WITH_OPTIONS = [SurveyQuestionType.RADIO, SurveyQuestionType.DROPDOWN, SurveyQuestionType.CHECKBOX]


class CodebookImporter:
    def __init__(self, project_json, dry_run, session, codes_allowed_for_reuse, logger):
        self.dry_run = dry_run
        self.session = session
        self.logger = logger

        self.codes_allowed_for_reuse = codes_allowed_for_reuse
        self.code_reuse_found = False
        self.invalid_codes_found = []
        self.questions_missing_options = []

        self.survey = None
        self.project_json = project_json

    def _save_database_object(self, obj):
        if not self.dry_run:
            self.session.add(obj)

    def initialize_code(self, value, display, code_type=None):
        new_code = Code(
            codeType=code_type,
            value=value,
            shortValue=value[:50],
            display=display,  # TODO: curation uses display, eventually refactor it so we can not have display on code
            system=CODE_SYSTEM,
            mapped=True,
            created=CLOCK.now()
        )
        existing_code_with_value = self.session.query(Code).filter(
            Code.value == value,
            Code.system == CODE_SYSTEM
        ).one_or_none()
        if existing_code_with_value:
            # Answer codes should automatically be allowed to be reused, anything else needs to be explicitly stated
            if code_type != CodeType.ANSWER and value not in self.codes_allowed_for_reuse:
                # At this point, it's not an answer code and it wasn't explicitly allowed for reuse
                # so set up the tool to stop saving and print out the code.
                # Let the script continue so that any other duplications can be caught.
                self.code_reuse_found = True
                self.logger.error(f'Code "{value}" is already in use')
                return None
            else:
                # Allows for reused codes to be a child (or parent) of other codes being imported
                return existing_code_with_value
        elif self.dry_run:
            self.logger.info(f'Found new "{code_type}" type code, value: {value}')
        elif not self.code_reuse_found:
            self._save_database_object(new_code)

        return new_code

    @staticmethod
    def is_code_value_valid(code_value):
        not_allowed_chars_regex = re.compile(r'[^a-zA-Z0-9_]')  # Regex for any characters that aren't allowed
        invalid_matches = not_allowed_chars_regex.search(code_value)
        return invalid_matches is None

    def parse_options(self, options_string, survey_question: SurveyQuestion):
        for option_text in options_string.split('|'):
            # There may be multiple commas in the display string, we want to split on the first to get the code
            code, display = (part.strip() for part in option_text.split(',', 1))
            option_code = self.initialize_code(code, display, CodeType.ANSWER)
            survey_question_option = SurveyQuestionOption(
                question=survey_question,
                code=option_code,
                display=display
            )
            # TODO: display should be on the questions/options now, not on the codes

            self._save_database_object(survey_question_option)

    def parse_question(self, field_name, description, field_type, item_json):
        question_code = self.initialize_code(field_name, description, CodeType.QUESTION)
        if question_code is not None:
            question_type = SurveyQuestionType(field_type.upper())
            survey_question = SurveyQuestion(
                survey=self.survey,
                code=question_code,
                display=description,
                questionType=question_type
            )
            self._save_database_object(survey_question)

            option_string = item_json['select_choices_or_calculations']
            if option_string:
                self.parse_options(option_string, survey_question)
            elif question_type in CODE_TYPES_WITH_OPTIONS:
                # The answers string was empty, but this is a type we'd expect to have options
                self.questions_missing_options.append(field_name)

    def parse_first_descriptive(self, field_name, description):
        module_code = self.initialize_code(field_name, description, CodeType.MODULE)
        # TODO: allow for module code sharing between two imports of the the same redcap project
        self.survey = Survey(
            redcapProjectId=self.project_json['project_id'],
            redcapProjectTitle=self.project_json['project_title'],
            code=module_code,
            importTime=datetime.utcnow()
        )
        self._save_database_object(self.survey)

    def import_data_dictionary_item(self, item_json):
        field_name = item_json['field_name']
        description = item_json['field_label']
        field_type = item_json['field_type']

        if field_name != 'record_id':  # Ignore the field Redcap automatically inserts into each project
            if not self.is_code_value_valid(field_name):
                self.invalid_codes_found.append(field_name)
            else:
                if field_type == 'descriptive':
                    # Only catch the first 'descriptive' field we see. That's the module code.
                    # Descriptive fields other than the first are considered to be readonly display text.
                    # So we don't want to save codes for them
                    if self.survey is None:
                        self.parse_first_descriptive(field_name, description)
                else:
                    self.parse_question(field_name, description, field_type, item_json)
