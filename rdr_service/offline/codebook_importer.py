import logging
import re

from rdr_service.clock import CLOCK
from rdr_service.model.code import Code, CodeType

CODE_SYSTEM = 'http://terminology.pmi-ops.org/CodeSystem/ppi'
CODE_TYPES_WITH_OPTIONS = ['radio', 'dropdown', 'checkbox']


class CodebookImporter:
    def __init__(self, dry_run, session, codes_allowed_for_reuse):
        self.dry_run = dry_run
        self.session = session
        self.codes_allowed_for_reuse = codes_allowed_for_reuse

        self.code_reuse_found = False
        self.module_code = None
        self.invalid_codes_found = []
        self.questions_missing_options = []

    def initialize_code(self, value, display, parent=None, code_type=None):
        new_code = Code(
            codeType=code_type,
            value=value,
            shortValue=value[:50],
            display=display,
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
                logging.error(f'Code "{value}" is already in use')
            else:
                # Allows for reused codes to be a child (or parent) of other codes being imported
                return existing_code_with_value
        elif self.dry_run:
            logging.info(f'Found new "{code_type}" type code, value: {value}')
        elif not self.code_reuse_found and not self.dry_run:
            # Associating a code with a parent adds it to the session too,
            # so it should only happen when we intend to save it.
            # (But the parent here could be empty, so we make sure the code gets to the session)
            new_code.parent = parent
            self.session.add(new_code)

        return new_code

    def import_answer_code(self, answer_text, question_code):
        # There may be multiple commas in the display string, we want to split on the first to get the code
        code, display = (part.strip() for part in answer_text.split(',', 1))
        self.initialize_code(code, display, question_code, CodeType.ANSWER)

    @staticmethod
    def is_code_value_valid(code_value):
        not_allowed_chars_regex = re.compile(r'[^a-zA-Z0-9_]')  # Regex for any characters that aren't allowed
        invalid_matches = not_allowed_chars_regex.search(code_value)
        return invalid_matches is None

    def import_data_dictionary_item(self, code_json):
        code_value = code_json['field_name']
        code_description = code_json['field_label']

        if code_value != 'record_id':  # Ignore the code Redcap automatically inserts into each project
            if not self.is_code_value_valid(code_value):
                self.invalid_codes_found.append(code_value)
            else:
                if code_json['field_type'] == 'descriptive':
                    # Only catch the first 'descriptive' field we see. That's the module code.
                    # Descriptive fields other than the first are considered to be readonly, display text.
                    # So we don't want to save codes for them
                    if not self.module_code:
                        new_code = self.initialize_code(code_value, code_description,
                                                        self.module_code, CodeType.MODULE)
                        self.module_code = new_code
                else:
                    new_code = self.initialize_code(code_value, code_description,
                                                    self.module_code, CodeType.QUESTION)

                    answers_string = code_json['select_choices_or_calculations']
                    if answers_string:
                        for answer_text in answers_string.split('|'):
                            self.import_answer_code(answer_text.strip(), new_code)
                    elif code_json['field_type'] in CODE_TYPES_WITH_OPTIONS:
                        # The answers string was empty, but this is a type we'd expect to have options
                        self.questions_missing_options.append(code_value)
