from ast import literal_eval
from datetime import datetime
from typing import Dict

import openpyxl
from sqlalchemy.orm import Session

from rdr_service.model.code import Code, CodeType
from rdr_service.model.ppsc_survey import (
    PpscSurveyDataDictionary, PpscSurveyQuestionDefinition, PpscSurveyQuestionOption,
    PpscQuestionDataType
)

QUESTION_CODE_INDEX = 0
PARTICIPANT_PROVIDED_INDEX = 3
DATA_TYPE_INDEX = 4
DISPLAY_LOGIC_INDEX = 9
DONT_DISPLAY_LOGIC_INDEX = 10
OPTIONS_ENUM_INDEX = 17
EXCLUSIVITY_INDEX = 18
PATTERN_INDEX = 21
MIN_INDEX = 22
MAX_INDEX = 23
MULTIPLE_OF_INDEX = 28


class PpscDictionaryParser:
    def __init__(self, file_name, dictionary_file_stream, session: Session):
        self.session = session
        self._code_cache: Dict[str, Code] = self._get_db_codes()

        self._data_dictionary = self._build_data_dictionary_object(file_name)

        self._parse_dictionary_file(dictionary_file_stream)
        self._save_data_dictionary()

    def _parse_dictionary_file(self, dictionary_file_stream):
        workbook = openpyxl.load_workbook(dictionary_file_stream)
        survey_worksheet = workbook.worksheets[2]
        if survey_worksheet.title != 'Survey Experience Variables':
            raise Exception(
                'Expected third sheet to be survey data dictionary named "Survey Experience Variables", '
                f'but got "{survey_worksheet.title}" instead'
            )

        last_filled_cell = survey_worksheet.dimensions.split(':')[-1]
        for row_cells in survey_worksheet[f'A2:{last_filled_cell}']:
            row_values = [cell.value for cell in row_cells]
            is_participant_provided = row_values[PARTICIPANT_PROVIDED_INDEX]
            if not is_participant_provided:
                continue

            question_code_str = row_values[QUESTION_CODE_INDEX]
            data_type = row_values[DATA_TYPE_INDEX]
            display_logic = row_values[DISPLAY_LOGIC_INDEX]
            dont_display_logic = row_values[DONT_DISPLAY_LOGIC_INDEX]
            regex_pattern_str = row_values[PATTERN_INDEX]
            min_value = row_values[MIN_INDEX]
            max_value = row_values[MAX_INDEX]
            multiple_of = row_values[MULTIPLE_OF_INDEX]

            question_option_list = None
            options_str = row_values[OPTIONS_ENUM_INDEX]
            if options_str:
                question_option_list = []
                option_code_list = literal_eval(options_str)
                exclusivity_list = literal_eval(row_values[EXCLUSIVITY_INDEX])
                for index, code_str in enumerate(option_code_list):
                    is_exclusive = exclusivity_list[index]
                    option_code = self._get_code(code_str, False)
                    question_option_list.append(
                        PpscSurveyQuestionOption(
                            code_id=option_code.codeId,
                            is_exclusive=is_exclusive
                        )
                    )

            question_code = self._get_code(question_code_str)
            question_obj = PpscSurveyQuestionDefinition(
                code_id=question_code.codeId,
                answer_format_pattern=regex_pattern_str,
                min_value=min_value,
                max_value=max_value,
                multiple_of=multiple_of
            )
            if question_option_list:
                question_obj.options = question_option_list
            if display_logic or dont_display_logic:
                question_obj.branching_logic = [
                    logic_str
                    for logic_str in [display_logic, dont_display_logic]
                    if logic_str
                ]

            self._store_data_type(data_type, question_obj)

            self._data_dictionary.questions.append(question_obj)

    @classmethod
    def _store_data_type(cls, data_type_str: str, question: PpscSurveyQuestionDefinition) -> PpscQuestionDataType:
        calculated_data_type = None

        if data_type_str == 'integer':
            calculated_data_type = PpscQuestionDataType.INTEGER
        elif data_type_str == 'number':
            calculated_data_type = PpscQuestionDataType.FLOAT
        elif data_type_str == 'array' or (data_type_str == 'string' and question.options):
            calculated_data_type = PpscQuestionDataType.SELECTION
        elif data_type_str == 'string':
            calculated_data_type = PpscQuestionDataType.TEXT

        if calculated_data_type is None:
            raise Exception(f'Unable to determine data type for a question ({question.code_id} "{data_type_str}")')

        question.data_type = calculated_data_type

    def _save_data_dictionary(self):
        self.session.add(self._data_dictionary)

    def _get_db_codes(self) -> Dict[str, Code]:
        db_results = self.session.query(Code).all()
        return {code.value.lower(): code for code in db_results}

    def _get_code(self, code_str, expecting_question=True) -> Code:
        code_str = code_str.lower()
        if code_str not in self._code_cache:
            new_code = Code(
                system='http://terminology.pmi-ops.org/CodeSystem/ppi',
                value=code_str,
                shortValue=code_str,
                codeType=(CodeType.QUESTION if expecting_question else CodeType.ANSWER),
                mapped=True,
                created=datetime.utcnow()
            )
            self.session.add(new_code)
            self.session.flush()
            self._code_cache[code_str] = new_code

        return self._code_cache[code_str]

    @classmethod
    def _build_data_dictionary_object(cls, file_path: str) -> PpscSurveyDataDictionary:
        file_name = file_path.split('_')
        file_name_parts = file_name[-1].split('-')

        return PpscSurveyDataDictionary(
            ppsc_survey_id=file_name_parts[0],          # 2001-...-...
            version_number=file_name_parts[1][1:]       # ....-v12-...
        )
