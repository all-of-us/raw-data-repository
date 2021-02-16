from datetime import datetime
import logging
from typing import List

from rdr_service.model.code import Code, CodeType
from rdr_service.model.survey import Survey, SurveyQuestion, SurveyQuestionOption, SurveyQuestionType
from rdr_service.tools.tool_libs.tool_base import cli_run, ToolBase

tool_cmd = 'survey-back-fill'
tool_desc = 'Create survey structure objects for codes'


class SurveyBackFill(ToolBase):
    @classmethod
    def get_direct_children_codes(cls, parent_code, session) -> List[Code]:
        return session.query(Code).filter(Code.parentId == parent_code.codeId)

    def get_descendant_codes(self, parent_code: Code, child_type: CodeType, session):
        children_matching_type = []
        children: List[Code] = self.get_direct_children_codes(parent_code, session)

        if children:
            direct_children_of_given_type = [child for child in children if child.codeType == child_type]
            children_matching_type.extend(direct_children_of_given_type)

            for child in children:
                children_matching_type.extend(self.get_descendant_codes(child, child_type, session))

        return children_matching_type

    def build_options(self, question_code, session):
        return [SurveyQuestionOption(
            code=code,
            display=code.display
        ) for code in self.get_descendant_codes(question_code, CodeType.ANSWER, session)]

    def build_questions(self, module_code, session):
        questions = []
        question_codes = self.get_descendant_codes(module_code, CodeType.QUESTION, session)
        for code in question_codes:
            survey_question_obj = SurveyQuestion(
                code=code,
                questionType=SurveyQuestionType.UNKNOWN,
                display=code.display
            )
            survey_question_obj.options = self.build_options(code, session)
            questions.append(survey_question_obj)

        return questions

    def run(self):
        with self.get_session() as session:
            module_code = session.query(Code).filter(Code.value == self.args.module_code).one()

            existing_survey = session.query(Survey).filter(Survey.code == module_code).one_or_none()
            if existing_survey:
                logging.info(f'Skipping {module_code.value} (survey already exists)')
            else:
                survey_obj = Survey(
                    code=module_code,
                    importTime=datetime.utcnow(),
                    redcapProjectTitle=module_code.display
                )
                survey_obj.questions = self.build_questions(module_code, session)
                session.add(survey_obj)


def add_additional_arguments(parser):
    parser.add_argument('--module-code', required=True, help='Module code value to back fill')


def run():
    return cli_run(tool_cmd, tool_desc, SurveyBackFill, add_additional_arguments)
