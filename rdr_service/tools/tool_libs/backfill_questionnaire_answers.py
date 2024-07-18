import argparse
import json

from rdr_service.code_constants import PPI_EXTRA_SYSTEM
from werkzeug.exceptions import BadRequest

from rdr_service.dao.code_dao import CodeDao
from rdr_service.dao.questionnaire_dao import QuestionnaireDao
from rdr_service.lib_fhir.fhirclient_1_0_6.models import questionnaireresponse as fhir_questionnaireresponse
from rdr_service.model.code import CodeType
from rdr_service.model.questionnaire_response import QuestionnaireResponseAnswer, QuestionnaireResponse
from rdr_service.tools.tool_libs.tool_base import cli_run, ToolBase

tool_cmd = 'questionnaire-answers-backfill'
tool_desc = ('Backfill the questionnaire answers table for questionnaires that are missing a concept code definition '
             '- Created for DA-4318')


class QuestionnaireAnswersBackfillTool(ToolBase):
    def run(self):
        super(QuestionnaireAnswersBackfillTool, self).run()

        questionnaire_ids = [self.args.questionnaire_ids]
        latest_id = -10
        with self.get_session() as session:
            found_responses = True
            while found_responses:
                questionnaire_response_query = session.query(
                    QuestionnaireResponse.questionnaireResponseId,
                    QuestionnaireResponse.resource,
                    QuestionnaireResponse.questionnaireId,
                ).filter(
                    QuestionnaireResponse.questionnaireResponseId > latest_id,
                    QuestionnaireResponse.created > '2021-11-01',
                    QuestionnaireResponse.questionnaireId.in_(questionnaire_ids),
                    QuestionnaireResponse.classificationType == 0,
                    QuestionnaireResponse.resource.like('%linkId": "60666%')
                ).order_by(QuestionnaireResponse.questionnaireResponseId).limit(500)

                found_responses = False
                for response_data in questionnaire_response_query:
                    found_responses = True

                    resource_json = json.loads(response_data.resource)
                    questionnaire_id = response_data.questionnaireId
                    fhir_qr = fhir_questionnaireresponse.QuestionnaireResponse(resource_json)
                    questionnaire = self._get_questionnaire(questionnaire_id)

                    if fhir_qr.group is not None:
                        # Extract answers from resource json
                        code_map, answers = self._extract_codes_and_answers(fhir_qr.group, questionnaire)

                        # Get or insert codes, and retrieve their database IDs.
                        code_id_map = CodeDao().get_internal_id_code_map(code_map)

                        # Now add the child answers, using the IDs in code_id_map
                        answers_to_insert = self._add_answers(code_id_map, answers,
                                                              response_data.questionnaireResponseId)
                        session.add_all(answers_to_insert)

                    latest_id = response_data.questionnaireResponseId

                if found_responses:
                    print(f'got to {latest_id}')
                    print('committing')
                    session.commit()

    @staticmethod
    def _get_questionnaire(questionnaire_id):
        q = QuestionnaireDao().get_with_children(int(questionnaire_id))
        return q

    @staticmethod
    def _add_answers(code_id_map, answers, questionnaire_response_id):
        answers_to_insert = []
        for answer, system_and_code in answers:
            if system_and_code:
                system, code = system_and_code
                answer.valueCodeId = code_id_map.get(system, code)
                answer.questionnaireResponseId = questionnaire_response_id
            answers_to_insert.append(answer)
        return answers_to_insert

    @classmethod
    def _extract_codes_and_answers(cls, group, q):
        """Returns (system, code) -> (display, code type, question code id) code map
    and (QuestionnaireResponseAnswer, (system, code)) answer pairs.
    """
        code_map = {}
        answers = []
        link_id_to_question = {}
        if q.questions:
            link_id_to_question = {question.linkId: question for question in q.questions}
        cls._populate_codes_and_answers(group, code_map, answers, link_id_to_question, q.questionnaireId)
        return code_map, answers

    @classmethod
    def _populate_codes_and_answers(cls, group, code_map, answers, link_id_to_question, questionnaire_id):
        """Populates code_map with (system, code) -> (display, code type, question code id)
    and answers with (QuestionnaireResponseAnswer, (system, code)) pairs."""
        if group.question:
            for question in group.question:
                if question.linkId and question.answer:
                    qq = link_id_to_question.get(question.linkId)
                    if qq and question.linkId == '60666':
                        for answer in question.answer:
                            qr_answer = QuestionnaireResponseAnswer(questionId=qq.questionnaireQuestionId)
                            system_and_code = None
                            ignore_answer = False
                            if answer.valueCoding:
                                if not answer.valueCoding.system:
                                    raise BadRequest(f"No system provided for valueCoding: {question.linkId}")
                                if not answer.valueCoding.code:
                                    raise BadRequest(f"No code provided for valueCoding: {question.linkId}")
                                if answer.valueCoding.system == PPI_EXTRA_SYSTEM:
                                    # Ignore answers from the ppi-extra system, as they aren't used for analysis.
                                    ignore_answer = True
                                else:
                                    system_and_code = (answer.valueCoding.system, answer.valueCoding.code)
                                    if not system_and_code in code_map:
                                        code_map[system_and_code] = (
                                            answer.valueCoding.display,
                                            CodeType.ANSWER,
                                            qq.codeId,
                                        )
                            if not ignore_answer:
                                if answer.valueDecimal is not None:
                                    qr_answer.valueDecimal = answer.valueDecimal
                                if answer.valueInteger is not None:
                                    qr_answer.valueInteger = answer.valueInteger
                                if answer.valueString is not None:
                                    answer_length = len(answer.valueString)
                                    max_length = QuestionnaireResponseAnswer.VALUE_STRING_MAXLEN
                                    if answer_length > max_length:
                                        raise BadRequest(
                                            f"String value too long (len={answer_length}); "
                                            f"must be less than {max_length}"
                                        )
                                    qr_answer.valueString = answer.valueString
                                if answer.valueDate is not None:
                                    qr_answer.valueDate = answer.valueDate.date
                                if answer.valueDateTime is not None:
                                    qr_answer.valueDateTime = answer.valueDateTime.date
                                if answer.valueBoolean is not None:
                                    qr_answer.valueBoolean = answer.valueBoolean
                                if answer.valueUri is not None:
                                    qr_answer.valueUri = answer.valueUri
                                answers.append((qr_answer, system_and_code))
                            if answer.group:
                                for sub_group in answer.group:
                                    cls._populate_codes_and_answers(
                                        sub_group, code_map, answers, link_id_to_question, questionnaire_id
                                    )

        if group.group:
            for sub_group in group.group:
                cls._populate_codes_and_answers(sub_group, code_map, answers, link_id_to_question, questionnaire_id)


def add_additional_arguments(parser: argparse.ArgumentParser):
    parser.add_argument(
        '--questionnaire_ids',
        help='Questionnaire IDs that need response answers backfilled',
        required=True
    )


def run():
    return cli_run(tool_cmd, tool_desc, QuestionnaireAnswersBackfillTool, add_additional_arguments)
