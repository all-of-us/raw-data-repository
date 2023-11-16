from collections import defaultdict
from datetime import datetime
import logging
from typing import Dict

from sqlalchemy.orm import joinedload, Session

from rdr_service.domain_model.response import Response
from rdr_service.model.code import Code
from rdr_service.model.ppi_validation_errors import PpiValidationErrors
from rdr_service.model.survey import Survey, SurveyQuestion, SurveyQuestionOption
from rdr_service.repository.questionnaire_response_repository import QuestionnaireResponseRepository
from rdr_service.services.response_validation.validation import BranchParsingError, ResponseValidator
from rdr_service.services.slack_utils import SlackMessageHandler
from rdr_service.dao.ppi_validation_errors_dao import PpiValidationErrorsDao


class ResponseValidationController:
    def __init__(
        self,
        session: Session,
        validation_errors_dao: PpiValidationErrorsDao,
        since_date: datetime,
        slack_webhook=None
    ):
        self._session = session
        self._since_date = since_date
        self._error_list = defaultdict(list)
        self._slack_webhook = slack_webhook
        self._summarize_results = slack_webhook is not None

        self._response_validator_map: Dict[str, ResponseValidator] = {}
        self.validation_errors_dao = validation_errors_dao

    def run_validation(self):
        response_list = QuestionnaireResponseRepository.get_responses_to_surveys(
            session=self._session,
            created_start_datetime=self._since_date
        )
        for participant_id, participant_responses in response_list.items():
            for response in participant_responses.responses.values():
                try:
                    self._check_response(response, participant_id=participant_id)
                except BranchParsingError:
                    logging.error(f'Error parsing branching logic for {response.survey_code}', exc_info=True)

        self._insert_data()
        self._send_error_message()

    def _check_response(self, response: Response, participant_id):
        # look at the response map, use validator if it's there, build it if it's not
        # store the errors for later handling
        if response.survey_code not in self._response_validator_map:
            self._response_validator_map[response.survey_code] = self._build_validator(survey_code=response.survey_code)

        validator = self._response_validator_map.get(response.survey_code)
        if validator:
            for error_list in validator.get_errors_in_response(response).values():
                for error in error_list:
                    if response.survey_code != 'EHRConsentPII':
                        # TODO: implement a way to detect when validation needed for sensitive EHR
                        self._error_list[(
                            response.survey_code,
                            error.question_code,
                            error.reason,
                            error.error_type,
                            participant_id,
                            error.answer_id[0]
                        )].append(
                            f'{response.survey_code} - question "{error.question_code}" '
                            f'Error: {error.reason} (P{participant_id}, ansID {error.answer_id[0]})'
                        )

    def _build_validator(self, survey_code):
        query = (
            self._session.query(Survey)
            .join(Code)
            .filter(
                Survey.replacedTime.is_(None),
                Code.value == survey_code,
                Survey.redcapProjectId.isnot(None)
            ).options(
                joinedload(Survey.questions).joinedload(SurveyQuestion.code),
                joinedload(Survey.questions).joinedload(SurveyQuestion.options).joinedload(SurveyQuestionOption.code)
            )
        )
        survey = query.one_or_none()

        if survey:
            return ResponseValidator(survey_definition=survey, session=self._session)
        else:
            return None

    def _send_error_message(self):
        if not self._error_list:
            self._output_result(f'No validation errors were found since {self._since_date}')
            return

        result_text = f'Validation errors for survey responses received since {self._since_date.date()}\n'
        result_list = []
        if self._summarize_results:
            error_counts = defaultdict(lambda: 0)

            # Condense the list into the number of times a specific error string was seen for a question in a survey
            for key in self._error_list:
                survey_code, question_code, error_str, *_ = key
                error_counts[(survey_code, question_code, error_str)] += 1

            for error_info, count in error_counts.items():
                survey_code, question_code, error_str = error_info
                result_list.append(
                    f'{survey_code} "{question_code}" Error: {error_str}, number affected answers: {count}'
                )
        else:
            for error_list in self._error_list.values():
                for error in error_list:
                    result_list.append(error)

        result_text += '\n'.join(sorted(result_list))
        self._output_result(result_text)

    def _insert_data(self):
        for key in self._error_list:
            survey_code, question_code, error_str, error_type, pid, answer_id = key

            data = PpiValidationErrors(
                eval_date=datetime.utcnow(),
                survey_code_value=survey_code,
                question_code=question_code,
                error_str=error_str,
                error_type=error_type,
                participant_id=pid,
                questionnaire_response_answer_id=answer_id
            )

            # Insert data into PPI validation table if offline job is running
            if self._summarize_results:
                self.validation_errors_dao.insert_with_session(session=self._session, obj=data)

    def _output_result(self, result_str):
        if self._slack_webhook:
            client = SlackMessageHandler(webhook_url=self._slack_webhook)
            client.send_message_to_webhook(
                message_data={
                    'text': result_str
                }
            )
        else:
            logging.info(result_str)
