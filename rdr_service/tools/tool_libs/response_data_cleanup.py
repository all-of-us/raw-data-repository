import argparse
import logging

from sqlalchemy.orm import joinedload, Session
from typing import Dict, List

from rdr_service.dao.participant_summary_dao import ParticipantSummaryDao
from rdr_service.dao.questionnaire_response_dao import QuestionnaireResponseDao
from rdr_service.model.code import Code
from rdr_service.model.questionnaire_response import QuestionnaireResponseAnswer
from rdr_service.model.survey import Survey, SurveyQuestion, SurveyQuestionOption
from rdr_service.offline.bigquery_sync import dispatch_participant_rebuild_tasks
from rdr_service.services.response_validation.validation import ResponseValidator, ValidationError
from rdr_service.services.system_utils import list_chunks
from rdr_service.tools.tool_libs.tool_base import cli_run, ToolBase


tool_cmd = 'response-cleanup'
tool_desc = 'Script for checking response data against validation rules'


class ResponseDataCleanup(ToolBase):
    def run(self):
        super(ResponseDataCleanup, self).run()
        print(f'will evaluate responses for {self.args.survey_code} survey')

        with self.get_session() as session, ValidationErrorHandler(
            session=session,
            project_id=self.gcp_env.project,
            invalidation_reason=self.args.reason
        ) as error_handler:
            validator = self.get_response_validator(session)
            participant_ids = ParticipantSummaryDao.get_all_consented_participant_ids(session)

            count = 0
            for id_chunk in list_chunks(participant_ids, chunk_size=1000):
                print(f'evaluated {count} of {len(participant_ids)}')
                count += 1000

                responses = QuestionnaireResponseDao.get_responses_to_surveys(
                    survey_codes=[self.args.survey_code],
                    participant_ids=id_chunk,
                    session=session
                )
                for participant_id, responses in responses.items():
                    errors = validator.get_errors_in_responses(responses)
                    if errors:
                        error_handler.handle_errors(errors, participant_id)

    def get_response_validator(self, session: Session) -> ResponseValidator:
        query = (
            session.query(Survey)
            .join(Code)
            .filter(
                Survey.replacedTime.is_(None),
                Code.value == self.args.survey_code
            ).options(
                joinedload(Survey.questions).joinedload(SurveyQuestion.code),
                joinedload(Survey.questions).joinedload(SurveyQuestion.options).joinedload(SurveyQuestionOption.code)
            )
        )
        survey = query.one()

        return ResponseValidator(survey_definition=survey, session=session)


class ValidationErrorHandler:
    def __init__(self, session: Session, invalidation_reason, project_id):
        self._participant_ids_to_rebuild = []
        self._answer_ids_to_invalidate = []

        self._session = session
        self._invalidation_reason = invalidation_reason
        self._project_id = project_id

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        logging.info(f'Invalidating {len(self._answer_ids_to_invalidate)} answers...')
        self._session.query(
            QuestionnaireResponseAnswer
        ).filter(
            QuestionnaireResponseAnswer.questionnaireResponseAnswerId.in_(self._answer_ids_to_invalidate)
        ).update(
            {
                QuestionnaireResponseAnswer.ignore: True,
                QuestionnaireResponseAnswer.ignore_reason: self._invalidation_reason
            },
            syncronize_session=False
        )
        self._session.commit()

        logging.info(
            f'... answers marked as invalid. '
            f'Sending rebuild task for {len(self._participant_ids_to_rebuild)} participants...'
        )
        dispatch_participant_rebuild_tasks(
            pid_list=self._participant_ids_to_rebuild,
            project_id=self._project_id
        )
        logging.info('... rebuild task sent.')

    def handle_errors(self, validation_errors: Dict[str, List[ValidationError]], participant_id):
        logging.info(f'Will invalidate answers for participant P{participant_id}')
        self._participant_ids_to_rebuild.append(participant_id)

        for errors_list in validation_errors.values():
            for error in errors_list:
                self._answer_ids_to_invalidate.extend(error.answer_id)


def add_additional_arguments(parser: argparse.ArgumentParser):
    parser.add_argument(
        '--survey-code',
        help='Survey code to check validation for',
        required=True
    )
    parser.add_argument(
        '--reason',
        help='General description of what is being checked for and a ticket number',
        required=True
    )


def run():
    return cli_run(tool_cmd, tool_desc, ResponseDataCleanup, add_additional_arguments)
