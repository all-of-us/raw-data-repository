from abc import ABC, abstractmethod
from datetime import datetime
import logging
from sqlalchemy import func
from typing import List, Type

from rdr_service.model.participant import Participant
from rdr_service.model.questionnaire_response import QuestionnaireResponse, QuestionnaireResponseAnswer


class ModelQualityChecker(ABC):

    def __init__(self, session):
        super(ModelQualityChecker, self).__init__()
        self.session = session

    @abstractmethod
    def run_data_quality_checks(self, for_data_since: datetime = None):
        ...


class ResponseQualityChecker(ModelQualityChecker):
    def run_data_quality_checks(self, for_data_since: datetime = None):
        query = (
            self.session.query(
                QuestionnaireResponse.questionnaireResponseId,
                QuestionnaireResponse.created,
                QuestionnaireResponse.authored,
                Participant.signUpTime,
                func.count(QuestionnaireResponseAnswer.questionnaireResponseAnswerId)
            )
            .join(Participant)
            .outerjoin(QuestionnaireResponseAnswer)
            .group_by(QuestionnaireResponse.questionnaireResponseId)
        )
        if for_data_since is not None:
            query = query.filter(QuestionnaireResponse.created >= for_data_since)

        for response_id, created_time, authored_time, participant_signup_time, answer_count in query.all():
            if authored_time is not None and authored_time > created_time:
                logging.warning(
                    f'Response {response_id} authored with future date of {authored_time} (received at {created_time})'
                )
            elif authored_time is not None and authored_time < participant_signup_time:
                logging.warning(
                    f'Response {response_id} authored at {authored_time} '
                    f'but participant signed up at {participant_signup_time}'
                )
            elif answer_count == 0:
                logging.warning(f'Response {response_id} has no answers')


class DataQualityChecker(ModelQualityChecker):
    """Acts as a facade and runs a registered collection of data quality checks"""

    _registered_checkers: List[Type[ModelQualityChecker]] = [
        ResponseQualityChecker
    ]

    def run_data_quality_checks(self, for_data_since: datetime = None):
        for checker_class in self._registered_checkers:
            checker = checker_class(self.session)
            checker.run_data_quality_checks(for_data_since=for_data_since)
