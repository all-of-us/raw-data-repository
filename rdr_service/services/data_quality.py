from abc import ABC, abstractmethod
from datetime import datetime
import logging
from sqlalchemy import func, or_
from typing import List, Type

from rdr_service.model.deceased_report import DeceasedReport
from rdr_service.model.participant import Participant
from rdr_service.model.patient_status import PatientStatus
from rdr_service.model.questionnaire import Questionnaire, QuestionnaireQuestion
from rdr_service.model.questionnaire_response import QuestionnaireResponse, QuestionnaireResponseAnswer


class _ModelQualityChecker(ABC):

    def __init__(self, session):
        super(_ModelQualityChecker, self).__init__()
        self.session = session

    @abstractmethod
    def run_data_quality_checks(self, for_data_since: datetime = None):
        ...


class DeceasedReportQualityChecker(_ModelQualityChecker):
    def run_data_quality_checks(self, for_data_since: datetime = None):
        has_future_authored_date_expression = DeceasedReport.authored > DeceasedReport.created
        has_future_date_of_death_expression = DeceasedReport.dateOfDeath > DeceasedReport.authored
        has_authored_before_signup_expression = DeceasedReport.authored < Participant.signUpTime
        query = (
            self.session.query(
                DeceasedReport.id,
                has_future_authored_date_expression,
                has_future_date_of_death_expression,
                has_authored_before_signup_expression
            ).join(Participant)
            .filter(or_(
                has_future_authored_date_expression,
                has_future_date_of_death_expression,
                has_authored_before_signup_expression
            ))
        )
        if for_data_since is not None:
            query = query.filter(DeceasedReport.created >= for_data_since)

        for _id, has_future_authored_date, has_future_date_of_death, has_authored_before_signup in query.all():
            issue_messages = []
            if has_future_authored_date:
                issue_messages.append('was authored with a future date')
            if has_authored_before_signup:
                issue_messages.append('was authored before participant signup')
            if has_future_date_of_death:
                issue_messages.append('has an effective date after the authored date')

            logging.warning(f'Issues found with DeceasedReport {_id}: {", ".join(issue_messages)}')


class PatientStatusQualityChecker(_ModelQualityChecker):
    def run_data_quality_checks(self, for_data_since: datetime = None):
        has_future_authored_date_expression = PatientStatus.authored > PatientStatus.created
        has_authored_before_signup_expression = PatientStatus.authored < Participant.signUpTime
        query = (
            self.session.query(
                PatientStatus.id,
                has_future_authored_date_expression,
                has_authored_before_signup_expression
            ).join(Participant, Participant.participantId == PatientStatus.participantId)
            .filter(
                or_(
                    has_future_authored_date_expression,
                    has_authored_before_signup_expression
                )
            )
        )
        if for_data_since is not None:
            query = query.filter(PatientStatus.created >= for_data_since)

        for _id, has_future_authored_date, has_authored_date_before_signup in query.all():
            if has_future_authored_date:
                logging.warning(f'PatientStatus {_id} was authored with a future date')
            elif has_authored_date_before_signup:
                logging.warning(f'PatientStatus {_id} was authored before the participant signed up')


class QuestionnaireQualityChecker(_ModelQualityChecker):
    def run_data_quality_checks(self, for_data_since: datetime = None):
        query = (
            self.session.query(
                Questionnaire.questionnaireId,
                Questionnaire.version
            ).select_from(Questionnaire)
            .outerjoin(
                QuestionnaireQuestion, (
                    QuestionnaireQuestion.questionnaireId == Questionnaire.questionnaireId
                    and QuestionnaireQuestion.questionnaireVersion == Questionnaire.version
                )
            ).filter(QuestionnaireQuestion.questionnaireQuestionId.is_(None))
        )
        if for_data_since is not None:
            query = query.filter(Questionnaire.created >= for_data_since)

        for _id, version in query.all():
            logging.warning(f'Questionnaire with id {_id} and version {version} was found with no questions.')


class ResponseQualityChecker(_ModelQualityChecker):
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


class DataQualityChecker(_ModelQualityChecker):
    """Acts as a facade and runs a registered collection of data quality checks"""

    _registered_checkers: List[Type[_ModelQualityChecker]] = [
        DeceasedReportQualityChecker,
        PatientStatusQualityChecker,
        QuestionnaireQualityChecker,
        ResponseQualityChecker
    ]

    def run_data_quality_checks(self, for_data_since: datetime = None):
        for checker_class in self._registered_checkers:
            checker = checker_class(self.session)
            checker.run_data_quality_checks(for_data_since=for_data_since)
