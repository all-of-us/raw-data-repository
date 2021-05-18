from datetime import datetime, timedelta
import logging
from sqlalchemy import and_, func, update
from sqlalchemy.orm import aliased
from typing import Type

from rdr_service.dao.database_factory import get_database
from rdr_service.model.questionnaire_response import QuestionnaireResponse


class ResponseDuplicationDetector:
    def __init__(self, duplication_threshold: int = 10):
        """
        Used to check the database for any new questionnaire response duplicates.

        :param duplication_threshold: The number of matching responses needed in a group before any of them will be
            considered duplicates. Any responses that have already been marked as duplicates count toward this total.
            Defaults to 10.
        """
        self.duplication_threshold = duplication_threshold

    @classmethod
    def _responses_are_duplicates(cls, newer_response: Type[QuestionnaireResponse],
                                  older_response: Type[QuestionnaireResponse]):
        return and_(
            newer_response.created > older_response.created,
            newer_response.externalId == older_response.externalId,
            newer_response.answerHash == older_response.answerHash,
            newer_response.participantId == older_response.participantId
        )

    def _get_duplicate_responses(self, session, earliest_response_date):
        older_duplicate = aliased(QuestionnaireResponse)  # joined as older responses to be updated as duplicates
        newer_duplicate = aliased(QuestionnaireResponse)  # used to keep isDuplicate = 0 on the latest response
        other_duplicate = aliased(QuestionnaireResponse)  # used to find the number of other duplicates there are
        return (
            session.query(
                QuestionnaireResponse.questionnaireResponseId,  # The latest one
                func.group_concat(older_duplicate.questionnaireResponseId.distinct()),  # Responses to mark as dups
                func.count(other_duplicate.questionnaireResponseId.distinct())  # Total number of duplicates
            ).join(
                older_duplicate,
                and_(
                    self._responses_are_duplicates(QuestionnaireResponse, older_response=older_duplicate),
                    older_duplicate.isDuplicate.is_(False)
                )
            ).join(
                other_duplicate,
                self._responses_are_duplicates(QuestionnaireResponse, older_response=other_duplicate)
            ).outerjoin(
                newer_duplicate,
                self._responses_are_duplicates(newer_duplicate, older_response=QuestionnaireResponse)
            ).filter(
                # We should use the newest duplicate, and mark the older ones with isDuplicate
                newer_duplicate.questionnaireResponseId.is_(None),
                QuestionnaireResponse.created >= earliest_response_date
            )
            .group_by(QuestionnaireResponse.questionnaireResponseId)
        ).all()

    def flag_duplicate_responses(self, num_days_ago=2):
        earliest_response_date = datetime.now() - timedelta(days=num_days_ago)

        with get_database().session() as session:
            duplicated_response_data = self._get_duplicate_responses(session, earliest_response_date)

            questionnaire_ids_to_mark_as_duplicates = []
            for latest_duplicate_response_id, previous_duplicate_ids_str, duplication_count in duplicated_response_data:
                duplicates_needed = self.duplication_threshold - 1
                if duplication_count >= duplicates_needed:  # duplicate_count doesn't count the latest response
                    previous_duplicate_ids = previous_duplicate_ids_str.split(',')
                    logging.warning(f'{previous_duplicate_ids} found as duplicates of {latest_duplicate_response_id}')

                    questionnaire_ids_to_mark_as_duplicates.extend(previous_duplicate_ids)

            if questionnaire_ids_to_mark_as_duplicates:
                session.execute(
                    update(QuestionnaireResponse)
                    .where(QuestionnaireResponse.questionnaireResponseId.in_(questionnaire_ids_to_mark_as_duplicates))
                    .values({
                        QuestionnaireResponse.isDuplicate: True
                    })
                )
