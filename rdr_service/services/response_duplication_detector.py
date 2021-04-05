import logging
from sqlalchemy import and_, func, update
from sqlalchemy.orm import aliased
from typing import Type

from rdr_service.dao.database_factory import get_database
from rdr_service.model.questionnaire_response import QuestionnaireResponse


class ResponseDuplicationDetector:
    @classmethod
    def _join_on_duplication(cls, newer_response: Type[QuestionnaireResponse],
                             older_response: Type[QuestionnaireResponse]):
        return and_(
            newer_response.created > older_response.created,
            newer_response.externalId == older_response.externalId,
            newer_response.answerHash == older_response.answerHash
        )

    @classmethod
    def flag_duplicate_responses(cls):
        with get_database().session() as session:
            older_duplicate = aliased(QuestionnaireResponse)
            newer_duplicate = aliased(QuestionnaireResponse)
            duplicated_response_ids = (
                session.query(
                    QuestionnaireResponse.questionnaireResponseId,
                    func.group_concat(older_duplicate.questionnaireResponseId)
                ).join(
                    older_duplicate,
                    and_(
                        cls._join_on_duplication(QuestionnaireResponse, older_response=older_duplicate),
                        older_duplicate.isDuplicate.is_(False)
                    )
                ).outerjoin(
                    newer_duplicate,
                    cls._join_on_duplication(newer_duplicate, older_response=QuestionnaireResponse)
                ).filter(
                    # We should use the newest duplicate, and mark the older ones with isDuplicate
                    newer_duplicate.questionnaireResponseId.is_(None)
                ).group_by(QuestionnaireResponse.questionnaireResponseId)
            ).all()

            questionnaire_ids_to_mark_as_duplicates = []
            for latest_duplicate_response_id, previous_duplicate_ids_str in duplicated_response_ids:
                previous_duplicate_ids = previous_duplicate_ids_str.split(',')
                logging.warning(f'{previous_duplicate_ids} found as duplicates of {latest_duplicate_response_id}')

                questionnaire_ids_to_mark_as_duplicates.extend(previous_duplicate_ids)

            session.execute(
                update(QuestionnaireResponse)
                .where(QuestionnaireResponse.questionnaireResponseId.in_(questionnaire_ids_to_mark_as_duplicates))
                .values({
                    QuestionnaireResponse.isDuplicate: True
                })
            )
            # TODO: index the externalId and answerHash column
