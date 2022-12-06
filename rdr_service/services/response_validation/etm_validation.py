from dataclasses import dataclass, field
from typing import List

from rdr_service.domain_model import etm as model


@dataclass
class ValidationResult:
    success: bool = True
    errors: List[str] = field(default_factory=list)


class EtmValidation:
    @classmethod
    def validate_response(cls, response: model.EtmResponse, questionnaire: model.EtmQuestionnaire) -> ValidationResult:
        result = ValidationResult()

        for required_metadata_name in questionnaire.metadata_name_list:
            if not any(metadata_obj.key == required_metadata_name for metadata_obj in response.metadata_list):
                result.success = False
                result.errors.append(f'Missing "{required_metadata_name}" metadata field')

        for required_outcome_name in questionnaire.outcome_name_list:
            if not any(outcome_obj.key == required_outcome_name for outcome_obj in response.outcome_list):
                result.success = False
                result.errors.append(f'Missing "{required_outcome_name}" outcome field')

        for required_question in [question for question in questionnaire.question_list if question.required]:
            if not any(answer.link_id == required_question.link_id for answer in response.answer_list):
                result.success = False
                result.errors.append(f'Missing answer for question "{required_question.link_id}"')

        return result
