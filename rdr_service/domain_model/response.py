from dataclasses import dataclass, field
from datetime import datetime
from typing import Dict, List


class ParticipantResponses:
    def __init__(self):
        self.responses: Dict[int, 'Response'] = {}
        self._responses_in_order = None

    @property
    def in_authored_order(self) -> List['Response']:
        if not self._responses_in_order:
            self._responses_in_order = sorted(
                self.responses.values(),
                key=lambda response: response.authored_datetime or False
            )

        return self._responses_in_order


@dataclass
class Response:
    id: int
    survey_code: str
    authored_datetime: datetime
    answered_codes: Dict[str, str] = field(default_factory=dict)

    def has_answer_for(self, question_code_str):
        return question_code_str in self.answered_codes
