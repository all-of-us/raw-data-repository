from dataclasses import dataclass, field
from datetime import datetime
from typing import List

from rdr_service.participant_enums import QuestionnaireStatus


@dataclass
class EtmQuestionnaire:
    id: int = None
    version: int = None
    created: datetime = None
    modified: datetime = None
    questionnaire_type: str = None
    semantic_version: str = None
    title: str = None
    resource_json: dict = None
    metadata_name_list: List[str] = field(default_factory=list)
    outcome_name_list: List[str] = field(default_factory=list)
    question_list: List['EtmQuestion'] = field(default_factory=list)


@dataclass
class EtmQuestion:
    link_id: str = None
    required: bool = None


@dataclass
class EtmResponse:
    id: int = None
    created: datetime = None
    modified: datetime = None
    authored: datetime = None
    questionnaire_type: str = None
    status: QuestionnaireStatus = None
    participant_id: int = None
    resource_json: dict = None
    metadata_list: List['EtmResponseExtension'] = field(default_factory=list)
    outcome_list: List['EtmResponseExtension'] = field(default_factory=list)
    answer_list: List['EtmResponseAnswer'] = None
    version: int = None


@dataclass
class EtmResponseExtension:
    key: str

    id: int = None
    value_string: str = None
    value_int: int = None
    value_decimal: float = None


@dataclass
class EtmResponseAnswer:
    id: int = None
    link_id: str = None
    answer: str = None
