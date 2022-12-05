from dataclasses import dataclass
from datetime import datetime
from typing import List

from rdr_service.participant_enums import QuestionnaireStatus


@dataclass
class EtmQuestionnaire:
    created: datetime
    modified: datetime
    questionnaire_type: str
    semantic_version: str
    name: str
    title: str
    resource_json: dict

    id: int = None
    version: int = None


@dataclass
class EtmResponse:
    created: datetime
    modified: datetime
    authored: datetime
    questionnaire_type: str
    status: QuestionnaireStatus
    participant_id: int
    resource_json: dict

    id: int = None
    metadata_list: List['EtmResponseExtension'] = None
    outcome_list: List['EtmResponseExtension'] = None
    answer_list: List['EtmResponseAnswer'] = None
    version: int = None  # TODO: needs to be set to the version of the questionnaire that was checked against


@dataclass
class EtmResponseExtension:
    key: str

    id: int = None
    value_string: str = None
    value_int: int = None
    value_decimal: float = None


@dataclass
class EtmResponseAnswer:
    link_id: str
    answer: str

    id: int = None
