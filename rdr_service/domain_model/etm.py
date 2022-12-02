from dataclasses import dataclass
from datetime import datetime


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
