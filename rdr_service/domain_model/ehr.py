from dataclasses import dataclass
from datetime import datetime


@dataclass
class ParticipantEhrFile:
    participant_id: int
    receipt_time: datetime
    hpo_id: int
