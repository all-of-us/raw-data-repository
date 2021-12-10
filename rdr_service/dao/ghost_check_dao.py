from dataclasses import dataclass
from datetime import datetime
from typing import List

from sqlalchemy.orm import Session

from rdr_service.dao.base_dao import BaseDao


@dataclass
class LatestGhostCheck:
    participant_id: int
    is_ghost: bool
    timestamp: datetime


class GhostCheckDao(BaseDao):
    @classmethod
    def get_participants_needing_checked(cls, session: Session, start_date: datetime) -> List[LatestGhostCheck]:
        """
        Loads the participants that should be checked for ghost status
        (i.e. to see if they exist in Vibrent's system).
        All participants that have not yet been checked, and all participants that haven't been checked
        since the provided start_date, will be returned.
        """
        ...

    def get_id(self, obj):
        ...

    def from_client_json(self):
        ...
