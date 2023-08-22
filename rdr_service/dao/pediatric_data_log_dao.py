from contextlib import ExitStack
from typing import Optional

from sqlalchemy.orm import Session

from rdr_service.dao.base_dao import with_session
from rdr_service.model.pediatric_data_log import PediatricDataLog, PediatricDataType


class PediatricDataLogDao:
    @classmethod
    @with_session
    def get_latest(
        cls, participant_id: int, data_type: PediatricDataType, session: Optional[Session] = None, lock_for_update=False
    ) -> Optional[PediatricDataLog]:
        query = session.query(PediatricDataLog).filter(
            PediatricDataLog.participant_id == participant_id,
            PediatricDataLog.data_type == data_type,
            PediatricDataLog.replaced_by_id.is_(None)
        )
        if lock_for_update:
            query = query.with_for_update()

        return query.one_or_none()

    @classmethod
    @with_session
    def insert(cls, data: PediatricDataLog, session: Optional[Session] = None):
        latest_data = cls.get_latest(
            participant_id=data.participant_id,
            data_type=data.data_type,
            session=session,
            lock_for_update=True
        )
        if latest_data:
            latest_data.replaced_by = data

        session.add(data)

