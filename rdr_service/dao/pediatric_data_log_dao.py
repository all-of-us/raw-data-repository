import logging
from typing import Optional

from sqlalchemy.orm import Session

from rdr_service.dao.base_dao import with_session
from rdr_service.model.pediatric_data_log import PediatricDataLog, PediatricDataType
from rdr_service.participant_enums import PediatricAgeRange


class PediatricDataLogDao:
    @classmethod
    @with_session
    def record_age_range(cls, participant_id: int, age_range_str: str, session: Session):
        if age_range_str == 'UNSET':
            # non-pediatric participants will get UNSET sent as their age range.
            # No need to record or log these as an error
            return None
        if age_range_str not in PediatricAgeRange.names():
            logging.error(f'Unrecognized age range value "{age_range_str}"')
            return None

        cls.insert(
            data=PediatricDataLog(
                participant_id=participant_id,
                data_type=PediatricDataType.AGE_RANGE,
                value=age_range_str
            ),
            session=session
        )

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
        # Skip inserting if the new record is identical to the latest
        if latest_data and latest_data.value == data.value:
            return

        session.add(data)
        if latest_data:
            latest_data.replaced_by = data

