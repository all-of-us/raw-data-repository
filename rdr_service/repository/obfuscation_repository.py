from datetime import datetime
import secrets
import string
from typing import Optional

import backoff
from sqlalchemy.orm import Session

from rdr_service.clock import CLOCK
from rdr_service.dao.database_utils import NamedLock
from rdr_service.model.obfuscation import Obfuscation


class _KeyCollision(Exception):
    ...

class ObfuscationRepository:
    def get(self, id_, session: Session) -> Optional[dict]:
        """Take in an id and return the associated data if it still exists"""
        obfuscated_data = session.query(Obfuscation).filter(
            Obfuscation.id == id_
        ).one_or_none()

        if obfuscated_data:
            return obfuscated_data.data
        else:
            return None

    @backoff.on_exception(backoff.constant, _KeyCollision, max_tries=10, jitter=None)
    def store(self, data: dict, expiration: datetime, session: Session) -> str:
        """Insert new data, assigning it a randomly generated key and returning that key"""

        new_object_key = self._generate_random_key()
        with NamedLock(
            name=f'rdr.summary.key.{new_object_key}',
            session=session,
            lock_failure_exception=_KeyCollision(f'unable to get named lock for obfuscation key {new_object_key}')
        ):
            existing_object = session.query(Obfuscation).filter(
                Obfuscation.id == new_object_key
            ).one_or_none()
            if existing_object is not None:
                raise _KeyCollision(f'Data object already exists with name {new_object_key}')

            obfuscation_obj = Obfuscation(
                id=new_object_key,
                expires=expiration,
                data=data
            )
            session.add(obfuscation_obj)
            session.flush()

        return obfuscation_obj.id

    @classmethod
    def delete_expired_data(cls, session: Session):
        """Remove all expired data"""
        session.query(
            Obfuscation
        ).filter(
            Obfuscation.expires < CLOCK.now()
        ).delete()

    @classmethod
    def _generate_random_key(cls):
        return ''.join(secrets.choice(string.ascii_uppercase + string.ascii_lowercase) for i in range(24))
