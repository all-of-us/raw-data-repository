
from rdr_service.dao import database_factory
from rdr_service.dao.database_utils import NamedLock
from tests.helpers.unittest_base import BaseTestCase


class DatabaseUtilsTest(BaseTestCase):
    def test_named_locks(self):
        database = database_factory.get_database()
        with database.session() as first_connection, database.session() as another_connection:

            # Should be able to take a named lock
            with NamedLock(name='first_lock', session=first_connection) as first_lock:
                self.assertTrue(first_lock.is_locked)

                # Should be able to take another lock with a different name
                with NamedLock(name='another_lock', session=first_connection) as another_lock:
                    self.assertTrue(another_lock.is_locked)

                # Should not be able to take a lock on something locked by another connection
                with self.assertRaises(IOError):
                    exception = IOError('error getting lock')
                    with NamedLock(
                        name='first_lock', session=another_connection, lock_failure_exception=exception,
                        lock_timeout_seconds=1
                    ):
                        ...

                # Should raise exception even if a type wasn't given
                with self.assertRaises(Exception):
                    with NamedLock(name='first_lock', session=another_connection, lock_timeout_seconds=1):
                        ...

            # Closing the context should release the lock, should be able to take the lock now
            with NamedLock(name='first_lock', session=another_connection) as lock:
                self.assertTrue(lock.is_locked)

