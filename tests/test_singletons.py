import datetime
import unittest

from rdr_service import singletons
from rdr_service.clock import FakeClock

TIME_1 = datetime.datetime(2016, 1, 1)
TIME_2 = datetime.datetime(2016, 1, 2)
TIME_3 = datetime.datetime(2016, 1, 4)


# TODO: represent in new test suite
class SingletonsTest(unittest.TestCase):

    foo_count = 0

    @staticmethod
    def foo():
        SingletonsTest.foo_count += 1
        return SingletonsTest.foo_count

    def setUp(self):
        SingletonsTest.foo_count = 0
        singletons.reset_for_tests()

    def test_get_no_ttl(self):
        with FakeClock(TIME_1):
            self.assertEqual(1, singletons.get(123, SingletonsTest.foo))

        with FakeClock(TIME_2):
            self.assertEqual(1, singletons.get(123, SingletonsTest.foo))

    def test_get_ttl(self):
        with FakeClock(TIME_1):
            self.assertEqual(1, singletons.get(123, SingletonsTest.foo, 86401))

        with FakeClock(TIME_2):
            self.assertEqual(1, singletons.get(123, SingletonsTest.foo, 86401))

        with FakeClock(TIME_3):
            self.assertEqual(2, singletons.get(123, SingletonsTest.foo, 86401))
