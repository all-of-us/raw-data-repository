from datetime import datetime

from rdr_service.resource.helpers import DateCollection
from tests.helpers.unittest_base import BaseTestCase


class DateCollectionTest(BaseTestCase):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.uses_database = False

    @staticmethod
    def _build_simple_intersection(start_of_first, end_of_first, start_of_second, end_of_second):
        first = DateCollection()
        first.add_start(start_of_first)
        if end_of_first is not None:
            first.add_stop(end_of_first)

        second = DateCollection()
        second.add_start(start_of_second)
        if end_of_second is not None:
            second.add_stop(end_of_second)

        return first.get_intersection(second), start_of_first, end_of_first, start_of_second, end_of_second

    def test_simple_collection(self):
        # Test first starts before and ends in second
        intersection, _, end_of_first, start_of_second, _ =\
            self._build_simple_intersection(start_of_first=datetime(2020, 6, 24),
                                            end_of_first=datetime(2020, 7, 4),
                                            start_of_second=datetime(2020, 7, 2),
                                            end_of_second=datetime(2020, 7, 10))

        self.assertEqual(1, len(intersection.ranges))
        date_range = intersection.ranges[0]
        self.assertEqual(start_of_second, date_range.start)
        self.assertEqual(end_of_first, date_range.end)

        # Test second starts before and ends in first
        intersection, start_of_first, _, _, end_of_second =\
            self._build_simple_intersection(start_of_first=datetime(2020, 6, 24),
                                            end_of_first=datetime(2020, 8, 4),
                                            start_of_second=datetime(2019, 11, 2),
                                            end_of_second=datetime(2020, 7, 10))

        self.assertEqual(1, len(intersection.ranges))
        date_range = intersection.ranges[0]
        self.assertEqual(start_of_first, date_range.start)
        self.assertEqual(end_of_second, date_range.end)

        # Test second starts and ends after first
        intersection, _, _, _, _ =\
            self._build_simple_intersection(start_of_first=datetime(2020, 6, 24),
                                            end_of_first=datetime(2020, 8, 4),
                                            start_of_second=datetime(2020, 10, 2),
                                            end_of_second=datetime(2020, 11, 10))

        self.assertFalse(intersection.any())

        # Test first starts and ends after second
        intersection, _, _, _, _ =\
            self._build_simple_intersection(start_of_first=datetime(2020, 6, 24),
                                            end_of_first=datetime(2020, 8, 4),
                                            start_of_second=datetime(2019, 10, 2),
                                            end_of_second=datetime(2019, 11, 10))

        self.assertFalse(intersection.any())

        # Test first in second
        intersection, start_of_first, end_of_first, _, _ =\
            self._build_simple_intersection(start_of_first=datetime(2020, 6, 24),
                                            end_of_first=datetime(2020, 8, 4),
                                            start_of_second=datetime(2019, 11, 2),
                                            end_of_second=datetime(2021, 7, 10))

        self.assertEqual(1, len(intersection.ranges))
        date_range = intersection.ranges[0]
        self.assertEqual(start_of_first, date_range.start)
        self.assertEqual(end_of_first, date_range.end)

        # Test second in first
        intersection, _, _, start_of_second, end_of_second =\
            self._build_simple_intersection(start_of_first=datetime(2018, 6, 24),
                                            end_of_first=datetime(2024, 8, 4),
                                            start_of_second=datetime(2019, 11, 2),
                                            end_of_second=datetime(2021, 7, 10))

        self.assertEqual(1, len(intersection.ranges))
        date_range = intersection.ranges[0]
        self.assertEqual(start_of_second, date_range.start)
        self.assertEqual(end_of_second, date_range.end)

        # Test first starts with second and ends first
        intersection, start_of_first, end_of_first, _, _ =\
            self._build_simple_intersection(start_of_first=datetime(2020, 6, 24),
                                            end_of_first=datetime(2020, 8, 4),
                                            start_of_second=datetime(2020, 6, 24),
                                            end_of_second=datetime(2020, 9, 10))

        self.assertEqual(1, len(intersection.ranges))
        date_range = intersection.ranges[0]
        self.assertEqual(start_of_first, date_range.start)
        self.assertEqual(end_of_first, date_range.end)

        # Test first starts with second and ends after
        intersection, start_of_first, _, _, end_of_second =\
            self._build_simple_intersection(start_of_first=datetime(2020, 6, 24),
                                            end_of_first=datetime(2020, 8, 4),
                                            start_of_second=datetime(2020, 6, 24),
                                            end_of_second=datetime(2020, 7, 10))

        self.assertEqual(1, len(intersection.ranges))
        date_range = intersection.ranges[0]
        self.assertEqual(start_of_first, date_range.start)
        self.assertEqual(end_of_second, date_range.end)

        # Test first starts before and ends with second
        intersection, _, _, start_of_second, end_of_second =\
            self._build_simple_intersection(start_of_first=datetime(2020, 6, 24),
                                            end_of_first=datetime(2020, 8, 4),
                                            start_of_second=datetime(2020, 7, 24),
                                            end_of_second=datetime(2020, 8, 4))

        self.assertEqual(1, len(intersection.ranges))
        date_range = intersection.ranges[0]
        self.assertEqual(start_of_second, date_range.start)
        self.assertEqual(end_of_second, date_range.end)

        # Test first starts after and ends with second
        intersection, start_of_first, _, _, end_of_second =\
            self._build_simple_intersection(start_of_first=datetime(2020, 7, 29),
                                            end_of_first=datetime(2020, 8, 4),
                                            start_of_second=datetime(2020, 7, 24),
                                            end_of_second=datetime(2020, 8, 4))

        self.assertEqual(1, len(intersection.ranges))
        date_range = intersection.ranges[0]
        self.assertEqual(start_of_first, date_range.start)
        self.assertEqual(end_of_second, date_range.end)

        # Test first ends at start of second
        intersection, _, _, _, _ =\
            self._build_simple_intersection(start_of_first=datetime(2020, 7, 29),
                                            end_of_first=datetime(2020, 8, 4),
                                            start_of_second=datetime(2020, 8, 4),
                                            end_of_second=datetime(2020, 8, 14))

        self.assertFalse(intersection.any())

        # Test second ends at start of first
        intersection, _, _, _, _ =\
            self._build_simple_intersection(start_of_first=datetime(2020, 8, 14),
                                            end_of_first=datetime(2021, 8, 4),
                                            start_of_second=datetime(2020, 8, 4),
                                            end_of_second=datetime(2020, 8, 14))

        self.assertFalse(intersection.any())

    def test_ongoing_ranges(self):
        # Test first starts in second but doesn't end
        intersection, start_of_first, _, _, end_of_second =\
            self._build_simple_intersection(start_of_first=datetime(2020, 7, 29),
                                            end_of_first=None,
                                            start_of_second=datetime(2020, 7, 24),
                                            end_of_second=datetime(2020, 8, 4))

        self.assertEqual(1, len(intersection.ranges))
        date_range = intersection.ranges[0]
        self.assertEqual(start_of_first, date_range.start)
        self.assertEqual(end_of_second, date_range.end)

        # Test first starts before second but doesn't end
        intersection, _, _, start_of_second, end_of_second =\
            self._build_simple_intersection(start_of_first=datetime(2020, 7, 29),
                                            end_of_first=None,
                                            start_of_second=datetime(2020, 8, 24),
                                            end_of_second=datetime(2020, 8, 4))

        self.assertEqual(1, len(intersection.ranges))
        date_range = intersection.ranges[0]
        self.assertEqual(start_of_second, date_range.start)
        self.assertEqual(end_of_second, date_range.end)

        # Test second starts in first but doesn't end
        intersection, _, end_of_first, start_of_second, _ =\
            self._build_simple_intersection(start_of_first=datetime(2020, 6, 29),
                                            end_of_first=datetime(2020, 8, 24),
                                            start_of_second=datetime(2020, 7, 24),
                                            end_of_second=None)

        self.assertEqual(1, len(intersection.ranges))
        date_range = intersection.ranges[0]
        self.assertEqual(start_of_second, date_range.start)
        self.assertEqual(end_of_first, date_range.end)

        # Test second starts before first but doesn't end
        intersection, start_of_first, end_of_first, _, _ =\
            self._build_simple_intersection(start_of_first=datetime(2020, 7, 29),
                                            end_of_first=datetime(2020, 8, 24),
                                            start_of_second=datetime(2019, 8, 24),
                                            end_of_second=None)

        self.assertEqual(1, len(intersection.ranges))
        date_range = intersection.ranges[0]
        self.assertEqual(start_of_first, date_range.start)
        self.assertEqual(end_of_first, date_range.end)

        # Test second starts after first and doesn't end
        intersection, _, _, _, _ =\
            self._build_simple_intersection(start_of_first=datetime(2020, 7, 29),
                                            end_of_first=datetime(2020, 8, 24),
                                            start_of_second=datetime(2020, 9, 24),
                                            end_of_second=None)
        self.assertFalse(intersection.any())

        # Test first starts after second and doesn't end
        intersection, _, _, _, _ =\
            self._build_simple_intersection(start_of_first=datetime(2020, 7, 29),
                                            end_of_first=None,
                                            start_of_second=datetime(2020, 3, 24),
                                            end_of_second=datetime(2020, 4, 24))
        self.assertFalse(intersection.any())

        # Test first starts with the end of second and doesn't end
        intersection, _, _, _, _ =\
            self._build_simple_intersection(start_of_first=datetime(2020, 4, 24),
                                            end_of_first=None,
                                            start_of_second=datetime(2020, 3, 24),
                                            end_of_second=datetime(2020, 4, 24))
        self.assertFalse(intersection.any())

        # Test second starts with the end of first and doesn't end
        intersection, _, _, _, _ =\
            self._build_simple_intersection(start_of_first=datetime(2019, 4, 24),
                                            end_of_first=datetime(2020, 3, 24),
                                            start_of_second=datetime(2020, 3, 24),
                                            end_of_second=None)
        self.assertFalse(intersection.any())

        # Test second starts first and neither end
        intersection, start_of_first, _, _, _ =\
            self._build_simple_intersection(start_of_first=datetime(2020, 4, 24),
                                            end_of_first=None,
                                            start_of_second=datetime(2020, 3, 24),
                                            end_of_second=None)
        self.assertEqual(1, len(intersection.ranges))
        date_range = intersection.ranges[0]
        self.assertEqual(start_of_first, date_range.start)
        self.assertIsNone(date_range.end)

        # Test first starts first and neither end
        intersection, _, _, start_of_second, _ =\
            self._build_simple_intersection(start_of_first=datetime(2019, 4, 24),
                                            end_of_first=None,
                                            start_of_second=datetime(2020, 3, 24),
                                            end_of_second=None)
        self.assertEqual(1, len(intersection.ranges))
        date_range = intersection.ranges[0]
        self.assertEqual(start_of_second, date_range.start)
        self.assertIsNone(date_range.end)

    def test_complex_intersections(self):
        first = DateCollection()
        first.add_start(datetime(2020, 3, 1))
        first.add_stop(datetime(2020, 3, 10))
        first.add_start(datetime(2020, 3, 25))
        first.add_stop(datetime(2020, 4, 21))
        first.add_start(datetime(2020, 4, 22))
        first.add_stop(datetime(2020, 5, 3))
        first.add_start(datetime(2020, 6, 22))

        second = DateCollection()
        second.add_start(datetime(2020, 3, 15))
        second.add_stop(datetime(2020, 3, 25))
        second.add_start(datetime(2020, 3, 29))
        second.add_stop(datetime(2020, 4, 7))
        second.add_start(datetime(2020, 4, 12))
        second.add_stop(datetime(2020, 4, 16))
        second.add_start(datetime(2020, 4, 18))
        second.add_stop(datetime(2020, 5, 10))
        second.add_start(datetime(2020, 5, 18))
        second.add_stop(datetime(2020, 6, 10))
        second.add_start(datetime(2020, 7, 10))
        second.add_stop(datetime(2020, 7, 15))
        second.add_start(datetime(2020, 7, 20))

        # first ranges:
        #   3/1__3/10       3/25_______________________________4/21  4/22__5/3                6/22______...
        # second ranges:
        #             3/15__3/25  3/29__4/7  4/12__4/16  4/18_________________5/10  5/18__6/10    7/10__7/15  7/20___...
        # expected result:
        #                         3/29__4/7  4/12__4/16  4/18__4/21  4/22__5/3                    7/10__7/15  7/20___...

        intersection = first.get_intersection(second)
        self.assertEqual(6, len(intersection.ranges))
        date_range = intersection.ranges[0]
        self.assertEqual(datetime(2020, 3, 29), date_range.start)
        self.assertEqual(datetime(2020, 4, 7), date_range.end)
        date_range = intersection.ranges[1]
        self.assertEqual(datetime(2020, 4, 12), date_range.start)
        self.assertEqual(datetime(2020, 4, 16), date_range.end)
        date_range = intersection.ranges[2]
        self.assertEqual(datetime(2020, 4, 18), date_range.start)
        self.assertEqual(datetime(2020, 4, 21), date_range.end)
        date_range = intersection.ranges[3]
        self.assertEqual(datetime(2020, 4, 22), date_range.start)
        self.assertEqual(datetime(2020, 5, 3), date_range.end)
        date_range = intersection.ranges[4]
        self.assertEqual(datetime(2020, 7, 10), date_range.start)
        self.assertEqual(datetime(2020, 7, 15), date_range.end)
        date_range = intersection.ranges[5]
        self.assertEqual(datetime(2020, 7, 20), date_range.start)
        self.assertIsNone(date_range.end)

    def test_two_starts_and_an_end(self):
        # Ensure that the utility can handle duplicate starts and a single end to them both.
        # This scenario could come up if we get two consent questionnaires for some reason,
        # and then only one NO response. In that case, the NO response should deactivate any previous
        # YESs we're tracking
        first = DateCollection()
        first.add_start(datetime(2020, 4, 10))
        first.add_start(datetime(2020, 5, 17))
        first.add_stop(datetime(2020, 8, 1))

        second = DateCollection()
        second.add_start(datetime(2020, 3, 8))
        second.add_stop(datetime(2020, 4, 18))
        second.add_start(datetime(2020, 4, 22))
        second.add_stop(datetime(2020, 5, 20))
        second.add_start(datetime(2020, 7, 22))

        # first range:         4/10_______________________________8/1
        # second ranges:  3/8________4/18    4/22__5/20     7/22_______...
        # expected result:     4/10__4/18    4/22__5/20     7/22__8/1

        intersection = first.get_intersection(second)
        self.assertEqual(3, len(intersection.ranges))
        date_range = intersection.ranges[0]
        self.assertEqual(datetime(2020, 4, 10), date_range.start)
        self.assertEqual(datetime(2020, 4, 18), date_range.end)
        date_range = intersection.ranges[1]
        self.assertEqual(datetime(2020, 4, 22), date_range.start)
        self.assertEqual(datetime(2020, 5, 20), date_range.end)
        date_range = intersection.ranges[2]
        self.assertEqual(datetime(2020, 7, 22), date_range.start)
        self.assertEqual(datetime(2020, 8, 1), date_range.end)
