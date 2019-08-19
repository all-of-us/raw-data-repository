import datetime

from rdr_service.dao.calendar_dao import CalendarDao, INTERVAL_DAY, INTERVAL_MONTH, INTERVAL_QUARTER, INTERVAL_WEEK
from rdr_service.model.calendar import Calendar
from rdr_service.test.unit_test.unit_test_util import SqlTestBase


# TODO: represent in new test suite
class MetricsEhrApiIntervalTest(SqlTestBase):
    def setUp(self, with_data=False, use_mysql=True):
        super(MetricsEhrApiIntervalTest, self).setUp(with_data=with_data, use_mysql=use_mysql)
        self.calendar_dao = CalendarDao()

    def _iter_dates_in_range(self, start, end):
        current = start
        while current <= end:
            yield current
            current += datetime.timedelta(days=1)

    def _fill_calendar_range(self, start, end):
        for date in self._iter_dates_in_range(start, end):
            self.calendar_dao.insert(Calendar(day=date))

    def test_interval_day(self):
        self._fill_calendar_range(datetime.date(2017, 12, 1), datetime.date(2018, 2, 1))

        query = self.calendar_dao.get_interval_query(
            start=datetime.datetime(2018, 1, 1),
            end=datetime.datetime(2018, 1, 4),
            interval_key=INTERVAL_DAY,
            include_end_date=True,
        )

        with self.calendar_dao.session() as session:
            cursor = session.execute(query)
        results = [dict(list(zip(list(cursor.keys()), row))) for row in cursor]

        self.assertEqual(
            results,
            [
                {"start_date": datetime.date(2018, 1, 1), "end_date": datetime.date(2018, 1, 2)},
                {"start_date": datetime.date(2018, 1, 2), "end_date": datetime.date(2018, 1, 3)},
                {"start_date": datetime.date(2018, 1, 3), "end_date": datetime.date(2018, 1, 4)},
                {"start_date": datetime.date(2018, 1, 4), "end_date": datetime.date(2018, 1, 5)},
            ],
        )

    def test_interval_week(self):
        self._fill_calendar_range(datetime.date(2017, 12, 1), datetime.date(2018, 3, 1))

        query = self.calendar_dao.get_interval_query(
            start=datetime.datetime(2018, 1, 1),
            end=datetime.datetime(2018, 2, 1),
            interval_key=INTERVAL_WEEK,
            include_end_date=True,
        )

        with self.calendar_dao.session() as session:
            cursor = session.execute(query)
        results = [dict(list(zip(list(cursor.keys()), row))) for row in cursor]

        self.assertEqual(
            results,
            [
                {"start_date": datetime.date(2017, 12, 31), "end_date": datetime.date(2018, 1, 7)},
                {"start_date": datetime.date(2018, 1, 7), "end_date": datetime.date(2018, 1, 14)},
                {"start_date": datetime.date(2018, 1, 14), "end_date": datetime.date(2018, 1, 21)},
                {"start_date": datetime.date(2018, 1, 21), "end_date": datetime.date(2018, 1, 28)},
                {"start_date": datetime.date(2018, 1, 28), "end_date": datetime.date(2018, 2, 4)},
            ],
        )

    def test_interval_month(self):
        self._fill_calendar_range(datetime.date(2017, 11, 1), datetime.date(2018, 8, 1))

        query = self.calendar_dao.get_interval_query(
            start=datetime.datetime(2018, 1, 1),
            end=datetime.datetime(2018, 6, 1),
            interval_key=INTERVAL_MONTH,
            include_end_date=True,
        )

        with self.calendar_dao.session() as session:
            cursor = session.execute(query)
        results = [dict(list(zip(list(cursor.keys()), row))) for row in cursor]

        self.assertEqual(
            results,
            [
                {"start_date": datetime.date(2018, 1, 1), "end_date": datetime.date(2018, 2, 1)},
                {"start_date": datetime.date(2018, 2, 1), "end_date": datetime.date(2018, 3, 1)},
                {"start_date": datetime.date(2018, 3, 1), "end_date": datetime.date(2018, 4, 1)},
                {"start_date": datetime.date(2018, 4, 1), "end_date": datetime.date(2018, 5, 1)},
                {"start_date": datetime.date(2018, 5, 1), "end_date": datetime.date(2018, 6, 1)},
                {"start_date": datetime.date(2018, 6, 1), "end_date": datetime.date(2018, 7, 1)},
            ],
        )

    def test_interval_quarter(self):
        self._fill_calendar_range(datetime.date(2017, 0o1, 1), datetime.date(2019, 0o1, 1))

        query = self.calendar_dao.get_interval_query(
            start=datetime.datetime(2018, 1, 1),
            end=datetime.datetime(2018, 12, 31),
            interval_key=INTERVAL_QUARTER,
            include_end_date=True,
        )

        with self.calendar_dao.session() as session:
            cursor = session.execute(query)
        results = [dict(list(zip(list(cursor.keys()), row))) for row in cursor]

        self.assertEqual(
            results,
            [
                {"start_date": datetime.date(2018, 1, 1), "end_date": datetime.date(2018, 4, 1)},
                {"start_date": datetime.date(2018, 4, 1), "end_date": datetime.date(2018, 7, 1)},
                {"start_date": datetime.date(2018, 7, 1), "end_date": datetime.date(2018, 10, 1)},
                {"start_date": datetime.date(2018, 10, 1), "end_date": datetime.date(2019, 1, 1)},
            ],
        )
