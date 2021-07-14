#
# This file is subject to the terms and conditions defined in the
# file 'LICENSE', which is part of this source code package.
#
# Misc helper functions for resources
#
import datetime
import os


class DateRange:
    def __init__(self, start, end):
        self.start = start
        self.end = end


class DateCollection:
    """
    Start and stop dates must be added in order for the intersection calculations to work.
    """
    def __init__(self):
        self.ranges = []
        self.latest_active_range = None

    @staticmethod
    def _convert_date_to_datetime(date):
        if type(date) is datetime.date:
            date = datetime.datetime(date.year, date.month, date.day)

        return date

    def add_stop(self, date_time):
        if self.latest_active_range:
            self.latest_active_range.end = self._convert_date_to_datetime(date_time)
            self.latest_active_range = None

    def add_start(self, date_time):
        if self.latest_active_range is None:
            date_time = self._convert_date_to_datetime(date_time)
            active_range = DateRange(date_time, None)
            self.latest_active_range = active_range
            self.ranges.append(active_range)

    def _add_range(self, date_range):
        self.ranges.append(date_range)

    def get_intersection(self, other_collection):
        # Ranges are in order for each of the lists
        # Each range represents an active range, so find the date ranges where both are active
        intersection = DateCollection()

        if self.ranges and other_collection.ranges:
            self_ranges = iter(self.ranges)
            other_ranges = iter(other_collection.ranges)

            active_self = next(self_ranges, None)
            active_other = next(other_ranges, None)

            # Look at self and other, if self starts and ends before other then maybe the next self overlaps other
            #  if self starts before other and ends after: add other to intersection (the next other might overlap)
            #  if self starts before and ends within other: add start of other and end of self (next self might overlap)
            #  if self starts within and ends after: add start of self and end of other (next other might overlap)
            #  if self starts after and ends after: then move to next other
            #  if self is entirely within other, then add self and see if next self overlaps

            while active_self is not None and active_other is not None:
                new_range = None
                need_next_self = need_next_other = False
                if active_self.end is None and active_other.end is None:
                    new_range = DateRange(max(active_self.start, active_other.start), active_other.end)
                    # Getting the next of either should end the loop since the ranges should be in order
                    need_next_other = True
                elif active_self.end is None:
                    if active_self.start < active_other.end:
                        new_range = DateRange(max(active_self.start, active_other.start), active_other.end)
                    need_next_other = True
                elif active_other.end is None:
                    if active_other.start < active_self.end:
                        new_range = DateRange(max(active_self.start, active_other.start), active_self.end)
                    need_next_self = True
                elif active_self.end <= active_other.start:
                    need_next_self = True
                elif active_other.end <= active_self.start:
                    need_next_other = True
                elif active_self.start <= active_other.start and\
                        active_self.end >= active_other.end:
                    new_range = DateRange(active_other.start, active_other.end)
                    need_next_other = True
                elif active_self.start <= active_other.start:
                    # Current self range starts before and ends within the other range
                    new_range = DateRange(active_other.start, active_self.end)
                    need_next_self = True
                elif active_self.start >= active_other.start and\
                        active_self.end >= active_other.end:
                    # Current self range starts within and ends after the other range
                    new_range = DateRange(active_self.start, active_other.end)
                    need_next_other = True
                else:  # Current self range starts and ends within the other
                    new_range = DateRange(active_self.start, active_self.end)
                    need_next_self = True

                if new_range is not None:
                    intersection._add_range(new_range)
                if need_next_self:
                    active_self = next(self_ranges, None)
                if need_next_other:
                    active_other = next(other_ranges, None)

        return intersection

    def any(self):
        return len(self.ranges) > 0


def _import_rural_zipcodes():
    """
    Load the file app_data/rural_zipcodes.txt and return a list of rural zipcodes.
    """
    paths = ('app_data', 'rdr_service/app_data', 'rest-api/app_data')
    codes = list()

    for path in paths:
        if os.path.exists(os.path.join(path, 'rural_zipcodes.txt')):
            with open(os.path.join(path, 'rural_zipcodes.txt')) as handle:
                # pylint: disable=unused-variable
                for count, line in enumerate(handle):
                    # If 5-digit zipcodes starting with 0 had the leading zero dropped in the source file, add it back
                    codes.append(line.split(',')[1].strip().zfill(5))
            break
    return codes


RURAL_ZIPCODES = _import_rural_zipcodes()
