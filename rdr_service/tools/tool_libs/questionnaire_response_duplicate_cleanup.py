import logging
import sys
import pytz

from calendar import monthrange
from datetime import datetime, timedelta
from rdr_service.services.response_duplication_detector import ResponseDuplicationDetector

_logger = logging.getLogger("rdr_logger")

# Tool_cmd and tool_desc name are required.
# Remember to add/update bash completion in 'tool_lib/tools.bash'
tool_cmd = "qr_duplicate_fix"
tool_desc = "Tool to run the questionnaire response duplicate detection logic over a long period of time."


def run():

    # Starting from year of oldest observed duplicates
    start = datetime(2017, 1, 31, 23, 59, 59, 999999, pytz.UTC)
    rdd = ResponseDuplicationDetector()

    while start < datetime.now(tz=pytz.UTC):
        days_in_month = monthrange(start.year, start.month)[1]
        rdd.flag_duplicate_responses(days_in_month + 1, start)
        next_month = start.month + 1 if start.month < 12 else 1
        start = start + timedelta(days=monthrange(start.year, next_month)[1])

# --- Main Program Call ---
if __name__ == "__main__":
    sys.exit(run())
