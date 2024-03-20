import logging
import pytz

from calendar import monthrange
from datetime import datetime, timedelta
from rdr_service.services.response_duplication_detector import ResponseDuplicationDetector
from rdr_service.tools.tool_libs.tool_base import cli_run, ToolBase

_logger = logging.getLogger("rdr_logger")

tool_cmd = "qr_duplicate_fix"
tool_desc = "Tool to run the questionnaire response duplicate detection logic over a long period of time."


class ResponseDuplicateFix(ToolBase):

    def run(self):
        # Will end at year of oldest observed duplicates.
        end = datetime(2017, 1, 1, 0, 0, 0, 0, pytz.UTC)
        response_detector = ResponseDuplicationDetector()
        from_ts = datetime.now(tz=pytz.UTC)
        full_list = []
        k = 0

        with self.get_session() as session:
            while end < from_ts and k <= 30:
                #days_in_month = monthrange(from_ts.year, from_ts.month)[1]
                next_month = (from_ts.month - 1) if from_ts.month > 1 else 12
                full_list.extend(response_detector._get_duplicate_responses(
                    session,
                    from_ts - timedelta(days=monthrange(from_ts.year, next_month)[1] + 1)
                    )
                )
                #response_detector.flag_duplicate_responses(days_in_month + 1, from_ts, self.args.project)
                from_ts = from_ts - timedelta(days=monthrange(from_ts.year, next_month)[1])
                k += 1
            print(full_list)


def run():
    cli_run(tool_cmd, tool_desc, ResponseDuplicateFix)
