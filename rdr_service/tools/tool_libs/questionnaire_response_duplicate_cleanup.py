import logging
import pytz

from calendar import monthrange
from datetime import datetime, timedelta
from rdr_service.services.response_duplication_detector import ResponseDuplicationDetector
from rdr_service.tools.tool_libs.tool_base import cli_run, ToolBase

_logger = logging.getLogger("rdr_logger")

# Tool_cmd and tool_desc name are required.
# Remember to add/update bash completion in 'tool_lib/tools.bash'
tool_cmd = "qr_duplicate_fix"
tool_desc = "Tool to run the questionnaire response duplicate detection logic over a long period of time."


class ResponseDuplicateFix(ToolBase):

    def run(self):

        # Will end at year of oldest observed duplicates.
        end = datetime(2017, 1, 1, 0, 0, 0, 0, pytz.UTC)
        response_detector = ResponseDuplicationDetector()
        from_ts = datetime.now()
        while end < from_ts:
            days_in_month = monthrange(from_ts.year, from_ts.month)[1]
            response_detector.flag_duplicate_responses(days_in_month + 1, from_ts, self.args.project)
            next_month = from_ts.month + 1 if from_ts.month > 1 else 12
            from_ts = from_ts - timedelta(days=monthrange(from_ts.year, next_month)[1])

        return


def run():
    cli_run(tool_cmd, tool_desc, ResponseDuplicateFix)
