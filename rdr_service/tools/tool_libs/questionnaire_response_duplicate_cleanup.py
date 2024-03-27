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
        super(ResponseDuplicateFix, self).run()

        # Will end at year of oldest observed duplicates.
        end = datetime(2017, 1, 1, 0, 0, 0, 0, pytz.UTC)
        response_detector = ResponseDuplicationDetector()
        from_ts = datetime.now(tz=pytz.UTC)
        all_qr_ids = []

        with self.get_session() as session:
            while end < from_ts:
                qr_ids = []
                days_in_month = monthrange(from_ts.year, from_ts.month)[1]
                next_month = (from_ts.month - 1) if from_ts.month > 1 else 12

                #get IDs ahead of resolution
                response_data = response_detector._get_duplicate_responses(
                    session,
                    from_ts - timedelta(days=monthrange(from_ts.year, next_month)[1] + 1)
                )

                for duplicate_entry in response_data:
                    if duplicate_entry[2] >= 1:
                        qr_ids.extend(duplicate_entry[1].split(','))
                all_qr_ids.extend(qr_ids)
                _logger.info(msg=f"Currently resolving {len(qr_ids)} duplicates")

                #run resolution logic
                response_detector.flag_duplicate_responses(days_in_month + 1, from_ts, self.args.project)
                from_ts = from_ts - timedelta(days=monthrange(from_ts.year, next_month)[1])
                _logger.info(msg=f"Next timestamp is: {from_ts}")
            _logger.info(f"resolved {len(all_qr_ids)} duplicates:\n{all_qr_ids}")


def run():
    cli_run(tool_cmd, tool_desc, ResponseDuplicateFix)
