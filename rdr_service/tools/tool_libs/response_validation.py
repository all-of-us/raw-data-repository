import argparse
from datetime import datetime, timedelta

from dateutil.parser import parse

from rdr_service.clock import CLOCK
from rdr_service.dao.ppi_validation_errors_dao import PpiValidationErrorsDao
from rdr_service.offline.response_validation import ResponseValidationController
from rdr_service.tools.tool_libs.tool_base import cli_run, ToolBase

tool_cmd = 'response-validation'
tool_desc = 'Validate response answers since a given start date'


class ResponseValidationScript(ToolBase):
    logger_name = None  # Override to have the base class configure the root logger to send messages to the console

    def run(self):
        super(ResponseValidationScript, self).run()

        since_date = parse(self.args.since)
        validation_dao = PpiValidationErrorsDao()

        with validation_dao.session() as session:
            controller = ResponseValidationController(
                session=session,
                validation_errors_dao=validation_dao,
                since_date=since_date
            )
            controller.run_validation()  # Controller will output validation errors using root logger


def add_additional_arguments(parser: argparse.ArgumentParser):
    parser.add_argument(
        '--since',
        help='Survey responses received since this date string will be checked',
        required=True
    )


def run():
    cli_run(tool_cmd, tool_desc, ResponseValidationScript, add_additional_arguments)
