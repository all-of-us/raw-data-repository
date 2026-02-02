"""The main API definition file for researchers-offline service endpoints."""
import logging
import traceback

from flask import Flask, got_request_exception, request
from sqlalchemy.exc import DBAPIError

from rdr_service import app_util
from rdr_service.config import GAE_PROJECT
from rdr_service.researchers_offline.participant_counts_over_time import calculate_participant_metrics
from rdr_service.services.flask import RESEARCHERS_OFFLINE_PREFIX, flask_start, flask_stop
from rdr_service.services.gcp_logging import begin_request_logging, end_request_logging,\
    flask_restful_log_exception_error
from rdr_service.workflow_management.researchers_offline.workbench_data_transfer_input_feed import \
    WorkbenchWorkspacesFeed, WorkbenchResearchersFeed


@app_util.auth_required_scheduler
def test_job():
    try:
        logging.info("Test Job Executed")
    except Exception as e:  # pylint: disable=broad-except
        logging.error(f"An error occurred: {e}\nStack trace: {traceback.format_exc()}")
        return "Error occurred", 500

    return '{"success": "true"}'

@app_util.auth_required_scheduler
def participant_counts_over_time():
    logging.info('Starting participant metrics calculation...')
    calculate_participant_metrics()
    return '{"success": "true"}'

@app_util.auth_required_scheduler
def workbench_workspaces_input_feed():
    logging.info('Starting workbench workspaces datafeed...')
    datafeed = request.get_json().get("datafeed")
    input_feed = WorkbenchWorkspacesFeed(project=GAE_PROJECT)
    input_feed.run_datafeed(datafeed)
    return '{ "success": "true" }'

@app_util.auth_required_scheduler
def workbench_researchers_input_feed():
    logging.info('Starting workbench researchers datafeed...')
    datafeed = request.get_json().get("datafeed")
    input_feed = WorkbenchResearchersFeed(project=GAE_PROJECT)
    input_feed.run_datafeed(datafeed)
    return '{ "success": "true" }'


def _build_pipeline_app():
    """Configure and return the app with non-resource pipeline-triggering endpoints."""
    researchers_offline = Flask(__name__)
    researchers_offline.config['TRAP_HTTP_EXCEPTIONS'] = True

    researchers_offline.add_url_rule(
        RESEARCHERS_OFFLINE_PREFIX + "TestJob",
        endpoint="test_job",
        view_func=test_job,
        methods=["GET", "POST"],
    )

    researchers_offline.add_url_rule(
        RESEARCHERS_OFFLINE_PREFIX + "ParticipantCountsOverTime",
        endpoint="participant_counts_over_time",
        view_func=participant_counts_over_time,
        methods=["GET"],
    )

    researchers_offline.add_url_rule(
        RESEARCHERS_OFFLINE_PREFIX + "WorkbenchWorkspacesInputFeed",
        endpoint="workbench_workspaces_input_feed",
        view_func=workbench_workspaces_input_feed,
        methods=["GET", "POST"]
    )

    researchers_offline.add_url_rule(
        RESEARCHERS_OFFLINE_PREFIX + "WorkbenchResearchersInputFeed",
        endpoint="workbench_researchers_input_feed",
        view_func=workbench_researchers_input_feed,
        methods=["GET", "POST"]
    )

    researchers_offline.add_url_rule('/_ah/start', endpoint='start', view_func=flask_start, methods=["GET"])
    researchers_offline.add_url_rule("/_ah/stop", endpoint="stop", view_func=flask_stop, methods=["GET"])

    researchers_offline.before_request(begin_request_logging)  # Must be first before_request() call.
    researchers_offline.before_request(app_util.request_logging)

    researchers_offline.after_request(app_util.add_headers)
    researchers_offline.after_request(end_request_logging)  # Must be last after_request() call.

    researchers_offline.register_error_handler(DBAPIError, app_util.handle_database_disconnect)

    got_request_exception.connect(flask_restful_log_exception_error, researchers_offline)

    return researchers_offline


app = _build_pipeline_app()
