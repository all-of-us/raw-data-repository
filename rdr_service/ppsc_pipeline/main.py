"""The main API definition file for ppsc-pipeline service endpoints."""
import logging
import traceback

from flask import Flask, got_request_exception
from sqlalchemy.exc import DBAPIError

from rdr_service import app_util
from rdr_service.ppsc.ppsc_data_transfer import PPSCDataTransferCore, PPSCDataTransferHealthData, PPSCDataTransferEHR, \
    PPSCDataTransferBiobank
from rdr_service.services.flask import PPSC_PIPELINE_PREFIX, flask_start, flask_stop
from rdr_service.services.gcp_logging import begin_request_logging, end_request_logging,\
    flask_restful_log_exception_error


@app_util.auth_required_scheduler
def test_job():
    try:
        logging.info("Test Job Executed")
    except Exception as e:  # pylint: disable=broad-except
        logging.error(f"An error occurred: {e}\nStack trace: {traceback.format_exc()}")
        return "Error occurred", 500

    return '{"success": "true"}'


@app_util.auth_required_scheduler
def ppsc_data_transfer_core():
    with PPSCDataTransferCore() as core_transfer:
        core_transfer.run_data_transfer()
    return '{ "success": "true" }'


@app_util.auth_required_scheduler
def ppsc_data_transfer_ehr():
    with PPSCDataTransferEHR() as ehr_transfer:
        ehr_transfer.run_data_transfer()
    return '{ "success": "true" }'


@app_util.auth_required_scheduler
def ppsc_data_transfer_health_data():
    with PPSCDataTransferHealthData() as health_transfer:
        health_transfer.run_data_transfer()
    return '{ "success": "true" }'


@app_util.auth_required_scheduler
def ppsc_data_transfer_biobank_sample():
    with PPSCDataTransferBiobank() as biobank_transfer:
        biobank_transfer.run_data_transfer()
    return '{ "success": "true" }'


def _build_pipeline_app():
    """Configure and return the app with non-resource pipeline-triggering endpoints."""
    ppsc_pipeline = Flask(__name__)
    ppsc_pipeline.config['TRAP_HTTP_EXCEPTIONS'] = True

    ppsc_pipeline.add_url_rule(
        PPSC_PIPELINE_PREFIX + "TestJob",
        endpoint="test_job",
        view_func=test_job,
        methods=["GET"],
    )

    # Cloud Scheduler - Scheduler jobs
    ppsc_pipeline.add_url_rule(
        PPSC_PIPELINE_PREFIX + "TransferCore",
        endpoint="ppsc_data_transfer_core",
        view_func=ppsc_data_transfer_core,
        methods=["GET"],
    )

    ppsc_pipeline.add_url_rule(
        PPSC_PIPELINE_PREFIX + "TransferEHR",
        endpoint="ppsc_data_transfer_ehr",
        view_func=ppsc_data_transfer_ehr,
        methods=["GET"],
    )

    ppsc_pipeline.add_url_rule(
        PPSC_PIPELINE_PREFIX + "TransferHealthData",
        endpoint="ppsc_data_transfer_health_data",
        view_func=ppsc_data_transfer_health_data,
        methods=["GET"],
    )

    ppsc_pipeline.add_url_rule(
        PPSC_PIPELINE_PREFIX + "TransferBiobankSample",
        endpoint="ppsc_data_transfer_biobank_sample",
        view_func=ppsc_data_transfer_biobank_sample,
        methods=["GET"],
    )

    ppsc_pipeline.add_url_rule('/_ah/start', endpoint='start', view_func=flask_start, methods=["GET"])
    ppsc_pipeline.add_url_rule("/_ah/stop", endpoint="stop", view_func=flask_stop, methods=["GET"])

    ppsc_pipeline.before_request(begin_request_logging)  # Must be first before_request() call.
    ppsc_pipeline.before_request(app_util.request_logging)

    ppsc_pipeline.after_request(app_util.add_headers)
    ppsc_pipeline.after_request(end_request_logging)  # Must be last after_request() call.

    ppsc_pipeline.register_error_handler(DBAPIError, app_util.handle_database_disconnect)

    got_request_exception.connect(flask_restful_log_exception_error, ppsc_pipeline)

    return ppsc_pipeline


app = _build_pipeline_app()
