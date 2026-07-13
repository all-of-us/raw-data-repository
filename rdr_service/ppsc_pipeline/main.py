"""The main API definition file for ppsc-pipeline service endpoints."""
import logging
import traceback
import datetime

from flask import Flask, got_request_exception, request
from sqlalchemy.exc import DBAPIError

from rdr_service import app_util
from rdr_service.config import GAE_PROJECT
from rdr_service.ppsc.ppsc_partner_data_sync import NphOptInSync
from rdr_service.ppsc.ppsc_partner_data_transfer import PPSCDataTransferCore, PPSCDataTransferHealthData, \
    PPSCDataTransferEHR, \
    PPSCDataTransferBiobank, RTIDataTransferNPHOptIn
from rdr_service.services.flask import PPSC_PIPELINE_PREFIX, flask_start, flask_stop
from rdr_service.services.gcp_logging import begin_request_logging, end_request_logging,\
    flask_restful_log_exception_error
from rdr_service.workflow_management.ppsc.ppsc_data_transfer_input_feed import InputFeed, Intake2SummaryFeed, \
    AwardeeInSiteFeed
from rdr_service.tools.tool_libs.GCSFileCopierToS3 import GCSFileCopierToS3
from rdr_service.tools.export_awardee_insite_data_to_csv import AwardeeInSiteDataExporter


@app_util.auth_required_scheduler
def test_job():
    try:
        logging.info("Test Job Executed")
    except Exception as e:  # pylint: disable=broad-except
        logging.error(f"An error occurred: {e}\nStack trace: {traceback.format_exc()}")
        return "Error occurred", 500

    return '{"success": "true"}'

@app_util.auth_required_scheduler
def awardee_insite_input_feed():
    datafeed = request.get_json().get("datafeed")
    input_feed = AwardeeInSiteFeed(project=GAE_PROJECT)
    input_feed.run_datafeed(datafeed)
    return '{ "success": "true" }'


@app_util.auth_required_scheduler
def export_awardee_insite_data_to_sites():
    AwardeeInSiteDataExporter().export_data()
    return '{ "success": "true" }'

@app_util.auth_required_scheduler
def ppsc_data_transfer_input_feed():
    datafeed = request.get_json().get("datafeed")
    start_date = request.get_json().get("earliest_date", '2025-03-28')
    end_date = request.get_json().get("latest_date", datetime.datetime.utcnow().strftime('%Y-%m-%d'))
    batch_size = request.get_json().get("batch_size", 800)
    input_feed = InputFeed(project=GAE_PROJECT)
    input_feed.run_datafeed(datafeed, start_date, end_date, batch_size)
    return '{ "success": "true" }'


@app_util.auth_required_scheduler
def ppsc_data_transfer_intake_2_summary_feed():
    datafeed = request.get_json().get("datafeed")
    intake_2_summary_feed = Intake2SummaryFeed(project=GAE_PROJECT)
    intake_2_summary_feed.run_datafeed(datafeed)
    return '{ "success": "true" }'

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


@app_util.auth_required_scheduler
def ppsc_rti_data_transfer_nph_opt_in():
    with RTIDataTransferNPHOptIn() as nph_opt_in_transfer:
        nph_opt_in_transfer.run_data_transfer()
    return '{ "success": "true" }'


@app_util.auth_required_scheduler
def ppsc_nph_opt_in_sync():
    NphOptInSync().run_sync()
    return '{ "success": "true" }'


@app_util.auth_required_scheduler
def copy_from_gcs_to_s3():
    """
    Copy EHR, Health Data Sharing Stream, and Core Data Report to PPSC AWS Bucket
    """
    gcs_bucket = request.get_json().get("gcs_bucket")
    s3_bucket = request.get_json().get("s3_bucket")
    copier = GCSFileCopierToS3(gcs_bucket=gcs_bucket, s3_bucket=s3_bucket)
    gcs_to_ppsc_directories_map = request.get_json().get("gcs_to_ppsc_directories_map", {
        "ehr_exports": "ehr-updates",
        "core_data": "core-data",
        "health_sharing_status": "health-sharing"
    })
    copier.run(gcs_to_ppsc_directories_map)
    return '{ "success": "true" }'


def _build_pipeline_app():
    """Configure and return the app with non-resource pipeline-triggering endpoints."""
    ppsc_pipeline = Flask(__name__)
    ppsc_pipeline.config['TRAP_HTTP_EXCEPTIONS'] = True

    ppsc_pipeline.add_url_rule(
        PPSC_PIPELINE_PREFIX + "TestJob",
        endpoint="test_job",
        view_func=test_job,
        methods=["GET", "POST"],
    )

    # Cloud Scheduler - Scheduler jobs
    ppsc_pipeline.add_url_rule(
        PPSC_PIPELINE_PREFIX + "AwardeeInSiteInputFeed",
        endpoint="awardee_insite_input_feed",
        view_func=awardee_insite_input_feed,
        methods=["GET", "POST"]
    )

    ppsc_pipeline.add_url_rule(
        PPSC_PIPELINE_PREFIX + "ExportAwardeeInSiteDataToSites",
        endpoint="export_awardee_insite_data_to_sites",
        view_func=export_awardee_insite_data_to_sites,
        methods=["GET"],
    )

    ppsc_pipeline.add_url_rule(
        PPSC_PIPELINE_PREFIX + "TransferInputFeed",
        endpoint="ppsc_data_transfer_input_feed",
        view_func=ppsc_data_transfer_input_feed,
        methods=["GET", "POST"],
    )

    ppsc_pipeline.add_url_rule(
        PPSC_PIPELINE_PREFIX + "Intake2SummaryFeed",
        endpoint="ppsc_data_transfer_intake_2_summary_feed",
        view_func=ppsc_data_transfer_intake_2_summary_feed,
        methods=["GET", "POST"],
    )

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

    ppsc_pipeline.add_url_rule(
        PPSC_PIPELINE_PREFIX + "TransferNPHOptIn",
        endpoint="ppsc_rti_data_transfer_nph_opt_in",
        view_func=ppsc_rti_data_transfer_nph_opt_in,
        methods=["GET"],
    )

    ppsc_pipeline.add_url_rule(
        PPSC_PIPELINE_PREFIX + "PPSCNphOptInSync",
        endpoint="ppsc_nph_opt_in_sync",
        view_func=ppsc_nph_opt_in_sync,
        methods=["GET"],
    )

    ppsc_pipeline.add_url_rule(
        PPSC_PIPELINE_PREFIX + "CopyFromGCSToS3",
        endpoint="copy_from_gcs_to_s3",
        view_func=copy_from_gcs_to_s3,
        methods=["POST"]
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
