"""The main API definition file for endpoints that trigger MapReduces and batch tasks."""
import logging
import os
import signal
from datetime import datetime

from flask import Flask, Response, got_request_exception
from sqlalchemy.exc import DBAPIError

from rdr_service import app_util
from rdr_service.offline.bigquery_sync import rebuild_bigquery_handler
from rdr_service.services.gcp_logging import begin_request_logging, end_request_logging, \
    flask_restful_log_exception_error

PREFIX = "/resource/"


@app_util.auth_required_cron
def resource_rebuild_cron():
    """ this should always be a manually run job, but we have to schedule it at least once a year. """
    now = datetime.utcnow()
    if now.day == 0o1 and now.month == 0o1:
        logging.info("skipping the scheduled run.")
        return '{"success": "true"}'
    rebuild_bigquery_handler()
    return '{"success": "true"}'


def start():
    return '{"success": "true"}'


def _stop():
    pid_file = '/tmp/supervisord.pid'
    if os.path.exists(pid_file):
        try:
            pid = int(open(pid_file).read())
            if pid:
                logging.info('******** Shutting down, sent supervisor the termination signal. ********')
                response = Response()
                response.status_code = 200
                end_request_logging(response)
                os.kill(pid, signal.SIGTERM)
        except TypeError:
            logging.warning('******** Shutting down, supervisor pid file is invalid. ********')
            pass
    return '{ "success": "true" }'


def _build_resource_app():
    _app = Flask(__name__)

    _app.add_url_rule(PREFIX, endpoint="/", view_func=start, methods=["GET"])

    _app.add_url_rule('/_ah/start', endpoint='start', view_func=start, methods=["GET"])
    _app.add_url_rule('/_ah/stop', endpoint='stop', view_func=_stop, methods=["GET"])

    _app.before_request(begin_request_logging)  # Must be first before_request() call.
    _app.before_request(app_util.request_logging)

    _app.after_request(app_util.add_headers)
    _app.after_request(end_request_logging)  # Must be last after_request() call.

    _app.register_error_handler(DBAPIError, app_util.handle_database_disconnect)

    got_request_exception.connect(flask_restful_log_exception_error, _app)

    return _app


app = _build_resource_app()
