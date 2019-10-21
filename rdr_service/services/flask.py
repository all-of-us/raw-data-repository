import json
import logging
import sys
import traceback

from werkzeug.exceptions import HTTPException
from werkzeug.wrappers import Response

from flask import Flask, got_request_exception

from rdr_service.json_encoder import RdrJsonEncoder
from rdr_service.model.utils import ParticipantIdConverter
from rdr_service.services.gcp_logging import FlaskGCPStackDriverLogging

app_log_service = FlaskGCPStackDriverLogging()
app = Flask(__name__)



app.before_request(app_log_service.begin_request)
app.after_request(app_log_service.end_request)

app.url_map.converters["participant_id"] = ParticipantIdConverter
app.config.setdefault("RESTFUL_JSON", {"cls": RdrJsonEncoder})

API_PREFIX = "/rdr/v1/"
TASK_PREFIX = API_PREFIX + "tasks/"

app_log_service.flush()

def finalize_request_logging(response):
    """
    Finalize and send log message(s) for request.
    :param response: Flask response object
    """
    app_log_service.end_request(response)
    return response


def _get_traceback(e):
    """
    Return a string formatted with the exception traceback.
    :param e: exception object
    :return: string
    """
    tb = None
    if e:
        tb = e.__traceback__ if hasattr(e, '__traceback__') else None

    if not tb:
        # pylint: disable=unused-variable
        etype, value, tb = sys.exc_info()

    if tb:
        tb_out = traceback.format_tb(tb)
    else:
        tb_out = ['No exception traceback available.', ]

    # Mimic the nice python exception and traceback print.
    e_error = e.__repr__()
    tb_heading = 'Traceback (most recent call last):'
    tb_data = ''.join(tb_out)
    message = '{0}\n{1}\n{2}'.format(e_error, tb_heading, tb_data)
    return message


def log_exception_error(e):
    """
    Log exception errors
    :param sender: Flask object
    :param e: exception object
    :return: flask response, status code
    """
    traceback_msg = _get_traceback(e)

    if isinstance(e, HTTPException):
        response = e.get_response()
        response.data = json.dumps({
            "code": e.code,
            "name": e.name,
            "description": e.description,
        })
        response.content_type = "application/json"
    else:
        response = Response()
        response.status_code = 500

    logging.error(traceback_msg)
    # app_log_service.end_request(response)

    # pass through HTTP errors
    if isinstance(e, HTTPException):
        return e

    # now you're handling non-HTTP exceptions only
    return response

# pylint: disable=unused-argument
def _flask_restful_log_exception_error(sender, exception, **kwargs):
    """
    Make sure we can log exception errors to Google when running under Gunicorn.
    """
    log_exception_error(exception)


# If we are being run under gunicorn, hookup gunicorn's logging handler.
if __name__ != '__main__':
    # https://medium.com/@trstringer/logging-flask-and-gunicorn-the-manageable-way-2e6f0b8beb2f
    gunicorn_logger = logging.getLogger('gunicorn.error')
    app.logger.handlers = gunicorn_logger.handlers
    app.logger.setLevel(gunicorn_logger.level)
    # https://github.com/flask-restful/flask-restful/issues/792
    got_request_exception.connect(_flask_restful_log_exception_error, app)
