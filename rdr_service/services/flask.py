import logging
import os
import sys
import traceback

from werkzeug.exceptions import HTTPException, InternalServerError
from werkzeug.wrappers import Response

from flask import Flask

from rdr_service import config
from rdr_service.json_encoder import RdrJsonEncoder
from rdr_service.model.utils import ParticipantIdConverter
from rdr_service.services.celery_utils import configure_celery
from rdr_service.services.gcp_logging import FlaskGCPStackDriverLogging

app_log_service = FlaskGCPStackDriverLogging()
app = Flask(__name__)

app.before_request(app_log_service.begin_request)
app.after_request(app_log_service.end_request)

app.url_map.converters["participant_id"] = ParticipantIdConverter
app.config.setdefault("RESTFUL_JSON", {"cls": RdrJsonEncoder})

# Add celery configuration information into Flask app.
if not os.environ.get("UNITTEST_FLAG", None):
    key = 'celery_db_connection_string'
else:
    key = 'unittest_celery_db_connection_string'

_result_backend = config.get_db_config()[key]
_broker_url = config.get_db_config()['celery_broker_url']

app.config.update(
    CELERY_BROKER_URL=_broker_url,
    RESULT_BACKEND=_result_backend,
)

API_PREFIX = "/rdr/v1/"

celery = configure_celery(app)

app_log_service.flush()

def finalize_request_logging(response):
    """
    Finalize and send log message(s) for request.
    :param response: Flask response object
    """
    app_log_service.end_request(response)
    return response


@app.errorhandler(Exception)
def handle_exception(e):
    """
    Log exception errors
    :param e: exception object
    :return: flask response, status code
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

    response = Response()
    response.status_code = 500
    response.data = "{'error': 'internal server error'}"

    logging.error(message)
    app_log_service.end_request(response)

    # pass through HTTP errors
    if isinstance(e, HTTPException):
        return e

    # now you're handling non-HTTP exceptions only
    return response

@app.errorhandler(InternalServerError)
def handle_500(e):
    """
    Log exception errors
    :param e: exception object
    :return: flask response, status code
    """
    tb = None
    original = getattr(e, "original_exception", None)
    if original:
        tb = original.__traceback__ if hasattr(original, '__traceback__') else None
    elif e:
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

    response = Response()
    response.status_code = 500
    response.data = "{'error': 'internal server error'}"

    logging.error(message)
    app_log_service.end_request(response)

    return response