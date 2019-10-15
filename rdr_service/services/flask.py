import logging
import traceback

from werkzeug.exceptions import HTTPException, InternalServerError
from werkzeug.wrappers import Response

from flask import Flask

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

@app.errorhandler(Exception)
def handle_exception(e):
    """
    Log exception errors
    :param e: exception object
    :return: flask response, status code
    """
    # Mimic the nice python exception and traceback print.
    e_error = e.__repr__()
    tb_heading = 'Traceback (most recent call last):'
    tb_data = ''.join(traceback.format_tb(e.__traceback__))
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
    original = getattr(e, "original_exception", None)
    tb = traceback.format_tb(original.__traceback__ if original else e.__traceback__)

    # Mimic the nice python exception and traceback print.
    e_error = e.__repr__()
    tb_heading = 'Traceback (most recent call last):'
    tb_data = ''.join(traceback.format_tb(tb))
    message = '{0}\n{1}\n{2}'.format(e_error, tb_heading, tb_data)

    response = Response()
    response.status_code = 500
    response.data = "{'error': 'internal server error'}"

    logging.error(message)
    app_log_service.end_request(response)

    return response

