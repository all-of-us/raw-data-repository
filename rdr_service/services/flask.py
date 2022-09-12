import logging
import multiprocessing
import os
import signal

from flask import Flask, Response

from rdr_service.config import get_config, get_db_config
from rdr_service.json_encoder import RdrJsonEncoder
from rdr_service.model.utils import ParticipantIdConverter
from rdr_service.services.gcp_logging import end_request_logging


app = Flask(__name__)

app.url_map.converters["participant_id"] = ParticipantIdConverter
app.config.setdefault("RESTFUL_JSON", {"cls": RdrJsonEncoder})

API_PREFIX = "/rdr/v1/"
OFFLINE_PREFIX = "/offline/"
RESOURCE_PREFIX = '/resource/'
TASK_PREFIX = "/resource/task/"


# If we are being run under gunicorn, hookup gunicorn's logging handler.
if __name__ != '__main__':
    # https://medium.com/@trstringer/logging-flask-and-gunicorn-the-manageable-way-2e6f0b8beb2f
    gunicorn_logger = logging.getLogger('gunicorn.error')
    app.logger.handlers = gunicorn_logger.handlers
    app.logger.setLevel(gunicorn_logger.level)

    print(f'CPUs: {multiprocessing.cpu_count()}.')


def flask_warmup():
    # Load configurations into the cache.
    # Not called in AppEngine2????
    get_config()
    get_db_config()
    return '{ "success": "true" }'


def flask_start():
    return '{"success": "true"}'


def flask_stop():
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
