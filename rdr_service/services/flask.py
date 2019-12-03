import multiprocessing
import logging

from flask import Flask

from rdr_service.json_encoder import RdrJsonEncoder
from rdr_service.model.utils import ParticipantIdConverter


app = Flask(__name__)

app.url_map.converters["participant_id"] = ParticipantIdConverter
app.config.setdefault("RESTFUL_JSON", {"cls": RdrJsonEncoder})

API_PREFIX = "/rdr/v1/"
TASK_PREFIX = API_PREFIX + "tasks/"


# If we are being run under gunicorn, hookup gunicorn's logging handler.
if __name__ != '__main__':
    # https://medium.com/@trstringer/logging-flask-and-gunicorn-the-manageable-way-2e6f0b8beb2f
    gunicorn_logger = logging.getLogger('gunicorn.error')
    app.logger.handlers = gunicorn_logger.handlers
    app.logger.setLevel(gunicorn_logger.level)

    print(f'CPUs: {multiprocessing.cpu_count()}.')
