import os

from flask import Flask

from rdr_service import config
from rdr_service.json_encoder import RdrJsonEncoder
from rdr_service.model.utils import ParticipantIdConverter
from rdr_service.services.celery_utils import configure_celery


app = Flask(__name__)

if 'GAE_SERVICE' in os.environ:
    from rdr_service.services.gcp_logging import FlaskGCPStackDriverLoggingMiddleware
    app = FlaskGCPStackDriverLoggingMiddleware(app)

app.url_map.converters["participant_id"] = ParticipantIdConverter
app.config.setdefault("RESTFUL_JSON", {"cls": RdrJsonEncoder})

# Add celery configuration information into Flask app.
_result_backend = config.get_db_config()['celery_db_connection_string']
_broker_url = config.get_db_config()['celery_broker_url']

app.config.update(
    CELERY_BROKER_URL=_broker_url,
    RESULT_BACKEND=_result_backend,
)

API_PREFIX = "/rdr/v1/"

celery = configure_celery(app)
