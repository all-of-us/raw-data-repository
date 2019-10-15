import os

from flask import Flask

from rdr_service.json_encoder import RdrJsonEncoder
from rdr_service.model.utils import ParticipantIdConverter

app = Flask(__name__)

if 'GAE_SERVICE' in os.environ:
    from rdr_service.services.gcp_logging import FlaskGCPStackDriverLoggingMiddleware
    app = FlaskGCPStackDriverLoggingMiddleware(app)

app.url_map.converters["participant_id"] = ParticipantIdConverter
app.config.setdefault("RESTFUL_JSON", {"cls": RdrJsonEncoder})

API_PREFIX = "/rdr/v1/"
TASK_PREFIX = API_PREFIX + "tasks/"
