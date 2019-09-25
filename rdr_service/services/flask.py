from flask import Flask

from rdr_service import config
from rdr_service.json_encoder import RdrJsonEncoder
from rdr_service.model.utils import ParticipantIdConverter
from rdr_service.services.celery_utils import configure_celery

app = Flask(__name__)
app.url_map.converters["participant_id"] = ParticipantIdConverter
app.config.setdefault("RESTFUL_JSON", {"cls": RdrJsonEncoder})

import os

print('****** Start Showing Environment *******')
for k, v in os.environ.items():
    print(f' ** {k}: {v}')
print('****** End Showing Environment *******')

# Add celery configuration information into Flask app.
_result_backend = config.get_db_config()['celery_db_connection_string']
_broker_url = config.get_db_config()['celery_broker_url']

app.config.update(
    CELERY_BROKER_URL=_broker_url,
    RESULT_BACKEND=_result_backend,
)

API_PREFIX = "/rdr/v1/"

celery = configure_celery(app)
