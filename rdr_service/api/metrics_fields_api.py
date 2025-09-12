from flask_restful import Resource

from rdr_service import app_util
from rdr_service.api_util import RDR
from rdr_service.offline import metrics_config


class MetricsFieldsApi(Resource):
    """API that returns the names and valid values for metric fields."""

    @app_util.auth_required(RDR)
    def get(self):
        return metrics_config.get_fields()
