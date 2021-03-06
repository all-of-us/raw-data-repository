"""Version API used for prober and release management.

No auth is required for this endpoint because it serves nothing sensitive.
"""
import logging

from flask import request
from flask_restful import Resource

from rdr_service.config import GAE_VERSION_ID


class VersionApi(Resource):
    """Api handler for retrieving version info."""

    def get(self):
        logging.info(str(request.headers))
        return {"version_id": GAE_VERSION_ID}
