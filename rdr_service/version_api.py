"""Version API used for prober and release management.

No auth is required for this endpoint because it serves nothing sensitive.
"""

import os
from rdr_service.config import GAE_VERSION_ID


from flask_restful import Resource


class VersionApi(Resource):
    """Api handler for retrieving version info."""

    def get(self):
        return {"version_id": GAE_VERSION_ID}
