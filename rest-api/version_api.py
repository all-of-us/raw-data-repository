"""Version API used for prober and release management.

No auth is required for this endpoint because it serves nothing sensitive.
"""

import os

from flask.ext.restful import Resource

class VersionApi(Resource):
  """Api handler for retrieving version info."""

  def get(self):
    return {'version_id': os.environ['CURRENT_VERSION_ID']}
