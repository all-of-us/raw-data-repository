import api_util
import json
import logging

from google.appengine.api import app_identity
from api_util import EXPORTER
from flask.ext.restful import Resource
from werkzeug.exceptions import BadRequest
from flask import request

class ExportApi(Resource):
  """API that exports data from our database to files in GCS."""

  @api_util.auth_required(EXPORTER)
  def post(self):
    resource = request.get_data()
    resource_json = json.loads(resource)
    database = resource_json.get('database')
    tables = resource_json.get('tables')
    app_id = app_identity.get_application_id()
    if app_id == "None":
      destination_bucket = app_identity.get_default_gcs_bucket_name()
    elif database == 'rdr':
      destination_bucket = '%s-rdr-export' % app_id
    elif database == 'cdm' or database == 'voc':
      destination_bucket = '%s-cdm' % app_id
    else:
      raise BadRequest("Invalid database: %s" % database)

