"""The API definition for the biobank samples API.

This defines the APIs and the handlers for the APIs.
"""

import api_util
import offline.biobank_samples_pipeline

from flask.ext.restful import Resource

class BiobankSamplesApi(Resource):
  @api_util.auth_required_cron_or_admin
  def get(self):
    print "=========== Starting Pipeline ============"
    offline.biobank_samples_pipeline.BiobankSamplesPipeline().start()
