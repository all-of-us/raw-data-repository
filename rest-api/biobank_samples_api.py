"""The API definition for the biobank samples API.

This defines the APIs and the handlers for the APIs.
"""

import api_util
import offline.biobank_samples_pipeline

from flask.ext.restful import Resource

class BiobankSamplesApi(Resource):
  @api_util.auth_required_cron_or_admin
  def get(self):
    bucket_name = config.getSetting(config.BIOBANK_SAMPLES_BUCKET_NAME, None)
    if not bucket_name: 
      print "No bucket configured for {}".format(config.BIOBANK_SAMPLES_BUCKET_NAME)
      return
    print "=========== Starting biobank samples pipeline ============"
    offline.biobank_samples_pipeline.BiobankSamplesPipeline(bucket_name).start()
