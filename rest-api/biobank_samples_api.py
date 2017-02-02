"""The API definition for the biobank samples API.

This method triggers the BiobankSamples pipeline.
"""

import api_util
import config
import logging
import offline.biobank_samples_pipeline

@api_util.auth_required_cron
def get():
  bucket_name = config.getSetting(config.BIOBANK_SAMPLES_BUCKET_NAME, None)
  if not bucket_name:
    logging.error("No bucket configured for %s", config.BIOBANK_SAMPLES_BUCKET_NAME)
    return '{"biobank-samples-pipeline-status": "error: no bucket configured"}'
  logging.info("=========== Starting biobank samples pipeline ============")
  offline.biobank_samples_pipeline.BiobankSamplesPipeline(bucket_name).start()
  return '{"biobank-samples-pipeline-status": "started"}'
