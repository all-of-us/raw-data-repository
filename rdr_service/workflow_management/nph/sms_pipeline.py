# This module is the entrypoint for SMS jobs scheduled through cloud scheduler

import logging

from rdr_service.cloud_utils.gcp_cloud_tasks import GCPCloudTask
from rdr_service.config import GAE_PROJECT, NPH_SMS_BUCKETS

TASK_QUEUE = 'nph'

def n1_generation():
    if GAE_PROJECT == "localhost":
        recipients = NPH_SMS_BUCKETS.get('test').keys()
    else:
        recipients = NPH_SMS_BUCKETS.get(GAE_PROJECT.split('-')[-1], 'test').keys()

    for destination in recipients:
        logging.info(f"Pushing cloud task for {destination}...")

        data = {
            "file_type": "N1_MC1",
            "recipient": destination
        }

        _task = GCPCloudTask()
        _task.execute("nph_sms_generation_task", payload=data, queue=TASK_QUEUE)
