# This module is the entrypoint for SMS jobs scheduled through cloud scheduler

from rdr_service.cloud_utils.gcp_cloud_tasks import GCPCloudTask
from rdr_service.config import GAE_PROJECT, NPH_SMS_BUCKETS
from rdr_service.dao.study_nph_sms_dao import SmsN0Dao

TASK_QUEUE = 'nph'


def n1_generation(file_type="N1_MC1"):
    n0_dao = SmsN0Dao()
    new_package_ids = n0_dao.get_n0_package_ids_without_n1()

    if GAE_PROJECT == "localhost":
        recipients = NPH_SMS_BUCKETS.get('test').keys()
    else:
        recipients = NPH_SMS_BUCKETS.get(GAE_PROJECT.split('-')[-1], 'test').keys()

    for package_id in new_package_ids:
        for recipient in recipients:
            if recipient not in package_id[1]:
                continue
            data = {
                "job": "FILE_GENERATION",
                "file_type": file_type,
                "recipient": recipient,
                "package_id": package_id[0]
            }

            _task = GCPCloudTask()
            _task.execute("nph_sms_generation_task", payload=data, queue=TASK_QUEUE)
