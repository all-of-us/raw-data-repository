import datetime
from typing import Optional

from flask import request
from flask_restful import Resource
from dateutil.parser import parse

from rdr_service.app_util import task_auth_required
from rdr_service.api.cloud_tasks_api import log_task_headers
from rdr_service.cloud_utils.gcp_cloud_tasks import GCPCloudTask
from rdr_service.config import GAE_PROJECT
from rdr_service.model.message_broker import MessageBrokerEventData
from rdr_service.dao.message_broker_dao import MessageBrokenEventDataDao


class StoreMessageBrokerEventDataTaskApi(Resource):
    """
    Cloud Task endpoint: store message broker event data.
    """
    def __init__(self):
        self.dao = MessageBrokenEventDataDao()
        self.event_type = None
        self.cloud_task = GCPCloudTask()

    @task_auth_required
    def post(self):
        log_task_headers()
        data = request.get_json(force=True)

        self.event_type = data.get('eventType')
        message_record_id = data.get('id')
        event_authored_time = parse(data.get('eventAuthoredTime'))
        participant_id = data.get('participantId')
        message_body = data.get('requestBody')

        for key, value in message_body.items():
            field_name = key
            value_date_time = None

            if isinstance(value, str):
                try:
                    value_date_time = parse(value)
                except (ValueError, OverflowError):
                    pass

            message_event_date = MessageBrokerEventData(
                messageRecordId=message_record_id,
                eventType=self.event_type,
                eventAuthoredTime=event_authored_time,
                participantId=participant_id,
                fieldName=field_name
            )
            if isinstance(value_date_time, datetime.datetime):
                message_event_date.valueDatetime = value
            elif isinstance(value, bool):
                message_event_date.valueBool = value
            elif isinstance(value, dict):
                message_event_date.valueJson = value
            elif isinstance(value, str):
                message_event_date.valueString = value
            elif isinstance(value, int):
                message_event_date.valueInteger = value

            self.dao.insert(message_event_date)

        self.call_genomic_ingest_data_task(message_record_id)
        return '{"success": "true"}'

    def call_genomic_ingest_data_task(self, message_record_id):

        def _get_task_endpoint() -> Optional[str]:
            endpoint = None
            data_event_types = [
                'informing_loop_started',
                'informing_loop_decision',
                'result_viewed',
                'result_ready'
            ]
            if self.event_type in data_event_types:
                endpoint = 'ingest_genomic_message_broker_data_task'
            elif 'appointment' in self.event_type:
                endpoint = 'ingest_genomic_message_broker_appointment_task'

            return endpoint

        task_endpoint = _get_task_endpoint()

        if task_endpoint and GAE_PROJECT != 'localhost':
            self.cloud_task.execute(
                task_endpoint,
                payload={
                    'message_record_id': message_record_id,
                    'event_type': self.event_type
                },
                queue='genomics'
            )

