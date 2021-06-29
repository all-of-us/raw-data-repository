import datetime
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
    @task_auth_required
    def post(self):
        log_task_headers()
        dao = MessageBrokenEventDataDao()
        data = request.get_json(force=True)
        message_record_id = data.get('id')
        event_type = data.get('eventType')
        event_authored_time = parse(data.get('eventAuthoredTime'))
        participant_id = data.get('participantId')
        message_body = data.get('requestBody')

        with dao.session() as session:
            for key, value in message_body.items():
                field_name = key

                value_date_time = None
                if isinstance(value, str):
                    try:
                        value_date_time = parse(value)
                    except ValueError:
                        pass

                message_event_date = MessageBrokerEventData(
                    messageRecordId=message_record_id,
                    eventType=event_type,
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
                inserted_obj = dao.insert_with_session(session, message_event_date)

        if GAE_PROJECT != 'localhost' \
                and 'informing_loop'.lower() in event_type:
            payload = {}
            informing_records = dao.get_informing_loop(
                inserted_obj.messageRecordId,
                event_type
            )
            if informing_records:
                payload['event_type'] = event_type
                payload['records'] = informing_records
                _task = GCPCloudTask()
                _task.execute('ingest_informing_loop',
                              payload=payload,
                              queue='genomics')

        return '{"success": "true"}'
