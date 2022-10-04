import logging
from typing import List, Optional

from rdr_service.model.message_broker import MessageBrokerEventData


class GenomicMessageBroker:

    @classmethod
    def set_value_from_parsed_values(
        cls,
        records: List[MessageBrokerEventData],
        field_names: List[str]
    ) -> Optional[str]:

        records = [records] if type(records) is not list else records
        field_records = list(filter(lambda x: x.fieldName in field_names, records))
        if not field_records:
            return None

        field_records = field_records[0].asdict()
        value = [v for k, v in field_records.items() if v is not None and 'value' in k]
        return value[0] if value else None

    @classmethod
    def ingest_appointment_data(cls, *, appointment_dao, records, module_type):
        appointment_id = list(filter(lambda x: x.fieldName == 'id', records))[0]
        logging.info(f'Inserting appointment event for Participant: {records[0].participantId}')

        for record in records:
            report_obj = appointment_dao.model_type(
                message_record_id=records[0].messageRecordId,
                participant_id=records[0].participantId,
                event_type=records[0].eventType,
                event_authored_time=records[0].eventAuthoredTime,
                module_type=module_type,
                appointment_id=appointment_id.valueInteger,
                appointment_timestamp=cls.set_value_from_parsed_values(record, ['appointment_timestamp']),
                appointment_timezone=cls.set_value_from_parsed_values(record, ['appointment_timezone']),
                source=cls.set_value_from_parsed_values(record, ['source']),
                location=cls.set_value_from_parsed_values(record, ['location']),
                contact_number=cls.set_value_from_parsed_values(record, ['contact_number']),
                language=cls.set_value_from_parsed_values(record, ['language']),
                cancellation_reason=cls.set_value_from_parsed_values(record, ['reason'])
            )
            appointment_dao.insert(report_obj)
