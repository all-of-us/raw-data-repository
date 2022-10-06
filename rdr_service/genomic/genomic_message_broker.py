import logging
from typing import List, Optional

from rdr_service.dao.genomics_dao import GenomicAppointmentEventDao
from rdr_service.model.message_broker import MessageBrokerEventData


class GenomicMessageBroker:

    module_fields = ['module_type', 'result_type']

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
    def ingest_appointment_data(cls, records):
        appointment_dao = GenomicAppointmentEventDao()

        appointment_id = list(filter(lambda x: x.fieldName == 'id', records))[0]
        participant_id = records[0].participantId

        logging.info(f'Inserting appointment event for participant: {participant_id}')

        report_obj = appointment_dao.model_type(
            message_record_id=records[0].messageRecordId,
            participant_id=participant_id,
            event_type=records[0].eventType,
            event_authored_time=records[0].eventAuthoredTime,
            module_type=cls.set_value_from_parsed_values(records, cls.module_fields),
            appointment_id=appointment_id.valueInteger,
            appointment_timestamp=cls.set_value_from_parsed_values(records, ['appointment_timestamp']),
            appointment_timezone=cls.set_value_from_parsed_values(records, ['appointment_timezone']),
            source=cls.set_value_from_parsed_values(records, ['source']),
            location=cls.set_value_from_parsed_values(records, ['location']),
            contact_number=cls.set_value_from_parsed_values(records, ['contact_number']),
            language=cls.set_value_from_parsed_values(records, ['language']),
            cancellation_reason=cls.set_value_from_parsed_values(records, ['reason'])
        )
        appointment_dao.insert(report_obj)
