import datetime
from unittest import mock

from rdr_service import clock
from rdr_service.dao.database_utils import format_datetime
from rdr_service.dao.genomics_dao import GenomicInformingLoopDao, GenomicResultViewedDao, GenomicMemberReportStateDao, \
    GenomicJobRunDao, GenomicAppointmentEventDao
from rdr_service.genomic.genomic_job_controller import GenomicJobController
from rdr_service.genomic_enums import GenomicWorkflowState, GenomicJob, GenomicReportState, GenomicSubProcessResult, \
    GenomicIncidentCode
from tests.helpers.unittest_base import BaseTestCase


class GenomicMessageBrokerIngestionTest(BaseTestCase):
    def setUp(self):
        super(GenomicMessageBrokerIngestionTest, self).setUp()
        self.informing_loop_dao = GenomicInformingLoopDao()
        self.result_viewed_dao = GenomicResultViewedDao()
        self.report_state_dao = GenomicMemberReportStateDao()
        self.appointment_dao = GenomicAppointmentEventDao()
        self.job_run_dao = GenomicJobRunDao()

    def test_informing_loop_ingestion_message_broker(self):
        loop_decision = 'informing_loop_decision'
        loop_started = 'informing_loop_started'
        # https://docs.google.com/document/d/1E1tNSi1mWwhBSCs9Syprbzl5E0SH3c_9oLduG1mzlcY/edit#heading=h.2m73apfm9irj
        loop_module_types = ['gem', 'hdr', 'pgx']

        gen_set = self.data_generator.create_database_genomic_set(
            genomicSetName=".",
            genomicSetCriteria=".",
            genomicSetVersion=1
        )

        participant = self.data_generator.create_database_participant()
        sample_id = '22222222'

        self.data_generator.create_database_genomic_set_member(
            genomicSetId=gen_set.id,
            sampleId=sample_id,
            participantId=participant.participantId,
            biobankId="1",
            genomeType="aou_array",
            genomicWorkflowState=GenomicWorkflowState.AW0
        )

        # gem array
        message_broker_record = self.data_generator.create_database_message_broker_record(
            participantId=participant.participantId,
            eventType=loop_decision,
            eventAuthoredTime=clock.CLOCK.now(),
            messageOrigin='example@example.com',
            requestBody={'module_type': loop_module_types[0], 'decision_value': 'yes'},
            requestTime=clock.CLOCK.now(),
            responseError='',
            responseCode='200',
            responseTime=clock.CLOCK.now()
        )

        for key, value in message_broker_record.requestBody.items():
            self.data_generator.create_database_message_broker_event_data(
                participantId=message_broker_record.participantId,
                messageRecordId=message_broker_record.id,
                eventType=message_broker_record.eventType,
                eventAuthoredTime=message_broker_record.eventAuthoredTime,
                fieldName=key,
                valueString=value
            )

        with GenomicJobController(GenomicJob.INGEST_INFORMING_LOOP) as controller:
            controller.ingest_records_from_message_broker_data(
                message_record_id=message_broker_record.id,
                event_type=loop_decision
            )

        decision_genomic_record = self.informing_loop_dao.get(1)

        self.assertIsNotNone(decision_genomic_record)
        self.assertIsNotNone(decision_genomic_record.event_type)
        self.assertIsNotNone(decision_genomic_record.module_type)
        self.assertIsNotNone(decision_genomic_record.decision_value)

        self.assertEqual(decision_genomic_record.message_record_id, message_broker_record.id)
        self.assertEqual(decision_genomic_record.participant_id, message_broker_record.participantId)
        self.assertEqual(decision_genomic_record.event_type, loop_decision)
        self.assertTrue(decision_genomic_record.module_type == 'gem')
        self.assertEqual(decision_genomic_record.decision_value, 'yes')
        self.assertEqual(decision_genomic_record.sample_id, sample_id)

        message_broker_record_two = self.data_generator.create_database_message_broker_record(
            participantId=participant.participantId,
            eventType=loop_started,
            eventAuthoredTime=clock.CLOCK.now(),
            messageOrigin='example@example.com',
            requestBody={'module_type': loop_module_types[0]},
            requestTime=clock.CLOCK.now(),
            responseError='',
            responseCode='200',
            responseTime=clock.CLOCK.now()
        )

        for key, value in message_broker_record_two.requestBody.items():
            self.data_generator.create_database_message_broker_event_data(
                participantId=message_broker_record_two.participantId,
                messageRecordId=message_broker_record_two.id,
                eventType=message_broker_record_two.eventType,
                eventAuthoredTime=message_broker_record_two.eventAuthoredTime,
                fieldName=key,
                valueString=value
            )

        with GenomicJobController(GenomicJob.INGEST_INFORMING_LOOP) as controller:
            controller.ingest_records_from_message_broker_data(
                message_record_id=message_broker_record_two.id,
                event_type=loop_started
            )

        started_genomic_record = self.informing_loop_dao.get(2)

        self.assertIsNotNone(started_genomic_record)
        self.assertIsNotNone(started_genomic_record.event_type)
        self.assertIsNotNone(started_genomic_record.module_type)
        self.assertIsNone(started_genomic_record.decision_value)

        self.assertEqual(started_genomic_record.message_record_id, message_broker_record_two.id)
        self.assertEqual(started_genomic_record.participant_id, message_broker_record_two.participantId)
        self.assertEqual(started_genomic_record.event_type, loop_started)
        self.assertEqual(started_genomic_record.module_type, loop_module_types[0])
        self.assertEqual(decision_genomic_record.sample_id, sample_id)

        sample_id = '22333333'

        self.data_generator.create_database_genomic_set_member(
            genomicSetId=gen_set.id,
            sampleId=sample_id,
            participantId=participant.participantId,
            biobankId="2",
            genomeType="aou_wgs",
            genomicWorkflowState=GenomicWorkflowState.AW0
        )

        # hdr wgs
        message_broker_record_three = self.data_generator.create_database_message_broker_record(
            participantId=participant.participantId,
            eventType=loop_decision,
            eventAuthoredTime=clock.CLOCK.now(),
            messageOrigin='example@example.com',
            requestBody={'module_type': loop_module_types[1], 'decision_value': 'yes'},
            requestTime=clock.CLOCK.now(),
            responseError='',
            responseCode='200',
            responseTime=clock.CLOCK.now()
        )

        for key, value in message_broker_record_three.requestBody.items():
            self.data_generator.create_database_message_broker_event_data(
                participantId=message_broker_record_three.participantId,
                messageRecordId=message_broker_record_three.id,
                eventType=message_broker_record_three.eventType,
                eventAuthoredTime=message_broker_record_three.eventAuthoredTime,
                fieldName=key,
                valueString=value
            )

        with GenomicJobController(GenomicJob.INGEST_INFORMING_LOOP) as controller:
            controller.ingest_records_from_message_broker_data(
                message_record_id=message_broker_record_three.id,
                event_type=loop_decision
            )

        decision_genomic_record = self.informing_loop_dao.get(3)

        self.assertIsNotNone(decision_genomic_record)
        self.assertIsNotNone(decision_genomic_record.event_type)
        self.assertIsNotNone(decision_genomic_record.module_type)
        self.assertIsNotNone(decision_genomic_record.decision_value)

        self.assertEqual(decision_genomic_record.message_record_id, message_broker_record_three.id)
        self.assertEqual(decision_genomic_record.participant_id, message_broker_record_three.participantId)
        self.assertEqual(decision_genomic_record.event_type, loop_decision)
        self.assertTrue(decision_genomic_record.module_type == 'hdr')
        self.assertEqual(decision_genomic_record.decision_value, 'yes')
        self.assertEqual(decision_genomic_record.sample_id, sample_id)

    def test_result_viewed_ingestion_message_broker(self):
        event_type = 'result_viewed'
        participant = self.data_generator.create_database_participant()
        # https://docs.google.com/document/d/1E1tNSi1mWwhBSCs9Syprbzl5E0SH3c_9oLduG1mzlcY/edit#heading=h.dtikttz25h22
        result_module_types = ['gem', 'hdr_v1', 'pgx_v1']

        gen_set = self.data_generator.create_database_genomic_set(
            genomicSetName=".",
            genomicSetCriteria=".",
            genomicSetVersion=1
        )

        sample_id = '2222222'

        self.data_generator.create_database_genomic_set_member(
            genomicSetId=gen_set.id,
            sampleId=sample_id,
            participantId=participant.participantId,
            biobankId="1",
            genomeType="aou_array",
            genomicWorkflowState=GenomicWorkflowState.AW0
        )

        message_broker_record = self.data_generator.create_database_message_broker_record(
            participantId=participant.participantId,
            eventType=event_type,
            eventAuthoredTime=clock.CLOCK.now(),
            messageOrigin='example@example.com',
            requestBody={'result_type': result_module_types[0]},
            requestTime=clock.CLOCK.now(),
            responseError='',
            responseCode='200',
            responseTime=clock.CLOCK.now()
        )

        for key, value in message_broker_record.requestBody.items():
            self.data_generator.create_database_message_broker_event_data(
                participantId=message_broker_record.participantId,
                messageRecordId=message_broker_record.id,
                eventType=message_broker_record.eventType,
                eventAuthoredTime=message_broker_record.eventAuthoredTime,
                fieldName=key,
                valueString=value
            )

        with GenomicJobController(GenomicJob.INGEST_RESULT_VIEWED) as controller:
            controller.ingest_records_from_message_broker_data(
                message_record_id=message_broker_record.id,
                event_type=event_type
            )

        result_viewed_genomic_record = self.result_viewed_dao.get_all()

        self.assertIsNotNone(result_viewed_genomic_record)
        self.assertEqual(len(result_viewed_genomic_record), 1)

        result_viewed_genomic_record = result_viewed_genomic_record[0]

        self.assertIsNotNone(result_viewed_genomic_record.event_type)
        self.assertIsNotNone(result_viewed_genomic_record.module_type)

        self.assertEqual(result_viewed_genomic_record.message_record_id, message_broker_record.id)
        self.assertEqual(result_viewed_genomic_record.participant_id, message_broker_record.participantId)
        self.assertEqual(result_viewed_genomic_record.event_type, event_type)
        self.assertTrue(result_viewed_genomic_record.module_type == 'gem')
        self.assertEqual(result_viewed_genomic_record.sample_id, sample_id)

        self.assertEqual(result_viewed_genomic_record.first_viewed, message_broker_record.eventAuthoredTime)
        self.assertEqual(result_viewed_genomic_record.last_viewed, message_broker_record.eventAuthoredTime)

        message_broker_record_two = self.data_generator.create_database_message_broker_record(
            participantId=participant.participantId,
            eventType=event_type,
            eventAuthoredTime=clock.CLOCK.now() + datetime.timedelta(days=1),
            messageOrigin='example@example.com',
            requestBody={'result_type': result_viewed_genomic_record.module_type},
            requestTime=clock.CLOCK.now(),
            responseError='',
            responseCode='200',
            responseTime=clock.CLOCK.now()
        )

        for key, value in message_broker_record_two.requestBody.items():
            self.data_generator.create_database_message_broker_event_data(
                participantId=message_broker_record_two.participantId,
                messageRecordId=message_broker_record_two.id,
                eventType=message_broker_record_two.eventType,
                eventAuthoredTime=message_broker_record_two.eventAuthoredTime,
                fieldName=key,
                valueString=value
            )

        with GenomicJobController(GenomicJob.INGEST_RESULT_VIEWED) as controller:
            controller.ingest_records_from_message_broker_data(
                message_record_id=message_broker_record_two.id,
                event_type=event_type
            )

        result_viewed_genomic_record = self.result_viewed_dao.get_all()

        self.assertIsNotNone(result_viewed_genomic_record)
        self.assertEqual(len(result_viewed_genomic_record), 1)

        result_viewed_genomic_record = result_viewed_genomic_record[0]

        self.assertEqual(result_viewed_genomic_record.first_viewed, message_broker_record.eventAuthoredTime)

        # check updated record has the last viewed time
        self.assertEqual(result_viewed_genomic_record.last_viewed, message_broker_record_two.eventAuthoredTime)
        self.assertEqual(result_viewed_genomic_record.message_record_id, message_broker_record.id)

        sample_id = '223333'

        self.data_generator.create_database_genomic_set_member(
            genomicSetId=gen_set.id,
            sampleId=sample_id,
            participantId=participant.participantId,
            biobankId="2",
            genomeType="aou_wgs",
            genomicWorkflowState=GenomicWorkflowState.AW0
        )

        message_broker_record_two = self.data_generator.create_database_message_broker_record(
            participantId=participant.participantId,
            eventType=event_type,
            eventAuthoredTime=clock.CLOCK.now(),
            messageOrigin='example@example.com',
            requestBody={'result_type': result_module_types[1]},
            requestTime=clock.CLOCK.now(),
            responseError='',
            responseCode='200',
            responseTime=clock.CLOCK.now()
        )

        for key, value in message_broker_record_two.requestBody.items():
            self.data_generator.create_database_message_broker_event_data(
                participantId=message_broker_record_two.participantId,
                messageRecordId=message_broker_record_two.id,
                eventType=message_broker_record_two.eventType,
                eventAuthoredTime=message_broker_record_two.eventAuthoredTime,
                fieldName=key,
                valueString=value
            )

        with GenomicJobController(GenomicJob.INGEST_RESULT_VIEWED) as controller:
            controller.ingest_records_from_message_broker_data(
                message_record_id=message_broker_record_two.id,
                event_type=event_type
            )

        result_viewed_genomic_record = self.result_viewed_dao.get(2)

        self.assertIsNotNone(result_viewed_genomic_record)

        self.assertIsNotNone(result_viewed_genomic_record.event_type)
        self.assertIsNotNone(result_viewed_genomic_record.module_type)

        self.assertEqual(result_viewed_genomic_record.message_record_id, message_broker_record_two.id)
        self.assertEqual(result_viewed_genomic_record.participant_id, message_broker_record_two.participantId)
        self.assertEqual(result_viewed_genomic_record.event_type, event_type)
        self.assertTrue(result_viewed_genomic_record.module_type == 'hdr_v1')
        self.assertEqual(result_viewed_genomic_record.sample_id, sample_id)

        self.assertEqual(result_viewed_genomic_record.first_viewed, message_broker_record_two.eventAuthoredTime)
        self.assertEqual(result_viewed_genomic_record.last_viewed, message_broker_record_two.eventAuthoredTime)

    def test_result_ready_ingestion_message_broker(self):
        event_type = 'result_ready'
        participant = self.data_generator.create_database_participant()
        # https://docs.google.com/document/d/1E1tNSi1mWwhBSCs9Syprbzl5E0SH3c_9oLduG1mzlcY/edit#heading=h.dtikttz25h22
        result_module_types = ['hdr_v1', 'pgx_v1']

        gen_set = self.data_generator.create_database_genomic_set(
            genomicSetName=".",
            genomicSetCriteria=".",
            genomicSetVersion=1
        )

        sample_id = '2222222'

        self.data_generator.create_database_genomic_set_member(
            genomicSetId=gen_set.id,
            sampleId=sample_id,
            participantId=participant.participantId,
            biobankId="1",
            genomeType="aou_wgs",
            genomicWorkflowState=GenomicWorkflowState.AW0
        )

        # HDR Positive Records
        message_broker_record_hdr_positive = self.data_generator.create_database_message_broker_record(
            participantId=participant.participantId,
            eventType=event_type,
            eventAuthoredTime=clock.CLOCK.now(),
            messageOrigin='example@example.com',
            requestBody={
                'result_type': result_module_types[0],
                'hdr_result_status': 'positive',
                'report_revision_number': 1
            },
            requestTime=clock.CLOCK.now(),
            responseError='',
            responseCode='200',
            responseTime=clock.CLOCK.now()
        )

        for key, value in message_broker_record_hdr_positive.requestBody.items():
            self.data_generator.create_database_message_broker_event_data(
                participantId=message_broker_record_hdr_positive.participantId,
                messageRecordId=message_broker_record_hdr_positive.id,
                eventType=message_broker_record_hdr_positive.eventType,
                eventAuthoredTime=message_broker_record_hdr_positive.eventAuthoredTime,
                fieldName=key,
                valueString=value if type(value) is str else None,
                valueInteger=value if type(value) is int else None
            )

        with GenomicJobController(GenomicJob.INGEST_RESULT_READY) as controller:
            controller.ingest_records_from_message_broker_data(
                message_record_id=message_broker_record_hdr_positive.id,
                event_type=event_type
            )

        result_ready_genomic_record = self.report_state_dao.get_all()

        self.assertIsNotNone(result_ready_genomic_record)
        self.assertEqual(len(result_ready_genomic_record), 1)

        result_ready_genomic_record = result_ready_genomic_record[0]

        self.assertIsNotNone(result_ready_genomic_record.event_type)
        self.assertIsNotNone(result_ready_genomic_record.module)

        self.assertEqual(result_ready_genomic_record.message_record_id, message_broker_record_hdr_positive.id)
        self.assertEqual(result_ready_genomic_record.participant_id, message_broker_record_hdr_positive.participantId)
        self.assertEqual(result_ready_genomic_record.event_type, event_type)
        self.assertTrue(result_ready_genomic_record.module == result_module_types[0])
        self.assertEqual(result_ready_genomic_record.sample_id, sample_id)

        self.assertEqual(result_ready_genomic_record.report_revision_number, 1)

        # check for correct report state
        self.assertEqual(result_ready_genomic_record.genomic_report_state, GenomicReportState.HDR_RPT_POSITIVE)
        self.assertEqual(result_ready_genomic_record.genomic_report_state_str, GenomicReportState.HDR_RPT_POSITIVE.name)

        # HDR Uninformative Records
        message_broker_record_hdr_uninformative = self.data_generator.create_database_message_broker_record(
            participantId=participant.participantId,
            eventType=event_type,
            eventAuthoredTime=clock.CLOCK.now(),
            messageOrigin='example@example.com',
            requestBody={
                'result_type': result_module_types[0],
                'hdr_result_status': 'uninformative',
                'report_revision_number': 0
            },
            requestTime=clock.CLOCK.now(),
            responseError='',
            responseCode='200',
            responseTime=clock.CLOCK.now()
        )

        for key, value in message_broker_record_hdr_uninformative.requestBody.items():
            self.data_generator.create_database_message_broker_event_data(
                participantId=message_broker_record_hdr_uninformative.participantId,
                messageRecordId=message_broker_record_hdr_uninformative.id,
                eventType=message_broker_record_hdr_uninformative.eventType,
                eventAuthoredTime=message_broker_record_hdr_uninformative.eventAuthoredTime,
                fieldName=key,
                valueString=value if type(value) is str else None,
                valueInteger=value if type(value) is int else None
            )

        with GenomicJobController(GenomicJob.INGEST_RESULT_READY) as controller:
            controller.ingest_records_from_message_broker_data(
                message_record_id=message_broker_record_hdr_uninformative.id,
                event_type=event_type
            )

        result_ready_genomic_record = self.report_state_dao.get_all()

        self.assertIsNotNone(result_ready_genomic_record)
        self.assertEqual(len(result_ready_genomic_record), 2)

        result_ready_genomic_record = result_ready_genomic_record[1]

        self.assertIsNotNone(result_ready_genomic_record.event_type)
        self.assertIsNotNone(result_ready_genomic_record.module)

        self.assertEqual(result_ready_genomic_record.message_record_id, message_broker_record_hdr_uninformative.id)
        self.assertEqual(result_ready_genomic_record.participant_id,
                         message_broker_record_hdr_uninformative.participantId)
        self.assertEqual(result_ready_genomic_record.event_type, event_type)
        self.assertTrue(result_ready_genomic_record.module == result_module_types[0])
        self.assertEqual(result_ready_genomic_record.sample_id, sample_id)

        self.assertEqual(result_ready_genomic_record.report_revision_number, 0)

        # check for correct report state
        self.assertEqual(result_ready_genomic_record.genomic_report_state, GenomicReportState.HDR_RPT_UNINFORMATIVE)
        self.assertEqual(result_ready_genomic_record.genomic_report_state_str,
                         GenomicReportState.HDR_RPT_UNINFORMATIVE.name)

        # PGX Records
        message_broker_record_pgx = self.data_generator.create_database_message_broker_record(
            participantId=participant.participantId,
            eventType=event_type,
            eventAuthoredTime=clock.CLOCK.now(),
            messageOrigin='example@example.com',
            requestBody={
                'result_type': result_module_types[1],
                'report_revision_number': 0
            },
            requestTime=clock.CLOCK.now(),
            responseError='',
            responseCode='200',
            responseTime=clock.CLOCK.now()
        )

        for key, value in message_broker_record_pgx.requestBody.items():
            self.data_generator.create_database_message_broker_event_data(
                participantId=message_broker_record_pgx.participantId,
                messageRecordId=message_broker_record_pgx.id,
                eventType=message_broker_record_pgx.eventType,
                eventAuthoredTime=message_broker_record_pgx.eventAuthoredTime,
                fieldName=key,
                valueString=value if type(value) is str else None,
                valueInteger=value if type(value) is int else None
            )

        with GenomicJobController(GenomicJob.INGEST_RESULT_READY) as controller:
            controller.ingest_records_from_message_broker_data(
                message_record_id=message_broker_record_pgx.id,
                event_type=event_type
            )

        result_ready_genomic_record = self.report_state_dao.get_all()

        self.assertIsNotNone(result_ready_genomic_record)
        self.assertEqual(len(result_ready_genomic_record), 3)

        result_ready_genomic_record = result_ready_genomic_record[2]

        self.assertIsNotNone(result_ready_genomic_record.event_type)
        self.assertIsNotNone(result_ready_genomic_record.module)

        self.assertEqual(result_ready_genomic_record.message_record_id, message_broker_record_pgx.id)
        self.assertEqual(result_ready_genomic_record.participant_id, message_broker_record_pgx.participantId)
        self.assertEqual(result_ready_genomic_record.event_type, event_type)
        self.assertTrue(result_ready_genomic_record.module == result_module_types[1])
        self.assertEqual(result_ready_genomic_record.sample_id, sample_id)

        self.assertEqual(result_ready_genomic_record.report_revision_number, 0)

        # check for correct report state
        self.assertEqual(result_ready_genomic_record.genomic_report_state, GenomicReportState.PGX_RPT_READY)
        self.assertEqual(result_ready_genomic_record.genomic_report_state_str, GenomicReportState.PGX_RPT_READY.name)

    def test_appointment_event_ingestion_message_broker(self):
        scheduled_event_type = 'appointment_scheduled'
        cancelled_event_type = 'appointment_cancelled'
        participant = self.data_generator.create_database_participant()
        appointment_id = 111

        gen_set = self.data_generator.create_database_genomic_set(
            genomicSetName=".",
            genomicSetCriteria=".",
            genomicSetVersion=1
        )

        sample_id = '2222222'

        self.data_generator.create_database_genomic_set_member(
            genomicSetId=gen_set.id,
            sampleId=sample_id,
            participantId=participant.participantId,
            biobankId="1",
            genomeType="aou_wgs",
            genomicWorkflowState=GenomicWorkflowState.AW0
        )

        message_broker_record = self.data_generator.create_database_message_broker_record(
            participantId=participant.participantId,
            eventType=scheduled_event_type,
            eventAuthoredTime=clock.CLOCK.now(),
            messageOrigin='example@example.com',
            requestBody={
                'module_type': 'hdr',
                'id': appointment_id,
                'source': 'Color',
                'appointment_timestamp': format_datetime(clock.CLOCK.now()),
                'appointment_timezone': 'America/Los_Angeles',
                'location': '123 address st',
                'contact_number': '17348675309',
                'language': 'EN'
            },
            requestTime=clock.CLOCK.now(),
            responseError='',
            responseCode='200',
            responseTime=clock.CLOCK.now()
        )

        for key, value in message_broker_record.requestBody.items():
            self.data_generator.create_database_message_broker_event_data(
                participantId=message_broker_record.participantId,
                messageRecordId=message_broker_record.id,
                eventType=message_broker_record.eventType,
                eventAuthoredTime=message_broker_record.eventAuthoredTime,
                fieldName=key,
                valueString=value if key not in ['id', 'appointment_timestamp'] else None,
                valueInteger=value if key == 'id' else None,
                valueDatetime=value if key == 'appointment_timestamp' else None
            )

        with GenomicJobController(GenomicJob.INGEST_APPOINTMENT) as controller:
            controller.ingest_records_from_message_broker_data(
                message_record_id=message_broker_record.id,
                event_type=scheduled_event_type
            )

        current_appointment_data = self.appointment_dao.get_all()

        # record for each line in message body
        self.assertEqual(len(current_appointment_data), len(message_broker_record.requestBody))

        # should be in every record
        self.assertTrue(all(obj.appointment_id == appointment_id for obj in current_appointment_data))
        self.assertTrue(all(obj.message_record_id == message_broker_record.id for obj in current_appointment_data))
        self.assertTrue(all(obj.module_type == 'hdr' for obj in current_appointment_data))
        self.assertTrue(all(obj.participant_id == participant.participantId for obj in current_appointment_data))
        self.assertTrue(all(obj.event_type == scheduled_event_type for obj in current_appointment_data))
        self.assertTrue(all(obj.event_authored_time is not None for obj in current_appointment_data))

        # should be in some record(s)
        self.assertTrue(any(obj.source is not None for obj in current_appointment_data))
        self.assertTrue(any(obj.appointment_time is not None for obj in current_appointment_data))
        self.assertTrue(any(obj.appointment_timezone is not None for obj in current_appointment_data))
        self.assertTrue(any(obj.location is not None for obj in current_appointment_data))
        self.assertTrue(any(obj.contact_number is not None for obj in current_appointment_data))
        self.assertTrue(any(obj.language is not None for obj in current_appointment_data))

        # should be None for all
        self.assertTrue(all(obj.cancellation_reason is None for obj in current_appointment_data))

        # cancelled records, same appointment_id : id
        message_broker_record = self.data_generator.create_database_message_broker_record(
            participantId=participant.participantId,
            eventType=cancelled_event_type,
            eventAuthoredTime=clock.CLOCK.now(),
            messageOrigin='example@example.com',
            requestBody={
                'module_type': 'hdr',
                'id': appointment_id,
                'source': 'Color',
                'reason': 'participant_initiated'
            },
            requestTime=clock.CLOCK.now(),
            responseError='',
            responseCode='200',
            responseTime=clock.CLOCK.now()
        )

        for key, value in message_broker_record.requestBody.items():
            self.data_generator.create_database_message_broker_event_data(
                participantId=message_broker_record.participantId,
                messageRecordId=message_broker_record.id,
                eventType=message_broker_record.eventType,
                eventAuthoredTime=message_broker_record.eventAuthoredTime,
                fieldName=key,
                valueString=value if key != 'id' else None,
                valueInteger=value if key == 'id' else None,
            )

        with GenomicJobController(GenomicJob.INGEST_APPOINTMENT) as controller:
            controller.ingest_records_from_message_broker_data(
                message_record_id=message_broker_record.id,
                event_type=cancelled_event_type
            )

        current_appointment_data = self.appointment_dao.get_all()
        current_appointment_data = list(filter(lambda x: x.message_record_id == message_broker_record.id,
                                               current_appointment_data))

        # record for each line in message body
        self.assertEqual(len(current_appointment_data), len(message_broker_record.requestBody))

        # should be in every record
        self.assertTrue(all(obj.appointment_id == appointment_id for obj in current_appointment_data))
        self.assertTrue(all(obj.message_record_id == message_broker_record.id for obj in current_appointment_data))
        self.assertTrue(all(obj.module_type == 'hdr' for obj in current_appointment_data))
        self.assertTrue(all(obj.participant_id == participant.participantId for obj in current_appointment_data))
        self.assertTrue(all(obj.event_type == cancelled_event_type for obj in current_appointment_data))
        self.assertTrue(all(obj.event_authored_time is not None for obj in current_appointment_data))

        # should be in some record(s)
        self.assertTrue(any(obj.source is not None for obj in current_appointment_data))
        self.assertTrue(any(obj.cancellation_reason is not None for obj in current_appointment_data))

        self.assertTrue(obj.cancellation_reason == message_broker_record.requestBody['reason'] for obj in
                        current_appointment_data if obj.cancellation_reason is not None)

    @mock.patch('rdr_service.genomic.genomic_job_controller.GenomicJobController.create_incident')
    def test_no_records_from_message_broker_task(self, incident_called):
        no_records_result = GenomicSubProcessResult.NO_RESULTS
        missing_record = GenomicIncidentCode.UNABLE_TO_RESOLVE_MESSAGE_BROKER_RECORD.name

        ingest_jobs_map = {
            GenomicJob.INGEST_INFORMING_LOOP: 'informing_loop_decision',
            GenomicJob.INGEST_RESULT_VIEWED: 'result_viewed',
            GenomicJob.INGEST_RESULT_READY: 'result_ready',
            GenomicJob.INGEST_APPOINTMENT: 'appointment_scheduled'
        }

        for ingest_job, event_type in ingest_jobs_map.items():
            with GenomicJobController(ingest_job) as controller:
                controller.ingest_records_from_message_broker_data(
                    message_record_id=1,
                    event_type=event_type
                )

            all_job_runs = self.job_run_dao.get_all()
            current_job_run = list(filter(lambda x: x.jobId == ingest_job, all_job_runs))[0]

            self.assertIsNotNone(current_job_run)
            self.assertTrue(current_job_run.runResult == no_records_result)

        ingest_incidents = []
        for obj in incident_called.call_args_list:
            if obj[1]['code'] == missing_record:
                ingest_incidents.append(obj[1])

        self.assertTrue(len(ingest_jobs_map.keys()), len(ingest_incidents))
        self.assertTrue(all(obj['slack'] is True for obj in ingest_incidents))



