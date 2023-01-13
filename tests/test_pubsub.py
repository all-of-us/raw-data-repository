import datetime
import json

import mock

from rdr_service import config
from rdr_service.cloud_utils.gcp_google_pubsub import submit_pipeline_pubsub_msg, submit_pipeline_pubsub_msg_from_model
from rdr_service.dao.participant_dao import ParticipantDao
from rdr_service.dao.participant_summary_dao import ParticipantSummaryDao
from rdr_service.dao.physical_measurements_dao import PhysicalMeasurementsDao
from rdr_service.model.participant import Participant
from rdr_service.model.utils import to_client_participant_id
from tests.helpers.unittest_base import BaseTestCase
from tests.test_data import load_biobank_order_json, load_measurement_json


@mock.patch('rdr_service.cloud_utils.gcp_google_pubsub._INSTANCE_MAPPING', {'localhost': 'abc'})


class PubSubTest(BaseTestCase):

    def setUp(self, *args, **kwargs):
        super().setUp(*args, **kwargs)

        self.participant = Participant(participantId=123, biobankId=555)
        self.participant_dao = ParticipantDao()
        self.participant_dao.insert(self.participant)
        self.summary_dao = ParticipantSummaryDao()
        self.path = "Participant/%s/BiobankOrder" % to_client_participant_id(self.participant.participantId)

        TIME_1 = datetime.datetime(2022, 12, 13)
        self.measurement_json = json.dumps(load_measurement_json(self.participant.participantId, TIME_1.isoformat()))

        # Override the settings, so we can fully test the pubsub code.
        config.override_setting('pdr_pipeline', {
            "allowed_projects": [
                "localhost"
            ],
            "excluded_table_list": [
                "log_position",
                "questionnaire_response_answer"
            ]
        })

    @mock.patch('rdr_service.cloud_utils.gcp_google_pubsub.publish_pubsub_message')
    def test_simple_valid_pubsub_msg(self, mock_pub_func):
        """ Test a simple and successful message publication """
        mock_pub_func.return_value={'messageIds': ['123']}

        resp = submit_pipeline_pubsub_msg(table='participant', action='update', pk_columns=['participant_id'],
                                          pk_values=['123456789'])
        self.assertIsInstance(resp, dict)
        self.assertIn('messageIds', resp)
        self.assertEqual(resp['messageIds'][0], '123')

        self.assertTrue(mock_pub_func.called)

    @mock.patch('rdr_service.cloud_utils.gcp_google_pubsub.publish_pubsub_message')
    def test_invalid_arguments(self, mock_pub_func):
        """ Test pub/sub with invalid arguments """
        mock_pub_func.return_value = {'messageIds': ['123']}

        # Test with project id not in allowed list.
        resp = submit_pipeline_pubsub_msg(table='participant', action='update', pk_columns=['participant_id'],
                                          pk_values=['123456789'], project='some-other-project-id')
        self.assertIsInstance(resp, dict)
        self.assertIn('error', resp)
        self.assertFalse(mock_pub_func.called)

        mock_pub_func.reset_mock(return_value={'messageIds': ['123']})

        # Test with invalid action
        resp = submit_pipeline_pubsub_msg(table='participant', action='remove', pk_columns=['participant_id'],
                                          pk_values=['123456789'])
        self.assertIsInstance(resp, dict)
        self.assertIn('error', resp)
        self.assertFalse(mock_pub_func.called)

    @mock.patch('rdr_service.cloud_utils.gcp_google_pubsub.publish_pubsub_message')
    def test_mismatched_columns_and_values(self, mock_pub_func):
        """ Test for mismatched number of PK columns and values """
        mock_pub_func.return_value = {'messageIds': ['123']}
        pk_columns = ['participant_id', 'other']
        pk_values = ['123456789']

        resp = submit_pipeline_pubsub_msg(table='participant', action='remove', pk_columns=pk_columns,
                                          pk_values=pk_values)
        self.assertIsInstance(resp, dict)
        self.assertIn('error', resp)
        self.assertFalse(mock_pub_func.called)

        mock_pub_func.reset_mock(return_value={'messageIds': ['123']})
        pk_columns = ['participant_id', 'other']
        pk_values = [['123456789', '123', 'abc'], ['987654321', '321', 'cba']]

        resp = submit_pipeline_pubsub_msg(table='participant', action='remove', pk_columns=pk_columns,
                                          pk_values=pk_values)
        self.assertIsInstance(resp, dict)
        self.assertIn('error', resp)
        self.assertFalse(mock_pub_func.called)

    @mock.patch('rdr_service.cloud_utils.gcp_google_pubsub.publish_pubsub_message')
    def test_pubsub_with_model_and_api(self, mock_pub_func):
        """ Test sending a pub/sub message from a SQLAlchemy model with child records via API call. """
        # Testing a Biobank order with sample records will test the recursive loop code, and
        #         we should see multiple Pub/Sub messages are sent.
        mock_pub_func.return_value = {'messageIds': ['123']}

        # Simulate a Biobank order API call with multiple order sample records attached.
        self.summary_dao.insert(self.participant_summary(self.participant))
        order_json = load_biobank_order_json(self.participant.participantId, filename="biobank_order_2.json")
        result = self.send_post(self.path, order_json)

        # Test response properties
        self.assertIsInstance(result, dict)
        self.assertEqual(result['id'], 'WEB1YLHV123')
        self.assertEqual(result['origin'], 'example')

        # Test Pub/Sub messages successfully sent.
        self.assertTrue(mock_pub_func.called)
        self.assertEqual(mock_pub_func.call_count, 3)

    @mock.patch('rdr_service.cloud_utils.gcp_google_pubsub.publish_pubsub_message')
    def test_pubsub_from_model(self, mock_pub_func):
        """ Create a model object with child model records and test sending pub/sub messages. """
        mock_pub_func.return_value = {'messageIds': ['123']}

        dao = PhysicalMeasurementsDao()

        # Setup participant summary record and create a physical measurements model object.
        self.summary_dao.insert(self.participant_summary(self.participant))
        measurement = dao.from_client_json(json.loads(self.measurement_json))

        self.assertIsNotNone(measurement.createdSiteId)
        self.assertIsNotNone(measurement.finalizedSiteId)
        measurement.participantId = self.participant.participantId
        # Save to the database so the primary key value is set and available.
        dao.insert(measurement)

        parents = submit_pipeline_pubsub_msg_from_model(measurement, 'rdr')

        # Test Pub/Sub messages successfully sent.
        self.assertEqual(len(parents), 2)
        self.assertIn('physical_measurements', parents)
        self.assertIn('measurement', parents)

        self.assertTrue(mock_pub_func.called)
        self.assertEqual(mock_pub_func.call_count, 2)
