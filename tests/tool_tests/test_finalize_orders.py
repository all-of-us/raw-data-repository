from datetime import datetime
import mock

from rdr_service.model.biobank_order import BiobankOrder, BiobankOrderHistory, BiobankOrderedSample,\
    BiobankOrderedSampleHistory
from rdr_service.participant_enums import OrderStatus
from rdr_service.tools.tool_libs.finalize_orders import FinalizeOrdersClass
from tests.helpers.unittest_base import BaseTestCase


class FinalizeOrdersTest(BaseTestCase):
    def setUp(self):
        super().setUp()

    @staticmethod
    def run_tool(input_data):
        environment = mock.MagicMock()
        environment.project = 'unit_test'

        args = mock.MagicMock()

        # Patching to bypass opening file and provide input data
        with mock.patch('rdr_service.tools.tool_libs.finalize_orders.open'),\
                mock.patch('rdr_service.tools.tool_libs.finalize_orders.csv') as mock_csv:
            mock_csv.DictReader.return_value = input_data

            finalize_orders_tool = FinalizeOrdersClass(args, environment)
            finalize_orders_tool.run()

    def test_order_finalization(self):
        participant_summary = self.data_generator.create_database_participant_summary()
        participant_id = participant_summary.participantId

        biobank_order_id = 'WEB1234'
        self.data_generator.create_database_biobank_order(biobankOrderId=biobank_order_id, participantId=participant_id)
        self.data_generator.create_database_biobank_ordered_sample(biobankOrderId=biobank_order_id, test='1ED10')
        self.data_generator.create_database_biobank_ordered_sample(biobankOrderId=biobank_order_id, test='1SAL')

        finalized_time = datetime(2020, 7, 3, 15, 1, 23)
        self.run_tool([{
            'Participant ID': f'P{participant_id}',
            'MayoLINK ID': biobank_order_id,
            'Finalized Time (UTC)': finalized_time.strftime('%Y-%m-%d %H:%M:%S')
        }])

        biobank_order = self.session.query(BiobankOrder).filter(BiobankOrder.biobankOrderId == biobank_order_id).one()
        self.assertEqual(finalized_time, biobank_order.finalizedTime)

        order_history = self.session.query(BiobankOrderHistory).filter(
            BiobankOrder.biobankOrderId == biobank_order_id
        ).one()
        self.assertEqual(finalized_time, order_history.finalizedTime)

        ordered_samples = self.session.query(BiobankOrderedSample).filter(
            BiobankOrderedSample.biobankOrderId == biobank_order_id
        ).all()
        for ordered_sample in ordered_samples:
            self.assertEqual(finalized_time, ordered_sample.finalized)

        ordered_samples_history = self.session.query(BiobankOrderedSampleHistory).filter(
            BiobankOrderedSample.biobankOrderId == biobank_order_id
        ).all()
        for ordered_sample_history in ordered_samples_history:
            self.assertEqual(finalized_time, ordered_sample_history.finalized)

        self.session.refresh(participant_summary)  # Reload data for the object we held onto
        self.assertEqual(OrderStatus.FINALIZED, participant_summary.sampleOrderStatus1ED10)
        self.assertEqual(finalized_time, participant_summary.sampleOrderStatus1ED10Time)
        self.assertEqual(OrderStatus.FINALIZED, participant_summary.sampleOrderStatus1SAL)
        self.assertEqual(finalized_time, participant_summary.sampleOrderStatus1SALTime)
