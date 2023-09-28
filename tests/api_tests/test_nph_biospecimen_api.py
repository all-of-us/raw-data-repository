

from rdr_service.dao.study_nph_dao import NphParticipantDao, NphSiteDao, NphOrderDao, NphOrderedSampleDao
from rdr_service.data_gen.generators.nph import NphDataGenerator

from tests.api_tests.test_nph_participant_api import generate_ordered_sample_data
from tests.helpers.unittest_base import BaseTestCase


class NphBiospecimenAPITest(BaseTestCase):
    def setUp(self):
        super().setUp()
        self.nph_data_gen = NphDataGenerator()
        self.nph_participant = NphParticipantDao()
        self.nph_site_dao = NphSiteDao()
        self.nph_order_dao = NphOrderDao()
        self.nph_order_sample_dao = NphOrderedSampleDao()

        for _ in range(2):
            self.nph_data_gen.create_database_participant()

        for i in range(1, 3):
            self.nph_data_gen.create_database_site(
                external_id=f"nph-test-site-{i}",
                name=f"nph-test-site-name-{i}",
                awardee_external_id="nph-test-hpo",
                organization_external_id="nph-test-org"
            )

        generate_ordered_sample_data()

    def test_biospecimen_non_existent_participant_id(self):
        non_existent_id = 22222222
        response = self.send_get(f'nph/Biospecimen/{non_existent_id}', expected_status=404)
        self.assertIsNotNone(response)
        self.assertEqual(response.status_code, 404)
        self.assertEqual(response.json.get('message'), f'NPH participant {non_existent_id} was not found')

    def test_biospecimen_by_participant_id(self):
        first_nph_participant = self.nph_participant.get_all()[0]
        response = self.send_get(f'nph/Biospecimen/{first_nph_participant.id}')
        self.assertIsNotNone(response)
        self.assertEqual(len(response), 1)

        response_data = response[0]
        self.assertEqual(len(response_data.keys()), 2)
        self.assertEqual(list(response_data.keys()), ['nph_participant_id', 'biospecimens'])
        self.assertEqual(response_data.get('nph_participant_id'), first_nph_participant.id)

        current_participant_orders = self.nph_order_dao.get_orders_by_participant_id(first_nph_participant.id)
        self.assertEqual(len(current_participant_orders), 2)

        current_order_ids = {obj.nph_order_id for obj in current_participant_orders}
        response_order_ids = {obj.get('orderID') for obj in response_data.get('biospecimens')}
        self.assertEqual(current_order_ids, response_order_ids)

        for response_order_id in response_order_ids:
            response_ordered_samples = [obj for obj in response_data.get('biospecimens') if obj.get('orderID') == response_order_id]
            # should have four ordered sample(s) for each order
            self.assertEqual(len(response_ordered_samples), 4)

            for response_ordered_sample in response_ordered_samples:
                self.assertIsNotNone(response_ordered_sample.get('biobankStatus'))
                # should have for stored sample(s) for each ordered sample
                self.assertEqual(len(response_ordered_sample.get('biobankStatus')), 1)

    def tearDown(self):
        super().tearDown()
        self.clear_table_after_test("nph.participant")
        self.clear_table_after_test("nph.site")
        self.clear_table_after_test("nph.order")
        self.clear_table_after_test("nph.ordered_sample")
        self.clear_table_after_test("nph.stored_sample")
