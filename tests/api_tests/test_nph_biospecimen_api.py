from typing import Union, List

from rdr_service import clock
from rdr_service.ancillary_study_resources.nph.enums import StoredSampleStatus
from rdr_service.dao.study_nph_dao import NphParticipantDao, NphSiteDao, NphOrderDao, NphOrderedSampleDao
from rdr_service.data_gen.generators.nph import NphDataGenerator, NphSmsDataGenerator

from tests.helpers.unittest_base import BaseTestCase


class NphBiospecimenAPITest(BaseTestCase):
    def setUp(self):
        super().setUp()
        self.nph_data_gen = NphDataGenerator()
        self.sms_data_gen = NphSmsDataGenerator()
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

    def create_nph_biospecimen_data(self, **kwargs):
        num_orders = kwargs.get('num_orders', 2)
        num_ordered_samples = kwargs.get('num_order_samples', 4)
        num_stored_samples = kwargs.get('num_stored_samples', 1)
        created_dates = kwargs.get('created_dates', clock.CLOCK.now())

        category = self.sms_data_gen.create_database_study_category(
            type_label="Test",
        )

        def set_dates(dates: Union[List[int], int], idx: int):
            if type(dates) is list:
                return dates[idx]
            return dates

        for participant in self.nph_participant.get_all():
            for i in range(num_orders):
                set_date = set_dates(created_dates, idx=0 if i % 2 == 0 else 1)
                order = self.sms_data_gen.create_database_order(
                    nph_order_id=f"10{participant.id + (i + 1) + participant.biobank_id}",
                    participant_id=participant.id,
                    notes="Test",
                    category_id=category.id,
                    created=set_date,
                    modified=set_date
                )
                for _ in range(num_ordered_samples):
                    self.sms_data_gen.create_database_ordered_sample(
                        order_id=order.id,
                        nph_sample_id=f'11{participant.id}'
                    )
                    for _ in range(num_stored_samples):
                        self.sms_data_gen.create_database_stored_sample(
                            biobank_id=participant.biobank_id,
                            sample_id=f'11{participant.id}',
                            lims_id=f"33{participant.id}",
                            status=StoredSampleStatus.RECEIVED
                        )

    def test_biospecimen_non_existent_participant_id(self):
        non_existent_id = 22222222
        response = self.send_get(f'nph/Biospecimen/{non_existent_id}', expected_status=404)
        self.assertIsNotNone(response)
        self.assertEqual(response.status_code, 404)
        self.assertEqual(response.json.get('message'), f'NPH participant {non_existent_id} was not found')

    def test_biospecimen_by_participant_id(self):
        self.create_nph_biospecimen_data()
        # generate_ordered_sample_data()

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

    # def test_biospecimen_by_last_modified(self):
    #     fake_date_one = parser.parse('2020-05-30T08:00:01-05:00')
    #     fake_date_two = parser.parse('2020-05-31T08:00:01-05:00')
    #     self.create_nph_biospecimen_data(
    #         created_dates=[
    #             fake_date_one,
    #             fake_date_two
    #         ]
    #     )
    #     response = self.send_get(f'nph/Biospecimen?last_modified={fake_date_one}')

    # def test_biospecimen_count_pagination(self):
    #     ...
    #
    # def test_biospecimen(self):
    #     fake_date_one = parser.parse('2020-05-30T08:00:01-05:00')
    #     # response = self.send_get('nph/Biospecimen/100000000')
    #     response = self.send_get(f'nph/Biospecimen?last_modified={fake_date_one}&_count=1')
    #     print('Darryl')
    #
    def tearDown(self):
        super().tearDown()
        self.clear_table_after_test("nph.participant")
        self.clear_table_after_test("nph.site")
        self.clear_table_after_test("nph.order")
        self.clear_table_after_test("nph.ordered_sample")
        self.clear_table_after_test("nph.stored_sample")
