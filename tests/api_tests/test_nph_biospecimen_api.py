from dateutil import parser
from datetime import datetime

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

        # nph activities
        for activity_name in ['ENROLLMENT', 'PAIRING']:
            self.nph_data_gen.create_database_activity(
                name=activity_name
            )

        # nph pairing event type
        self.nph_data_gen.create_database_pairing_event_type(name="INITIAL")

        for _ in range(2):
            self.nph_data_gen.create_database_participant()

        for i in range(1, 3):
            self.nph_data_gen.create_database_site(
                external_id=f"nph-test-site-{i}",
                name=f"nph-test-site-name-{i}",
                awardee_external_id=f"nph-test-hpo-{i}",
                organization_external_id=f"nph-test-org-{i}"
            )

    def create_nph_biospecimen_data(self, **kwargs):
        num_orders = kwargs.get('num_orders', 2)
        num_ordered_samples = kwargs.get('num_order_samples', 4)
        num_stored_samples = kwargs.get('num_stored_samples', 1)
        created_date = kwargs.get('created_dates', clock.CLOCK.now())

        category = self.sms_data_gen.create_database_study_category(
            type_label="Test",
        )

        for num, participant in enumerate(self.nph_participant.get_all()):

            # create pairing data / should be only 2 sites for pairing
            self.nph_data_gen.create_database_pairing_event(
                participant_id=participant.id,
                event_authored_time=datetime(2023, 1, 1, 12, 1),
                site_id=2 if num % 2 != 0 else 1
            )

            for i in range(num_orders):
                order = self.sms_data_gen.create_database_order(
                    nph_order_id=f"10{participant.id + (i + 1) + participant.biobank_id}",
                    participant_id=participant.id,
                    notes="Test",
                    category_id=category.id,
                    created=created_date,
                    modified=created_date
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

    def test_biospecimen_by_last_modified_returns_correctly(self):
        fake_date_one = parser.parse('2020-05-28T08:00:01-05:00')
        fake_date_two = parser.parse('2020-05-30T08:00:01-05:00')
        fake_date_three = parser.parse('2020-05-31T08:00:01-05:00')

        with clock.FakeClock(fake_date_two):
            self.create_nph_biospecimen_data(
                num_orders=1,
                created_date=fake_date_two
            )

        response = self.send_get(f'nph/Biospecimen?last_modified={fake_date_three}')
        self.assertIsNotNone(response)
        self.assertEqual(response.get('entry'), [])

        response = self.send_get(f'nph/Biospecimen?last_modified={fake_date_one}')
        self.assertIsNotNone(response)
        self.assertIsNotNone(response.get('entry'))
        self.assertEqual(len(response.get('entry')), 2)

    def test_biospecimen_last_modified_pagination(self):
        fake_date_one = parser.parse('2020-05-28T08:00:01-05:00')
        fake_date_two = parser.parse('2020-05-30T08:00:01-05:00')
        first_nph_participant = self.nph_participant.get_all()[0]
        second_nph_participant = self.nph_participant.get_all()[1]
        num_count = 1

        with clock.FakeClock(fake_date_two):
            self.create_nph_biospecimen_data(
                created_date=fake_date_two
            )

        response = self.send_get(f'nph/Biospecimen?last_modified={fake_date_one}&_count={num_count}')
        self.assertIsNotNone(response)
        self.assertIsNotNone(response.get('entry'))
        self.assertEqual(len(response.get('entry')), num_count)

        response_data = response.get('entry')[0]['resource'][0]
        self.assertEqual(list(response_data.keys()), ['nph_participant_id', 'biospecimens'])
        self.assertEqual(response_data.get('nph_participant_id'), first_nph_participant.id)

        # should have next link
        self.assertIsNotNone(response.get('link'))
        self.assertEqual(response['link'][0]['relation'], 'next')

        self.assertEqual(len(response['entry']), 1)
        self.assertIsNotNone(response['entry'][0]['fullUrl'])

        current_response_nph_pid = response['entry'][0]['resource'][0]['nph_participant_id']
        self.assertEqual(current_response_nph_pid, first_nph_participant.id)
        self.assertTrue(f'rdr/v1/nph/Biospecimen/{current_response_nph_pid}'
                        in response['entry'][0]['fullUrl'])

        next_pagination_link = response['link'][0]['url'].split('v1/')[-1]

        next_response = self.send_get(next_pagination_link)
        self.assertIsNotNone(next_response.get('entry'))
        self.assertEqual(len(next_response.get('entry')), num_count)

        response_data = next_response.get('entry')[0]['resource'][0]
        self.assertEqual(list(response_data.keys()), ['nph_participant_id', 'biospecimens'])
        self.assertEqual(response_data.get('nph_participant_id'), second_nph_participant.id)

        # should not have next link
        self.assertIsNone(next_response.get('link'))

        self.assertEqual(len(next_response['entry']), 1)
        self.assertIsNotNone(next_response['entry'][0]['fullUrl'])

        current_response_nph_pid = next_response['entry'][0]['resource'][0]['nph_participant_id']
        self.assertEqual(current_response_nph_pid, second_nph_participant.id)
        self.assertTrue(f'rdr/v1/nph/Biospecimen/{current_response_nph_pid}'
                        in next_response['entry'][0]['fullUrl'])

    # FILTER DB ATTRIBUTE MAPPING FOR NPH SITE = {
    #     'nph_paired_site': 'external_id',
    #     'nph_paired_org': 'organization_external_id',
    #     'nph_paired_awardee': 'awardee_external_id'
    # }

    def test_filter_by_nph_paired_site(self):
        self.create_nph_biospecimen_data()
        first_nph_site = self.nph_site_dao.get_all()[0]
        response = self.send_get(f'nph/Biospecimen?nph_paired_site={first_nph_site.external_id}')
        self.assertIsNotNone(response)

    def test_filter_by_nph_paired_org(self):
        self.create_nph_biospecimen_data()
        first_nph_site = self.nph_site_dao.get_all()[0]
        response = self.send_get(f'nph/Biospecimen?nph_paired_org={first_nph_site.organization_external_id}')
        self.assertIsNotNone(response)

    def test_filter_by_nph_paired_awardee(self):
        self.create_nph_biospecimen_data()
        first_nph_site = self.nph_site_dao.get_all()[0]
        response = self.send_get(f'nph/Biospecimen?nph_paired_awardee={first_nph_site.awardee_external_id}')
        self.assertIsNotNone(response)

    def tearDown(self):
        super().tearDown()
        self.clear_table_after_test("nph.participant")
        self.clear_table_after_test("nph.site")
        self.clear_table_after_test("nph.order")
        self.clear_table_after_test("nph.ordered_sample")
        self.clear_table_after_test("nph.stored_sample")
        self.clear_table_after_test("nph.activity")
        self.clear_table_after_test("nph.pairing_event_type")
        self.clear_table_after_test("nph.participant_event_activity")
        self.clear_table_after_test("nph.pairing_event")
