import random

from faker import Faker

from rdr_service import clock
from rdr_service.dao.ppsc_dao import ParticipantDao
from rdr_service.dao.ppsc_partner_transfer_dao import RTIDataTransferBaseDao
from rdr_service.dao.rex_dao import RexStudyDao, RexParticipantMappingDao
from rdr_service.dao.study_nph_dao import EligibleParticipantsDao, NphParticipantDao
from rdr_service.data_gen.generators.nph import NphDataGenerator
from rdr_service.data_gen.generators.ppsc import PPSCDataGenerator
from rdr_service.model.ppsc_partner_data_transfer import RTINphOptIn
from rdr_service.ppsc.ppsc_partner_data_sync import NphOptInSync
from tests.helpers.unittest_base import BaseTestCase


class PPSCPartnerDataSyncTest(BaseTestCase):
    def setUp(self):
        super().setUp()
        self.ppsc_data_gen = PPSCDataGenerator()
        self.nph_data_gen = NphDataGenerator()
        self.participant_dao = ParticipantDao()
        self.eligible_dao = EligibleParticipantsDao()
        self.nph_opt_in_dao = RTIDataTransferBaseDao(RTINphOptIn)
        self.rex_study_dao = RexStudyDao()
        self.rex_mapping_dao = RexParticipantMappingDao()
        self.nph_participant_dao = NphParticipantDao()
        self.faker = Faker()

        activities = [
            "ENROLLMENT",
            "Consent",
            "Survey Completion",
            "Profile Updates",
            "Withdrawal",
            "Deactivation",
            "Participant Status",
            "Attribution",
            "NPH Opt In"
        ]
        for activity in activities:
            self.ppsc_data_gen.create_database_activity(
                name=activity
            )

    def test_get_eligible_nph_participants(self) -> None:

        # REX study records
        for study in ['rdr', 'nph']:
            self.rex_study_dao.insert(
                self.rex_study_dao.model_type(**{
                    'schema_name': study
                }))

        # eligible participants
        for num in range(10):
            participant = self.ppsc_data_gen.create_database_participant()
            nph_id = f'1{num}000000000'
            self.nph_data_gen.create_database_eligible_participants(
                participant_id=nph_id,
                primary_participant_id=None if num % 2 != 0 else participant.id,
                active=0 if num % 2 != 0 else 1
            )

        current_participant_ids = [obj.id for obj in self.participant_dao.get_all()]

        # profile update records / Nph Opt In Events
        for num in range(4):
            nph_opt_in_elements = {
                'activity_status': 'submitted_yes',
                'activity_date_time': clock.CLOCK.now(),
            }

            participant_event_activity_nph = self.ppsc_data_gen.create_database_participant_event_activity(
                participant_id=current_participant_ids[num],
                activity_id=9  # NPH Opt In
            )
            for key in nph_opt_in_elements:
                self.ppsc_data_gen.create_database_nph_opt_in_event(
                    participant_id=current_participant_ids[num],
                    event_type_name='NPH Opt In',
                    event_id=participant_event_activity_nph.id,
                    data_element_name=key,
                    data_element_value=nph_opt_in_elements[key],
                    event_authored_time=clock.CLOCK.now()
                )

            # add primary consent event
            self.ppsc_data_gen.create_database_consent_event(
                participant_id=current_participant_ids[num],
                event_type_name='Primary Consent',
                event_id=participant_event_activity_nph.id,
                event_authored_time=clock.CLOCK.now(),
                data_element_name='activity_status',
                data_element_value='Yes',
            )

            profile_elements = {
                'piiname_first': self.faker.first_name(),
                'piiname_last': self.faker.last_name(),
                'piicontactinformation_email': self.faker.email(),
                'piicontactinformation_phone': 11111111,
                'streetaddress_piizip': 11111,
                'language_preference': random.choice(['en', 'es'])
            }

            participant_event_activity_profile = self.ppsc_data_gen.create_database_participant_event_activity(
                participant_id=current_participant_ids[num],
                activity_id=4  # Profile Updates
            )
            for key in profile_elements:
                self.ppsc_data_gen.create_database_profile_updates_event(
                    participant_id=current_participant_ids[num],
                    event_type_name='Profile Data',
                    event_id=participant_event_activity_profile.id,
                    data_element_name=key,
                    data_element_value=profile_elements[key],
                    event_authored_time=clock.CLOCK.now()
                )

            # add test participant
            if current_participant_ids[num] == 100000001:
                status_elements = {
                    'activity_status': 'test',
                    'activity_date_time': clock.CLOCK.now()
                }
                for key in status_elements:
                    self.ppsc_data_gen.create_database_participant_status_event(
                        participant_id=current_participant_ids[num],
                        event_type_name='Test Account',
                        event_id=participant_event_activity_profile.id,
                        data_element_name=key,
                        data_element_value=status_elements[key],
                        event_authored_time=clock.CLOCK.now()
                    )

        nph_opt_in_sync = NphOptInSync()
        nph_opt_in_sync.run_sync()

        self.assertEqual(len(nph_opt_in_sync.items_ready_for_sync), 1)
        self.assertEqual(nph_opt_in_sync.usable_nph_objects, [])  # should have been used in sync
        current_sync_ids = [obj.participant_id for obj in nph_opt_in_sync.items_ready_for_sync]

        # check test participant not included
        self.assertTrue(100000001 not in current_sync_ids)

        # eligible records
        updated_eligible_records = [obj for obj in self.eligible_dao.get_all()
                                    if obj.primary_participant_id in current_sync_ids]

        self.assertEqual(len(updated_eligible_records), len(current_sync_ids))
        self.assertTrue(all(obj.active == 1 for obj in updated_eligible_records))

        # nph opt in records
        current_opt_in_records = [obj for obj in self.nph_opt_in_dao.get_all()]
        self.assertEqual(len(current_opt_in_records), len(nph_opt_in_sync.items_ready_for_sync))
        self.assertTrue(all(obj.first_name is not None for obj in current_opt_in_records))
        self.assertTrue(all(obj.last_name is not None for obj in current_opt_in_records))
        self.assertTrue(all(obj.email is not None for obj in current_opt_in_records))
        self.assertTrue(all(obj.phone is not None for obj in current_opt_in_records))
        self.assertTrue(all(obj.zip_code is not None for obj in current_opt_in_records))
        self.assertTrue(all(obj.language_preference is not None and obj.language_preference in [1, 2] for obj in
                            current_opt_in_records))

        # check participant mapping
        current_mappings = self.rex_mapping_dao.get_all()
        self.assertEqual(len(current_mappings), len(nph_opt_in_sync.items_ready_for_sync))

        # check NPH record was created
        current_nph_participants = self.nph_participant_dao.get_all()
        synced_nph_id = current_opt_in_records[0].nph_participant_id
        self.assertTrue(synced_nph_id in [obj.id for obj in current_nph_participants])

    def tearDown(self):
        super().tearDown()
        self.clear_table_after_test("ppsc.activity")
        self.clear_table_after_test("ppsc.participant")
        self.clear_table_after_test("ppsc.participant_event_activity")
        self.clear_table_after_test("ppsc.profile_updates_event")
        self.clear_table_after_test("ppsc.nph_opt_in_event")
        self.clear_table_after_test("nph.participant")
