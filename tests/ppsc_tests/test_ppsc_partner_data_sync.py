import random

from faker import Faker

from rdr_service import clock
from rdr_service.dao.ppsc_dao import ParticipantDao
from rdr_service.data_gen.generators.nph import NphDataGenerator
from rdr_service.data_gen.generators.ppsc import PPSCDataGenerator
from rdr_service.ppsc.ppsc_partner_data_sync import NphOptInSync
from tests.helpers.unittest_base import BaseTestCase


class PPSCPartnerDataSyncTest(BaseTestCase):
    def setUp(self):
        super().setUp()
        self.ppsc_data_gen = PPSCDataGenerator()
        self.nph_data_gen = NphDataGenerator()
        self.participant_dao = ParticipantDao()
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

            profile_elements = {
                'piiname_first': self.faker.first_name(),
                'piiname_last': self.faker.last_name(),
                'piicontactinformation_email': self.faker.email(),
                'piicontactinformation_phone': 11111111,
                'streetaddress_piizip': 11111,
                'language_preference': random.choice(['English', 'Spanish'])
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

        nph_opt_in_sync = NphOptInSync()
        nph_opt_in_sync.run_sync()

