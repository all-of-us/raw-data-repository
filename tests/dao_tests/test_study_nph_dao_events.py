import datetime

from rdr_service.dao.study_nph_dao import NphSiteDao, NphPairingEventDao, NphParticipantDao
from rdr_service.data_gen.generators.nph import NphDataGenerator
from tests.helpers.unittest_base import BaseTestCase


class NphPairingEventTest(BaseTestCase):
    def setUp(self):
        super().setUp()
        self.nph_site_dao = NphSiteDao()
        self.nph_pairing_event_dao = NphPairingEventDao()
        self.nph_data_gen = NphDataGenerator()
        self.participant_dao = NphParticipantDao()

    def test_get_participant_paired_site(self):

        self.initialize_data()

        # Create test participants and pairing events
        for _ in range(2):
            participant = self.nph_data_gen.create_database_participant()
            self.nph_data_gen.create_database_pairing_event(
                participant_id=participant.id,
                event_authored_time=datetime.datetime(2023, 1, 1, 12, 0),
                site_id=1
            )

        self.nph_data_gen.create_database_pairing_event(
            participant_id=100000000,
            event_authored_time=datetime.datetime(2023, 1, 1, 12, 1),
            site_id=2
        )

        paired_site_p1 = self.nph_pairing_event_dao.get_participant_paired_site(100000000)
        self.assertEqual(2, paired_site_p1.site_id)

        paired_site_p2 = self.nph_pairing_event_dao.get_participant_paired_site(100000001)
        self.assertEqual(1, paired_site_p2.site_id)

    def initialize_data(self):
        for activity_name in ['ENROLLMENT', 'PAIRING', 'CONSENT']:
            self.nph_data_gen.create_database_activity(
                name=activity_name
            )

        self.nph_data_gen.create_database_pairing_event_type(name="INITIAL")

        for i in range(1, 3):
            self.nph_data_gen.create_database_site(
                external_id=f"nph-test-site-{i}",
                name=f"nph-test-site-{i}",
                awardee_external_id="nph-test-hpo",
                organization_external_id="nph-test-org"
            )



