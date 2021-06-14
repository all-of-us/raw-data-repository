from datetime import datetime

from rdr_service.dao.consent_dao import ConsentDao
from rdr_service.model.consent_file import ConsentSyncStatus, ConsentType
from tests.helpers.unittest_base import BaseTestCase


class ConsentFileDaoTest(BaseTestCase):
    def setUp(self, *args, **kwargs) -> None:
        super(ConsentFileDaoTest, self).setUp(*args, **kwargs)
        self.consent_dao = ConsentDao(None)

    def test_loading_summaries_with_consent(self):
        """Check that participant summaries with any consents in the given time range are loaded"""
        primary_participant = self._init_summary_with_consent_dates(
            primary=datetime(2020, 4, 1)
        )
        later_primary_participant = self._init_summary_with_consent_dates(
            primary=datetime(2021, 1, 1)
        )
        cabor_participant = self._init_summary_with_consent_dates(
            primary=datetime(2019, 8, 27),
            cabor=datetime(2020, 4, 27)
        )
        later_cabor_participant = self._init_summary_with_consent_dates(
            primary=datetime(2019, 8, 27),
            cabor=datetime(2021, 1, 27)
        )
        ehr_participant = self._init_summary_with_consent_dates(
            primary=datetime(2017, 9, 2),
            ehr=datetime(2020, 5, 2)
        )
        later_ehr_participant = self._init_summary_with_consent_dates(
            primary=datetime(2017, 9, 2),
            ehr=datetime(2021, 3, 2)
        )
        gror_participant = self._init_summary_with_consent_dates(
            primary=datetime(2020, 1, 4),
            gror=datetime(2020, 6, 7)
        )
        later_gror_participant = self._init_summary_with_consent_dates(
            primary=datetime(2020, 1, 4),
            gror=datetime(2021, 3, 7)
        )
        self._init_summary_with_consent_dates(  # make one participant that has everything before the date window
            primary=datetime(2019, 7, 1),
            cabor=datetime(2019, 8, 1),
            ehr=datetime(2019, 9, 1),
            gror=datetime(2019, 10, 1)
        )

        self.assertListsMatch(
            expected_list=[
                primary_participant,
                cabor_participant,
                ehr_participant,
                gror_participant
            ],
            actual_list=self.consent_dao.get_participants_with_consents_in_range(
                start_date=datetime(2020, 3, 1),
                end_date=datetime(2020, 7, 1)
            ),
            id_attribute='participantId'
        )
        self.assertListsMatch(
            expected_list=[
                primary_participant,
                cabor_participant,
                ehr_participant,
                gror_participant,
                later_primary_participant,
                later_cabor_participant,
                later_ehr_participant,
                later_gror_participant
            ],
            actual_list=self.consent_dao.get_participants_with_consents_in_range(
                start_date=datetime(2020, 3, 1)
            ),
            id_attribute='participantId'
        )

    def test_getting_files_to_correct(self):
        """Test that all the consent files that need correcting are loaded"""
        # Create files that are ready to sync
        self.data_generator.create_database_consent_file(
            type=ConsentType.PRIMARY,
            sync_status=ConsentSyncStatus.READY_FOR_SYNC
        )
        self.data_generator.create_database_consent_file(
            type=ConsentType.CABOR,
            sync_status=ConsentSyncStatus.READY_FOR_SYNC
        )
        self.data_generator.create_database_consent_file(
            type=ConsentType.EHR,
            sync_status=ConsentSyncStatus.READY_FOR_SYNC
        )
        self.data_generator.create_database_consent_file(
            type=ConsentType.GROR,
            sync_status=ConsentSyncStatus.READY_FOR_SYNC
        )
        # Create files that need correcting
        not_ready_primary = self.data_generator.create_database_consent_file(
            type=ConsentType.PRIMARY,
            sync_status=ConsentSyncStatus.NEEDS_CORRECTING
        )
        not_ready_cabor = self.data_generator.create_database_consent_file(
            type=ConsentType.CABOR,
            sync_status=ConsentSyncStatus.NEEDS_CORRECTING
        )
        not_ready_ehr = self.data_generator.create_database_consent_file(
            type=ConsentType.EHR,
            sync_status=ConsentSyncStatus.NEEDS_CORRECTING
        )
        not_ready_gror = self.data_generator.create_database_consent_file(
            type=ConsentType.GROR,
            sync_status=ConsentSyncStatus.NEEDS_CORRECTING
        )

        self.assertListsMatch(
            expected_list=[
                not_ready_primary, not_ready_cabor, not_ready_ehr, not_ready_gror
            ],
            actual_list=self.consent_dao.get_files_needing_correction(),
            id_attribute='id'
        )

    def assertListsMatch(self, expected_list, actual_list, id_attribute):
        self.assertEqual(len(expected_list), len(actual_list))

        actual_id_list = [getattr(actual, id_attribute) for actual in actual_list]
        for expected_summary in expected_list:
            self.assertIn(getattr(expected_summary, id_attribute), actual_id_list)

    def _init_summary_with_consent_dates(self, primary, cabor=None, ehr=None, gror=None):
        return self.data_generator.create_database_participant_summary(
            consentForStudyEnrollmentFirstYesAuthored=primary,
            consentForCABoRAuthored=cabor,
            consentForElectronicHealthRecordsAuthored=ehr,
            consentForGenomicsRORAuthored=gror
        )
