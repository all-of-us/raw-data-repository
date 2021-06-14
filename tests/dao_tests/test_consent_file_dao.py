from datetime import datetime
from typing import List

from rdr_service.dao.consent_dao import ConsentDao
from rdr_service.model.participant_summary import ParticipantSummary
from tests.helpers.unittest_base import BaseTestCase


class ConsentFileDaoTest(BaseTestCase):
    def test_loading_summaries_with_consent(self):
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

        consent_dao = ConsentDao(None)
        self.assertSummariesMatch(
            [
                primary_participant,
                cabor_participant,
                ehr_participant,
                gror_participant
            ],
            consent_dao.get_participants_with_consents_in_range(
                start_date=datetime(2020, 3, 1),
                end_date=datetime(2020, 7, 1)
            )
        )
        self.assertSummariesMatch(
            [
                primary_participant,
                cabor_participant,
                ehr_participant,
                gror_participant,
                later_primary_participant,
                later_cabor_participant,
                later_ehr_participant,
                later_gror_participant
            ],
            consent_dao.get_participants_with_consents_in_range(
                start_date=datetime(2020, 3, 1)
            )
        )

    def assertSummariesMatch(self, expected_list: List[ParticipantSummary], actual_list: List[ParticipantSummary]):
        self.assertEqual(len(expected_list), len(actual_list))

        actual_id_list = [actual.participantId for actual in actual_list]
        for expected_summary in expected_list:
            self.assertIn(expected_summary.participantId, actual_id_list)

    def _init_summary_with_consent_dates(self, primary, cabor=None, ehr=None, gror=None):
        return self.data_generator.create_database_participant_summary(
            consentForStudyEnrollmentFirstYesAuthored=primary,
            consentForCABoRAuthored=cabor,
            consentForElectronicHealthRecordsAuthored=ehr,
            consentForGenomicsRORAuthored=gror
        )
