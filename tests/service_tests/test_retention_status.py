from datetime import datetime, timedelta

from rdr_service.participant_enums import ParticipantCohort
from rdr_service.services.retention_calculation import Consent, RetentionEligibility, RetentionEligibilityDependencies
from tests.helpers.unittest_base import BaseTestCase


class RetentionCalculationTest(BaseTestCase):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.uses_database = False

    def setUp(self, *args, **kwargs):
        super().setUp(*args, **kwargs)
        self.participant_info = self._get_eligible_participant()

    def test_participant_eligibility(self):
        # Set up an eligible participant and make sure they show as eligible
        self.assertTrue(
            RetentionEligibility(self.participant_info).is_eligible
        )

    def test_deceased_not_eligible(self):
        self.participant_info.is_deceased = True
        self.assertFalse(
            RetentionEligibility(self.participant_info).is_eligible
        )

    def test_withdrawn_not_eligible(self):
        self.participant_info.is_withdrawn = True
        self.assertFalse(
            RetentionEligibility(self.participant_info).is_eligible
        )

    def test_missing_ehr_intent_not_eligible(self):
        self.participant_info.first_ehr_consent = None
        self.assertFalse(
            RetentionEligibility(self.participant_info).is_eligible
        )

    def test_missing_basics_not_eligible(self):
        self.participant_info.basics_response_timestamp = None
        self.assertFalse(
            RetentionEligibility(self.participant_info).is_eligible
        )

    def test_default_eligible_date(self):
        self.assertEqual(
            datetime(2021, 12, 1),
            RetentionEligibility(self.participant_info).retention_eligible_date
        )

    def test_date_for_not_eligible(self):
        # Make sure the eligibility date returns as None for a participant that is not eligible
        self.participant_info.first_ehr_consent = None
        self.assertIsNone(
            RetentionEligibility(self.participant_info).retention_eligible_date
        )

    def test_for_latest(self):
        self.participant_info.basics_response_timestamp = datetime(2023, 5, 12)
        self.assertEqual(
            self.participant_info.basics_response_timestamp,
            RetentionEligibility(self.participant_info).retention_eligible_date
        )

    def test_default_not_active(self):
        self.assertFalse(
            RetentionEligibility(self.participant_info).is_actively_retained
        )

    def test_is_active_cope(self):
        # Check that a recent survey sets them as actively retained
        self.participant_info.latest_cope_response_timestamp = datetime.today()
        self.assertTrue(
            RetentionEligibility(self.participant_info).is_actively_retained
        )

    def test_recent_gror_cohort_3(self):
        # Check that a cohort 3 participant with a recent GROR *is not* actively retained
        self.participant_info.consent_cohort = ParticipantCohort.COHORT_3
        self.participant_info.gror_response_timestamp = datetime.today()
        self.assertFalse(
            RetentionEligibility(self.participant_info).is_actively_retained
        )

    def test_recent_gror_cohort_1(self):
        # ... but any earlier cohort with a recent GROR *is* actively retained
        self.participant_info.consent_cohort = ParticipantCohort.COHORT_1
        self.participant_info.gror_response_timestamp = datetime.today()
        self.assertTrue(
            RetentionEligibility(self.participant_info).is_actively_retained
        )

    def test_no_active_retention_date(self):
        # Check that the default, non-active, information gives None for the active retention date
        self.participant_info = self._get_eligible_participant()
        self.assertIsNone(
            RetentionEligibility(self.participant_info).last_active_retention_date
        )

    def test_latest_activity_date(self):
        # Check that the latest activity date is used as the active retention date
        self.participant_info.remote_pm_response_timestamp = datetime(2023, 1, 17)
        self.participant_info.sdoh_response_timestamp = datetime(2023, 2, 4)
        self.assertEqual(
            self.participant_info.sdoh_response_timestamp,
            RetentionEligibility(self.participant_info).last_active_retention_date
        )

    def test_default_not_passively_retained(self):
        # Check that the default data for the tests is not passively retained
        self.assertFalse(
            RetentionEligibility(self.participant_info).is_passively_retained
        )

    def test_ehr_upload_is_passive(self):
        # Check that an eligible participant with recent EHR data is passively retained
        self.participant_info.has_uploaded_ehr_file = True
        self.participant_info.latest_ehr_upload_timestamp = datetime.today() - timedelta(days=450)
        self.assertTrue(
            RetentionEligibility(self.participant_info).is_passively_retained
        )

    @classmethod
    def _get_eligible_participant(cls):
        return cls._build_retention_data(
            first_ehr_authored=datetime(2021, 10, 10),
            basics_authored=datetime(2021, 10, 10),
            overallhealth_authored=datetime(2021, 10, 10),
            lifestyle_authored=datetime(2021, 10, 10),
            dna_samples_timestamp=datetime(2021, 12, 1)
        )

    @classmethod
    def _build_retention_data(
        cls,
        primary_consent_authored: datetime = datetime(2021, 4, 1),
        first_ehr_authored: datetime = None,
        is_deceased: bool = False,
        is_withdrawn: bool = False,
        dna_samples_timestamp: datetime = None,
        cohort: ParticipantCohort = ParticipantCohort.COHORT_3,
        has_uploaded_ehr_file: bool = False,
        latest_ehr_upload_timestamp: datetime = None,
        basics_authored: datetime = None,
        overallhealth_authored: datetime = None,
        lifestyle_authored: datetime = None,
        healthcare_authored: datetime = None,
        family_health_authored: datetime = None,
        medical_history_authored: datetime = None,
        fam_med_history_authored: datetime = None,
        sdoh_authored: datetime = None,
        latest_cope_authored: datetime = None,
        remote_pm_authored: datetime = None,
        life_func_authored: datetime = None,
        reconsent_authored: datetime = None,
        gror_authored: datetime = None
    ) -> RetentionEligibilityDependencies:
        default_data = RetentionEligibilityDependencies(
            primary_consent=Consent(
                is_consent_provided=True,
                authored_timestamp=primary_consent_authored
            ),
            first_ehr_consent=None,
            is_deceased=is_deceased,
            is_withdrawn=is_withdrawn,
            dna_samples_timestamp=dna_samples_timestamp,
            consent_cohort=cohort,
            has_uploaded_ehr_file=has_uploaded_ehr_file,
            latest_ehr_upload_timestamp=latest_ehr_upload_timestamp,
            basics_response_timestamp=basics_authored,
            overallhealth_response_timestamp=overallhealth_authored,
            lifestyle_response_timestamp=lifestyle_authored,
            healthcare_access_response_timestamp=healthcare_authored,
            family_health_response_timestamp=family_health_authored,
            medical_history_response_timestamp=medical_history_authored,
            fam_med_history_response_timestamp=fam_med_history_authored,
            sdoh_response_timestamp=sdoh_authored,
            latest_cope_response_timestamp=latest_cope_authored,
            remote_pm_response_timestamp=remote_pm_authored,
            life_func_response_timestamp=life_func_authored,
            reconsent_response_timestamp=reconsent_authored,
            gror_response_timestamp=gror_authored
        )
        if first_ehr_authored is not None:
            default_data.first_ehr_consent = Consent(
                is_consent_provided=True,
                authored_timestamp=first_ehr_authored
            )

        return default_data
