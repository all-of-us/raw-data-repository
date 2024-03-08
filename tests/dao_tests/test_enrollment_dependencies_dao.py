from datetime import datetime

from rdr_service.clock import FakeClock
from rdr_service.dao.enrollment_dependencies_dao import EnrollmentDependenciesDao
from rdr_service.model.enrollment_dependencies import EnrollmentDependencies
from rdr_service.participant_enums import ParticipantCohortEnum
from tests.helpers.unittest_base import BaseTestCase

class EnrollmentDependenciesDaoTest(BaseTestCase):
    def setUp(self, *args, **kwargs):
        super().setUp(*args, **kwargs)
        participant_summary = self.data_generator.create_database_participant_summary()
        self.participant_id = participant_summary.participantId

    def test_none_by_default(self):
        """When there isn't anything for a participant, None should be returned by default"""
        result = EnrollmentDependenciesDao.get_enrollment_dependencies(
            participant_id=self.participant_id,
            session=self.session
        )
        self.assertIsNone(result)

    def test_setting_data_element_creates_object(self):
        """If there's nothing in the database yet, then setting an enrollment datapoint should create one"""
        # Defensive check, this test isn't really checking that something gets created if it's already there
        self.assertIsNone(
            EnrollmentDependenciesDao.get_enrollment_dependencies(
                participant_id=self.participant_id,
                session=self.session
            )
        )

        created_time = datetime(2020, 7, 6)
        with FakeClock(created_time):
            EnrollmentDependenciesDao.set_basics_survey_authored_time(datetime.now(), self.participant_id, self.session)

        db_obj = EnrollmentDependenciesDao.get_enrollment_dependencies(
            participant_id=self.participant_id,
            session=self.session
        )
        self.assertIsNotNone(db_obj)
        self.assertEqual(created_time, db_obj.created)
        self.assertEqual(created_time, db_obj.modified)

    def test_setting_data_element_updates_existing(self):
        """If there's already an object in the database, we should update the object that is already there"""
        created_time = datetime(2021, 3, 1)
        with FakeClock(created_time):
            EnrollmentDependenciesDao.set_gror_consent_authored_time(
                datetime(1998, 1, 5), self.participant_id, self.session
            )

            # Loading in FakeClock context to get the created date set correctly
            db_obj = self.session.query(EnrollmentDependencies).filter(
                EnrollmentDependencies.participant_id == self.participant_id
            ).one()

        modified_time = datetime(2022, 4, 8)
        new_authored_date = datetime(2022, 1, 1)
        with FakeClock(modified_time):
            EnrollmentDependenciesDao.set_basics_survey_authored_time(
                new_authored_date, self.participant_id, self.session
            )

        self.assertEqual(created_time, db_obj.created)
        self.assertEqual(modified_time, db_obj.modified)

    def test_not_replacing_existing_values(self):
        """
        If there's already a value set for a participant, don't set a new one (to ignore reconsenting to EHR and
        erroneous replays of something like the basics). Enrollment status looks for the earliest timestamp for
        datapoints.
        """
        created_time = datetime(2001, 3, 4)
        authored_time = datetime(1998, 1, 5)
        EnrollmentDependenciesDao.set_gror_consent_authored_time(
            authored_time, self.participant_id, self.session
        )

        with FakeClock(created_time):
            EnrollmentDependenciesDao.set_gror_consent_authored_time(
                authored_time, self.participant_id, self.session
            )

        with FakeClock(datetime(2022, 1, 1)):
            EnrollmentDependenciesDao.set_gror_consent_authored_time(
                datetime(2100, 1, 1), self.participant_id, self.session
            )

        db_obj = self.session.query(EnrollmentDependencies).filter(
            EnrollmentDependencies.participant_id == self.participant_id
        ).one()
        self.assertEqual(created_time, db_obj.created)
        self.assertEqual(created_time, db_obj.modified)
        self.assertEqual(authored_time, db_obj.gror_consent_authored_time)

    def test_setting_consent_cohort(self):
        value = ParticipantCohortEnum.COHORT_2
        EnrollmentDependenciesDao.set_consent_cohort(
            value,
            participant_id=self.participant_id,
            session=self.session
        )
        db_obj = self.session.query(EnrollmentDependencies).filter(
            EnrollmentDependencies.participant_id == self.participant_id
        ).one()
        self.assertEqual(value, db_obj.consent_cohort)

    def test_setting_primary_consent_authored_time(self):
        value = self.fake.date_time()
        EnrollmentDependenciesDao.set_primary_consent_authored_time(
            value,
            participant_id=self.participant_id,
            session=self.session
        )
        db_obj = self.session.query(EnrollmentDependencies).filter(
            EnrollmentDependencies.participant_id == self.participant_id
        ).one()
        self.assertEqual(value, db_obj.primary_consent_authored_time)

    def test_setting_intent_to_share_ehr_time(self):
        value = self.fake.date_time()
        EnrollmentDependenciesDao.set_intent_to_share_ehr_time(
            value,
            participant_id=self.participant_id,
            session=self.session
        )
        db_obj = self.session.query(EnrollmentDependencies).filter(
            EnrollmentDependencies.participant_id == self.participant_id
        ).one()
        self.assertEqual(value, db_obj.intent_to_share_ehr_time)

    def test_setting_full_ehr_consent_authored_time(self):
        value = self.fake.date_time()
        EnrollmentDependenciesDao.set_full_ehr_consent_authored_time(
            value,
            participant_id=self.participant_id,
            session=self.session
        )
        db_obj = self.session.query(EnrollmentDependencies).filter(
            EnrollmentDependencies.participant_id == self.participant_id
        ).one()
        self.assertEqual(value, db_obj.full_ehr_consent_authored_time)

    def test_setting_gror_consent_authored_time(self):
        value = self.fake.date_time()
        EnrollmentDependenciesDao.set_gror_consent_authored_time(
            value,
            participant_id=self.participant_id,
            session=self.session
        )
        db_obj = self.session.query(EnrollmentDependencies).filter(
            EnrollmentDependencies.participant_id == self.participant_id
        ).one()
        self.assertEqual(value, db_obj.gror_consent_authored_time)

    def test_setting_dna_consent_update_time(self):
        value = self.fake.date_time()
        EnrollmentDependenciesDao.set_dna_consent_update_time(
            value,
            participant_id=self.participant_id,
            session=self.session
        )
        db_obj = self.session.query(EnrollmentDependencies).filter(
            EnrollmentDependencies.participant_id == self.participant_id
        ).one()
        self.assertEqual(value, db_obj.dna_consent_update_time)

    def test_setting_basics_survey_authored_time(self):
        value = self.fake.date_time()
        EnrollmentDependenciesDao.set_basics_survey_authored_time(
            value,
            participant_id=self.participant_id,
            session=self.session
        )
        db_obj = self.session.query(EnrollmentDependencies).filter(
            EnrollmentDependencies.participant_id == self.participant_id
        ).one()
        self.assertEqual(value, db_obj.basics_survey_authored_time)

    def test_setting_overall_health_survey_authored_time(self):
        value = self.fake.date_time()
        EnrollmentDependenciesDao.set_overall_health_survey_authored_time(
            value,
            participant_id=self.participant_id,
            session=self.session
        )
        db_obj = self.session.query(EnrollmentDependencies).filter(
            EnrollmentDependencies.participant_id == self.participant_id
        ).one()
        self.assertEqual(value, db_obj.overall_health_survey_authored_time)

    def test_setting_lifestyle_survey_authored_time(self):
        value = self.fake.date_time()
        EnrollmentDependenciesDao.set_lifestyle_survey_authored_time(
            value,
            participant_id=self.participant_id,
            session=self.session
        )
        db_obj = self.session.query(EnrollmentDependencies).filter(
            EnrollmentDependencies.participant_id == self.participant_id
        ).one()
        self.assertEqual(value, db_obj.lifestyle_survey_authored_time)

    def test_setting_exposures_survey_authored_time(self):
        value = self.fake.date_time()
        EnrollmentDependenciesDao.set_exposures_survey_authored_time(
            value,
            participant_id=self.participant_id,
            session=self.session
        )
        db_obj = self.session.query(EnrollmentDependencies).filter(
            EnrollmentDependencies.participant_id == self.participant_id
        ).one()
        self.assertEqual(value, db_obj.exposures_survey_authored_time)

    def test_setting_biobank_received_dna_time(self):
        value = self.fake.date_time()
        EnrollmentDependenciesDao.set_biobank_received_dna_time(
            value,
            participant_id=self.participant_id,
            session=self.session
        )
        db_obj = self.session.query(EnrollmentDependencies).filter(
            EnrollmentDependencies.participant_id == self.participant_id
        ).one()
        self.assertEqual(value, db_obj.biobank_received_dna_time)

    def test_setting_wgs_sequencing_time(self):
        value = self.fake.date_time()
        EnrollmentDependenciesDao.set_wgs_sequencing_time(
            value,
            participant_id=self.participant_id,
            session=self.session
        )
        db_obj = self.session.query(EnrollmentDependencies).filter(
            EnrollmentDependencies.participant_id == self.participant_id
        ).one()
        self.assertEqual(value, db_obj.wgs_sequencing_time)

    def test_setting_first_ehr_file_received_time(self):
        value = self.fake.date_time()
        EnrollmentDependenciesDao.set_first_ehr_file_received_time(
            value,
            participant_id=self.participant_id,
            session=self.session
        )
        db_obj = self.session.query(EnrollmentDependencies).filter(
            EnrollmentDependencies.participant_id == self.participant_id
        ).one()
        self.assertEqual(value, db_obj.first_ehr_file_received_time)

    def test_setting_first_mediated_ehr_received_time(self):
        value = self.fake.date_time()
        EnrollmentDependenciesDao.set_first_mediated_ehr_received_time(
            value,
            participant_id=self.participant_id,
            session=self.session
        )
        db_obj = self.session.query(EnrollmentDependencies).filter(
            EnrollmentDependencies.participant_id == self.participant_id
        ).one()
        self.assertEqual(value, db_obj.first_mediated_ehr_received_time)

    def test_setting_physical_measurements_time(self):
        value = self.fake.date_time()
        EnrollmentDependenciesDao.set_physical_measurements_time(
            value,
            participant_id=self.participant_id,
            session=self.session
        )
        db_obj = self.session.query(EnrollmentDependencies).filter(
            EnrollmentDependencies.participant_id == self.participant_id
        ).one()
        self.assertEqual(value, db_obj.physical_measurements_time)

    def test_setting_weight_physical_measurements_time(self):
        value = self.fake.date_time()
        EnrollmentDependenciesDao.set_weight_physical_measurements_time(
            value,
            participant_id=self.participant_id,
            session=self.session
        )
        db_obj = self.session.query(EnrollmentDependencies).filter(
            EnrollmentDependencies.participant_id == self.participant_id
        ).one()
        self.assertEqual(value, db_obj.weight_physical_measurements_time)

    def test_setting_height_physical_measurements_time(self):
        value = self.fake.date_time()
        EnrollmentDependenciesDao.set_height_physical_measurements_time(
            value,
            participant_id=self.participant_id,
            session=self.session
        )
        db_obj = self.session.query(EnrollmentDependencies).filter(
            EnrollmentDependencies.participant_id == self.participant_id
        ).one()
        self.assertEqual(value, db_obj.height_physical_measurements_time)

    def test_setting_is_pediatric_participant(self):
        value = True
        EnrollmentDependenciesDao.set_is_pediatric_participant(
            value,
            participant_id=self.participant_id,
            session=self.session
        )
        db_obj = self.session.query(EnrollmentDependencies).filter(
            EnrollmentDependencies.participant_id == self.participant_id
        ).one()
        self.assertEqual(value, db_obj.is_pediatric_participant)

    def test_setting_has_linked_guardian_account(self):
        value = False
        EnrollmentDependenciesDao.set_has_linked_guardian_account(
            value,
            participant_id=self.participant_id,
            session=self.session
        )
        db_obj = self.session.query(EnrollmentDependencies).filter(
            EnrollmentDependencies.participant_id == self.participant_id
        ).one()
        self.assertEqual(value, db_obj.has_linked_guardian_account)
