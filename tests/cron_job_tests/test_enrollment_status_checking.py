from datetime import datetime, timedelta

from rdr_service.participant_enums import EnrollmentStatus, PhysicalMeasurementsStatus, QuestionnaireStatus, \
    SampleStatus
from tests.helpers.unittest_base import BaseTestCase


class CheckEnrollmentStatusTest(BaseTestCase):
    """Tests checking enrollment status of participants cron job"""

    def setUp(self):
        super().setUp()

    # test core participant meets requirements
    def test_core_meets_req(self):
        from rdr_service.offline.enrollment_check import check_enrollment
        person, ps_dao = self.setup_participant()
        # missing questionnaires and pm status
        self.assertEqual(check_enrollment(create_ticket=False), False)
        # update required attributes
        person.questionnaireOnLifestyle = QuestionnaireStatus.SUBMITTED
        person.questionnaireOnOverallHealth = QuestionnaireStatus.SUBMITTED
        person.questionnaireOnTheBasics = QuestionnaireStatus.SUBMITTED
        person.clinicPhysicalMeasurementsStatus = PhysicalMeasurementsStatus.COMPLETED
        with ps_dao.session() as session:
            session.add(person)
        self.assertEqual(check_enrollment(create_ticket=False), True)

    def setup_participant(self):
        """ A full participant (core) is defined as:
        completed the primary informed consent process
        HIPAA Authorization/EHR consent
        required PPI modules (Basics, Overall Health, and Lifestyle modules)
        provide physical measurements
        at least one biosample suitable for genetic sequencing.
        """
        twenty_nine = datetime.now() - timedelta(days=29)
        p = self.data_generator._participant_with_defaults(participantId=6666666, biobankId=9999999, version=1,
                                            lastModified=twenty_nine, signUpTime=twenty_nine)
        valid_kwargs = dict(
            participantId=p.participantId,
            biobankId=p.biobankId,
            withdrawalStatus=p.withdrawalStatus,
            dateOfBirth=datetime(2000, 1, 1),
            firstName="foo",
            lastName="bar",
            zipCode="12345",
            sampleStatus1ED04=SampleStatus.RECEIVED,
            sampleStatus1SAL2=SampleStatus.RECEIVED,
            samplesToIsolateDNA=SampleStatus.RECEIVED,
            consentForStudyEnrollmentTime=datetime(2019, 1, 1),
            numCompletedBaselinePPIModules=3,
            consentForStudyEnrollment=1,
            consentForElectronicHealthRecords=1,
            enrollmentStatus=EnrollmentStatus.FULL_PARTICIPANT,
            lastModified=twenty_nine)

        person = self.data_generator._participant_summary_with_defaults(**valid_kwargs)
        from rdr_service.dao.participant_summary_dao import ParticipantSummaryDao
        from rdr_service.dao.participant_dao import ParticipantDao
        dao = ParticipantDao()
        with dao.session() as session:
            session.add(p)
        ps_dao = ParticipantSummaryDao()
        with ps_dao.session() as session:
            session.add(person)

        return person, ps_dao
