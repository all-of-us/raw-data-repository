#
# This file is subject to the terms and conditions defined in the
# file 'LICENSE', which is part of this source code package.
#
from datetime import datetime, timedelta

from rdr_service.participant_enums import QuestionnaireResponseClassificationType
from rdr_service.resource.calculators import EnrollmentStatusCalculator
from rdr_service.resource.constants import ParticipantEventEnum as p_event, PDREnrollmentStatusEnum, ConsentCohortEnum
from tests.helpers.unittest_base import BaseTestCase


def get_basic_activity():
    """
    A basic set of activity records for a 'Cohort 1' and CoreParticipant participant.
    """
    return [
        {'timestamp': datetime(2018, 3, 6, 0, 0), 'group': 'Profile', 'group_id': 1,
         'event': p_event.EHRFirstReceived},
        {'timestamp': datetime(2018, 3, 6, 20, 20, 57), 'group': 'Profile', 'group_id': 1,
         'event': p_event.SignupTime},
        {'timestamp': datetime(2018, 3, 6, 20, 35, 12), 'group': 'QuestionnaireModule', 'group_id': 40,
         'event': p_event.ConsentPII, 'answer': 'ConsentPermission_Yes',
         'classification_type': str(QuestionnaireResponseClassificationType.COMPLETE),
         'answer_id': 767},
        {'timestamp': datetime(2018, 3, 6, 20, 43, 50), 'group': 'QuestionnaireModule', 'group_id': 40,
         'event': p_event.EHRConsentPII, 'answer': 'ConsentPermission_Yes',
         'classification_type': str(QuestionnaireResponseClassificationType.COMPLETE),
         'answer_id': 767},
        {'timestamp': datetime(2018, 3, 6, 20, 46, 48), 'group': 'QuestionnaireModule', 'group_id': 40,
         'classification_type': str(QuestionnaireResponseClassificationType.COMPLETE),
         'event': p_event.TheBasics, 'ConsentAnswer': None},
        {'timestamp': datetime(2018, 3, 6, 20, 49, 0), 'group': 'QuestionnaireModule', 'group_id': 40,
         'classification_type': str(QuestionnaireResponseClassificationType.COMPLETE),
         'event': p_event.OverallHealth, 'ConsentAnswer': None},
        {'timestamp': datetime(2018, 3, 6, 20, 51, 6), 'group': 'QuestionnaireModule', 'group_id': 40,
         'classification_type': str(QuestionnaireResponseClassificationType.COMPLETE),
         'event': p_event.Lifestyle, 'ConsentAnswer': None},
        {'timestamp': datetime(2018, 3, 28, 20, 18, 59), 'group': 'Biobank', 'group_id': 20,
         'event': p_event.BiobankConfirmed, 'dna_tests': 3, 'basline_tests': 4},
        {'timestamp': datetime(2018, 5, 17, 2, 11, 37), 'group': 'Biobank', 'group_id': 20,
         'event': p_event.BiobankOrder, 'dna_tests': 0, 'basline_tests': 0},
        # ROC-295: duplicate record, manually cancelled
        {'timestamp': datetime(2018, 5, 21, 18, 9, 8), 'group': 'Profile', 'group_id': 1,
         'event': p_event.PhysicalMeasurements, 'status': 'CANCELLED', 'status_id': 2},
        {'timestamp': datetime(2018, 5, 21, 18, 9, 12), 'group': 'Profile', 'group_id': 1,
         'event': p_event.PhysicalMeasurements, 'status': 'COMPLETED', 'status_id': 1},
        {'timestamp': datetime(2019, 6, 13, 0, 0), 'group': 'Profile', 'group_id': 1,
         'event': p_event.EHRLastReceived}
    ]


class EnrollmentStatusCalculatorTest(BaseTestCase):

    # EnrollmentStatusCalculator object
    esc = None

    def __init__(self, *args, **kwargs):
        super(EnrollmentStatusCalculatorTest, self).__init__(*args, **kwargs)
        self.uses_database = False

    def setUp(self, with_data=False, with_consent_codes=False) -> None:
        super().setUp(with_data, with_consent_codes)
        self.esc = EnrollmentStatusCalculator()

    def _shift_timestamps(self, activity, days):
        """
        Shift the timestamp value for each activity record by the number of days.
        :param activity: List of activity dictionary objects.
        :param days: Number of days to shift the timestamps by.
        :return: list
        """
        data = list()
        for ev in activity:
            ev['timestamp'] = ev['timestamp'] + timedelta(days=days)
            data.append(ev)
        return data

    def test_basic_activity(self):
        """ A simple test of the basic activity list. """
        self.esc.run(get_basic_activity())
        self.assertEqual(self.esc.status, PDREnrollmentStatusEnum.CoreParticipant)
        self.assertEqual(self.esc.registered_time, datetime(2018, 3, 6, 20, 20, 57))
        self.assertEqual(self.esc.participant_time, datetime(2018, 3, 6, 20, 35, 12))
        self.assertEqual(self.esc.participant_plus_ehr_time, datetime(2018, 3, 6, 20, 43, 50))
        self.assertEqual(self.esc.core_participant_minus_pm_time, datetime(2018, 3, 28, 20, 18, 59))
        self.assertEqual(self.esc.core_participant_time, datetime(2018, 5, 21, 18, 9, 12))

        self.assertNotEqual(self.esc.core_participant_time, datetime(2021, 1, 1, 0, 0, 0))
        self.assertEqual(self.esc.cohort, ConsentCohortEnum.COHORT_1)

        # Test physical measurements timestamp to ensure second PM record was used.
        self.assertEqual(self.esc._physical_measurements.first_ts, datetime(2018, 5, 21, 18, 9, 12))

    def test_duplicate_baseline_modules(self):
        """ Test that duplicate baseline modules are excluded in enrollment status calculation. """
        activity = get_basic_activity()
        # Add additional "TheBasics" to activity.
        activity.append(
            {'timestamp': datetime(2018, 3, 6, 20, 47, 21), 'group': 'QuestionnaireModule', 'group_id': 40,
             'classification_type': str(QuestionnaireResponseClassificationType.COMPLETE),
            'event': p_event.TheBasics, 'ConsentAnswer': None}
        )
        self.esc.run(activity)
        bm_count = 0
        for ev in self.esc._baseline_modules.values:
            if ev.event == p_event.TheBasics:
                self.assertEqual(ev.timestamp, datetime(2018, 3, 6, 20, 46, 48))
                bm_count += 1
            elif ev.event == p_event.OverallHealth:
                self.assertEqual(ev.timestamp, datetime(2018, 3, 6, 20, 49, 0))
                bm_count += 1
            elif ev.event == p_event.Lifestyle:
                self.assertEqual(ev.timestamp, datetime(2018, 3, 6, 20, 51, 6))
                bm_count += 1

        self.assertEqual(bm_count, 3)

    def test_cohort_2(self):
        """ Shift activity dates so we look like a cohort 2 participant. """
        activity = self._shift_timestamps(get_basic_activity(), 180)
        self.esc.run(activity)
        self.assertEqual(self.esc.status, PDREnrollmentStatusEnum.CoreParticipant)
        self.assertEqual(self.esc.cohort, ConsentCohortEnum.COHORT_2)

    def test_cohort_3_no_gror(self):
        """ Shift activity dates so we look like a cohort 3 participant with no GROR consent. """
        activity = self._shift_timestamps(get_basic_activity(), 800)
        self.esc.run(activity)
        # No GROR response means they cannot elevate to Core/Core Minus PM status
        self.assertEqual(self.esc.status, PDREnrollmentStatusEnum.ParticipantPlusEHR)
        self.assertEqual(self.esc.cohort, ConsentCohortEnum.COHORT_3)

    def test_cohort_3_gror_no_answer(self):
        """ Shift activity dates so we look like a cohort 3 participant with GROR consent with CheckDNA_No answer. """
        # Make a deep copy so we don't mess with the get_basic_activity() timestamp values.
        activity = get_basic_activity()
        activity.append(
            {'timestamp': datetime(2018, 3, 6, 20, 43, 50), 'group': 'QuestionnaireModule', 'group_id': 40,
             'classification_type': str(QuestionnaireResponseClassificationType.COMPLETE),
             'event': p_event.GROR, 'answer': 'CheckDNA_No',
             'answer_id': 767}
        )
        # Shift the activity and then run the calculation.  GROR response needs to be present with any answer to reach
        # Core Participant.
        activity = self._shift_timestamps(activity, 800)
        self.esc.run(activity)
        self.assertEqual(self.esc.status, PDREnrollmentStatusEnum.CoreParticipant)
        self.assertEqual(self.esc.cohort, ConsentCohortEnum.COHORT_3)

    def test_cohort_3_gror_yes_answer(self):
        """ Shift activity dates so we look like a cohort 3 participant with GROR consent with CheckDNA_Yes answer. """
        # Make a deep copy so we don't mess with the get_basic_activity() timestamp values.
        activity = get_basic_activity()
        activity.append(
            {'timestamp': datetime(2018, 3, 6, 21, 24, 10), 'group': 'QuestionnaireModule', 'group_id': 40,
             'classification_type': str(QuestionnaireResponseClassificationType.COMPLETE),
             'event': p_event.GROR, 'answer': 'CheckDNA_Yes',
             'answer_id': 767}
        )
        # Shift the activity and then run the calculation.
        activity = self._shift_timestamps(activity, 800)
        self.esc.run(activity)
        self.assertEqual(self.esc.status, PDREnrollmentStatusEnum.CoreParticipant)
        self.assertEqual(self.esc.cohort, ConsentCohortEnum.COHORT_3)

    def test_cohort_3_gror_yes_no_answer(self):
        """ Shift activity dates so we look like a cohort 3 participant with GROR consent with multiple answers. """
        # Make a deep copy so we don't mess with the get_basic_activity() timestamp values.
        activity = get_basic_activity()
        activity.append(
            {'timestamp': datetime(2018, 3, 6, 21, 24, 10), 'group': 'QuestionnaireModule', 'group_id': 40,
             'classification_type': str(QuestionnaireResponseClassificationType.COMPLETE),
             'event': p_event.GROR, 'answer': 'CheckDNA_Yes',
             'answer_id': 767}
        )
        activity.append(
            {'timestamp': datetime(2018, 3, 7, 15, 11, 19), 'group': 'QuestionnaireModule', 'group_id': 40,
             'classification_type': str(QuestionnaireResponseClassificationType.COMPLETE),
             'event': p_event.GROR, 'answer': 'CheckDNA_No',
             'answer_id': 767}
        )
        # Shift the activity and then run the calculation.
        activity = self._shift_timestamps(activity, 800)
        self.esc.run(activity)
        # As long as we have at least one valid GROR response, it doesn't need to be a 'yes' consent
        self.assertEqual(self.esc.status, PDREnrollmentStatusEnum.CoreParticipant)
        self.assertEqual(self.esc.cohort, ConsentCohortEnum.COHORT_3)

    def test_core_minus_pm(self):
        """ Test that we only reach CoreParticipantMinusPM status """
        self.esc.run(get_basic_activity()[0:-2])
        self.assertEqual(self.esc.status, PDREnrollmentStatusEnum.CoreParticipantMinusPM)
        self.assertEqual(self.esc.registered_time, datetime(2018, 3, 6, 20, 20, 57))
        self.assertEqual(self.esc.participant_time, datetime(2018, 3, 6, 20, 35, 12))
        self.assertEqual(self.esc.participant_plus_ehr_time, datetime(2018, 3, 6, 20, 43, 50))
        self.assertEqual(self.esc.core_participant_minus_pm_time, datetime(2018, 3, 28, 20, 18, 59))
        self.assertEqual(self.esc.core_participant_time, None)
        self.assertEqual(self.esc.cohort, ConsentCohortEnum.COHORT_1)

    def test_participant_plus_ehr(self):
        """ Test that we only reach ParticipantPlusEHR status """
        self.esc.run(get_basic_activity()[0:-5])
        self.assertEqual(self.esc.status, PDREnrollmentStatusEnum.ParticipantPlusEHR)
        self.assertEqual(self.esc.registered_time, datetime(2018, 3, 6, 20, 20, 57))
        self.assertEqual(self.esc.participant_time, datetime(2018, 3, 6, 20, 35, 12))
        self.assertEqual(self.esc.participant_plus_ehr_time, datetime(2018, 3, 6, 20, 43, 50))
        self.assertEqual(self.esc.core_participant_minus_pm_time, None)
        self.assertEqual(self.esc.core_participant_time, None)
        self.assertEqual(self.esc.cohort, ConsentCohortEnum.COHORT_1)

    def test_participant(self):
        """ Test that we only reach Participant status """
        self.esc.run(get_basic_activity()[0:3])
        self.assertEqual(self.esc.status, PDREnrollmentStatusEnum.Participant)
        self.assertEqual(self.esc.registered_time, datetime(2018, 3, 6, 20, 20, 57))
        self.assertEqual(self.esc.participant_time, datetime(2018, 3, 6, 20, 35, 12))
        self.assertEqual(self.esc.participant_plus_ehr_time, None)
        self.assertEqual(self.esc.core_participant_minus_pm_time, None)
        self.assertEqual(self.esc.core_participant_time, None)
        self.assertEqual(self.esc.cohort, ConsentCohortEnum.COHORT_1)

    def test_registered(self):
        """ Test that we only reach Registered status """
        activity = [
            {'timestamp': datetime(2018, 3, 6, 20, 20, 57), 'group': 'Profile', 'group_id': 1,
             'event': p_event.SignupTime}
        ]
        self.esc.run(activity)
        self.assertEqual(self.esc.status, PDREnrollmentStatusEnum.Registered)
        self.assertEqual(self.esc.registered_time, datetime(2018, 3, 6, 20, 20, 57))
        self.assertEqual(self.esc.participant_time, None)
        self.assertEqual(self.esc.participant_plus_ehr_time, None)
        self.assertEqual(self.esc.core_participant_minus_pm_time, None)
        self.assertEqual(self.esc.core_participant_time, None)
        self.assertEqual(self.esc.cohort, None)

    def test_registered_authored_before_signup_time(self):
        """ Test that a ConsentPII authored prior to the sign-up-time value is used """
        activity = [
            {'timestamp': datetime(2018, 3, 5, 16, 35, 55), 'group': 'QuestionnaireModule', 'group_id': 40,
             'classification_type': str(QuestionnaireResponseClassificationType.COMPLETE),
             'event': p_event.ConsentPII, 'answer': 'ConsentPermission_Yes',
             'answer_id': 767},
            {'timestamp': datetime(2018, 3, 6, 8, 10, 30), 'group': 'Profile', 'group_id': 1,
             'event': p_event.SignupTime},
        ]

        self.esc.run(activity)
        self.assertEqual(self.esc.status, PDREnrollmentStatusEnum.Participant)
        self.assertEqual(self.esc.registered_time, datetime(2018, 3, 5, 16, 35, 55))
        self.assertEqual(self.esc.participant_time, datetime(2018, 3, 5, 16, 35, 55))

    def test_participant_authored_before_signup_time(self):
        """ Test that a ConsentPII authored prior to the sign-up-time value is used """
        activity = [
            {'timestamp': datetime(2018, 3, 5, 16, 35, 55), 'group': 'QuestionnaireModule', 'group_id': 40,
             'classification_type': str(QuestionnaireResponseClassificationType.COMPLETE),
             'event': p_event.ConsentPII, 'answer': 'ConsentPermission_Yes',
             'answer_id': 767},
            {'timestamp': datetime(2018, 3, 5, 16, 43, 50), 'group': 'QuestionnaireModule', 'group_id': 40,
             'classification_type': str(QuestionnaireResponseClassificationType.COMPLETE),
             'event': p_event.EHRConsentPII, 'answer': 'ConsentPermission_Yes',
             'answer_id': 767},
            {'timestamp': datetime(2018, 3, 6, 8, 10, 30), 'group': 'Profile', 'group_id': 1,
             'event': p_event.SignupTime},
        ]

        self.esc.run(activity)
        self.assertEqual(self.esc.status, PDREnrollmentStatusEnum.ParticipantPlusEHR)
        self.assertEqual(self.esc.registered_time, datetime(2018, 3, 5, 16, 35, 55))
        self.assertEqual(self.esc.participant_time, datetime(2018, 3, 5, 16, 35, 55))
        self.assertEqual(self.esc.participant_plus_ehr_time, datetime(2018, 3, 5, 16, 43, 50))

    def test_no_core_timestamps_for_participant_status(self):
        """ Test that none of the ehr or core timestamps are populated when EHR Consent is No """
        activity = get_basic_activity()
        # Change EHR Consent from Yes to No
        for item in activity:
            if item['event'] == p_event.EHRConsentPII:
                item['answer'] = 'ConsentPermission_No'

        self.esc.run(activity)
        self.assertEqual(self.esc.status, PDREnrollmentStatusEnum.Participant)
        self.assertEqual(self.esc.registered_time, datetime(2018, 3, 6, 20, 20, 57))
        self.assertEqual(self.esc.participant_time, datetime(2018, 3, 6, 20, 35, 12))
        self.assertEqual(self.esc.participant_plus_ehr_time, None)
        self.assertEqual(self.esc.core_participant_minus_pm_time, None)
        self.assertEqual(self.esc.core_participant_time, None)
