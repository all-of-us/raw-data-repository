#
# This file is subject to the terms and conditions defined in the
# file 'LICENSE', which is part of this source code package.
#
import copy
from datetime import datetime, timedelta

from rdr_service.resource.calculators import EnrollmentStatusCalculator
from rdr_service.resource.constants import ParticipantEventEnum as p_event, PDREnrollmentStatusEnum, ConsentCohortEnum
from tests.helpers.unittest_base import BaseTestCase


# A basic set of activity records for a 'Cohort 1' and CoreParticipant participant.
# Remember to make a deep copy if you are going to change the data here.
BASIC_ACTIVITY = [
    {'timestamp': datetime(2018, 3, 6, 0, 0), 'group': 'Profile', 'group_id': 1,
     'event': p_event.EHRFirstReceived},
    {'timestamp': datetime(2018, 3, 6, 20, 20, 57), 'group': 'Profile', 'group_id': 1,
     'event': p_event.SignupTime},
    {'timestamp': datetime(2018, 3, 6, 20, 35, 12), 'group': 'QuestionnaireModule', 'group_id': 40,
     'event': p_event.ConsentPII, 'ConsentAnswer': None, 'answer': 'ConsentPermission_Yes',
     'answer_id': 767},
    {'timestamp': datetime(2018, 3, 6, 20, 43, 50), 'group': 'QuestionnaireModule', 'group_id': 40,
     'event': p_event.EHRConsentPII, 'ConsentAnswer': None, 'answer': 'ConsentPermission_Yes',
     'answer_id': 767},
    {'timestamp': datetime(2018, 3, 6, 20, 46, 48), 'group': 'QuestionnaireModule', 'group_id': 40,
     'event': p_event.TheBasics, 'ConsentAnswer': None},
    {'timestamp': datetime(2018, 3, 6, 20, 49), 'group': 'QuestionnaireModule', 'group_id': 40,
     'event': p_event.OverallHealth, 'ConsentAnswer': None},
    {'timestamp': datetime(2018, 3, 6, 20, 51, 6), 'group': 'QuestionnaireModule', 'group_id': 40,
     'event': p_event.Lifestyle, 'ConsentAnswer': None},
    {'timestamp': datetime(2018, 3, 28, 20, 18, 59), 'group': 'Biobank', 'group_id': 20,
     'event': p_event.BiobankConfirmed, 'dna_tests': 3, 'basline_tests': 4},
    {'timestamp': datetime(2018, 5, 17, 2, 11, 37), 'group': 'Biobank', 'group_id': 20,
     'event': p_event.BiobankOrder, 'dna_tests': 0, 'basline_tests': 0},
    {'timestamp': datetime(2018, 5, 21, 18, 9, 12), 'group': 'Profile', 'group_id': 1,
     'event': p_event.PhysicalMeasurements, 'finalized': datetime(2018, 3, 8, 18, 9, 12)},
    {'timestamp': datetime(2019, 6, 13, 0, 0), 'group': 'Profile', 'group_id': 1,
     'event': p_event.EHRLastReceived}
]


class EnrollmentStatusCalculatorTest(BaseTestCase):

    # EnrollmentStatusCalculator object
    esc = None

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
        # Make a deep copy so we don't mess with the BASIC_ACTIVITY timestamp values.
        act_ = copy.deepcopy(activity)
        for ev in act_:
            ev['timestamp'] = ev['timestamp'] + timedelta(days=days)
            data.append(ev)
        return data

    def test_basic_activity(self):
        """ A simple test of the basic activity list. """
        self.esc.run(BASIC_ACTIVITY)
        self.assertEqual(self.esc.status, PDREnrollmentStatusEnum.CoreParticipant)
        self.assertEqual(self.esc.registered_ts, datetime(2018, 3, 6, 20, 20, 57))
        self.assertEqual(self.esc.participant_ts, datetime(2018, 3, 6, 20, 35, 12))
        self.assertEqual(self.esc.participant_plus_ehr_ts, datetime(2018, 3, 6, 20, 43, 50))
        self.assertEqual(self.esc.core_participant_minus_pm_ts, datetime(2018, 3, 28, 20, 18, 59))
        self.assertEqual(self.esc.core_participant_ts, datetime(2018, 5, 21, 18, 9, 12))

        self.assertNotEqual(self.esc.core_participant_ts, datetime(2021, 1, 1, 0, 0, 0))
        self.assertEqual(self.esc.cohort, ConsentCohortEnum.COHORT_1)

    def test_cohort_2(self):
        """ Shift activity dates so we look like a cohort 2 participant. """
        activity = self._shift_timestamps(BASIC_ACTIVITY, 180)
        self.esc.run(activity)
        self.assertEqual(self.esc.status, PDREnrollmentStatusEnum.CoreParticipant)
        self.assertEqual(self.esc.cohort, ConsentCohortEnum.COHORT_2)

    def test_cohort_3_no_gror(self):
        """ Shift activity dates so we look like a cohort 3 participant with no GROR consent. """
        activity = self._shift_timestamps(BASIC_ACTIVITY, 800)
        self.esc.run(activity)
        # Since BASIC_ACTIVITY does not have a GROR consent, we should only reach ParticipantPlusEHR status.
        self.assertEqual(self.esc.status, PDREnrollmentStatusEnum.ParticipantPlusEHR)
        self.assertEqual(self.esc.cohort, ConsentCohortEnum.COHORT_3)

    def test_cohort_3_gror_no_answer(self):
        """ Shift activity dates so we look like a cohort 3 participant with GROR consent with CheckDNA_No answer. """
        # Make a deep copy so we don't mess with the BASIC_ACTIVITY timestamp values.
        activity = copy.deepcopy(BASIC_ACTIVITY)
        activity.append(
            {'timestamp': datetime(2018, 3, 6, 20, 43, 50), 'group': 'QuestionnaireModule', 'group_id': 40,
             'event': p_event.GROR, 'ConsentAnswer': None, 'answer': 'CheckDNA_No',
             'answer_id': 767},
        )
        # Shift the activity and then run the calculation.
        activity = self._shift_timestamps(activity, 800)
        self.esc.run(activity)
        self.assertEqual(self.esc.status, PDREnrollmentStatusEnum.ParticipantPlusEHR)
        self.assertEqual(self.esc.cohort, ConsentCohortEnum.COHORT_3)

    def test_cohort_3_gror_yes_answer(self):
        """ Shift activity dates so we look like a cohort 3 participant with GROR consent with CheckDNA_Yes answer. """
        # Make a deep copy so we don't mess with the BASIC_ACTIVITY timestamp values.
        activity = copy.deepcopy(BASIC_ACTIVITY)
        activity.append(
            {'timestamp': datetime(2018, 3, 6, 21, 24, 10), 'group': 'QuestionnaireModule', 'group_id': 40,
             'event': p_event.GROR, 'ConsentAnswer': None, 'answer': 'CheckDNA_Yes',
             'answer_id': 767},
        )
        # Shift the activity and then run the calculation.
        activity = self._shift_timestamps(activity, 800)
        self.esc.run(activity)
        self.assertEqual(self.esc.status, PDREnrollmentStatusEnum.CoreParticipant)
        self.assertEqual(self.esc.cohort, ConsentCohortEnum.COHORT_3)

    def test_cohort_3_gror_yes_no_answer(self):
        """ Shift activity dates so we look like a cohort 3 participant with GROR consent with multiple answers. """
        # Make a deep copy so we don't mess with the BASIC_ACTIVITY timestamp values.
        activity = copy.deepcopy(BASIC_ACTIVITY)
        activity.append(
            {'timestamp': datetime(2018, 3, 6, 21, 24, 10), 'group': 'QuestionnaireModule', 'group_id': 40,
             'event': p_event.GROR, 'ConsentAnswer': None, 'answer': 'CheckDNA_Yes',
             'answer_id': 767},
        )
        activity.append(
            {'timestamp': datetime(2018, 3, 7, 15, 11, 19), 'group': 'QuestionnaireModule', 'group_id': 40,
             'event': p_event.GROR, 'ConsentAnswer': None, 'answer': 'CheckDNA_No',
             'answer_id': 767},
        )
        # Shift the activity and then run the calculation.
        activity = self._shift_timestamps(activity, 800)
        self.esc.run(activity)
        # We should only reach ParticipantPlusEHR status with a Yes and then a No GROR consent.
        self.assertEqual(self.esc.status, PDREnrollmentStatusEnum.ParticipantPlusEHR)
        self.assertEqual(self.esc.cohort, ConsentCohortEnum.COHORT_3)

    def test_core_minus_pm(self):
        """ Test that we only reach CoreParticipantMinusPM status """
        self.esc.run(BASIC_ACTIVITY[0:-2])
        self.assertEqual(self.esc.status, PDREnrollmentStatusEnum.CoreParticipantMinusPM)
        self.assertEqual(self.esc.registered_ts, datetime(2018, 3, 6, 20, 20, 57))
        self.assertEqual(self.esc.participant_ts, datetime(2018, 3, 6, 20, 35, 12))
        self.assertEqual(self.esc.participant_plus_ehr_ts, datetime(2018, 3, 6, 20, 43, 50))
        self.assertEqual(self.esc.core_participant_minus_pm_ts, datetime(2018, 3, 28, 20, 18, 59))
        self.assertEqual(self.esc.core_participant_ts, None)
        self.assertEqual(self.esc.cohort, ConsentCohortEnum.COHORT_1)

    def test_participant_plus_ehr(self):
        """ Test that we only reach ParticipantPlusEHR status """
        self.esc.run(BASIC_ACTIVITY[0:-5])
        self.assertEqual(self.esc.status, PDREnrollmentStatusEnum.ParticipantPlusEHR)
        self.assertEqual(self.esc.registered_ts, datetime(2018, 3, 6, 20, 20, 57))
        self.assertEqual(self.esc.participant_ts, datetime(2018, 3, 6, 20, 35, 12))
        self.assertEqual(self.esc.participant_plus_ehr_ts, datetime(2018, 3, 6, 20, 43, 50))
        self.assertEqual(self.esc.core_participant_minus_pm_ts, None)
        self.assertEqual(self.esc.core_participant_ts, None)
        self.assertEqual(self.esc.cohort, ConsentCohortEnum.COHORT_1)

    def test_participant(self):
        """ Test that we only reach Participant status """
        self.esc.run(BASIC_ACTIVITY[0:4])
        self.assertEqual(self.esc.status, PDREnrollmentStatusEnum.Participant)
        self.assertEqual(self.esc.registered_ts, datetime(2018, 3, 6, 20, 20, 57))
        self.assertEqual(self.esc.participant_ts, datetime(2018, 3, 6, 20, 35, 12))
        self.assertEqual(self.esc.participant_plus_ehr_ts, None)
        self.assertEqual(self.esc.core_participant_minus_pm_ts, None)
        self.assertEqual(self.esc.core_participant_ts, None)
        self.assertEqual(self.esc.cohort, ConsentCohortEnum.COHORT_1)

    def test_registered(self):
        """ Test that we only reach Registered status """
        self.esc.run(BASIC_ACTIVITY[0:3])
        self.assertEqual(self.esc.status, PDREnrollmentStatusEnum.Registered)
        self.assertEqual(self.esc.registered_ts, datetime(2018, 3, 6, 20, 20, 57))
        self.assertEqual(self.esc.participant_ts, None)
        self.assertEqual(self.esc.participant_plus_ehr_ts, None)
        self.assertEqual(self.esc.core_participant_minus_pm_ts, None)
        self.assertEqual(self.esc.core_participant_ts, None)
        self.assertEqual(self.esc.cohort, None)
