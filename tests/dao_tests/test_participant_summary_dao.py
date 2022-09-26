import copy
import datetime
import json
import mock
import time
from base64 import urlsafe_b64decode, urlsafe_b64encode

from rdr_service import clock, config
from rdr_service.code_constants import BIOBANK_TESTS
from rdr_service.dao.base_dao import json_serial
from rdr_service.dao.biobank_order_dao import BiobankOrderDao
from rdr_service.dao.biobank_stored_sample_dao import BiobankStoredSampleDao
from rdr_service.dao.participant_dao import ParticipantDao
from rdr_service.dao.participant_summary_dao import ParticipantSummaryDao
from rdr_service.dao.physical_measurements_dao import PhysicalMeasurementsDao
from rdr_service.model.biobank_order import BiobankOrder, BiobankOrderIdentifier, BiobankOrderedSample
from rdr_service.model.biobank_stored_sample import BiobankStoredSample
from rdr_service.model.measurements import PhysicalMeasurements
from rdr_service.model.participant import Participant
from rdr_service.model.participant_summary import ParticipantSummary
from rdr_service.participant_enums import (
    EnrollmentStatus,
    EnrollmentStatusV30,
    EnrollmentStatusV31,
    PhysicalMeasurementsStatus,
    QuestionnaireStatus,
    SampleStatus,
    SelfReportedPhysicalMeasurementsStatus
)
from rdr_service.query import FieldFilter, Operator, OrderBy, Query
from rdr_service.services.system_utils import DateRange
from tests.test_data import load_measurement_json
from tests.helpers.unittest_base import BaseTestCase
from tests.helpers.mysql_helper_data import PITT_HPO_ID

NUM_BASELINE_PPI_MODULES = 3

TIME_1 = datetime.datetime(2019, 2, 24)
TIME_2 = datetime.datetime(2019, 2, 25)
TIME_3 = datetime.datetime(2019, 2, 27)
# for HPRO QA test scenarios
TIME_4 = datetime.datetime(2019, 1, 28)
TIME_5 = datetime.datetime(2019, 2, 11)
TIME_6 = datetime.datetime(2018, 12, 4)
TIME_7 = datetime.datetime(2019, 2, 18)
TIME_7_5 = datetime.datetime(2019, 2, 19)

TIME_8 = datetime.datetime(2018, 4, 4)
TIME_9 = datetime.datetime(2018, 4, 3)
TIME_10 = datetime.datetime(2019, 1, 1)
TIME_11 = datetime.datetime(2019, 1, 2)
TIME_12 = datetime.datetime(2019, 1, 3)


class ParticipantSummaryDaoTest(BaseTestCase):
    def setUp(self):
        super().setUp()
        self.dao = ParticipantSummaryDao()
        self.order_dao = BiobankOrderDao()
        self.measurement_dao = PhysicalMeasurementsDao()
        self.participant_dao = ParticipantDao()
        self.no_filter_query = Query([], None, 2, None)
        self.one_filter_query = Query([FieldFilter("participantId", Operator.EQUALS, 1)], None, 2, None)
        self.two_filter_query = Query(
            [FieldFilter("participantId", Operator.EQUALS, 1), FieldFilter("hpoId", Operator.EQUALS, PITT_HPO_ID)],
            None,
            2,
            None,
        )
        self.ascending_biobank_id_query = Query([], OrderBy("biobankId", True), 2, None)
        self.descending_biobank_id_query = Query([], OrderBy("biobankId", False), 2, None)
        self.enrollment_status_order_query = Query([], OrderBy("enrollmentStatusV3_1", True), 2, None)
        self.hpo_id_order_query = Query([], OrderBy("hpoId", True), 2, None)
        self.first_name_order_query = Query([], OrderBy("firstName", True), 2, None)

        response_dao_patch = mock.patch(
            'rdr_service.dao.participant_summary_dao.QuestionnaireResponseRepository'
        )
        response_dao_mock = response_dao_patch.start()
        self.mock_ehr_interest_ranges = response_dao_mock.get_interest_in_sharing_ehr_ranges
        self.mock_ehr_interest_ranges.return_value = []
        self.addCleanup(response_dao_patch.stop)

    def assert_no_results(self, query):
        results = self.dao.query(query)
        self.assertEqual([], results.items)
        self.assertIsNone(results.pagination_token)

    def assert_results(self, query, items, pagination_token=None):
        results = self.dao.query(query)
        self.assertListAsDictEquals(items, results.items)
        self.assertEqual(
            pagination_token,
            results.pagination_token,
            "Pagination tokens don't match; decoded = %s, %s"
            % (_decode_token(pagination_token), _decode_token(results.pagination_token)),
        )

    def test_query_with_total(self):
        num_participants = 5
        query = Query([], None, 10, None, include_total=True)
        results = self.dao.query(query)
        self.assertEqual(results.total, 0)
        for i in range(num_participants):
            participant = Participant(participantId=i, biobankId=i)
            self._insert(participant)
        results = self.dao.query(query)
        self.assertEqual(results.total, num_participants)

    def testQuery_noSummaries(self):
        self.assert_no_results(self.no_filter_query)
        self.assert_no_results(self.one_filter_query)
        self.assert_no_results(self.two_filter_query)
        self.assert_no_results(self.ascending_biobank_id_query)
        self.assert_no_results(self.descending_biobank_id_query)

    def _insert(self, participant, first_name=None, last_name=None):
        self.participant_dao.insert(participant)
        summary = self.participant_summary(participant)
        summary.enrollmentStatus = EnrollmentStatus.INTERESTED
        summary.enrollmentStatusV3_0 = EnrollmentStatusV30.PARTICIPANT
        summary.enrollmentStatusV3_1 = EnrollmentStatusV31.PARTICIPANT
        if first_name:
            summary.firstName = first_name
        if last_name:
            summary.lastName = last_name
        self.dao.insert(summary)
        return participant

    def testQuery_oneSummary(self):
        participant = Participant(participantId=1, biobankId=2)
        self._insert(participant)
        summary = self.dao.get(1)
        self.assert_results(self.no_filter_query, [summary])
        self.assert_results(self.one_filter_query, [summary])
        self.assert_no_results(self.two_filter_query)
        self.assert_results(self.ascending_biobank_id_query, [summary])
        self.assert_results(self.descending_biobank_id_query, [summary])

    def testUnicodeNameRoundTrip(self):
        name = self.fake.first_name()
        participant = self._insert(Participant(participantId=1, biobankId=2))
        summary = self.dao.get(participant.participantId)
        summary.firstName = name
        self.dao.update(summary)
        fetched_summary = self.dao.get(participant.participantId)
        self.assertEqual(name, fetched_summary.firstName)

    def testQuery_twoSummaries(self):
        participant_1 = Participant(participantId=1, biobankId=2)
        self._insert(participant_1, "Alice", "Smith")
        participant_2 = Participant(participantId=2, biobankId=1)
        self._insert(participant_2, "Zed", "Zebra")
        ps_1 = self.dao.get(1)
        ps_2 = self.dao.get(2)
        self.assert_results(self.no_filter_query, [ps_1, ps_2])
        self.assert_results(self.one_filter_query, [ps_1])
        self.assert_no_results(self.two_filter_query)
        self.assert_results(self.ascending_biobank_id_query, [ps_2, ps_1])
        self.assert_results(self.descending_biobank_id_query, [ps_1, ps_2])

    def testQuery_threeSummaries_paginate(self):
        participant_1 = Participant(participantId=1, biobankId=4)
        self._insert(participant_1, "Alice", "Aardvark")
        participant_2 = Participant(participantId=2, biobankId=1)
        self._insert(participant_2, "Bob", "Builder")
        participant_3 = Participant(participantId=3, biobankId=3)
        self._insert(participant_3, "Chad", "Caterpillar")
        ps_1 = self.dao.get(1)
        ps_2 = self.dao.get(2)
        ps_3 = self.dao.get(3)
        self.assert_results(self.no_filter_query, [ps_1, ps_2], _make_pagination_token(["Builder", "Bob", None, 2]))
        self.assert_results(self.one_filter_query, [ps_1])
        self.assert_no_results(self.two_filter_query)
        self.assert_results(
            self.ascending_biobank_id_query, [ps_2, ps_3], _make_pagination_token([3, "Caterpillar", "Chad", None, 3])
        )
        self.assert_results(
            self.descending_biobank_id_query, [ps_1, ps_3], _make_pagination_token([3, "Caterpillar", "Chad", None, 3])
        )

        self.assert_results(
            _with_token(self.no_filter_query, _make_pagination_token(["Builder", "Bob", None, 2])), [ps_3]
        )
        self.assert_results(
            _with_token(self.ascending_biobank_id_query, _make_pagination_token([3, "Caterpillar", "Chad", None, 3])),
            [ps_1],
        )
        self.assert_results(
            _with_token(self.descending_biobank_id_query, _make_pagination_token([3, "Caterpillar", "Chad", None, 3])),
            [ps_2],
        )

    def testQuery_fourFullSummaries_paginate(self):
        participant_1 = Participant(participantId=1, biobankId=4)
        self._insert(participant_1, "Bob", "Jones")
        participant_2 = Participant(participantId=2, biobankId=1)
        self._insert(participant_2, "Bob", "Jones")
        participant_3 = Participant(participantId=3, biobankId=3)
        self._insert(participant_3, "Bob", "Jones")
        participant_4 = Participant(participantId=4, biobankId=2)
        self._insert(participant_4, "Bob", "Jones")
        ps_1 = self.dao.get(1)
        ps_2 = self.dao.get(2)
        ps_3 = self.dao.get(3)
        ps_4 = self.dao.get(4)

        ps_1.lastName = "Jones"
        ps_1.firstName = "Bob"
        ps_1.dateOfBirth = datetime.date(1978, 10, 9)
        ps_1.hpoId = PITT_HPO_ID
        self.dao.update(ps_1)

        ps_2.lastName = "Aardvark"
        ps_2.firstName = "Bob"
        ps_2.dateOfBirth = datetime.date(1978, 10, 10)
        ps_2.enrollmentStatusV3_1 = EnrollmentStatusV31.PARTICIPANT_PLUS_EHR
        self.dao.update(ps_2)

        ps_3.lastName = "Jones"
        ps_3.firstName = "Bob"
        ps_3.dateOfBirth = datetime.date(1978, 10, 10)
        ps_3.hpoId = PITT_HPO_ID
        ps_3.enrollmentStatusV3_1 = EnrollmentStatusV31.PARTICIPANT_PLUS_EHR
        self.dao.update(ps_3)

        ps_4.lastName = "Jones"
        ps_4.enrollmentStatusV3_1 = EnrollmentStatusV31.CORE_PARTICIPANT
        self.dao.update(ps_4)

        self.assert_results(self.no_filter_query, [ps_2, ps_4], _make_pagination_token(["Jones", "Bob", None, 4]))
        self.assert_results(self.one_filter_query, [ps_1])
        self.assert_results(self.two_filter_query, [ps_1])
        self.assert_results(
            self.ascending_biobank_id_query, [ps_2, ps_4], _make_pagination_token([2, "Jones", "Bob", None, 4])
        )
        self.assert_results(
            self.descending_biobank_id_query,
            [ps_1, ps_3],
            _make_pagination_token([3, "Jones", "Bob", datetime.date(1978, 10, 10), 3]),
        )
        self.assert_results(
            self.hpo_id_order_query, [ps_2, ps_4], _make_pagination_token([0, "Jones", "Bob", None, 4])
        )
        self.assert_results(
            self.enrollment_status_order_query,
            [ps_1, ps_2],
            _make_pagination_token(["PARTICIPANT_PLUS_EHR", "Aardvark", "Bob", datetime.date(1978, 10, 10), 2]),
        )

        self.assert_results(
            _with_token(self.no_filter_query, _make_pagination_token(["Jones", "Bob", None, 4])), [ps_1, ps_3]
        )
        self.assert_results(
            _with_token(self.ascending_biobank_id_query, _make_pagination_token([2, "Jones", "Bob", None, 4])),
            [ps_3, ps_1],
        )
        self.assert_results(
            _with_token(
                self.descending_biobank_id_query,
                _make_pagination_token([3, "Jones", "Bob", datetime.date(1978, 10, 10), 3]),
            ),
            [ps_4, ps_2],
        )
        self.assert_results(
            _with_token(self.hpo_id_order_query, _make_pagination_token([0, "Jones", "Bob", None, 4])), [ps_1, ps_3]
        )
        self.assert_results(
            _with_token(
                self.enrollment_status_order_query,
                _make_pagination_token(["PARTICIPANT_PLUS_EHR", "Aardvark", "Bob", datetime.date(1978, 10, 10), 2]),
            ),
            [ps_3, ps_4],
        )

    def test_update_from_samples(self):
        # baseline_tests = ['BASELINE1', 'BASELINE2']
        baseline_tests = ["1PST8", "2PST8"]

        config.override_setting(config.BASELINE_SAMPLE_TEST_CODES, baseline_tests)
        self.dao.update_from_biobank_stored_samples()  # safe noop

        p_baseline_samples = self._insert(Participant(participantId=1, biobankId=11))
        p_mixed_samples = self._insert(Participant(participantId=2, biobankId=22))
        p_no_samples = self._insert(Participant(participantId=3, biobankId=33))
        p_unconfirmed = self._insert(Participant(participantId=4, biobankId=44))
        self.assertEqual(self.dao.get(p_baseline_samples.participantId).numBaselineSamplesArrived, 0)

        def get_p_baseline_last_modified():
            return self.dao.get(p_baseline_samples.participantId).lastModified

        p_baseline_last_modified1 = get_p_baseline_last_modified()

        sample_dao = BiobankStoredSampleDao()

        def add_sample(participant, test_code, sample_id):
            TIME = datetime.datetime(2018, 3, 2)
            sample_dao.insert(
                BiobankStoredSample(
                    biobankStoredSampleId=sample_id,
                    biobankId=participant.biobankId,
                    biobankOrderIdentifier="KIT",
                    test=test_code,
                    confirmed=TIME,
                )
            )

        add_sample(p_baseline_samples, baseline_tests[0], "11111")
        add_sample(p_baseline_samples, baseline_tests[1], "22223")
        add_sample(p_mixed_samples, baseline_tests[0], "11112")
        add_sample(p_mixed_samples, "NOT1", "44441")
        # add unconfirmed sample
        sample_dao.insert(
            BiobankStoredSample(
                biobankStoredSampleId=55555,
                biobankId=p_unconfirmed.biobankId,
                biobankOrderIdentifier="KIT",
                test=baseline_tests[1],
                confirmed=None,
            )
        )
        # sleep 1 sec to make lastModified different
        time.sleep(1)
        self.dao.update_from_biobank_stored_samples()

        p_baseline_last_modified2 = get_p_baseline_last_modified()
        self.assertNotEqual(p_baseline_last_modified2, p_baseline_last_modified1)

        self.assertEqual(self.dao.get(p_baseline_samples.participantId).numBaselineSamplesArrived, 2)
        self.assertEqual(self.dao.get(p_mixed_samples.participantId).numBaselineSamplesArrived, 1)
        self.assertEqual(self.dao.get(p_no_samples.participantId).numBaselineSamplesArrived, 0)
        self.assertEqual(self.dao.get(p_unconfirmed.participantId).numBaselineSamplesArrived, 0)

        M_baseline_samples = self._insert(Participant(participantId=9, biobankId=99))
        add_sample(M_baseline_samples, baseline_tests[0], "999")
        M_first_update = self.dao.get(M_baseline_samples.participantId)
        # sleep 1 sec to make lastModified different
        time.sleep(1)
        self.dao.update_from_biobank_stored_samples()
        add_sample(M_baseline_samples, baseline_tests[1], "9999")
        M_second_update = self.dao.get(M_baseline_samples.participantId)
        # sleep 1 sec to make lastModified different
        time.sleep(1)
        self.dao.update_from_biobank_stored_samples()

        self.assertNotEqual(M_first_update.lastModified, M_second_update.lastModified)
        self.assertEqual(get_p_baseline_last_modified(), p_baseline_last_modified2)

    def test_update_from_samples_changed_tests(self):
        baseline_tests = ["1PST8", "2PST8"]
        config.override_setting(config.BASELINE_SAMPLE_TEST_CODES, baseline_tests)
        self.dao.update_from_biobank_stored_samples()  # safe noop

        participant = self._insert(Participant(participantId=1, biobankId=11))
        self.assertEqual(self.dao.get(participant.participantId).numBaselineSamplesArrived, 0)

        sample_dao = BiobankStoredSampleDao()

        def add_sample(test_code, sample_id):
            TIME = datetime.datetime(2018, 3, 2)
            sample_dao.insert(
                BiobankStoredSample(
                    biobankStoredSampleId=sample_id,
                    biobankId=participant.biobankId,
                    biobankOrderIdentifier="KIT",
                    test=test_code,
                    confirmed=TIME,
                )
            )

        add_sample(baseline_tests[0], "11111")
        add_sample(baseline_tests[1], "22223")
        self.dao.update_from_biobank_stored_samples()
        summary = self.dao.get(participant.participantId)
        init_last_modified = summary.lastModified
        self.assertEqual(summary.numBaselineSamplesArrived, 2)
        # sleep 1 sec to make lastModified different
        time.sleep(1)
        # Simulate removal of one of the baseline tests from config.json.
        baseline_tests.pop()
        config.override_setting(config.BASELINE_SAMPLE_TEST_CODES, baseline_tests)
        self.dao.update_from_biobank_stored_samples()

        summary = self.dao.get(participant.participantId)
        self.assertEqual(summary.numBaselineSamplesArrived, 1)
        self.assertNotEqual(init_last_modified, summary.lastModified)

    def test_only_update_dna_sample(self):
        dna_tests = ["1ED10", "1SAL2"]

        config.override_setting(config.DNA_SAMPLE_TEST_CODES, dna_tests)
        self.dao.update_from_biobank_stored_samples()  # safe noop

        p_dna_samples = self._insert(Participant(participantId=1, biobankId=11))

        self.assertEqual(self.dao.get(p_dna_samples.participantId).samplesToIsolateDNA, None)
        self.assertEqual(self.dao.get(p_dna_samples.participantId).enrollmentStatusCoreStoredSampleTime, None)
        self.assertEqual(self.dao.get(p_dna_samples.participantId).enrollmentStatusCoreOrderedSampleTime, None)

        sample_dao = BiobankStoredSampleDao()

        def add_sample(participant, test_code, sample_id, confirmed_time):
            sample_dao.insert(
                BiobankStoredSample(
                    biobankStoredSampleId=sample_id,
                    biobankId=participant.biobankId,
                    biobankOrderIdentifier="KIT",
                    test=test_code,
                    confirmed=confirmed_time,
                )
            )

        confirmed_time_0 = datetime.datetime(2018, 3, 1)
        add_sample(p_dna_samples, dna_tests[0], "11111", confirmed_time_0)

        self.dao.update_from_biobank_stored_samples()

        self.assertEqual(self.dao.get(p_dna_samples.participantId).samplesToIsolateDNA, SampleStatus.RECEIVED)
        # only update dna sample will not update enrollmentStatusCoreStoredSampleTime
        self.assertEqual(self.dao.get(p_dna_samples.participantId).enrollmentStatusCoreStoredSampleTime, None)
        self.assertEqual(self.dao.get(p_dna_samples.participantId).enrollmentStatusCoreOrderedSampleTime, None)

    def test_sample_time_with_missing_status(self):
        """
        Legacy stored samples that are missing a status value should be treated as UNSET when finding
        the sample status time for participant summary
        """

        participant = self._insert(Participant(participantId=1, biobankId=11))
        confirmed_time = datetime.datetime(2018, 3, 1)
        sample = self.data_generator.create_database_biobank_stored_sample(
            biobankId=participant.biobankId,
            test='1ED10',
            confirmed=confirmed_time
        )

        # Sqlalchemy uses the default set for the status column when inserting the sample
        # (even if we set the field to None when creating it).
        # But setting it to None and then updating gets the NULL to appear and recreates what we're seeing in Prod.
        sample.status = None
        self.session.commit()

        self.dao.update_from_biobank_stored_samples()

        participant_summary = self.dao.get(participant.participantId)
        self.assertEqual(confirmed_time, participant_summary.sampleStatus1ED10Time)

    def testUpdateEnrollmentStatus(self):
        ehr_consent_authored_time = datetime.datetime(2018, 3, 1)
        self.mock_ehr_interest_ranges.return_value = [
            DateRange(start=ehr_consent_authored_time)
        ]
        summary = ParticipantSummary(
            participantId=1,
            biobankId=2,
            consentForStudyEnrollment=QuestionnaireStatus.SUBMITTED,
            consentForElectronicHealthRecords=QuestionnaireStatus.SUBMITTED,
            consentForElectronicHealthRecordsAuthored=ehr_consent_authored_time,
            enrollmentStatus=EnrollmentStatus.INTERESTED,
            enrollmentStatusV3_0=EnrollmentStatusV30.PARTICIPANT,
            enrollmentStatusV3_1=EnrollmentStatusV31.PARTICIPANT
        )
        self.dao.update_enrollment_status(summary, session=mock.MagicMock())
        self.assertEqual(EnrollmentStatusV31.PARTICIPANT_PLUS_EHR, summary.enrollmentStatusV3_1)
        self.assertEqual(ehr_consent_authored_time, summary.enrollmentStatusParticipantPlusEhrV3_1Time)

        sample_time = datetime.datetime(2019, 3, 1)
        summary = ParticipantSummary(
            participantId=1,
            biobankId=2,
            consentForStudyEnrollment=QuestionnaireStatus.SUBMITTED,
            consentForElectronicHealthRecords=QuestionnaireStatus.SUBMITTED,
            consentForElectronicHealthRecordsAuthored=ehr_consent_authored_time,
            numCompletedBaselinePPIModules=NUM_BASELINE_PPI_MODULES,
            samplesToIsolateDNA=SampleStatus.RECEIVED,
            questionnaireOnTheBasicsAuthored=ehr_consent_authored_time,
            questionnaireOnLifestyleAuthored=ehr_consent_authored_time,
            questionnaireOnOverallHealthAuthored=ehr_consent_authored_time,
            clinicPhysicalMeasurementsStatus=PhysicalMeasurementsStatus.UNSET,
            selfReportedPhysicalMeasurementsStatus=SelfReportedPhysicalMeasurementsStatus.UNSET,
            enrollmentStatus=EnrollmentStatus.MEMBER,
            sampleStatus2ED10Time=sample_time,
            enrollmentStatusV3_0=EnrollmentStatusV30.PARTICIPANT,
            enrollmentStatusV3_1=EnrollmentStatusV31.PARTICIPANT
        )
        self.dao.update_enrollment_status(summary, session=mock.MagicMock())
        self.assertEqual(EnrollmentStatusV31.CORE_MINUS_PM, summary.enrollmentStatusV3_1)
        self.assertEqual(sample_time, summary.enrollmentStatusCoreMinusPmV3_1Time)

        summary.clinicPhysicalMeasurementsStatus = PhysicalMeasurementsStatus.COMPLETED
        summary.clinicPhysicalMeasurementsFinalizedTime = datetime.datetime(2022, 7, 12)
        self.dao.update_enrollment_status(summary, session=mock.MagicMock())
        self.assertEqual(EnrollmentStatusV31.CORE_PARTICIPANT, summary.enrollmentStatusV3_1)
        self.assertEqual(summary.clinicPhysicalMeasurementsFinalizedTime, summary.enrollmentStatusCoreV3_1Time)

    def testUpdateEnrollmentStatusSelfReportedPm(self):
        ehr_consent_authored_time = datetime.datetime(2018, 3, 1)
        self.mock_ehr_interest_ranges.return_value = [
            DateRange(start=ehr_consent_authored_time)
        ]

        sample_time = datetime.datetime(2019, 3, 1)
        summary = ParticipantSummary(
            participantId=1,
            biobankId=2,
            consentForStudyEnrollment=QuestionnaireStatus.SUBMITTED,
            consentForElectronicHealthRecords=QuestionnaireStatus.SUBMITTED,
            consentForElectronicHealthRecordsAuthored=ehr_consent_authored_time,
            numCompletedBaselinePPIModules=NUM_BASELINE_PPI_MODULES,
            samplesToIsolateDNA=SampleStatus.RECEIVED,
            clinicPhysicalMeasurementsStatus=PhysicalMeasurementsStatus.UNSET,
            selfReportedPhysicalMeasurementsStatus=SelfReportedPhysicalMeasurementsStatus.UNSET,
            enrollmentStatus=EnrollmentStatus.MEMBER,
            questionnaireOnTheBasicsAuthored=ehr_consent_authored_time,
            questionnaireOnLifestyleAuthored=ehr_consent_authored_time,
            questionnaireOnOverallHealthAuthored=ehr_consent_authored_time,
            sampleStatus2ED10Time=sample_time,
            enrollmentStatusV3_0=EnrollmentStatusV30.PARTICIPANT,
            enrollmentStatusV3_1=EnrollmentStatusV31.PARTICIPANT
        )
        self.dao.update_enrollment_status(summary, session=mock.MagicMock())
        self.assertEqual(EnrollmentStatusV31.CORE_MINUS_PM, summary.enrollmentStatusV3_1)

        summary.selfReportedPhysicalMeasurementsStatus = SelfReportedPhysicalMeasurementsStatus.COMPLETED
        summary.selfReportedPhysicalMeasurementsAuthored = datetime.datetime(2019, 7, 12)
        self.dao.update_enrollment_status(summary, session=mock.MagicMock())
        self.assertEqual(EnrollmentStatusV31.CORE_PARTICIPANT, summary.enrollmentStatusV3_1)

    def testDowngradeCoreMinusPm(self):
        """Check that a participant that has achieved CORE_MINUS_PM status isn't downgraded from it"""
        sample_time = datetime.datetime(2019, 3, 1)
        self.mock_ehr_interest_ranges.return_value = []

        summary = ParticipantSummary(
            participantId=1,
            biobankId=2,
            consentForStudyEnrollment=QuestionnaireStatus.SUBMITTED,
            numCompletedBaselinePPIModules=NUM_BASELINE_PPI_MODULES,
            samplesToIsolateDNA=SampleStatus.RECEIVED,
            clinicPhysicalMeasurementsStatus=PhysicalMeasurementsStatus.UNSET,
            sampleStatus2ED10Time=sample_time,
            enrollmentStatusCoreMinusPMTime=sample_time,
            enrollmentStatus=EnrollmentStatus.CORE_MINUS_PM,
            enrollmentStatusV3_0=EnrollmentStatusV30.CORE_MINUS_PM,
            enrollmentStatusV3_1=EnrollmentStatusV31.CORE_MINUS_PM
        )
        self.dao.update_enrollment_status(summary, session=mock.MagicMock())
        self.assertEqual(EnrollmentStatusV31.CORE_MINUS_PM, summary.enrollmentStatusV3_1)

    def testCoreStatusRemains(self):
        status_time = datetime.datetime(2020, 6, 1)
        participant_summary = self.data_generator._participant_summary_with_defaults(
            enrollmentStatusV3_1=EnrollmentStatusV31.CORE_PARTICIPANT,
            enrollmentStatusCoreV3_1Time=status_time
        )
        self.dao.update_enrollment_status(participant_summary, session=mock.MagicMock())
        self.assertEqual(EnrollmentStatusV31.CORE_PARTICIPANT, participant_summary.enrollmentStatusV3_1)
        self.assertEqual(status_time, participant_summary.enrollmentStatusCoreV3_1Time)

    def testUpdateEnrollmentStatusLastModified(self):
        """DA-631: enrollment_status update should update last_modified."""
        participant = self._insert(Participant(participantId=6, biobankId=66))
        # collect current modified and enrollment status
        summary = self.dao.get(participant.participantId)
        test_dt = datetime.datetime(2018, 11, 1)

        def reset_summary():
            # change summary so enrollment status will be changed from INTERESTED to MEMBER.
            summary.enrollmentStatusV3_1 = EnrollmentStatusV31.PARTICIPANT
            summary.lastModified = test_dt
            summary.consentForStudyEnrollment = QuestionnaireStatus.SUBMITTED
            summary.consentForElectronicHealthRecords = QuestionnaireStatus.SUBMITTED
            summary.clinicPhysicalMeasurementsStatus = PhysicalMeasurementsStatus.COMPLETED
            summary.samplesToIsolateDNA = SampleStatus.RECEIVED
            self.dao.update(summary)

        ## Test Step 1: Validate update_from_biobank_stored_samples() changes lastModified.
        self.mock_ehr_interest_ranges.return_value = [
            DateRange(start=datetime.datetime(2018, 10, 3))
        ]
        reset_summary()

        # Update and reload summary record
        self.dao.update_from_biobank_stored_samples(
            participant_id=participant.participantId,
            biobank_ids=[participant.biobankId]
        )
        summary = self.dao.get(participant.participantId)

        # Test that status has changed and lastModified is also different
        self.assertEqual(EnrollmentStatusV31.PARTICIPANT_PLUS_EHR, summary.enrollmentStatusV3_1)
        self.assertNotEqual(test_dt, summary.lastModified)

        ## Test Step 2: Validate that update_enrollment_status() changes the lastModified property.
        reset_summary()
        summary = self.dao.get(participant.participantId)

        self.assertEqual(test_dt, summary.lastModified)

        # update_enrollment_status() does not touch the db, it only modifies object properties.
        self.dao.update_enrollment_status(summary, session=mock.MagicMock())

        self.assertEqual(EnrollmentStatusV31.PARTICIPANT_PLUS_EHR, summary.enrollmentStatusV3_1)
        self.assertNotEqual(test_dt, summary.lastModified)

    def testNumberDistinctVisitsCounts(self):
        self.participant = self._insert(Participant(participantId=7, biobankId=77))
        # insert biobank order
        order = self.order_dao.insert(self._make_biobank_order())
        summary = self.dao.get(self.participant.participantId)
        self.assertEqual(summary.numberDistinctVisits, 1)
        cancel_request = self.cancel_biobank_order()
        # cancel biobank order
        self.order_dao.update_with_patch(order.biobankOrderId, cancel_request, order.version)
        summary = self.dao.get(self.participant.participantId)
        # distinct count should be 0
        self.assertEqual(summary.numberDistinctVisits, 0)

        self.measurement_json = json.dumps(load_measurement_json(self.participant.participantId, TIME_1.isoformat()))
        # insert physical measurement
        measurement = self.measurement_dao.insert(self._make_physical_measurements())
        summary = self.dao.get(self.participant.participantId)
        # count should be 1
        self.assertEqual(summary.numberDistinctVisits, 1)

        # cancel the measurement
        cancel_measurement = self.get_restore_or_cancel_info()
        with self.measurement_dao.session() as session:
            self.measurement_dao.update_with_patch(measurement.physicalMeasurementsId, session, cancel_measurement)

        summary = self.dao.get(self.participant.participantId)
        # count should be 0
        self.assertEqual(summary.numberDistinctVisits, 0)

        with clock.FakeClock(TIME_1):
            self.order_dao.insert(
                self._make_biobank_order(
                    biobankOrderId="2",
                    identifiers=[BiobankOrderIdentifier(system="b", value="d")],
                    samples=[
                        BiobankOrderedSample(
                            biobankOrderId="2",
                            test=BIOBANK_TESTS[0],
                            description="description",
                            processingRequired=True,
                        )
                    ],
                )
            )
        with clock.FakeClock(TIME_2):
            self.measurement_dao.insert(self._make_physical_measurements(physicalMeasurementsId=2))
            summary = self.dao.get(self.participant.participantId)

            # A PM on another day should add a new distinct count.
            self.assertEqual(summary.numberDistinctVisits, 2)

        with clock.FakeClock(TIME_3):
            self.order_dao.insert(
                self._make_biobank_order(
                    biobankOrderId="3",
                    identifiers=[BiobankOrderIdentifier(system="s", value="s")],
                    samples=[
                        BiobankOrderedSample(
                            biobankOrderId="3",
                            finalized=TIME_3,
                            test=BIOBANK_TESTS[1],
                            description="another description",
                            processingRequired=False,
                        )
                    ],
                )
            )

            # a physical measurement on same day as biobank order does not add distinct visit.
            self.measurement_dao.insert(self._make_physical_measurements(physicalMeasurementsId=6))

            # another biobank order on the same day should also not add a distinct visit
            self.order_dao.insert(
                self._make_biobank_order(
                    biobankOrderId="7",
                    identifiers=[BiobankOrderIdentifier(system="x", value="x")],
                    samples=[
                        BiobankOrderedSample(
                            biobankOrderId="7",
                            finalized=TIME_3,
                            test=BIOBANK_TESTS[1],
                            description="another description",
                            processingRequired=False,
                        )
                    ],
                )
            )

            summary = self.dao.get(self.participant.participantId)
            # 1 from each of TIME_1 TIME_2 TIME_3
            self.assertEqual(summary.numberDistinctVisits, 3)

    def test_qa_scenarios_for_pmb_visits(self):
        """ PDR at https://docs.google.com/document/d/1sL54f-I91RvhjIprrdbwD8TlR9Jq91MX2ELf1EtJdxc/edit#heading=h.bqo8kt3igsrw<Paste> """
        self.participant = self._insert(Participant(participantId=6, biobankId=66))

        # test scenario 1
        with clock.FakeClock(TIME_4):
            self.measurement_json = json.dumps(
                load_measurement_json(self.participant.participantId, TIME_4.isoformat())
            )
            self.measurement_dao.insert(
                self._make_physical_measurements(
                    physicalMeasurementsId=666, participantId=self.participant.participantId, finalized=TIME_4
                )
            )
            summary = self.dao.get(self.participant.participantId)
            self.assertEqual(summary.numberDistinctVisits, 1)

        with clock.FakeClock(TIME_5):
            self.measurement_json = json.dumps(
                load_measurement_json(self.participant.participantId, TIME_5.isoformat())
            )
            self.measurement_dao.insert(self._make_physical_measurements(physicalMeasurementsId=669, finalized=TIME_5))
            summary = self.dao.get(self.participant.participantId)
            self.assertEqual(summary.numberDistinctVisits, 2)

        # test scenario 2
        with clock.FakeClock(TIME_6):
            self.participant = self._insert(Participant(participantId=9, biobankId=13))
            self.measurement_json = json.dumps(
                load_measurement_json(self.participant.participantId, TIME_6.isoformat())
            )
            self.measurement_dao.insert(self._make_physical_measurements(physicalMeasurementsId=8, finalized=TIME_6))
            self.order_dao.insert(
                self._make_biobank_order(
                    biobankOrderId="2",
                    identifiers=[BiobankOrderIdentifier(system="b", value="d")],
                    samples=[
                        BiobankOrderedSample(
                            biobankOrderId="2",
                            finalized=TIME_7,
                            test=BIOBANK_TESTS[0],
                            description="description",
                            processingRequired=True,
                        )
                    ],
                )
            )

            summary = self.dao.get(self.participant.participantId)
            # distinct count should be 2
            self.assertEqual(summary.numberDistinctVisits, 2)

        # test scenario 3
        with clock.FakeClock(TIME_6):
            self.participant = self._insert(Participant(participantId=66, biobankId=42))
            self.measurement_json = json.dumps(
                load_measurement_json(self.participant.participantId, TIME_6.isoformat())
            )
            self.measurement_dao.insert(
                self._make_physical_measurements(physicalMeasurementsId=12, createdSiteId=2, finalized=TIME_6)
            )

            self.order_dao.insert(
                self._make_biobank_order(
                    biobankOrderId="18",
                    finalizedSiteId=1,
                    identifiers=[BiobankOrderIdentifier(system="x", value="y")],
                    samples=[
                        BiobankOrderedSample(
                            biobankOrderId="18",
                            finalized=TIME_6,
                            test=BIOBANK_TESTS[0],
                            description="description",
                            processingRequired=True,
                        )
                    ],
                )
            )

            summary = self.dao.get(self.participant.participantId)
            # distinct count should be 1
            self.assertEqual(summary.numberDistinctVisits, 1)

        # test scenario 4
        with clock.FakeClock(TIME_8):
            self.participant = self._insert(Participant(participantId=6613, biobankId=142))
            self.measurement_json = json.dumps(
                load_measurement_json(self.participant.participantId, TIME_8.isoformat())
            )
            self.measurement_dao.insert(self._make_physical_measurements(physicalMeasurementsId=129, finalized=TIME_8))

            order = self.order_dao.insert(
                self._make_biobank_order(
                    biobankOrderId="999",
                    identifiers=[BiobankOrderIdentifier(system="s", value="s")],
                    samples=[
                        BiobankOrderedSample(
                            biobankOrderId="999",
                            finalized=TIME_8,
                            test=BIOBANK_TESTS[1],
                            description="description",
                            processingRequired=True,
                        )
                    ],
                )
            )
            summary = self.dao.get(self.participant.participantId)
            # distinct count should be 1
            self.assertEqual(summary.numberDistinctVisits, 1)

            # change finalized time, recalculating count
            with self.order_dao.session() as session:
                existing_order = copy.deepcopy(order)
                order.samples[0].finalized = TIME_9
                self.order_dao._do_update(session, order, existing_order)

            summary = self.dao.get(self.participant.participantId)
            self.assertEqual(summary.numberDistinctVisits, 1)

            # change test, should not change count.
            with self.order_dao.session() as session:
                existing_order = copy.deepcopy(order)
                order.samples[0].test = BIOBANK_TESTS[0]
                self.order_dao._do_update(session, order, existing_order)

            summary = self.dao.get(self.participant.participantId)
            self.assertEqual(summary.numberDistinctVisits, 1)

        # test scenario 5
        with clock.FakeClock(TIME_12):
            self.participant = self._insert(Participant(participantId=3000, biobankId=2019))

            self.order_dao.insert(
                self._make_biobank_order(
                    biobankOrderId="700",
                    identifiers=[BiobankOrderIdentifier(system="n", value="s")],
                    samples=[
                        BiobankOrderedSample(
                            biobankOrderId="700",
                            finalized=TIME_10,
                            test=BIOBANK_TESTS[1],
                            description="description",
                            processingRequired=True,
                        )
                    ],
                )
            )
            summary = self.dao.get(self.participant.participantId)
            # distinct count should be 1
            self.assertEqual(summary.numberDistinctVisits, 1)

            other_order = self.order_dao.insert(
                self._make_biobank_order(
                    biobankOrderId="701",
                    identifiers=[BiobankOrderIdentifier(system="n", value="t")],
                    samples=[
                        BiobankOrderedSample(
                            biobankOrderId="701",
                            finalized=TIME_11,
                            test=BIOBANK_TESTS[1],
                            description="description",
                            processingRequired=True,
                        )
                    ],
                )
            )
            summary = self.dao.get(self.participant.participantId)
            # distinct count should be 2
            self.assertEqual(summary.numberDistinctVisits, 2)

            order = self.order_dao.insert(
                self._make_biobank_order(
                    biobankOrderId="702",
                    identifiers=[BiobankOrderIdentifier(system="n", value="u")],
                    samples=[
                        BiobankOrderedSample(
                            biobankOrderId="702",
                            finalized=TIME_12,
                            test=BIOBANK_TESTS[1],
                            description="description",
                            processingRequired=True,
                        )
                    ],
                )
            )
            summary = self.dao.get(self.participant.participantId)
            # distinct count should be 3
            self.assertEqual(summary.numberDistinctVisits, 3)

            self.measurement_json = json.dumps(
                load_measurement_json(self.participant.participantId, TIME_12.isoformat())
            )
            self.measurement_dao.insert(
                self._make_physical_measurements(physicalMeasurementsId=120, finalized=TIME_12)
            )

            summary = self.dao.get(self.participant.participantId)
            # distinct count should be 3
            self.assertEqual(summary.numberDistinctVisits, 3)
            cancel_request = self.cancel_biobank_order()
            # cancel biobank order with PM on same day
            self.order_dao.update_with_patch(order.biobankOrderId, cancel_request, order.version)
            summary = self.dao.get(self.participant.participantId)
            # distinct count should be 3 (the PM on same day still counts)
            self.assertEqual(summary.numberDistinctVisits, 3)

            self.measurement_json = json.dumps(
                load_measurement_json(self.participant.participantId, TIME_1.isoformat())
            )
            self.measurement_dao.insert(self._make_physical_measurements(physicalMeasurementsId=150, finalized=TIME_1))
            summary = self.dao.get(self.participant.participantId)
            # distinct count should be 4
            self.assertEqual(summary.numberDistinctVisits, 4)
            # cancel order with pm on different day
            self.order_dao.update_with_patch(other_order.biobankOrderId, cancel_request, order.version)
            summary = self.dao.get(self.participant.participantId)
            # distinct count should be 3
            self.assertEqual(summary.numberDistinctVisits, 3)

    def test_pm_restore_cancel_biobank_restore_cancel(self):
        self.participant = self._insert(Participant(participantId=9, biobankId=13))
        self.measurement_json = json.dumps(load_measurement_json(self.participant.participantId, TIME_4.isoformat()))
        measurement = self.measurement_dao.insert(
            self._make_physical_measurements(physicalMeasurementsId=669, finalized=TIME_4)
        )
        summary = self.dao.get(self.participant.participantId)
        self.assertEqual(summary.numberDistinctVisits, 1)

        with clock.FakeClock(TIME_5):
            order = self.order_dao.insert(
                self._make_biobank_order(
                    biobankOrderId="2",
                    identifiers=[BiobankOrderIdentifier(system="b", value="d")],
                    samples=[
                        BiobankOrderedSample(
                            biobankOrderId="2",
                            finalized=TIME_5,
                            test=BIOBANK_TESTS[0],
                            description="description",
                            processingRequired=True,
                        )
                    ],
                )
            )

        with clock.FakeClock(TIME_7):
            summary = self.dao.get(self.participant.participantId)
            # distinct count should be 2
            self.assertEqual(summary.numberDistinctVisits, 2)

            # cancel the measurement
            cancel_measurement = self.get_restore_or_cancel_info()
            with self.measurement_dao.session() as session:
                self.measurement_dao.update_with_patch(measurement.physicalMeasurementsId, session, cancel_measurement)

            summary = self.dao.get(self.participant.participantId)
            self.assertEqual(summary.numberDistinctVisits, 1)

        with clock.FakeClock(TIME_7):
            restore_measurement = self.get_restore_or_cancel_info(status="restored")
            with self.measurement_dao.session() as session:
                self.measurement_dao.update_with_patch(
                    measurement.physicalMeasurementsId, session, restore_measurement
                )

            summary = self.dao.get(self.participant.participantId)
            self.assertEqual(summary.numberDistinctVisits, 2)

            cancel_request = self.cancel_biobank_order()
            order = self.order_dao.update_with_patch(order.biobankOrderId, cancel_request, order.version)

            summary = self.dao.get(self.participant.participantId)
            self.assertEqual(summary.numberDistinctVisits, 1)

            restore_order = self.get_restore_or_cancel_info(status="restored")
            restore_order["amendedReason"] = "some reason"
            self.order_dao.update_with_patch(order.biobankOrderId, restore_order, order.version)
            summary = self.dao.get(self.participant.participantId)
            self.assertEqual(summary.numberDistinctVisits, 2)

    def test_amending_biobank_order_distinct_visit_count(self):
        self.participant = self._insert(Participant(participantId=9, biobankId=13))
        with clock.FakeClock(TIME_5):
            order = self.order_dao.insert(
                self._make_biobank_order(
                    biobankOrderId="2",
                    identifiers=[BiobankOrderIdentifier(system="b", value="d")],
                    samples=[
                        BiobankOrderedSample(
                            biobankOrderId="2",
                            finalized=TIME_5,
                            test=BIOBANK_TESTS[0],
                            description="description",
                            processingRequired=True,
                        )
                    ],
                )
            )

            summary = self.dao.get(self.participant.participantId)
            self.assertEqual(summary.numberDistinctVisits, 1)

        with clock.FakeClock(TIME_7):
            amend_order = self._get_amended_info(order)
            with self.order_dao.session() as session:
                self.order_dao._do_update(session, amend_order, order)

            # Shouldn't change on a simple amendment (unless finalized time on samples change)
            summary = self.dao.get(self.participant.participantId)
            self.assertEqual(summary.numberDistinctVisits, 1)

        with clock.FakeClock(TIME_7_5):
            cancel_request = self.cancel_biobank_order()
            order = self.order_dao.update_with_patch(order.biobankOrderId, cancel_request, order.version)

        # A cancelled order (even after amending) should reduce count (unless some other valid order on same day)
        summary = self.dao.get(self.participant.participantId)
        self.assertEqual(summary.numberDistinctVisits, 0)

    def test_get_participant_summary_from_pid(self):
        participant_one = self.data_generator.create_database_participant()
        pid = participant_one.participantId

        participant_summary = self.dao.get_by_participant_id(pid)
        self.assertIsNone(participant_summary)

        self.data_generator.create_database_participant_summary(participant=participant_one)

        participant_summary = self.dao.get_by_participant_id(pid)
        self.assertIsNotNone(participant_summary)
        self.assertEqual(pid, participant_summary.participantId)

    def test_create_synthetic_pm_fields(self):
        summary_1 = ParticipantSummary(
            participantId=1
        )
        results = self.dao.to_client_json(summary_1)
        self.assertEqual(results["physicalMeasurementsStatus"], "UNSET")
        self.assertEqual(results["clinicPhysicalMeasurementsStatus"], "UNSET")
        self.assertEqual(results["selfReportedPhysicalMeasurementsStatus"], "UNSET")
        self.assertEqual(results["physicalMeasurementsCollectType"], "UNSET")

        summary_2 = ParticipantSummary(
            participantId=2,
            clinicPhysicalMeasurementsStatus=PhysicalMeasurementsStatus.COMPLETED,
            clinicPhysicalMeasurementsFinalizedTime=TIME_1
        )
        results = self.dao.to_client_json(summary_2)
        self.assertEqual(results["physicalMeasurementsStatus"], "COMPLETED")
        self.assertEqual(results["clinicPhysicalMeasurementsStatus"], "COMPLETED")
        self.assertEqual(results["selfReportedPhysicalMeasurementsStatus"], "UNSET")
        self.assertEqual(results["physicalMeasurementsFinalizedTime"], TIME_1.isoformat())
        self.assertEqual(results["physicalMeasurementsCollectType"], "SITE")

        summary_3 = ParticipantSummary(
            participantId=3,
            selfReportedPhysicalMeasurementsStatus=PhysicalMeasurementsStatus.COMPLETED,
            selfReportedPhysicalMeasurementsAuthored=TIME_2
        )
        results = self.dao.to_client_json(summary_3)
        self.assertEqual(results["physicalMeasurementsStatus"], "COMPLETED")
        self.assertEqual(results["clinicPhysicalMeasurementsStatus"], "UNSET")
        self.assertEqual(results["selfReportedPhysicalMeasurementsStatus"], "COMPLETED")
        self.assertEqual(results["physicalMeasurementsFinalizedTime"], TIME_2.isoformat())
        self.assertEqual(results["physicalMeasurementsCollectType"], "SELF_REPORTED")

        summary_4 = ParticipantSummary(
            participantId=4,
            selfReportedPhysicalMeasurementsStatus=PhysicalMeasurementsStatus.COMPLETED,
            selfReportedPhysicalMeasurementsAuthored=TIME_2,
            clinicPhysicalMeasurementsStatus=PhysicalMeasurementsStatus.COMPLETED,
            clinicPhysicalMeasurementsFinalizedTime=TIME_1
        )
        results = self.dao.to_client_json(summary_4)
        self.assertEqual(results["physicalMeasurementsStatus"], "COMPLETED")
        self.assertEqual(results["clinicPhysicalMeasurementsStatus"], "COMPLETED")
        self.assertEqual(results["selfReportedPhysicalMeasurementsStatus"], "COMPLETED")
        self.assertEqual(results["physicalMeasurementsFinalizedTime"], TIME_2.isoformat())
        self.assertEqual(results["physicalMeasurementsCollectType"], "SELF_REPORTED")

        summary_5 = ParticipantSummary(
            participantId=5,
            selfReportedPhysicalMeasurementsStatus=PhysicalMeasurementsStatus.COMPLETED,
            selfReportedPhysicalMeasurementsAuthored=TIME_4,
            clinicPhysicalMeasurementsStatus=PhysicalMeasurementsStatus.COMPLETED,
            clinicPhysicalMeasurementsFinalizedTime=TIME_3
        )
        results = self.dao.to_client_json(summary_5)
        self.assertEqual(results["physicalMeasurementsStatus"], "COMPLETED")
        self.assertEqual(results["clinicPhysicalMeasurementsStatus"], "COMPLETED")
        self.assertEqual(results["selfReportedPhysicalMeasurementsStatus"], "COMPLETED")
        self.assertEqual(results["physicalMeasurementsFinalizedTime"], TIME_3.isoformat())
        self.assertEqual(results["physicalMeasurementsCollectType"], "SITE")

    def test_parse_enums_from_resource(self):
        resource = {
            "withdrawalStatus": "NO_USE",
            "suspensionStatus": "NOT_SUSPENDED"
        }

        all_strings = all([val for val in resource.values()
                           if isinstance(type(val), str)])
        self.assertTrue(all_strings)

        resource = self.dao.parse_resource_enums(resource)

        all_enums = all([key for key in resource.keys()
                         if type(key).__class__.__name__ == '_EnumClass'])
        self.assertTrue(all_enums)

    @staticmethod
    def _get_amended_info(order):
        amendment = dict(
            amendedReason="I had to change something",
            amendedInfo={
                "author": {"system": "https://www.pmi-ops.org/healthpro-username", "value": "mike@pmi-ops.org"},
                "site": {"system": "https://www.pmi-ops.org/site-id", "value": "hpo-site-monroeville"},
            },
        )

        order.amendedReason = amendment["amendedReason"]
        order.amendedInfo = amendment["amendedInfo"]
        return order

    def _make_biobank_order(self, **kwargs):
        """Makes a new BiobankOrder (same values every time) with valid/complete defaults.

    Kwargs pass through to BiobankOrder constructor, overriding defaults.
    """
        for k, default_value in (
            ("biobankOrderId", "1"),
            ("created", clock.CLOCK.now()),
            ("participantId", self.participant.participantId),
            ("sourceSiteId", 1),
            ("sourceUsername", "fred@pmi-ops.org"),
            ("collectedSiteId", 1),
            ("collectedUsername", "joe@pmi-ops.org"),
            ("processedSiteId", 1),
            ("processedUsername", "sue@pmi-ops.org"),
            ("finalizedSiteId", 2),
            ("finalizedUsername", "bob@pmi-ops.org"),
            ("identifiers", [BiobankOrderIdentifier(system="a", value="c")]),
            (
                "samples",
                [
                    BiobankOrderedSample(
                        biobankOrderId="1",
                        test=BIOBANK_TESTS[0],
                        description="description",
                        finalized=TIME_1,
                        processingRequired=True,
                    )
                ],
            ),
        ):
            if k not in kwargs:
                kwargs[k] = default_value
        return BiobankOrder(**kwargs)

    def _make_physical_measurements(self, **kwargs):
        """Makes a new PhysicalMeasurements (same values every time) with valid/complete defaults.

    Kwargs pass through to PM constructor, overriding defaults.
    """
        for k, default_value in (
            ("physicalMeasurementsId", 1),
            ("participantId", self.participant.participantId),
            ("createdSiteId", 1),
            ("finalized", TIME_3),
            ("finalizedSiteId", 2),
        ):
            if k not in kwargs:
                kwargs[k] = default_value
        record = PhysicalMeasurements(**kwargs)
        PhysicalMeasurementsDao.store_record_fhir_doc(record, self.measurement_json)
        return record


def _with_token(query, token):
    return Query(query.field_filters, query.order_by, query.max_results, token)


def _make_pagination_token(vals):
    vals_json = json.dumps(vals, default=json_serial)
    return urlsafe_b64encode(str.encode(vals_json))


def _decode_token(token):
    if token is None:
        return None
    return json.loads(urlsafe_b64decode(token))
