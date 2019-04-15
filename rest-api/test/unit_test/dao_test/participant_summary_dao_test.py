import datetime
import json
import time
from base64 import urlsafe_b64encode, urlsafe_b64decode

import clock
import config
from code_constants import BIOBANK_TESTS
from dao.base_dao import json_serial
from dao.biobank_order_dao import BiobankOrderDao
from dao.biobank_stored_sample_dao import BiobankStoredSampleDao
from dao.participant_dao import ParticipantDao
from dao.participant_summary_dao import ParticipantSummaryDao
from dao.physical_measurements_dao import PhysicalMeasurementsDao
from model.biobank_order import BiobankOrder, BiobankOrderIdentifier, BiobankOrderedSample
from model.biobank_stored_sample import BiobankStoredSample
from model.measurements import PhysicalMeasurements
from model.participant import Participant
from model.participant_summary import ParticipantSummary
from participant_enums import EnrollmentStatus, PhysicalMeasurementsStatus, SampleStatus, \
  QuestionnaireStatus
from query import Query, Operator, FieldFilter, OrderBy
from test_data import load_measurement_json
from unit_test_util import NdbTestBase, PITT_HPO_ID, cancel_biobank_order, \
  get_restore_or_cancel_info


NUM_BASELINE_PPI_MODULES = 3

TIME_1 = datetime.datetime(2019, 2, 24)
TIME_2 = datetime.datetime(2019, 2, 25)
TIME_3 = datetime.datetime(2019, 2, 27)

class ParticipantSummaryDaoTest(NdbTestBase):
  def setUp(self):
    super(ParticipantSummaryDaoTest, self).setUp(use_mysql=True)
    self.dao = ParticipantSummaryDao()
    self.order_dao = BiobankOrderDao()
    self.measurement_dao = PhysicalMeasurementsDao()
    self.participant_dao = ParticipantDao()
    self.no_filter_query = Query([], None, 2, None)
    self.one_filter_query = Query([FieldFilter("participantId", Operator.EQUALS, 1)],
                                  None, 2, None)
    self.two_filter_query = Query([FieldFilter("participantId", Operator.EQUALS, 1),
                                   FieldFilter("hpoId", Operator.EQUALS, PITT_HPO_ID)],
                                  None, 2, None)
    self.ascending_biobank_id_query = Query([], OrderBy("biobankId", True), 2, None)
    self.descending_biobank_id_query = Query([], OrderBy("biobankId", False), 2, None)
    self.enrollment_status_order_query = Query([], OrderBy("enrollmentStatus", True), 2, None)
    self.hpo_id_order_query = Query([], OrderBy("hpoId", True), 2, None)
    self.first_name_order_query = Query([], OrderBy("firstName", True), 2, None)

  def assert_no_results(self, query):
    results = self.dao.query(query)
    self.assertEquals([], results.items)
    self.assertIsNone(results.pagination_token)

  def assert_results(self, query, items, pagination_token=None):
    results = self.dao.query(query)
    self.assertListAsDictEquals(items, results.items)
    self.assertEquals(pagination_token, results.pagination_token,
                      "Pagination tokens don't match; decoded = %s, %s" %
                      (_decode_token(pagination_token), _decode_token(results.pagination_token)))

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
    with self.assertRaises(UnicodeEncodeError):
      str(name)  # sanity check that the name contains non-ASCII
    participant = self._insert(Participant(participantId=1, biobankId=2))
    summary = self.dao.get(participant.participantId)
    summary.firstName = name
    self.dao.update(summary)
    fetched_summary = self.dao.get(participant.participantId)
    self.assertEquals(name, fetched_summary.firstName)

  def testQuery_twoSummaries(self):
    participant_1 = Participant(participantId=1, biobankId=2)
    self._insert(participant_1, 'Alice', 'Smith')
    participant_2 = Participant(participantId=2, biobankId=1)
    self._insert(participant_2, 'Zed', 'Zebra')
    ps_1 = self.dao.get(1)
    ps_2 = self.dao.get(2)
    self.assert_results(self.no_filter_query, [ps_1, ps_2])
    self.assert_results(self.one_filter_query, [ps_1])
    self.assert_no_results(self.two_filter_query)
    self.assert_results(self.ascending_biobank_id_query, [ps_2, ps_1])
    self.assert_results(self.descending_biobank_id_query, [ps_1, ps_2])

  def testQuery_threeSummaries_paginate(self):
    participant_1 = Participant(participantId=1, biobankId=4)
    self._insert(participant_1, 'Alice', 'Aardvark')
    participant_2 = Participant(participantId=2, biobankId=1)
    self._insert(participant_2, 'Bob', 'Builder')
    participant_3 = Participant(participantId=3, biobankId=3)
    self._insert(participant_3, 'Chad', 'Caterpillar')
    ps_1 = self.dao.get(1)
    ps_2 = self.dao.get(2)
    ps_3 = self.dao.get(3)
    self.assert_results(self.no_filter_query, [ps_1, ps_2],
                        _make_pagination_token(['Builder', 'Bob', None, 2]))
    self.assert_results(self.one_filter_query, [ps_1])
    self.assert_no_results(self.two_filter_query)
    self.assert_results(self.ascending_biobank_id_query, [ps_2, ps_3],
                        _make_pagination_token([3, 'Caterpillar', 'Chad', None, 3]))
    self.assert_results(self.descending_biobank_id_query, [ps_1, ps_3],
                        _make_pagination_token([3, 'Caterpillar', 'Chad', None, 3]))

    self.assert_results(_with_token(self.no_filter_query,
                                    _make_pagination_token(['Builder', 'Bob', None, 2])), [ps_3])
    self.assert_results(_with_token(self.ascending_biobank_id_query,
                                    _make_pagination_token([3, 'Caterpillar', 'Chad', None, 3])),
                        [ps_1])
    self.assert_results(_with_token(self.descending_biobank_id_query,
                                    _make_pagination_token([3, 'Caterpillar', 'Chad', None, 3])),
                        [ps_2])

  def testQuery_fourFullSummaries_paginate(self):
    participant_1 = Participant(participantId=1, biobankId=4)
    self._insert(participant_1, 'Bob', 'Jones')
    participant_2 = Participant(participantId=2, biobankId=1)
    self._insert(participant_2, 'Bob', 'Jones')
    participant_3 = Participant(participantId=3, biobankId=3)
    self._insert(participant_3, 'Bob', 'Jones')
    participant_4 = Participant(participantId=4, biobankId=2)
    self._insert(participant_4, 'Bob', 'Jones')
    ps_1 = self.dao.get(1)
    ps_2 = self.dao.get(2)
    ps_3 = self.dao.get(3)
    ps_4 = self.dao.get(4)

    ps_1.lastName = 'Jones'
    ps_1.firstName = 'Bob'
    ps_1.dateOfBirth = datetime.date(1978, 10, 9)
    ps_1.hpoId = PITT_HPO_ID
    self.dao.update(ps_1)

    ps_2.lastName = 'Aardvark'
    ps_2.firstName = 'Bob'
    ps_2.dateOfBirth = datetime.date(1978, 10, 10)
    ps_2.enrollmentStatus = EnrollmentStatus.MEMBER
    self.dao.update(ps_2)

    ps_3.lastName = 'Jones'
    ps_3.firstName = 'Bob'
    ps_3.dateOfBirth = datetime.date(1978, 10, 10)
    ps_3.hpoId = PITT_HPO_ID
    ps_3.enrollmentStatus = EnrollmentStatus.MEMBER
    self.dao.update(ps_3)

    ps_4.lastName = 'Jones'
    ps_4.enrollmentStatus = EnrollmentStatus.FULL_PARTICIPANT
    self.dao.update(ps_4)

    self.assert_results(self.no_filter_query, [ps_2, ps_4],
                        _make_pagination_token(['Jones', 'Bob', None, 4]))
    self.assert_results(self.one_filter_query, [ps_1])
    self.assert_results(self.two_filter_query, [ps_1])
    self.assert_results(self.ascending_biobank_id_query, [ps_2, ps_4],
                        _make_pagination_token([2, 'Jones', 'Bob', None, 4]))
    self.assert_results(self.descending_biobank_id_query, [ps_1, ps_3],
                        _make_pagination_token([3, 'Jones', 'Bob', datetime.date(1978, 10, 10), 3]))
    self.assert_results(self.hpo_id_order_query, [ps_2, ps_4],
                        _make_pagination_token([0, 'Jones', 'Bob', None, 4]))
    self.assert_results(self.enrollment_status_order_query, [ps_1, ps_2],
                        _make_pagination_token(['MEMBER', 'Aardvark', 'Bob',
                                                datetime.date(1978, 10, 10), 2]))

    self.assert_results(_with_token(self.no_filter_query,
                                    _make_pagination_token(['Jones', 'Bob', None, 4])),
                        [ps_1, ps_3])
    self.assert_results(_with_token(self.ascending_biobank_id_query,
                                    _make_pagination_token([2, 'Jones', 'Bob', None, 4])),
                        [ps_3, ps_1])
    self.assert_results(_with_token(self.descending_biobank_id_query,
                                    _make_pagination_token([3, 'Jones', 'Bob',
                                                            datetime.date(1978, 10, 10), 3])),
                        [ps_4, ps_2])
    self.assert_results(_with_token(self.hpo_id_order_query,
                                    _make_pagination_token([0, 'Jones', 'Bob', None, 4])),
                        [ps_1, ps_3])
    self.assert_results(_with_token(self.enrollment_status_order_query,
                                    _make_pagination_token(['MEMBER', 'Aardvark', 'Bob',
                                                datetime.date(1978, 10, 10), 2])),
                        [ps_3, ps_4])

  def test_update_from_samples(self):
    # baseline_tests = ['BASELINE1', 'BASELINE2']
    baseline_tests = ["1PST8", "2PST8"]

    config.override_setting(config.BASELINE_SAMPLE_TEST_CODES, baseline_tests)
    self.dao.update_from_biobank_stored_samples()  # safe noop

    p_baseline_samples = self._insert(Participant(participantId=1, biobankId=11))
    p_mixed_samples = self._insert(Participant(participantId=2, biobankId=22))
    p_no_samples = self._insert(Participant(participantId=3, biobankId=33))
    p_unconfirmed = self._insert(Participant(participantId=4, biobankId=44))
    self.assertEquals(self.dao.get(p_baseline_samples.participantId).numBaselineSamplesArrived, 0)

    def get_p_baseline_last_modified():
      return self.dao.get(p_baseline_samples.participantId).lastModified
    p_baseline_last_modified1 = get_p_baseline_last_modified()

    sample_dao = BiobankStoredSampleDao()

    def add_sample(participant, test_code, sample_id):
      TIME = datetime.datetime(2018, 3, 2)
      sample_dao.insert(BiobankStoredSample(
          biobankStoredSampleId=sample_id, biobankId=participant.biobankId,
        biobankOrderIdentifier='KIT', test=test_code, confirmed=TIME))

    add_sample(p_baseline_samples, baseline_tests[0], '11111')
    add_sample(p_baseline_samples, baseline_tests[1], '22223')
    add_sample(p_mixed_samples, baseline_tests[0], '11112')
    add_sample(p_mixed_samples, 'NOT1', '44441')
    # add unconfirmed sample
    sample_dao.insert(BiobankStoredSample(biobankStoredSampleId=55555,
                                          biobankId=p_unconfirmed.biobankId,
                                          biobankOrderIdentifier='KIT', test=baseline_tests[1],
                                          confirmed=None))
    # sleep 1 sec to make lastModified different
    time.sleep(1)
    self.dao.update_from_biobank_stored_samples()

    p_baseline_last_modified2 = get_p_baseline_last_modified()
    self.assertNotEquals(p_baseline_last_modified2, p_baseline_last_modified1)

    self.assertEquals(self.dao.get(p_baseline_samples.participantId).numBaselineSamplesArrived, 2)
    self.assertEquals(self.dao.get(p_mixed_samples.participantId).numBaselineSamplesArrived, 1)
    self.assertEquals(self.dao.get(p_no_samples.participantId).numBaselineSamplesArrived, 0)
    self.assertEquals(self.dao.get(p_unconfirmed.participantId).numBaselineSamplesArrived, 0)

    M_baseline_samples = self._insert(Participant(participantId=9, biobankId=99))
    add_sample(M_baseline_samples, baseline_tests[0], '999')
    M_first_update = self.dao.get(M_baseline_samples.participantId)
    # sleep 1 sec to make lastModified different
    time.sleep(1)
    self.dao.update_from_biobank_stored_samples()
    add_sample(M_baseline_samples, baseline_tests[1], '9999')
    M_second_update = self.dao.get(M_baseline_samples.participantId)
    # sleep 1 sec to make lastModified different
    time.sleep(1)
    self.dao.update_from_biobank_stored_samples()

    self.assertNotEqual(M_first_update.lastModified, M_second_update.lastModified)
    self.assertEquals(get_p_baseline_last_modified(), p_baseline_last_modified2)

  def test_update_from_samples_changed_tests(self):
    baseline_tests = ["1PST8", "2PST8"]
    config.override_setting(config.BASELINE_SAMPLE_TEST_CODES, baseline_tests)
    self.dao.update_from_biobank_stored_samples()  # safe noop

    participant = self._insert(Participant(participantId=1, biobankId=11))
    self.assertEquals(self.dao.get(participant.participantId).numBaselineSamplesArrived, 0)

    sample_dao = BiobankStoredSampleDao()
    def add_sample(test_code, sample_id):
      TIME = datetime.datetime(2018, 3, 2)
      sample_dao.insert(BiobankStoredSample(
          biobankStoredSampleId=sample_id, biobankId=participant.biobankId,
          biobankOrderIdentifier='KIT', test=test_code, confirmed=TIME))

    add_sample(baseline_tests[0], '11111')
    add_sample(baseline_tests[1], '22223')
    self.dao.update_from_biobank_stored_samples()
    summary = self.dao.get(participant.participantId)
    init_last_modified = summary.lastModified
    self.assertEquals(summary.numBaselineSamplesArrived, 2)
    # sleep 1 sec to make lastModified different
    time.sleep(1)
    # Simulate removal of one of the baseline tests from config.json.
    baseline_tests.pop()
    config.override_setting(config.BASELINE_SAMPLE_TEST_CODES, baseline_tests)
    self.dao.update_from_biobank_stored_samples()

    summary = self.dao.get(participant.participantId)
    self.assertEquals(summary.numBaselineSamplesArrived, 1)
    self.assertNotEqual(init_last_modified, summary.lastModified)

  def test_only_update_dna_sample(self):
    dna_tests = ["1ED10", "1SAL2"]

    config.override_setting(config.DNA_SAMPLE_TEST_CODES, dna_tests)
    self.dao.update_from_biobank_stored_samples()  # safe noop

    p_dna_samples = self._insert(Participant(participantId=1, biobankId=11))

    self.assertEquals(self.dao.get(p_dna_samples.participantId).samplesToIsolateDNA, None)
    self.assertEquals(
      self.dao.get(p_dna_samples.participantId).enrollmentStatusCoreStoredSampleTime, None)
    self.assertEquals(
      self.dao.get(p_dna_samples.participantId).enrollmentStatusCoreOrderedSampleTime, None)

    sample_dao = BiobankStoredSampleDao()

    def add_sample(participant, test_code, sample_id, confirmed_time):
      sample_dao.insert(BiobankStoredSample(
          biobankStoredSampleId=sample_id, biobankId=participant.biobankId,
          biobankOrderIdentifier='KIT', test=test_code, confirmed=confirmed_time))

    confirmed_time_0 = datetime.datetime(2018, 3, 1)
    add_sample(p_dna_samples, dna_tests[0], '11111', confirmed_time_0)

    self.dao.update_from_biobank_stored_samples()

    self.assertEquals(self.dao.get(p_dna_samples.participantId).samplesToIsolateDNA,
                      SampleStatus.RECEIVED)
    # only update dna sample will not update enrollmentStatusCoreStoredSampleTime
    self.assertEquals(
      self.dao.get(p_dna_samples.participantId).enrollmentStatusCoreStoredSampleTime, None)
    self.assertEquals(
      self.dao.get(p_dna_samples.participantId).enrollmentStatusCoreOrderedSampleTime, None)

  def test_calculate_enrollment_status(self):
    self.assertEquals(EnrollmentStatus.FULL_PARTICIPANT,
                      self.dao.calculate_enrollment_status(True,
                                                           NUM_BASELINE_PPI_MODULES,
                                                           PhysicalMeasurementsStatus.COMPLETED,
                                                           SampleStatus.RECEIVED))
    self.assertEquals(EnrollmentStatus.MEMBER,
                      self.dao.calculate_enrollment_status(True,
                                                           NUM_BASELINE_PPI_MODULES - 1,
                                                           PhysicalMeasurementsStatus.COMPLETED,
                                                           SampleStatus.RECEIVED))
    self.assertEquals(EnrollmentStatus.MEMBER,
                      self.dao.calculate_enrollment_status(True,
                                                           NUM_BASELINE_PPI_MODULES,
                                                           PhysicalMeasurementsStatus.UNSET,
                                                           SampleStatus.RECEIVED))
    self.assertEquals(EnrollmentStatus.MEMBER,
                      self.dao.calculate_enrollment_status(True,
                                                           NUM_BASELINE_PPI_MODULES,
                                                           PhysicalMeasurementsStatus.COMPLETED,
                                                           SampleStatus.UNSET))
    self.assertEquals(EnrollmentStatus.INTERESTED,
                      self.dao.calculate_enrollment_status(False,
                                                           NUM_BASELINE_PPI_MODULES,
                                                           PhysicalMeasurementsStatus.COMPLETED,
                                                           SampleStatus.RECEIVED))

  def testUpdateEnrollmentStatus(self):
    ehr_consent_time = datetime.datetime(2018, 3, 1)
    summary = ParticipantSummary(
        participantId=1,
        biobankId=2,
        consentForStudyEnrollment=QuestionnaireStatus.SUBMITTED,
        consentForElectronicHealthRecords=QuestionnaireStatus.SUBMITTED,
        consentForElectronicHealthRecordsTime=ehr_consent_time,
        enrollmentStatus=EnrollmentStatus.INTERESTED)
    self.dao.update_enrollment_status(summary)
    self.assertEquals(EnrollmentStatus.MEMBER, summary.enrollmentStatus)
    self.assertEquals(ehr_consent_time, summary.enrollmentStatusMemberTime)


  def testUpdateEnrollmentStatusLastModified(self):
    """
    DA-631: enrollment_status update should update last_modified.
    """

    participant = self._insert(Participant(participantId=6, biobankId=66))
    # collect current modified and enrollment status
    summary = self.dao.get(participant.participantId)
    test_dt = datetime.datetime(2018, 11, 1)

    def reset_summary():
      # change summary so enrollment status will be changed from INTERESTED to MEMBER.
      summary.enrollmentStatus = EnrollmentStatus.INTERESTED
      summary.lastModified = test_dt
      summary.consentForStudyEnrollment = QuestionnaireStatus.SUBMITTED
      summary.consentForElectronicHealthRecords = QuestionnaireStatus.SUBMITTED
      summary.physicalMeasurementsStatus = PhysicalMeasurementsStatus.COMPLETED
      summary.samplesToIsolateDNA = SampleStatus.RECEIVED
      self.dao.update(summary)

    ## Test Step 1: Validate update_from_biobank_stored_samples() changes lastModified.
    reset_summary()

    # Update and reload summary record
    self.dao.update_from_biobank_stored_samples(participant_id=participant.participantId)
    summary = self.dao.get(participant.participantId)

    # Test that status has changed and lastModified is also different
    self.assertEquals(EnrollmentStatus.MEMBER, summary.enrollmentStatus)
    self.assertNotEqual(test_dt, summary.lastModified)

    ## Test Step 2: Validate that update_enrollment_status() changes the lastModified property.
    reset_summary()
    summary = self.dao.get(participant.participantId)

    self.assertEqual(test_dt, summary.lastModified)

    # update_enrollment_status() does not touch the db, it only modifies object properties.
    self.dao.update_enrollment_status(summary)

    self.assertEquals(EnrollmentStatus.MEMBER, summary.enrollmentStatus)
    self.assertNotEqual(test_dt, summary.lastModified)

  def testNumberDistinctVisitsCounts(self):
    self.participant = self._insert(Participant(participantId=7, biobankId=77))
    # insert biobank order
    order = self.order_dao.insert(self._make_biobank_order())
    summary = self.dao.get(self.participant.participantId)
    self.assertEquals(summary.numberDistinctVisits, 1)
    cancel_request = cancel_biobank_order()
    # cancel biobank order
    self.order_dao.update_with_patch(order.biobankOrderId, cancel_request, order.version)
    summary = self.dao.get(self.participant.participantId)
    # distinct count should be 0
    self.assertEquals(summary.numberDistinctVisits, 0)

    self.measurement_json = json.dumps(load_measurement_json(self.participant.participantId,
                                                             TIME_1.isoformat()))
    # insert physical measurement
    measurement = self.measurement_dao.insert(self._make_physical_measurements())
    summary = self.dao.get(self.participant.participantId)
    # count should be 1
    self.assertEquals(summary.numberDistinctVisits, 1)

    # cancel the measurement
    cancel_measurement = get_restore_or_cancel_info()
    with self.measurement_dao.session() as session:
      self.measurement_dao.update_with_patch(measurement.physicalMeasurementsId, session,
                                             cancel_measurement)

    summary = self.dao.get(self.participant.participantId)
    # count should be 0
    self.assertEquals(summary.numberDistinctVisits, 0)

    with clock.FakeClock(TIME_1):
      self.order_dao.insert(self._make_biobank_order(biobankOrderId='2', identifiers=[
        BiobankOrderIdentifier(system='b', value='d')], samples=[BiobankOrderedSample(
                                                        biobankOrderId = '2',
                                                        test=BIOBANK_TESTS[0],
                                                        description='description',
                                                        processingRequired=True)]))
    with clock.FakeClock(TIME_2):
      self.measurement_dao.insert(self._make_physical_measurements(
        physicalMeasurementsId=2))
      summary = self.dao.get(self.participant.participantId)
      self.assertEquals(summary.numberDistinctVisits, 2)

    with clock.FakeClock(TIME_3):
      self.order_dao.insert(self._make_biobank_order(biobankOrderId='3', identifiers=[
        BiobankOrderIdentifier(system='s', value='s')], samples=[BiobankOrderedSample(
        biobankOrderId = '3',
        test=BIOBANK_TESTS[1],
        description='another description',
        processingRequired=False)]))

      # a physical measurement on same day as biobank order does not add distinct visit.
      self.measurement_dao.insert(self._make_physical_measurements(
        physicalMeasurementsId=6))
      summary = self.dao.get(self.participant.participantId)

      # another biobank order on the same day should also not add a distinct visit
      self.order_dao.insert(self._make_biobank_order(biobankOrderId='7', identifiers=[
        BiobankOrderIdentifier(system='x', value='x')], samples=[BiobankOrderedSample(
        biobankOrderId = '7',
        test=BIOBANK_TESTS[1],
        description='another description',
        processingRequired=False)]))

      self.assertEquals(summary.numberDistinctVisits, 3)

  def _make_biobank_order(self, **kwargs):
    """Makes a new BiobankOrder (same values every time) with valid/complete defaults.

    Kwargs pass through to BiobankOrder constructor, overriding defaults.
    """
    for k, default_value in (
        ('biobankOrderId', '1'),
        ('created', clock.CLOCK.now()),
        ('participantId', self.participant.participantId),
        ('sourceSiteId', 1),
        ('sourceUsername', 'fred@pmi-ops.org'),
        ('collectedSiteId', 1),
        ('collectedUsername', 'joe@pmi-ops.org'),
        ('processedSiteId', 1),
        ('processedUsername', 'sue@pmi-ops.org'),
        ('finalizedSiteId', 2),
        ('finalizedUsername', 'bob@pmi-ops.org'),
        ('identifiers', [BiobankOrderIdentifier(system='a', value='c')]),
        ('samples', [BiobankOrderedSample(
            biobankOrderId='1',
            test=BIOBANK_TESTS[0],
            description='description',
            processingRequired=True)])):
      if k not in kwargs:
        kwargs[k] = default_value
    return BiobankOrder(**kwargs)

  def _make_physical_measurements(self, **kwargs):
    """Makes a new PhysicalMeasurements (same values every time) with valid/complete defaults.

    Kwargs pass through to PM constructor, overriding defaults.
    """
    for k, default_value in (
        ('physicalMeasurementsId', 1),
        ('participantId', self.participant.participantId),
        ('resource', self.measurement_json),
        ('createdSiteId', 1),
        ('finalizedSiteId', 2)):
      if k not in kwargs:
        kwargs[k] = default_value
    return PhysicalMeasurements(**kwargs)

def _with_token(query, token):
  return Query(query.field_filters, query.order_by, query.max_results, token)

def _make_pagination_token(vals):
  vals_json = json.dumps(vals, default=json_serial)
  return urlsafe_b64encode(vals_json)

def _decode_token(token):
  if token is None:
    return None
  return json.loads(urlsafe_b64decode(token))
