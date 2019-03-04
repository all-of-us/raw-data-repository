import datetime

from clock import FakeClock
from dao.calendar_dao import CalendarDao
from dao.ehr_dao import EhrReceiptDao
from dao.hpo_dao import HPODao
from dao.participant_dao import ParticipantDao
from dao.participant_summary_dao import ParticipantSummaryDao
from dao.site_dao import SiteDao
from model.calendar import Calendar
from model.hpo import HPO
from model.participant import Participant
from model.participant_summary import ParticipantSummary
from model.site import Site
from participant_enums import EnrollmentStatus, OrganizationType, TEST_HPO_NAME, TEST_HPO_ID, \
  make_primary_provider_link_for_name, EhrStatus, QuestionnaireStatus
from test.unit_test.unit_test_util import FlaskTestBase


TIME_1 = datetime.datetime(2017, 12, 31)

REQUIRED_PPI_MODULE_COUNT = 3  # NOTE: could not import from config in test runner


def _questionnaire_response_url(participant_id):
  return 'Participant/%s/QuestionnaireResponse' % participant_id


def iter_dates(start, end):
  """
  generate datetime.date objects for each day from `start` to `end`
  """
  i = start
  while i <= end:
    yield i
    i += datetime.timedelta(days=1)


class MetricsEhrApiTestBase(FlaskTestBase):

  def setUp(self, **kwargs):
    super(MetricsEhrApiTestBase, self).setUp(use_mysql=True, **kwargs)
    self.dao = ParticipantDao()
    self.ps_dao = ParticipantSummaryDao()
    self.ehr_receipt_dao = EhrReceiptDao()
    self.ps = ParticipantSummary()
    self.calendar_dao = CalendarDao()
    self.site_dao = SiteDao()
    self.hpo_dao = HPODao()
    self.hpo_dao.insert(HPO(hpoId=TEST_HPO_ID, name=TEST_HPO_NAME, displayName='Test',
                            organizationType=OrganizationType.UNSET))

  def _make_participant(
    self, participant, first_name=None, last_name=None, hpo_name=None,
    unconsented=False, time_int=None, time_study=None, time_mem=None, time_fp=None,
    time_fp_stored=None, gender_id=None, dob=None, state_id=None,
    site_id=None
  ):
    """
    Create a participant in a transient test database.

    Note: copied from ParticipantCountsOverTimeApiTest

    :param participant: Participant object
    :param first_name: First name
    :param last_name: Last name
    :param hpo_name: HPO name (one of PITT or AZ_TUCSON)
    :param time_int: Time that participant fulfilled INTERESTED criteria
    :param time_mem: Time that participant fulfilled MEMBER criteria
    :param time_fp: Time that participant fulfilled FULL_PARTICIPANT criteria
    :return: Participant object
    """

    if unconsented is True:
      enrollment_status = None
    elif time_mem is None:
      enrollment_status = EnrollmentStatus.INTERESTED
    elif time_fp is None:
      enrollment_status = EnrollmentStatus.MEMBER
    else:
      enrollment_status = EnrollmentStatus.FULL_PARTICIPANT

    with FakeClock(time_int):
      self.dao.insert(participant)

    participant.providerLink = make_primary_provider_link_for_name(hpo_name)
    with FakeClock(time_mem):
      self.dao.update(participant)

    if enrollment_status is None:
      return None

    summary = self.participant_summary(participant)

    if first_name:
      summary.firstName = first_name
    if last_name:
      summary.lastName = last_name

    if gender_id:
      summary.genderIdentityId = gender_id
    if dob:
      summary.dateOfBirth = dob
    else:
      summary.dateOfBirth = datetime.date(1978, 10, 10)
    if state_id:
      summary.stateId = state_id

    summary.enrollmentStatus = enrollment_status

    summary.enrollmentStatusMemberTime = time_mem
    summary.enrollmentStatusCoreOrderedSampleTime = time_fp
    summary.enrollmentStatusCoreStoredSampleTime = time_fp_stored

    summary.hpoId = self.hpo_dao.get_by_name(hpo_name).hpoId
    summary.siteId = site_id

    if time_study is not None:
      with FakeClock(time_mem):
        summary.consentForStudyEnrollment = QuestionnaireStatus.SUBMITTED
        summary.consentForStudyEnrollmentTime = time_study

    if time_mem is not None:
      with FakeClock(time_mem):
        summary.consentForElectronicHealthRecords = QuestionnaireStatus.SUBMITTED
        summary.consentForElectronicHealthRecordsTime = time_mem

    if time_fp is not None:
      with FakeClock(time_fp):
        if not summary.consentForElectronicHealthRecords:
          summary.consentForElectronicHealthRecords = QuestionnaireStatus.SUBMITTED
          summary.consentForElectronicHealthRecordsTime = time_fp
        summary.questionnaireOnTheBasicsTime = time_fp
        summary.questionnaireOnLifestyleTime = time_fp
        summary.questionnaireOnOverallHealthTime = time_fp
        summary.physicalMeasurementsFinalizedTime = time_fp
        summary.physicalMeasurementsTime = time_fp
        summary.sampleOrderStatus1ED04Time = time_fp
        summary.sampleOrderStatus1SALTime = time_fp
        summary.sampleStatus1ED04Time = time_fp
        summary.sampleStatus1SALTime = time_fp
        summary.biospecimenOrderTime = time_fp
        summary.numCompletedBaselinePPIModules = REQUIRED_PPI_MODULE_COUNT

    self.ps_dao.insert(summary)

    return summary

  def _update_ehr(self, participant_summary, receipt_time, update_time):
    self.ehr_receipt_dao.insert(
      self.ps_dao.update_with_new_ehr(participant_summary, receipt_time, update_time)
    )
    self.ps_dao.update(participant_summary)


class MetricsEhrApiOverTimeTest(MetricsEhrApiTestBase):

  def test_consented_counts(self):
    # Set up data
    for date in iter_dates(
      datetime.date(2017, 12, 30),
      datetime.date(2018, 2, 1)
    ):
      calendar_day = Calendar(day=date)
      CalendarDao().insert(calendar_day)

    # noinspection PyArgumentList
    participant_1 = Participant(participantId=1, biobankId=4)
    self._make_participant(
      participant_1, 'Alice', 'Aardvark', 'PITT',
      time_int=datetime.datetime(2018, 1, 2),
      time_mem=datetime.datetime(2018, 1, 3),
      time_fp=datetime.datetime(2018, 1, 4)
    )

    # noinspection PyArgumentList
    participant_2 = Participant(participantId=2, biobankId=5)
    self._make_participant(
      participant_2, 'Bo', 'Badger', 'AZ_TUCSON',
      time_int=datetime.datetime(2018, 1, 3),
      time_mem=datetime.datetime(2018, 1, 4),
      time_fp=datetime.datetime(2018, 1, 5)
    )

    # Begin testing
    response = self.send_get('MetricsEHR', request_data={
      'start_date': '2018-01-01',
      'end_date': '2018-01-06',
      'interval': 'day'
    })['metrics_over_time']

    counts_by_date = {
      day['date']: day['metrics']['EHR_CONSENTED']
      for day in response
    }

    self.assertEqual(counts_by_date['2018-01-01'], 0)
    self.assertEqual(counts_by_date['2018-01-02'], 0)
    self.assertEqual(counts_by_date['2018-01-03'], 1)
    self.assertEqual(counts_by_date['2018-01-04'], 2)
    self.assertEqual(counts_by_date['2018-01-05'], 2)
    self.assertEqual(counts_by_date['2018-01-05'], 2)

  def test_received_counts(self):
    # Set up data
    for date in iter_dates(
      datetime.date(2017, 12, 30),
      datetime.date(2018, 2, 1)
    ):
      calendar_day = Calendar(day=date)
      CalendarDao().insert(calendar_day)

    # noinspection PyArgumentList
    participant_1 = Participant(participantId=1, biobankId=4)
    summary_1 = self._make_participant(
      participant_1, 'Alice', 'Aardvark', 'PITT',
      time_int=datetime.datetime(2018, 1, 2),
      time_mem=datetime.datetime(2018, 1, 3),
      time_fp=datetime.datetime(2018, 1, 4),
      site_id=1
    )
    self._update_ehr(
      summary_1,
      receipt_time=datetime.datetime(2018, 1, 5),
      update_time=datetime.datetime(2018, 1, 5)
    )

    # noinspection PyArgumentList
    participant_2 = Participant(participantId=2, biobankId=5)
    summary_2 = self._make_participant(
      participant_2, 'Bo', 'Badger', 'AZ_TUCSON',
      time_int=datetime.datetime(2018, 1, 3),
      time_mem=datetime.datetime(2018, 1, 4),
      time_fp=datetime.datetime(2018, 1, 5),
      site_id=2
    )
    self._update_ehr(
      summary_2,
      receipt_time=datetime.datetime(2018, 1, 6),
      update_time=datetime.datetime(2018, 1, 7)
    )

    # Begin testing
    response = self.send_get('MetricsEHR', request_data={
      'start_date': '2018-01-01',
      'end_date': '2018-01-08',
      'interval': 'day'
    })['metrics_over_time']

    counts_by_date = {
      day['date']: day['metrics']['EHR_RECEIVED']
      for day in response
    }

    self.assertEqual(counts_by_date['2018-01-01'], 0)
    self.assertEqual(counts_by_date['2018-01-02'], 0)
    self.assertEqual(counts_by_date['2018-01-03'], 0)
    self.assertEqual(counts_by_date['2018-01-04'], 0)
    self.assertEqual(counts_by_date['2018-01-05'], 1)
    self.assertEqual(counts_by_date['2018-01-06'], 2)
    self.assertEqual(counts_by_date['2018-01-07'], 2)
    self.assertEqual(counts_by_date['2018-01-08'], 2)

  def test_site_counts(self):
    # Set up data
    for date in iter_dates(
      datetime.date(2017, 12, 30),
      datetime.date(2018, 2, 1)
    ):
      calendar_day = Calendar(day=date)
      CalendarDao().insert(calendar_day)

    # noinspection PyArgumentList
    participant_1 = Participant(participantId=1, biobankId=4)
    summary_1 = self._make_participant(
      participant_1, 'A', 'Aardvark', 'PITT',
      time_int=datetime.datetime(2018, 1, 2),
      time_mem=datetime.datetime(2018, 1, 3),
      time_fp=datetime.datetime(2018, 1, 4),
      site_id=1
    )
    self._update_ehr(
      summary_1,
      receipt_time=datetime.datetime(2018, 1, 5),
      update_time=datetime.datetime(2018, 1, 6)
    )

    # noinspection PyArgumentList
    participant_2 = Participant(participantId=2, biobankId=5)
    summary_2 = self._make_participant(
      participant_2, 'B', 'Badger', 'PITT',
      time_int=datetime.datetime(2018, 1, 2),
      time_mem=datetime.datetime(2018, 1, 3),
      time_fp=datetime.datetime(2018, 1, 4),
      site_id=1
    )
    self._update_ehr(
      summary_2,
      receipt_time=datetime.datetime(2018, 1, 5),
      update_time=datetime.datetime(2018, 1, 6)
    )

    # noinspection PyArgumentList
    participant_3 = Participant(participantId=3, biobankId=6)
    summary_3 = self._make_participant(
      participant_3, 'C', 'Chicken', 'PITT',
      time_int=datetime.datetime(2018, 1, 3),
      time_mem=datetime.datetime(2018, 1, 4),
      time_fp=datetime.datetime(2018, 1, 5),
      site_id=1
    )
    self._update_ehr(
      summary_3,
      receipt_time=datetime.datetime(2018, 1, 6),
      update_time=datetime.datetime(2018, 1, 7)
    )

    # noinspection PyArgumentList
    participant_4 = Participant(participantId=4, biobankId=7)
    summary_4 = self._make_participant(
      participant_4, 'D', 'Dog', 'AZ_TUCSON',
      time_int=datetime.datetime(2018, 1, 3),
      time_mem=datetime.datetime(2018, 1, 4),
      time_fp=datetime.datetime(2018, 1, 5),
      site_id=2
    )
    self._update_ehr(
      summary_4,
      receipt_time=datetime.datetime(2018, 1, 6),
      update_time=datetime.datetime(2018, 1, 7)
    )
    self._update_ehr(
      summary_4,
      receipt_time=datetime.datetime(2018, 1, 7),
      update_time=datetime.datetime(2018, 1, 8)
    )

    # Begin testing
    response = self.send_get('MetricsEHR', request_data={
      'start_date': '2018-01-01',
      'end_date': '2018-01-08',
      'interval': 'day'
    })['metrics_over_time']

    #import pprint
    #pprint.pprint(response)

    self.assertEqual(
      [row['date'] for row in response],
      [
        '2018-01-01',
        '2018-01-02',
        '2018-01-03',
        '2018-01-04',
        '2018-01-05',
        '2018-01-06',
        '2018-01-07',
        '2018-01-08',
      ]
    )

    self.assertEqual(
      [row['metrics']['SITES_ACTIVE'] for row in response],
      [0, 0, 0, 0, 1, 2, 1, 0]
    )

    counts_by_date = {
      day['date']: day['metrics']['SITES_ACTIVE']
      for day in response
    }

    self.assertEqual(counts_by_date['2018-01-01'], 0)
    self.assertEqual(counts_by_date['2018-01-02'], 0)
    self.assertEqual(counts_by_date['2018-01-03'], 0)
    self.assertEqual(counts_by_date['2018-01-04'], 0)
    self.assertEqual(counts_by_date['2018-01-05'], 1)
    self.assertEqual(counts_by_date['2018-01-06'], 2)
    self.assertEqual(counts_by_date['2018-01-07'], 1)
    self.assertEqual(counts_by_date['2018-01-08'], 0)


class MetricsEhrApiSiteTest(MetricsEhrApiTestBase):

  def setUp(self, **kwargs):
    super(MetricsEhrApiSiteTest, self).setUp()
    self.site_dao = SiteDao()

  def _make_site(self, id_, name, google_group, hpo_name):
    site = Site(siteId=id_, siteName=name, googleGroup=google_group,
                hpoId=self.hpo_dao.get_by_name(hpo_name).hpoId)
    self.site_dao.insert(site)
    return site

  def test_cutoff_date_filtering(self):
    # Set up test data
    site_1 = self._make_site(10, 'Site A', 'group_a', 'PITT')
    site_2 = self._make_site(11, 'Site B', 'group_b', 'AZ_TUCSON')

    # noinspection PyArgumentList
    participant_1 = Participant(participantId=1, biobankId=4)
    summary_1 = self._make_participant(
      participant_1, 'A', 'Aardvark', 'PITT',
      time_int=datetime.datetime(2018, 1, 1),
      time_study=datetime.datetime(2018, 1, 2),
      time_mem=datetime.datetime(2018, 1, 3),
      time_fp=datetime.datetime(2018, 1, 4),
      site_id=site_1.siteId
    )
    self._update_ehr(
      summary_1,
      receipt_time=datetime.datetime(2018, 1, 5),
      update_time=datetime.datetime(2018, 1, 6),
    )

    # noinspection PyArgumentList
    participant_2 = Participant(participantId=2, biobankId=5)
    summary_2 = self._make_participant(
      participant_2, 'B', 'Badger', 'PITT',
      time_int=datetime.datetime(2018, 1, 2),
      time_study=datetime.datetime(2018, 1, 3),
      time_mem=datetime.datetime(2018, 1, 4),
      time_fp=datetime.datetime(2018, 1, 5),
      site_id=site_1.siteId
    )
    self._update_ehr(
      summary_2,
      receipt_time=datetime.datetime(2018, 1, 6),
      update_time=datetime.datetime(2018, 1, 7),
    )

    # Begin testing
    response = self.send_get('MetricsEHR', request_data={
      'start_date': '2018-01-01',
      'end_date': '2018-01-01',
      'interval': 'day'
    })
    self.assertEqual(
      response['site_metrics'][str(site_1.siteId)],
      {
        u'site_id': site_1.siteId,
        u'site_name': unicode(site_1.siteName),
        u'total_participants': 1,
        u'total_primary_consented': 0,
        u'total_ehr_consented': 0,
        u'total_core_participants': 0,
        u'total_ehr_data_received': 0,
        # NOTE: The last_ehr_submission_date is incorrect but as accurate as we can get with
        #       the current data structure.
        u'last_ehr_submission_date': u'2018-01-07',
      }
    )
    self.assertEqual(
      response['site_metrics'][str(site_2.siteId)],
      {
        u'site_id': site_2.siteId,
        u'site_name': unicode(site_2.siteName),
        u'total_participants': 0,
        u'total_primary_consented': 0,
        u'total_ehr_consented': 0,
        u'total_core_participants': 0,
        u'total_ehr_data_received': 0,
        u'last_ehr_submission_date': None,
      }
    )

    response = self.send_get('MetricsEHR', request_data={
      'start_date': '2018-01-02',
      'end_date': '2018-01-02',
      'interval': 'day'
    })
    self.assertEqual(
      response['site_metrics'][str(site_1.siteId)],
      {
        u'site_id': site_1.siteId,
        u'site_name': unicode(site_1.siteName),
        u'total_participants': 2,
        u'total_primary_consented': 1,
        u'total_ehr_consented': 0,
        u'total_core_participants': 0,
        u'total_ehr_data_received': 0,
        # NOTE: The last_ehr_submission_date is incorrect but as accurate as we can get with
        #       the current data structure.
        u'last_ehr_submission_date': u'2018-01-07',
      }
    )

    response = self.send_get('MetricsEHR', request_data={
      'start_date': '2018-01-03',
      'end_date': '2018-01-03',
      'interval': 'day'
    })
    self.assertEqual(
      response['site_metrics'][str(site_1.siteId)],
      {
        u'site_id': site_1.siteId,
        u'site_name': unicode(site_1.siteName),
        u'total_participants': 2,
        u'total_primary_consented': 2,
        u'total_ehr_consented': 1,
        u'total_core_participants': 0,
        u'total_ehr_data_received': 0,
        # NOTE: The last_ehr_submission_date is incorrect but as accurate as we can get with
        #       the current data structure.
        u'last_ehr_submission_date': u'2018-01-07',
      }
    )

    response = self.send_get('MetricsEHR', request_data={
      'start_date': '2018-01-04',
      'end_date': '2018-01-04',
      'interval': 'day'
    })
    self.assertEqual(
      response['site_metrics'][str(site_1.siteId)],
      {
        u'site_id': site_1.siteId,
        u'site_name': unicode(site_1.siteName),
        u'total_participants': 2,
        u'total_primary_consented': 2,
        u'total_ehr_consented': 2,
        u'total_core_participants': 1,
        u'total_ehr_data_received': 0,
        # NOTE: The last_ehr_submission_date is incorrect but as accurate as we can get with
        #       the current data structure.
        u'last_ehr_submission_date': u'2018-01-07',
      }
    )

    response = self.send_get('MetricsEHR', request_data={
      'start_date': '2018-01-05',
      'end_date': '2018-01-05',
      'interval': 'day'
    })
    self.assertEqual(
      response['site_metrics'][str(site_1.siteId)],
      {
        u'site_id': site_1.siteId,
        u'site_name': unicode(site_1.siteName),
        u'total_participants': 2,
        u'total_primary_consented': 2,
        u'total_ehr_consented': 2,
        u'total_core_participants': 2,
        u'total_ehr_data_received': 1,
        # NOTE: The last_ehr_submission_date is incorrect but as accurate as we can get with
        #       the current data structure.
        u'last_ehr_submission_date': u'2018-01-07',
      }
    )

    response = self.send_get('MetricsEHR', request_data={
      'start_date': '2018-01-06',
      'end_date': '2018-01-06',
      'interval': 'day'
    })
    self.assertEqual(
      response['site_metrics'][str(site_1.siteId)],
      {
        u'site_id': site_1.siteId,
        u'site_name': unicode(site_1.siteName),
        u'total_participants': 2,
        u'total_primary_consented': 2,
        u'total_ehr_consented': 2,
        u'total_core_participants': 2,
        u'total_ehr_data_received': 2,
        # NOTE: The last_ehr_submission_date is incorrect but as accurate as we can get with
        #       the current data structure.
        u'last_ehr_submission_date': u'2018-01-07',
      }
    )
