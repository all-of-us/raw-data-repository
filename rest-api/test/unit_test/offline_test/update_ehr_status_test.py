import StringIO
import calendar
import collections
import csv
import datetime
import unittest

import cloudstorage
import mock
from clock import FakeClock
from dao.ehr_dao import EhrReceiptDao
from dao.hpo_dao import HPODao
from dao.participant_dao import ParticipantDao
from dao.participant_summary_dao import ParticipantSummaryDao
from model.ehr import EhrReceipt
import offline.update_ehr_status
from model.hpo import HPO
from participant_enums import EhrStatus
from test.unit_test.unit_test_util import FlaskTestBase, run_deferred_tasks, CloudStorageSqlTestBase, TestBase


class UpdateEhrStatusTest(CloudStorageSqlTestBase, FlaskTestBase):
  def setUp(self, **kwargs):
    super(UpdateEhrStatusTest, self).setUp(use_mysql=True, **kwargs)
    FlaskTestBase.doSetUp(self)
    TestBase.setup_fake(self)
    self.taskqueue.FlushQueue('default')
    self.participant_dao = ParticipantDao()
    self.summary_dao = ParticipantSummaryDao()
    self.ehr_receipt_dao = EhrReceiptDao()

  def _create_participant(self, id_):
    participant = self._participant_with_defaults(participantId=id_, biobankId=id_)
    self.participant_dao.insert(participant)
    summary = self.participant_summary(participant)
    self.summary_dao.insert(summary)
    return participant

  def test_get_participant_ids_from_person_file_mock_cloudstorage(self):
    filename = '/fake_bucket/foo.csv'
    contents = StringIO.StringIO()
    writer = csv.writer(contents)
    writer.writerow(['a', 'b', 'c'])
    writer.writerow(['1', '2', '3'])
    writer.writerow(['2', '4', '6'])
    writer.writerow(['3', '6', '9'])
    contents.seek(0)
    with cloudstorage.open(filename, mode='w') as f:
      f.write(contents.read().encode('utf-8'))
    results = offline.update_ehr_status._get_participant_ids_from_person_file(filename)
    self.assertEqual(results, [1,2,3])


class SubmissionNameDateParsingTestCase(unittest.TestCase):
  date = datetime.date(2019, 1, 1)

  def _do_test_submission(self, submission):
    self.assertEqual(
      offline.update_ehr_status._parse_date_from_submission_name(submission),
      self.date,
      'failed to parse: "{}"'.format(submission)
    )

  def test_iso_format(self):
    self._do_test_submission('2019-01-01')

  def test_unpadded(self):
    self._do_test_submission('2019-1-1')

  def test_undelimited(self):
    self._do_test_submission('20190101')

  def test_iso_with_postfix(self):
    self._do_test_submission('2019-01-01-postfix')
    self._do_test_submission('2019-01-01-double-postfix')
    self._do_test_submission('2019-01-01-1postfix')
    self._do_test_submission('2019-01-01-postfix2')
    self._do_test_submission('2019-01-01-123')

  def test_iso_with_prefix(self):
    self._do_test_submission('prefix-2019-01-01')
    self._do_test_submission('double-prefix-2019-01-01')
    self._do_test_submission('1prefix-2019-01-01')
    self._do_test_submission('prefix2-2019-01-01')
    self._do_test_submission('123-2019-01-01')


class GetHposUpdatedTestCase(unittest.TestCase):

  MockGCSFileStat = collections.namedtuple('MockGCSFileStat', ['filename', 'st_ctime'])

  def test_parse_hpo_id_and_date(self):
    hpo_id_string, date = offline.update_ehr_status._parse_hpo_id_and_date_from_person_filename(
      '/fake_bucket/hpo1/hpo1_bucket/2019-01-02/person.csv'
    )
    self.assertEqual(hpo_id_string, 'hpo1')
    self.assertEqual(date, datetime.date(2019, 1, 2))

  def test_basic(self):
    bucket_stat_list = [
      self.MockGCSFileStat(
        '/fake_bucket/hpo1/hpo1_bucket/2019-01-02/person.csv',
        calendar.timegm(datetime.date(2019, 1, 2).timetuple())
      ),
    ]
    results = offline.update_ehr_status._get_hpos_updated_from_file_stat_list_since_datetime(
      bucket_stat_list,
      datetime.date(2019, 1, 1)
    )
    self.assertEqual(len(results), 1)
    self.assertEqual(results[0], {
      'hpo_id_string': 'hpo1',
      'person_file': '/fake_bucket/hpo1/hpo1_bucket/2019-01-02/person.csv',
      'updated_date': datetime.date(2019, 1, 2),
    })

  def test_gets_latest(self):
    bucket_stat_list = [
      self.MockGCSFileStat(
        '/fake_bucket/hpo1/hpo1_bucket/2019-01-02/person.csv',
        calendar.timegm(datetime.date(2019, 1, 2).timetuple())
      ),
      self.MockGCSFileStat(
        '/fake_bucket/hpo1/hpo1_bucket/2019-01-03/person.csv',
        calendar.timegm(datetime.date(2019, 1, 3).timetuple())
      ),
    ]
    results = offline.update_ehr_status._get_hpos_updated_from_file_stat_list_since_datetime(
      bucket_stat_list,
      datetime.date(2019, 1, 1)
    )
    self.assertEqual(len(results), 1)
    self.assertEqual(results[0], {
      'hpo_id_string': 'hpo1',
      'person_file': '/fake_bucket/hpo1/hpo1_bucket/2019-01-03/person.csv',
      'updated_date': datetime.date(2019, 1, 3),
    })

  def test_gets_latest_alphabetical_when_tied(self):
    bucket_stat_list = [
      self.MockGCSFileStat(
        '/fake_bucket/hpo1/hpo1_bucket/2019-01-02-v2/person.csv',
        calendar.timegm(datetime.date(2019, 1, 2).timetuple())
      ),
      self.MockGCSFileStat(
        '/fake_bucket/hpo1/hpo1_bucket/2019-01-02-v1/person.csv',
        calendar.timegm(datetime.date(2019, 1, 2).timetuple())
      ),
    ]
    results = offline.update_ehr_status._get_hpos_updated_from_file_stat_list_since_datetime(
      bucket_stat_list,
      datetime.date(2019, 1, 1)
    )
    self.assertEqual(len(results), 1)
    self.assertEqual(results[0], {
      'hpo_id_string': 'hpo1',
      'person_file': '/fake_bucket/hpo1/hpo1_bucket/2019-01-02-v2/person.csv',
      'updated_date': datetime.date(2019, 1, 2),
    })

  @mock.patch('offline.update_ehr_status.cloudstorage_api')
  def test_get_participant_ids_from_person_file_logic(self, mock_gcs):
    contents = StringIO.StringIO()
    writer = csv.writer(contents)
    writer.writerow(['a', 'b', 'c'])
    writer.writerow(['1', '2', '3'])
    writer.writerow(['2', '4', '6'])
    writer.writerow(['3', '6', '9'])
    contents.seek(0)
    mock_gcs.open.return_value.__enter__.return_value = contents
    filename = 'foo'
    results = offline.update_ehr_status._get_participant_ids_from_person_file(filename)
    self.assertEqual(results, [1,2,3])


class UpdateEhrStatusFullExecutionTest(CloudStorageSqlTestBase, FlaskTestBase):
  def setUp(self, **kwargs):
    super(UpdateEhrStatusFullExecutionTest, self).setUp(use_mysql=True, **kwargs)
    FlaskTestBase.doSetUp(self)
    TestBase.setup_fake(self)
    self.taskqueue.FlushQueue('default')
    self.hpo_dao = HPODao()
    self.participant_dao = ParticipantDao()
    self.summary_dao = ParticipantSummaryDao()
    self.ehr_receipt_dao = EhrReceiptDao()
    self.config_bucket_name_patcher = mock.patch(
      'offline.update_ehr_status._get_curation_bucket_name',
      return_value='fake_bucket'
    )
    self.config_bucket_name_patcher.start()
    self.addCleanup(self.config_bucket_name_patcher.stop)
    self.setup_initial_data()

  def setup_initial_data(self):
    self._write_csv('/fake_bucket/hpo_foo/foo/2019-01-01/person.csv', [
      ['pid', 'foo'],
      ['11', 'bar'],
      ['12', 'baz'],
    ])
    self._write_csv('/fake_bucket/hpo_bar/foo/2019-01-02/person.csv', [
      ['pid', 'foo'],
      ['13', 'bar'],
      ['14', 'baz'],
    ])

    self.hpo_foo = self._make_hpo(int_id=10, string_id='hpo_foo')
    self.hpo_bar = self._make_hpo(int_id=11, string_id='hpo_bar')

    self.participants = {
      participant.participantId: {
        'participant': participant,
        'summary': summary,
      }
      for participant, summary in [
        self._make_participant(hpo=self.hpo_foo, int_id=11),
        self._make_participant(hpo=self.hpo_foo, int_id=12),
        self._make_participant(hpo=self.hpo_bar, int_id=13),
        self._make_participant(hpo=self.hpo_bar, int_id=14),
      ]
    }

  @staticmethod
  def _write_csv(filename, iterable):
    contents = StringIO.StringIO()
    writer = csv.writer(contents)
    writer.writerows(iterable)
    contents.seek(0)
    with cloudstorage.open(filename, mode='w') as f:
      f.write(contents.read().encode('utf-8'))

  def _make_hpo(self, int_id, string_id):
    hpo = HPO(hpoId=int_id, name=string_id)
    self.hpo_dao.insert(hpo)
    return hpo

  def _make_participant(self, hpo, int_id):
    participant = self._participant_with_defaults(participantId=int_id, biobankId=int_id)
    participant.hpoId = hpo.hpoId
    self.participant_dao.insert(participant)
    summary = self.participant_summary(participant)
    self.summary_dao.insert(summary)
    return participant, summary

  def test_updates_participant_summaries(self):
    for participant_id in [11,12,13,14]:
      summary = self.summary_dao.get(participant_id)
      print summary, summary.ehrStatus
      self.assertEqual(summary.ehrStatus, EhrStatus.NOT_PRESENT)

    with FakeClock(datetime.datetime(2019, 1, 2)):
      offline.update_ehr_status.update_ehr_status()
      run_deferred_tasks(self)

    summary = self.summary_dao.get(11)
    self.assertEqual(summary.ehrStatus, EhrStatus.NOT_PRESENT)

    summary = self.summary_dao.get(12)
    self.assertEqual(summary.ehrStatus, EhrStatus.NOT_PRESENT)

    summary = self.summary_dao.get(13)
    self.assertEqual(summary.ehrStatus, EhrStatus.PRESENT)
    self.assertEqual(summary.ehrReceiptTime, datetime.datetime(2019,1,2))
    self.assertEqual(summary.ehrUpdateTime, datetime.datetime(2019,1,2))

    summary = self.summary_dao.get(14)
    self.assertEqual(summary.ehrStatus, EhrStatus.PRESENT)
    self.assertEqual(summary.ehrReceiptTime, datetime.datetime(2019,1,2))
    self.assertEqual(summary.ehrUpdateTime, datetime.datetime(2019,1,2))
