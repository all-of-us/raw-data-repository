import StringIO
import calendar
import collections
import csv
import datetime
import sqlalchemy
import unittest

import cloudstorage
import mock
from clock import FakeClock
from dao.ehr_dao import EhrReceiptDao
from dao.hpo_dao import HPODao
from dao.organization_dao import OrganizationDao
from dao.participant_dao import ParticipantDao
from dao.participant_summary_dao import ParticipantSummaryDao
from model.ehr import EhrReceipt
import offline.update_ehr_status
from model.hpo import HPO
from model.organization import Organization
from participant_enums import EhrStatus
from test.unit_test.unit_test_util import FlaskTestBase, run_deferred_tasks, \
  CloudStorageSqlTestBase, TestBase


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

  def test_get_submission_info(self):
    info_obj = offline.update_ehr_status._get_submission_info_from_filename(
      '/fake_bucket/hpo1/aou123/2019-01-02/person.csv'
    )
    self.assertEqual(info_obj.bucket_name, 'aou123')
    self.assertEqual(info_obj.date, datetime.date(2019, 1, 2))
    self.assertEqual(info_obj.person_file, '/fake_bucket/hpo1/aou123/2019-01-02/person.csv')

  @mock.patch('offline.update_ehr_status._get_org_id_by_bucket_name_map')
  def test_basic(self, mock_get_map):
    mock_get_map.return_value = {
      'aou123': 'SOME_ORG'
    }
    bucket_stat_list = [
      self.MockGCSFileStat(
        '/fake_bucket/hpo1/aou123/2019-01-02/person.csv',
        calendar.timegm(datetime.date(2019, 1, 2).timetuple())
      ),
    ]
    results = offline.update_ehr_status._get_organization_info_list(
      bucket_stat_list,
      datetime.date(2019, 1, 1)
    )
    self.assertEqual(len(results), 1)
    self.assertEqual(
      results[0],
      (
        'SOME_ORG',
        datetime.date(2019, 1, 2),
        '/fake_bucket/hpo1/aou123/2019-01-02/person.csv'
      )
    )

  @mock.patch('offline.update_ehr_status._get_org_id_by_bucket_name_map')
  def test_missing_bucket_name(self, mock_get_map):
    mock_get_map.return_value = {
      'aou123': 'SOME_ORG'
    }
    bucket_stat_list = [
      self.MockGCSFileStat(
        '/fake_bucket/hpo1/aou456/2019-01-02/person.csv',
        calendar.timegm(datetime.date(2019, 1, 2).timetuple())
      ),
    ]
    results = offline.update_ehr_status._get_organization_info_list(
      bucket_stat_list,
      datetime.date(2019, 1, 1)
    )
    self.assertEqual(len(results), 0)

  @mock.patch('offline.update_ehr_status._get_org_id_by_bucket_name_map')
  def test_gets_latest(self, mock_get_map):
    mock_get_map.return_value = {
      'aou123': 'SOME_ORG'
    }
    bucket_stat_list = [
      self.MockGCSFileStat(
        '/fake_bucket/hpo1/aou123/2019-01-02/person.csv',
        calendar.timegm(datetime.date(2019, 1, 2).timetuple())
      ),
      self.MockGCSFileStat(
        '/fake_bucket/hpo1/aou123/2019-01-03/person.csv',
        calendar.timegm(datetime.date(2019, 1, 3).timetuple())
      ),
    ]
    results = offline.update_ehr_status._get_organization_info_list(
      bucket_stat_list,
      datetime.date(2019, 1, 1)
    )
    self.assertEqual(len(results), 1)
    self.assertEqual(
      results[0],
      (
        'SOME_ORG',
        datetime.date(2019, 1, 3),
        '/fake_bucket/hpo1/aou123/2019-01-03/person.csv'
      )
    )

  @mock.patch('offline.update_ehr_status._get_org_id_by_bucket_name_map')
  def test_gets_latest_alphabetical_when_tied(self, mock_get_map):
    mock_get_map.return_value = {
      'aou123': 'SOME_ORG'
    }
    bucket_stat_list = [
      self.MockGCSFileStat(
        '/fake_bucket/hpo1/aou123/2019-01-02-v2/person.csv',
        calendar.timegm(datetime.date(2019, 1, 2).timetuple())
      ),
      self.MockGCSFileStat(
        '/fake_bucket/hpo1/aou123/2019-01-02-v1/person.csv',
        calendar.timegm(datetime.date(2019, 1, 2).timetuple())
      ),
    ]
    results = offline.update_ehr_status._get_organization_info_list(
      bucket_stat_list,
      datetime.date(2019, 1, 1)
    )
    self.assertEqual(len(results), 1)
    self.assertEqual(
      results[0],
      (
        'SOME_ORG',
        datetime.date(2019, 1, 2),
        '/fake_bucket/hpo1/aou123/2019-01-02-v2/person.csv'
      )
    )

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
    self.org_dao = OrganizationDao()
    self.participant_dao = ParticipantDao()
    self.summary_dao = ParticipantSummaryDao()
    self.ehr_receipt_dao = EhrReceiptDao()

    self.config_bucket_name_patcher = mock.patch(
      'offline.update_ehr_status._get_curation_bucket_name',
      return_value='fake_bucket'
    )
    self.addCleanup(self.config_bucket_name_patcher.stop)
    self.config_bucket_name_patcher.start()

    self.get_sheet_id_patcher = mock.patch(
      'offline.update_ehr_status._get_sheet_id',
      return_value='12345'
    )
    self.addCleanup(self.get_sheet_id_patcher.stop)
    self.get_sheet_id_patcher.start()

    self.get_org_id_by_bucket_name_map_patcher = mock.patch(
      'offline.update_ehr_status._get_org_id_by_bucket_name_map',
      return_value={
        'aou-foo-a': 'FOO_A',
        'aou-foo-b': 'FOO_B',
        'aou-bar-a': 'BAR_A',
      }
    )
    self.addCleanup(self.get_org_id_by_bucket_name_map_patcher.stop)
    self.get_org_id_by_bucket_name_map_patcher.start()

    self.hpo_foo = self._make_hpo(int_id=10, string_id='hpo_foo')
    self.hpo_bar = self._make_hpo(int_id=11, string_id='hpo_bar')

    self.org_foo_a = self._make_org(hpo=self.hpo_foo, int_id=10, external_id='FOO_A')
    self.org_foo_b = self._make_org(hpo=self.hpo_foo, int_id=11, external_id='FOO_B')
    self.org_bar_a = self._make_org(hpo=self.hpo_bar, int_id=12, external_id='BAR_A')

    self._make_participant(hpo=self.hpo_foo, org=self.org_foo_a, int_id=11)
    self._make_participant(hpo=self.hpo_foo, org=self.org_foo_b, int_id=12)
    self._make_participant(hpo=self.hpo_bar, org=self.org_bar_a, int_id=13)
    self._make_participant(hpo=self.hpo_bar, org=self.org_bar_a, int_id=14)

  def tearDown(self):
    super(UpdateEhrStatusFullExecutionTest, self).tearDown()
    FlaskTestBase.doTearDown(self)

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

  def _make_org(self, hpo, int_id, external_id):
    org = Organization(
      organizationId=int_id,
      externalId=external_id,
      displayName='SOME ORG',
      hpoId=hpo.hpoId
    )
    self.org_dao.insert(org)
    return org

  def _make_participant(self, hpo, org, int_id):
    participant = self._participant_with_defaults(participantId=int_id, biobankId=int_id)
    participant.hpoId = hpo.hpoId
    participant.organizationId = org.organizationId
    self.participant_dao.insert(participant)
    summary = self.participant_summary(participant)
    self.summary_dao.insert(summary)
    return participant, summary

  def _get_all_ehr_receipts(self):
    with self.ehr_receipt_dao.session() as session:
      cursor = session.execute(sqlalchemy.select([EhrReceipt]))
      return [dict(zip(cursor.keys(), row)) for row in cursor]

  def test_initial_update_participant_summaries(self):
    self._write_csv('/fake_bucket/hpo_foo/aou-foo-a/2019-01-01/person.csv', [
      ['pid', 'foo'],
      ['11', 'bar'],
    ])
    self._write_csv('/fake_bucket/hpo_foo/aou-foo-b/2019-01-01/person.csv', [
      ['pid', 'foo'],
      ['12', 'bar'],
    ])

    with FakeClock(datetime.datetime(2019, 1, 1)):
      offline.update_ehr_status.update_ehr_status()
      run_deferred_tasks(self)

    summary = self.summary_dao.get(11)
    self.assertEqual(summary.ehrStatus, EhrStatus.PRESENT)
    self.assertEqual(summary.ehrReceiptTime, datetime.datetime(2019,1,1))
    self.assertEqual(summary.ehrUpdateTime, datetime.datetime(2019,1,1))

    summary = self.summary_dao.get(12)
    self.assertEqual(summary.ehrStatus, EhrStatus.PRESENT)
    self.assertEqual(summary.ehrReceiptTime, datetime.datetime(2019,1,1))
    self.assertEqual(summary.ehrUpdateTime, datetime.datetime(2019,1,1))

  def test_updates_participant_summaries(self):
    self._write_csv('/fake_bucket/hpo_foo/aou-foo-a/2019-01-01/person.csv', [
      ['pid', 'foo'],
      ['11', 'bar'],
    ])

    with FakeClock(datetime.datetime(2019, 1, 1)):
      offline.update_ehr_status.update_ehr_status()
      run_deferred_tasks(self)

    self._write_csv('/fake_bucket/hpo_foo/aou-foo-a/2019-01-02/person.csv', [
      ['pid', 'foo'],
      ['11', 'bar'],
    ])
    self._write_csv('/fake_bucket/hpo_foo/aou-foo-b/2019-01-02/person.csv', [
      ['pid', 'foo'],
      ['12', 'bar'],
    ])

    with FakeClock(datetime.datetime(2019, 1, 2)):
      offline.update_ehr_status.update_ehr_status()
      run_deferred_tasks(self)

    summary = self.summary_dao.get(11)
    self.assertEqual(summary.ehrStatus, EhrStatus.PRESENT)
    self.assertEqual(summary.ehrReceiptTime, datetime.datetime(2019,1,1))
    self.assertEqual(summary.ehrUpdateTime, datetime.datetime(2019,1,2))

    summary = self.summary_dao.get(12)
    self.assertEqual(summary.ehrStatus, EhrStatus.PRESENT)
    self.assertEqual(summary.ehrReceiptTime, datetime.datetime(2019,1,2))
    self.assertEqual(summary.ehrUpdateTime, datetime.datetime(2019,1,2))

  def test_creates_receipts(self):
    # round 1
    self._write_csv('/fake_bucket/hpo_foo/aou-foo-a/2019-01-01/person.csv', [
      ['pid', 'foo'],
      ['11', 'bar'],
    ])
    with FakeClock(datetime.datetime(2019, 1, 1)):
      offline.update_ehr_status.update_ehr_status()
      run_deferred_tasks(self)

    receipts = self._get_all_ehr_receipts()
    self.assertEqual(len(receipts), 1)
    self.assertEqual(receipts[0]['organization_id'], self.org_foo_a.organizationId)
    self.assertEqual(receipts[0]['receipt_time'], datetime.datetime(2019, 1, 1))

    # round 2
    self._write_csv('/fake_bucket/hpo_foo/aou-foo-a/2019-01-02/person.csv', [
      ['pid', 'foo'],
      ['11', 'bar'],
    ])
    self._write_csv('/fake_bucket/hpo_foo/aou-foo-b/2019-01-02/person.csv', [
      ['pid', 'foo'],
      ['12', 'bar'],
    ])
    with FakeClock(datetime.datetime(2019, 1, 2)):
      offline.update_ehr_status.update_ehr_status()
      run_deferred_tasks(self)

    # round 3
    self._write_csv('/fake_bucket/hpo_bar/aou-bar-a/2019-01-03/person.csv', [
      ['pid', 'foo'],
      ['13', 'bar'],
      ['14', 'baz'],
    ])
    with FakeClock(datetime.datetime(2019, 1, 3)):
      offline.update_ehr_status.update_ehr_status()
      run_deferred_tasks(self)

    receipts = self._get_all_ehr_receipts()
    self.assertEqual(
      [
        (int(receipt['organization_id']), receipt['receipt_time'])
        for receipt in receipts
      ],
      [
        (self.org_foo_a.organizationId, datetime.datetime(2019, 1, 1)),
        (self.org_foo_a.organizationId, datetime.datetime(2019, 1, 2)),
        (self.org_foo_b.organizationId, datetime.datetime(2019, 1, 2)),
        (self.org_bar_a.organizationId, datetime.datetime(2019, 1, 3)),
      ]
    )

  def test_ignores_invalid_submission_folder_names(self):
    self._write_csv('/fake_bucket/hpo_foo/aou-bar-a/2019-01-01/person.csv', [
      ['pid', 'foo'],
      ['13', 'bar'],
    ])

    with FakeClock(datetime.datetime(2019, 1, 1)):
      offline.update_ehr_status.update_ehr_status()
      run_deferred_tasks(self)

    self._write_csv('/fake_bucket/hpo_foo/aou-bar-a/fuzz/person.csv', [
      ['pid', 'foo'],
      ['13', 'bar'],
      ['14', 'baz'],
    ])

    with FakeClock(datetime.datetime(2019, 1, 3)):
      offline.update_ehr_status.update_ehr_status()
      run_deferred_tasks(self)

    summary = self.summary_dao.get(13)
    self.assertEqual(summary.ehrStatus, EhrStatus.PRESENT)
    self.assertEqual(summary.ehrReceiptTime, datetime.datetime(2019,1,1))
    self.assertEqual(summary.ehrUpdateTime, datetime.datetime(2019,1,1))

    summary = self.summary_dao.get(14)
    self.assertEqual(summary.ehrStatus, None)
