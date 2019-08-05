import datetime
import time

from rdr_service import config
from clock import FakeClock
from cloudstorage import cloudstorage_api  # stubbed by testbed
from rdr_service.dao.participant_dao import ParticipantDao, ParticipantHistoryDao
from rdr_service.model.participant import Participant
from rdr_service.offline import exclude_ghost_participants
from test.unit_test.unit_test_util import CloudStorageSqlTestBase, NdbTestBase, TestBase


BUCKET_NAME = 'all-of-us-rdr-test-ghost-accounts'
TIME = datetime.datetime(2019, 1, 30)
TIME_2 = datetime.datetime(2019, 1, 31)
TIME_3 = datetime.datetime(2019, 2, 1)

_FAKE_BUCKET = 'rdr_fake_bucket'


class MarkGhostParticipantsTest(CloudStorageSqlTestBase, NdbTestBase):
  """Tests setting a flag on participants as a ghost account with date added.
  """
  def setUp(self):
    super(MarkGhostParticipantsTest, self).setUp(use_mysql=True)
    NdbTestBase.doSetUp(self)
    TestBase.setup_fake(self)
    config.override_setting(config.GHOST_ID_BUCKET, [_FAKE_BUCKET])
    self.participant_dao = ParticipantDao()
    self.p_history = ParticipantHistoryDao()

  def _write_cloud_csv(self, file_name, contents_str):
    with cloudstorage_api.open('/%s/%s' % (_FAKE_BUCKET, file_name), mode='w') as cloud_file:
      cloud_file.write(contents_str.encode('utf-8'))

  def _setup_participants(self):
    self.participant1 = Participant(participantId=1, biobankId=1)
    self.participant2 = Participant(participantId=2, biobankId=2)
    self.participant_dao.insert(self.participant1)
    self.participant_dao.insert(self.participant2)
    self.assertEqual(self.participant1.isGhostId, None)
    self.assertEqual(self.participant1.dateAddedGhost, None)
    self.assertEqual(self.participant2.isGhostId, None)
    self.assertEqual(self.participant2.dateAddedGhost, None)

  def _setup_file(self, wrong_pid=False):
    # mock up a ghost pid csv
    header = 'participant_id, regisered_date'
    if not wrong_pid:
      row1 = str(self.participant1.participantId) + ',' + str(TIME)
      row2 = str(self.participant2.participantId) + ',' + str(TIME_2)
    else:
      row1 = 'P12345'
      row2 = 'P67890'
    csv_contents = '\n'.join([header, row1, row2])
    self._write_cloud_csv('ghost_pids.csv', csv_contents)

  def tearDown(self):
    super(MarkGhostParticipantsTest, self).tearDown()

  def test_mark_ghost_participant(self):
    self._setup_participants()
    self._setup_file()

    with FakeClock(TIME_3):
      exclude_ghost_participants.mark_ghost_participants()

    person1 = self.participant_dao.get(self.participant1.participantId)
    person2 = self.participant_dao.get(self.participant2.participantId)
    self.assertEqual(person1.isGhostId, 1)
    self.assertEqual(person1.dateAddedGhost, TIME_3)
    self.assertEqual(person2.isGhostId, 1)
    self.assertEqual(person2.dateAddedGhost, TIME_3)

  def test_participant_history_is_updated(self):
    self._setup_participants()
    self._setup_file()

    with FakeClock(TIME_3):
      exclude_ghost_participants.mark_ghost_participants()
    # version 2 should have ghost id flag set.
    history = self.p_history.get([1, 2])
    self.assertEqual(history.isGhostId, 1)
    self.assertEqual(history.dateAddedGhost, TIME_3)

  def test_find_latest_csv(self):
    # The cloud storage testbed does not expose an injectable time function.
    # Creation time is stored at second granularity.
    self._write_cloud_csv('a_lex_first_created_first.csv', 'any contents')
    time.sleep(1.0)
    self._write_cloud_csv('z_lex_last_created_middle.csv', 'any contents')
    time.sleep(1.0)
    created_last = 'b_lex_middle_created_last.csv'
    self._write_cloud_csv(created_last, 'any contents')

    _, latest_filename = exclude_ghost_participants.get_latest_pid_file(
      _FAKE_BUCKET)
    self.assertEquals(latest_filename, '/%s/%s' % (_FAKE_BUCKET, created_last))

  def test_no_participant_to_mark(self):
    # make sure a csv with bad PIDS doesn't blow up.
    self._setup_participants()
    self._setup_file(wrong_pid=True)

    with FakeClock(TIME_3):
      exclude_ghost_participants.mark_ghost_participants()
