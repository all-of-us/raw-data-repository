import csv
import datetime

from clock import FakeClock
from code_constants import METRIC_FIELD_TO_QUESTION_CODE, FIELD_TO_QUESTIONNAIRE_MODULE_CODE
from model.code import CodeType
from dao.participant_dao import ParticipantDao
from google.appengine.ext import deferred
from model.participant import Participant
from mapreduce import test_support
from offline.metrics_export import MetricsExport, PARTICIPANTS_CSV, HPO_IDS_CSV, ANSWERS_CSV
from offline.sql_exporter import SqlExporter
from offline_test.gcs_utils import assertCsvContents
from participant_enums import UNSET_HPO_ID
from test_data import primary_provider_link
from testlib import testutil
from unit_test_util import SqlTestBase, PITT_HPO_ID, run_deferred_tasks

BUCKET_NAME = 'pmi-drc-biobank-test.appspot.com'
TIME = datetime.datetime(2016, 1, 1)
TIME_2 = datetime.datetime(2016, 1, 2)
TIME_3 = datetime.datetime(2016, 1, 3)
TIME_FORMAT = "%Y-%m-%d %H:%M:%S.%f"

class MetricsExportTest(testutil.CloudStorageTestBase):

  def setUp(self):
    super(MetricsExportTest, self).setUp()
    testutil.HandlerTestBase.setUp(self)
    SqlTestBase.setup_database()
    self.taskqueue.FlushQueue("default")

  def tearDown(self):
    super(MetricsExportTest, self).tearDown()
    SqlTestBase.teardown_database()

  def testMetricExport(self):
    SqlTestBase.setup_hpos()
    SqlTestBase.setup_codes(METRIC_FIELD_TO_QUESTION_CODE.values(), code_type=CodeType.QUESTION)
    SqlTestBase.setup_codes(FIELD_TO_QUESTIONNAIRE_MODULE_CODE.values(), code_type=CodeType.MODULE)
    participant_dao = ParticipantDao()

    with FakeClock(TIME):
      participant = Participant(participantId=1, biobankId=2)
      participant_dao.insert(participant)

    with FakeClock(TIME_2):
      participant = Participant(participantId=1, version=1, biobankId=2,
                                providerLink=primary_provider_link('PITT'))
      participant_dao.update(participant)

    MetricsExport.start_export_tasks(BUCKET_NAME, TIME_3)
    run_deferred_tasks(self)

    assertCsvContents(self, BUCKET_NAME, TIME_3.isoformat() + "/" + HPO_IDS_CSV,
                      [['participant_id', 'hpo_id', 'last_modified'],
                       ['1', str(UNSET_HPO_ID), TIME.strftime(TIME_FORMAT)],
                       ['1', str(PITT_HPO_ID), TIME_2.strftime(TIME_FORMAT)]])

