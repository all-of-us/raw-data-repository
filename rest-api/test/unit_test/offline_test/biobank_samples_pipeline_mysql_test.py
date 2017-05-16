import datetime

import clock
from clock import FakeClock
from code_constants import BIOBANK_TESTS, RACE_QUESTION_CODE, RACE_WHITE_CODE, RACE_AIAN_CODE
from code_constants import PPI_SYSTEM
from concepts import Concept
from model.code import CodeType
from dao import database_utils
from dao.biobank_order_dao import BiobankOrderDao
from dao.biobank_stored_sample_dao import BiobankStoredSampleDao
from dao.participant_dao import ParticipantDao
from dao.participant_summary_dao import ParticipantSummaryDao
from offline import biobank_samples_pipeline
from unit_test_util import FlaskTestBase, InMemorySqlExporter, make_questionnaire_response_json
from model.biobank_order import BiobankOrder, BiobankOrderedSample
from model.biobank_stored_sample import BiobankStoredSample
from model.utils import to_client_biobank_id, to_client_participant_id
from model.participant import Participant
from participant_enums import WithdrawalStatus

# Expected names for the reconciliation_data columns in output CSVs.
_CSV_COLUMN_NAMES = (
  'biobank_id',

  'sent_test',
  'sent_count',
  'sent_order_id',
  'sent_collection_time',
  'sent_finalized_time',
  'source_site_name',
  'source_site_consortium',
  'source_site_mayolink_client_number',
  'source_site_hpo',
  'finalized_site_name',
  'finalized_site_consortium',
  'finalized_site_mayolink_client_number',
  'finalized_site_hpo',
  'finalized_username',

  'received_test',
  'received_count',
  'received_sample_id',
  'received_time',

  'elapsed_hours',
)


class MySqlReconciliationTest(FlaskTestBase):
  """Biobank samples pipeline tests requiring slower MySQL (not SQLite)."""
  def setUp(self):
    super(MySqlReconciliationTest, self).setUp(use_mysql=True)
    self.participant_dao = ParticipantDao()
    self.summary_dao = ParticipantSummaryDao()
    self.order_dao = BiobankOrderDao()
    self.sample_dao = BiobankStoredSampleDao()

  def _withdraw(self, participant, withdrawal_time):
    with FakeClock(withdrawal_time):
      participant.withdrawalStatus = WithdrawalStatus.NO_USE
      self.participant_dao.update(participant)


  def _insert_participant(self, race_codes=[]):
    participant = self.participant_dao.insert(Participant())
    # satisfies the consent requirement
    self.summary_dao.insert(self.participant_summary(participant))

    if race_codes:
      self._submit_race_questionnaire_response(to_client_participant_id(participant.participantId),
                                               race_codes)
    return participant

  def _insert_order(self, participant, order_id, tests, order_time):
    order = BiobankOrder(
        biobankOrderId=order_id,
        participantId=participant.participantId,
        sourceSiteId=1,
        finalizedSiteId=1,
        finalizedUsername='bob@pmi-ops.org',
        created=order_time,
        samples=[])
    for test_code in tests:
      order.samples.append(BiobankOrderedSample(
          biobankOrderId=order.biobankOrderId,
          test=test_code,
          description=u'test',
          processingRequired=False,
          collected=order_time,
          finalized=order_time))
    return self.order_dao.insert(order)

  def _insert_samples(self, participant, tests, sample_ids, received_time):
    for test_code, sample_id in zip(tests, sample_ids):
      self.sample_dao.insert(BiobankStoredSample(
          biobankStoredSampleId=sample_id,
          biobankId=participant.biobankId,
          test=test_code,
          confirmed=received_time))

  def _submit_race_questionnaire_response(self, participant_id,
                                          race_answers):
    code_answers = []
    for answer in race_answers:
      _add_code_answer(code_answers, "race", answer)
    qr = make_questionnaire_response_json(participant_id, self._questionnaire_id,
                                          code_answers=code_answers)
    self.send_post('Participant/%s/QuestionnaireResponse' % participant_id, qr)


  def test_reconciliation_query(self):
    self.setup_codes([RACE_QUESTION_CODE], CodeType.QUESTION)
    self.setup_codes([RACE_AIAN_CODE, RACE_WHITE_CODE,
                      "AIAN_AmericanIndian",
                      "AIAN_AlaskaNative",
                      "AIAN_CentralSouthAmericanIndian"],
                     CodeType.ANSWER)
    self._questionnaire_id = self.create_questionnaire('questionnaire3.json')
    # MySQL and Python sub-second rounding differs, so trim micros from generated times.
    order_time = clock.CLOCK.now().replace(microsecond=0)
    old_order_time = order_time - datetime.timedelta(days=10)
    within_36_hours = order_time + datetime.timedelta(hours=35)
    old_within_36_hours =  old_order_time + datetime.timedelta(hours=35)
    late_time = order_time + datetime.timedelta(hours=37)
    old_late_time = old_order_time + datetime.timedelta(hours=37)
    file_time = order_time + datetime.timedelta(hours=35) + datetime.timedelta(minutes=59)
    two_days_ago = file_time - datetime.timedelta(days=2)

    # On time, recent order and samples; shows up in rx
    p_on_time = self._insert_participant()
    self._insert_order(p_on_time, 'GoodOrder', BIOBANK_TESTS[:2], order_time)
    self._insert_samples(p_on_time, BIOBANK_TESTS[:2], ['GoodSample1', 'GoodSample2'], within_36_hours)

    # On time order and samples from 10 days ago; shows up in rx
    p_old_on_time = self._insert_participant(race_codes=["AIAN_AmericanIndian"])
    self._insert_order(p_old_on_time, 'OldGoodOrder', BIOBANK_TESTS[:2], old_order_time)
    self._insert_samples(p_old_on_time, BIOBANK_TESTS[:2], ['OldGoodSample1', 'OldGoodSample2'],
                         old_within_36_hours)

    # Late, recent order and samples; shows up in rx and late. (But not missing, as it hasn't been
    # 36 hours since the order.)
    p_late_and_missing = self._insert_participant()
    o_late_and_missing = self._insert_order(
        p_late_and_missing, 'SlowOrder', BIOBANK_TESTS[:2], order_time)
    self._insert_samples(p_late_and_missing, [BIOBANK_TESTS[0]], ['LateSample'], late_time)

    # Late order and samples from 10 days ago; shows up in rx (but not missing, as it was too 
    # long ago.
    p_old_late_and_missing = self._insert_participant()
    self._insert_order(p_old_late_and_missing, 'OldSlowOrder', BIOBANK_TESTS[:2], old_order_time)
    self._insert_samples(p_old_late_and_missing, [BIOBANK_TESTS[0]], ['OldLateSample'],
                         old_late_time)

    
    # Order with missing sample from 2 days ago; shows up in rx and missing.
    p_two_days_missing = self._insert_participant()
    self._insert_order(p_two_days_missing, 'TwoDaysMissingOrder', BIOBANK_TESTS[:2], 
                       two_days_ago)
    
    # Recent samples with no matching order; shows up in missing.
    p_extra = self._insert_participant(race_codes=[RACE_WHITE_CODE])
    self._insert_samples(p_extra, [BIOBANK_TESTS[-1]], ['NobodyOrderedThisSample'], order_time)

    # Old samples with no matching order; shows up in rx.
    p_old_extra = self._insert_participant(race_codes=[RACE_AIAN_CODE])
    self._insert_samples(p_old_extra, [BIOBANK_TESTS[-1]], ['OldNobodyOrderedThisSample'],
                         old_order_time)

    # Withdrawn participants don't show up in any reports except withdrawal report.

    p_withdrawn_old_on_time = self._insert_participant(race_codes=["AIAN_AmericanIndian"])
    self._insert_order(p_withdrawn_old_on_time, 'OldWithdrawnGoodOrder', BIOBANK_TESTS[:2],
                       old_order_time)
    self._insert_samples(p_withdrawn_old_on_time, BIOBANK_TESTS[:2],
                         ['OldWithdrawnGoodSample1', 'OldWithdrawnGoodSample2'],
                         old_within_36_hours)
    self._withdraw(p_withdrawn_old_on_time, within_36_hours)

    p_withdrawn_late_and_missing = self._insert_participant()
    self._insert_order(p_withdrawn_late_and_missing, 'WithdrawnSlowOrder', BIOBANK_TESTS[:2],
                       order_time)
    self._insert_samples(p_withdrawn_late_and_missing, [BIOBANK_TESTS[0]],
                         ['WithdrawnLateSample'], late_time)
    self._withdraw(p_withdrawn_late_and_missing, within_36_hours)

    p_withdrawn_old_late_and_missing = self._insert_participant()
    self._insert_order(p_withdrawn_old_late_and_missing, 'WithdrawnOldSlowOrder', BIOBANK_TESTS[:2],
                       old_order_time)
    self._insert_samples(p_withdrawn_old_late_and_missing, [BIOBANK_TESTS[0]],
                         ['WithdrawnOldLateSample'], old_late_time)
    self._withdraw(p_withdrawn_old_late_and_missing, old_late_time)

    p_withdrawn_extra = self._insert_participant(race_codes=[RACE_WHITE_CODE])
    self._insert_samples(p_withdrawn_extra, [BIOBANK_TESTS[-1]],
                         ['WithdrawnNobodyOrderedThisSample'], order_time)
    self._withdraw(p_withdrawn_extra, within_36_hours)

    p_withdrawn_old_extra = self._insert_participant(race_codes=[RACE_AIAN_CODE])
    self._insert_samples(p_withdrawn_old_extra, [BIOBANK_TESTS[-1]],
                         ['WithdrawnOldNobodyOrderedThisSample'], old_order_time)
    self._withdraw(p_withdrawn_old_extra, within_36_hours)

    p_withdrawn_race_change = self._insert_participant(race_codes=[RACE_AIAN_CODE])
    p_withdrawn_race_change_id = to_client_participant_id(p_withdrawn_race_change.participantId)
    self._submit_race_questionnaire_response(p_withdrawn_race_change_id, [RACE_WHITE_CODE])
    self._withdraw(p_withdrawn_race_change, within_36_hours)

    # for the same participant/test, 3 orders sent and only 2 samples received. Shows up in both
    # missing (we are missing one sample) and late (the two samples that were received were after
    # 36 hours.)    
    p_repeated = self._insert_participant()
    for repetition in xrange(3):
      self._insert_order(
          p_repeated,
          'RepeatedOrder%d' % repetition,
          [BIOBANK_TESTS[0]],
          two_days_ago + datetime.timedelta(hours=repetition))
      if repetition != 2:
        self._insert_samples(
            p_repeated,
            [BIOBANK_TESTS[0]],
            ['RepeatedSample%d' % repetition],
            within_36_hours + datetime.timedelta(hours=repetition))

    received, late, missing, withdrawals = 'rx.csv', 'late.csv', 'missing.csv', 'withdrawals.csv'
    exporter = InMemorySqlExporter(self)
    biobank_samples_pipeline._query_and_write_reports(exporter, file_time, 
                                                      received, late, missing, withdrawals)

    exporter.assertFilesEqual((received, late, missing, withdrawals))

    # sent-and-received: 4 on-time, 2 late, none of the missing/extra/repeated ones;
    # includes orders/samples from more than 7 days ago
    exporter.assertRowCount(received, 6)
    exporter.assertColumnNamesEqual(received, _CSV_COLUMN_NAMES)
    row = exporter.assertHasRow(received, {
        'biobank_id': to_client_biobank_id(p_on_time.biobankId),
        'sent_test': BIOBANK_TESTS[0],
        'received_test': BIOBANK_TESTS[0]})
    # Also check the values of all remaining fields on one row.
    self.assertEquals(row['source_site_name'], 'Monroeville Urgent Care Center')
    self.assertEquals(row['source_site_consortium'], 'Pittsburgh')
    self.assertEquals(row['source_site_mayolink_client_number'], '7035769')
    self.assertEquals(row['source_site_hpo'], 'PITT')
    self.assertEquals(row['finalized_site_name'], 'Monroeville Urgent Care Center')
    self.assertEquals(row['finalized_site_consortium'], 'Pittsburgh')
    self.assertEquals(row['finalized_site_mayolink_client_number'], '7035769')
    self.assertEquals(row['finalized_site_hpo'], 'PITT')
    self.assertEquals(row['finalized_username'], 'bob@pmi-ops.org')
    self.assertEquals(row['sent_finalized_time'], database_utils.format_datetime(order_time))
    self.assertEquals(row['sent_collection_time'], database_utils.format_datetime(order_time))
    self.assertEquals(row['received_time'], database_utils.format_datetime(within_36_hours))
    self.assertEquals(row['sent_count'], '1')
    self.assertEquals(row['received_count'], '1')
    self.assertEquals(row['sent_order_id'], 'GoodOrder')
    self.assertEquals(row['received_sample_id'], 'GoodSample1')
    # the other sent-and-received rows
    exporter.assertHasRow(received, {
        'biobank_id': to_client_biobank_id(p_on_time.biobankId), 'sent_test': BIOBANK_TESTS[1]})
    exporter.assertHasRow(received, {
        'biobank_id': to_client_biobank_id(p_late_and_missing.biobankId),
        'sent_test': BIOBANK_TESTS[0]})
    exporter.assertHasRow(received, {
        'biobank_id': to_client_biobank_id(p_old_on_time.biobankId), 'sent_test': BIOBANK_TESTS[0]})
    exporter.assertHasRow(received, {
        'biobank_id': to_client_biobank_id(p_old_on_time.biobankId), 'sent_test': BIOBANK_TESTS[1]})
    exporter.assertHasRow(received, {
        'biobank_id': to_client_biobank_id(p_old_late_and_missing.biobankId),
        'sent_test': BIOBANK_TESTS[0]})

    # sent-and-received: 2 late; don't include orders/samples from more than 7 days ago
    exporter.assertRowCount(late, 2)
    exporter.assertColumnNamesEqual(late, _CSV_COLUMN_NAMES)
    exporter.assertHasRow(late, {
        'biobank_id': to_client_biobank_id(p_late_and_missing.biobankId),
        'sent_order_id': o_late_and_missing.biobankOrderId,
        'elapsed_hours': '37'})
    exporter.assertHasRow(late, {
        'biobank_id': to_client_biobank_id(p_repeated.biobankId),            
        'elapsed_hours': '46'})

    # orders/samples where something went wrong; don't include orders/samples from more than 7
    # days ago, or where 36 hours hasn't elapsed yet.
    exporter.assertRowCount(missing, 4)
    exporter.assertColumnNamesEqual(missing, _CSV_COLUMN_NAMES)
    # sample received, nothing ordered
    exporter.assertHasRow(missing, {
        'biobank_id': to_client_biobank_id(p_extra.biobankId), 'sent_order_id': ''})
    # order received, no sample
    exporter.assertHasRow(missing, {
        'biobank_id': to_client_biobank_id(p_two_days_missing.biobankId), 
        'sent_order_id': 'TwoDaysMissingOrder',
        'sent_test': BIOBANK_TESTS[0]})
    exporter.assertHasRow(missing, {
        'biobank_id': to_client_biobank_id(p_two_days_missing.biobankId), 
        'sent_order_id': 'TwoDaysMissingOrder',
        'sent_test': BIOBANK_TESTS[1]})

    # 3 orders sent, only 2 received
    multi_sample_row = exporter.assertHasRow(missing, {
        'biobank_id': to_client_biobank_id(p_repeated.biobankId),
        'sent_count': '3',
        'received_count': '2'})

    # Also verify the comma-joined fields of the row with multiple orders/samples.
    self.assertItemsEqual(
        multi_sample_row['sent_order_id'].split(','),
        ['RepeatedOrder1', 'RepeatedOrder0', 'RepeatedOrder2'])
    self.assertItemsEqual(
        multi_sample_row['received_sample_id'].split(','),
        ['RepeatedSample0', 'RepeatedSample1'])

    # We don't include the old withdrawal.
    exporter.assertRowCount(withdrawals, 5)
    exporter.assertHasRow(withdrawals, {
      'biobank_id': to_client_biobank_id(p_withdrawn_old_on_time.biobankId),
      'withdrawal_time': database_utils.format_datetime(within_36_hours),
      'is_native_american': 'Y'})
    exporter.assertHasRow(withdrawals, {
      'biobank_id': to_client_biobank_id(p_withdrawn_late_and_missing.biobankId),
      'withdrawal_time': database_utils.format_datetime(within_36_hours),
      'is_native_american': 'N'})
    exporter.assertHasRow(withdrawals, {
      'biobank_id': to_client_biobank_id(p_withdrawn_extra.biobankId),
      'withdrawal_time': database_utils.format_datetime(within_36_hours),
      'is_native_american': 'N'})
    exporter.assertHasRow(withdrawals, {
      'biobank_id': to_client_biobank_id(p_withdrawn_old_extra.biobankId),
      'withdrawal_time': database_utils.format_datetime(within_36_hours),
      'is_native_american': 'Y'})
    exporter.assertHasRow(withdrawals, {
      'biobank_id': to_client_biobank_id(p_withdrawn_race_change.biobankId),
      'withdrawal_time': database_utils.format_datetime(within_36_hours),
      'is_native_american': 'N'})

def _add_code_answer(code_answers, link_id, code):
  if code:
    code_answers.append((link_id, Concept(PPI_SYSTEM, code)))
