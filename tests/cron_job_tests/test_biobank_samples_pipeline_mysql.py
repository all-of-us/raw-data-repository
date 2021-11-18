import datetime

from rdr_service import clock
from rdr_service.clock import FakeClock
from rdr_service.code_constants import BIOBANK_TESTS, PPI_SYSTEM, RACE_AIAN_CODE, RACE_QUESTION_CODE, RACE_WHITE_CODE
from rdr_service.concepts import Concept
from rdr_service.dao import database_utils
from rdr_service.dao.biobank_order_dao import BiobankOrderDao
from rdr_service.dao.biobank_stored_sample_dao import BiobankStoredSampleDao
from rdr_service.dao.participant_dao import ParticipantDao
from rdr_service.dao.participant_summary_dao import ParticipantSummaryDao
from rdr_service.model.biobank_order import BiobankOrder, BiobankOrderIdentifier, BiobankOrderedSample
from rdr_service.model.biobank_stored_sample import BiobankStoredSample
from rdr_service.model.biobank_mail_kit_order import BiobankMailKitOrder
from rdr_service.dao.mail_kit_order_dao import MailKitOrderDao
from rdr_service.model.code import CodeType
from rdr_service.model.config_utils import to_client_biobank_id
from rdr_service.model.participant import Participant
from rdr_service.model.utils import to_client_participant_id
from rdr_service.offline import biobank_samples_pipeline
from rdr_service.offline.biobank_samples_pipeline import _KIT_ID_SYSTEM, _TRACKING_NUMBER_SYSTEM
from rdr_service.participant_enums import WithdrawalStatus
from tests.helpers.unittest_base import BaseTestCase, InMemorySqlExporter

# Expected names for the reconciliation_data columns in output CSVs.
_CSV_COLUMN_NAMES = (
    "biobank_id",
    "sent_test",
    "sent_count",
    "sent_order_id",
    "sent_collection_time",
    "sent_processed_time",
    "sent_finalized_time",
    "source_site_name",
    "source_site_mayolink_client_number",
    "source_site_hpo",
    "source_site_hpo_type",
    "finalized_site_name",
    "finalized_site_mayolink_client_number",
    "finalized_site_hpo",
    "finalized_site_hpo_type",
    "finalized_username",
    "received_test",
    "received_count",
    "received_sample_id",
    "received_time",
    "Sample Family Create Date",
    "elapsed_hours",
    "biospecimen_kit_id",
    "fedex_tracking_number",
    "is_native_american",
    "notes_collected",
    "notes_processed",
    "notes_finalized",
    "edited_cancelled_restored_status_flag",
    "edited_cancelled_restored_name",
    "edited_cancelled_restored_site_name",
    "edited_cancelled_restored_site_time",
    "edited_cancelled_restored_site_reason",
    "biobank_order_origin",
    "participant_origin"
)


class MySqlReconciliationTest(BaseTestCase):

    def setUp(self):
        super(MySqlReconciliationTest, self).setUp()
        self.participant_dao = ParticipantDao()
        self.summary_dao = ParticipantSummaryDao()
        self.order_dao = BiobankOrderDao()
        self.sample_dao = BiobankStoredSampleDao()

    def _withdraw(self, participant, withdrawal_time):
        with FakeClock(withdrawal_time):
            participant.withdrawalStatus = WithdrawalStatus.NO_USE
            self.participant_dao.update(participant)

    def _modify_order(self, modify_type, order):
        if modify_type == "CANCELLED":
            cancelled_request = self._get_cancel_patch()
            self.order_dao.update_with_patch(order.biobankOrderId, cancelled_request, order.version)
        elif modify_type == "AMENDED":
            amended_info = self._get_amended_info(order)
            amended_info.amendedSiteId = 2
            with self.order_dao.session() as session:
                self.order_dao._do_update(session, order, amended_info)
        elif modify_type == "RESTORED":
            cancelled_request = self._get_cancel_patch()
            cancelled_order = self.order_dao.update_with_patch(order.biobankOrderId, cancelled_request, order.version)

            restore_request = self._get_restore_patch()
            self.order_dao.update_with_patch(order.biobankOrderId, restore_request, cancelled_order.version)

    @staticmethod
    def _get_cancel_patch():
        return {
            "amendedReason": "I messed something up :( ",
            "cancelledInfo": {
                "author": {"system": "https://www.pmi-ops.org/healthpro-username", "value": "mike@pmi-ops.org"},
                "site": {"system": "https://www.pmi-ops.org/site-id", "value": "hpo-site-monroeville"},
            },
            "status": "cancelled",
        }

    @staticmethod
    def _get_restore_patch():
        return {
            "amendedReason": 'I didn"t mess something up :( ',
            "restoredInfo": {
                "author": {"system": "https://www.pmi-ops.org/healthpro-username", "value": "mike@pmi-ops.org"},
                "site": {"system": "https://www.pmi-ops.org/site-id", "value": "hpo-site-monroeville"},
            },
            "status": "restored",
        }

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

    def _insert_participant(self, race_codes=[]):
        participant = self.participant_dao.insert(Participant())
        # satisfies the consent requirement
        self.summary_dao.insert(self.participant_summary(participant))

        if race_codes:
            self._submit_race_questionnaire_response(to_client_participant_id(participant.participantId), race_codes)
        return participant

    def _insert_order(
        self,
        participant,
        order_id,
        tests,
        order_time,
        finalized_tests=None,
        kit_id=None,
        tracking_number=None,
        collected_note=None,
        processed_note=None,
        finalized_note=None,
        order_origin=None,
    ):
        order = BiobankOrder(
            biobankOrderId=order_id,
            participantId=participant.participantId,
            sourceSiteId=1,
            finalizedSiteId=2,
            collectedSiteId=1,
            finalizedUsername="bob@pmi-ops.org",
            created=order_time,
            collectedNote=collected_note,
            processedNote=processed_note,
            finalizedNote=finalized_note,
            orderOrigin=order_origin,
            samples=[],
        )
        id_1 = BiobankOrderIdentifier(system="https://orders.mayomedicallaboratories.com", value=order_id)
        id_2 = BiobankOrderIdentifier(system="https://www.pmi-ops.org", value="O%s" % order_id)
        order.identifiers.append(id_1)
        order.identifiers.append(id_2)
        if kit_id:
            order.identifiers.append(BiobankOrderIdentifier(system=_KIT_ID_SYSTEM, value=kit_id))
        if tracking_number:
            order.identifiers.append(BiobankOrderIdentifier(system=_TRACKING_NUMBER_SYSTEM, value=tracking_number))
        for test_code in tests:
            finalized_time = order_time
            if finalized_tests and not test_code in finalized_tests:
                finalized_time = None
            order.samples.append(
                BiobankOrderedSample(
                    biobankOrderId=order.biobankOrderId,
                    test=test_code,
                    description="test",
                    processingRequired=False,
                    collected=order_time,
                    processed=order_time,
                    finalized=finalized_time,
                )
            )
        return self.order_dao.insert(order)

    def _insert_samples(self, participant, tests, sample_ids, identifier, confirmed_time, created_time):
        for test_code, sample_id in zip(tests, sample_ids):
            self.sample_dao.insert(
                BiobankStoredSample(
                    biobankStoredSampleId=sample_id,
                    biobankId=participant.biobankId,
                    biobankOrderIdentifier=identifier,
                    test=test_code,
                    confirmed=confirmed_time,
                    created=created_time,
                )
            )

    def _submit_race_questionnaire_response(self, participant_id, race_answers):
        code_answers = []
        for answer in race_answers:
            _add_code_answer(code_answers, "race", answer)
        qr = self.make_questionnaire_response_json(participant_id, self._questionnaire_id, code_answers=code_answers)
        self.send_post("Participant/%s/QuestionnaireResponse" % participant_id, qr)

    def _create_dv_order(self, participant_obj, missing=False,
                         received=False, is_test=None):
        dt = 11 if missing else 2
        mayo_create_time = clock.CLOCK.now().replace(microsecond=0) - datetime.timedelta(days=dt)

        # since not using Mayolink API for test, need a biobank order
        order = self._insert_order(
            participant_obj,
            f'DVOrderMissing{participant_obj.participantId}',
            [BIOBANK_TESTS[13]],
            mayo_create_time
        )

        dv_dao = MailKitOrderDao()
        dv_order_obj = BiobankMailKitOrder(
            participantId=participant_obj.participantId,
            version=1,
            biobankOrderId=order.biobankOrderId,
            isTestSample=is_test,
        )

        # Samples for the 'received' dv order test
        if not missing and received:
            self._insert_samples(participant_obj,
                                 [BIOBANK_TESTS[12]],
                                 [f"PresentDVSample{participant_obj.participantId}"],
                                 f"PresentDVIdentifier{participant_obj.participantId}",
                                 clock.CLOCK.now().replace(microsecond=0),
                                 mayo_create_time)

        return dv_dao.insert(dv_order_obj)

    def test_reconciliation_query(self):
        self.setup_codes([RACE_QUESTION_CODE], CodeType.QUESTION)
        self.setup_codes([RACE_AIAN_CODE, RACE_WHITE_CODE], CodeType.ANSWER)
        self._questionnaire_id = self.create_questionnaire("questionnaire3.json")
        # MySQL and Python sub-second rounding differs, so trim micros from generated times.
        order_time = clock.CLOCK.now().replace(microsecond=0)
        old_order_time = order_time - datetime.timedelta(days=11)
        within_24_hours = order_time + datetime.timedelta(hours=23)
        old_within_24_hours = old_order_time + datetime.timedelta(hours=23)
        late_time = order_time + datetime.timedelta(hours=25)
        old_late_time = old_order_time + datetime.timedelta(hours=25)
        file_time = order_time + datetime.timedelta(hours=23) + datetime.timedelta(minutes=59)
        two_days_ago = file_time - datetime.timedelta(days=2)

        # On time, recent order and samples; shows up in rx
        p_on_time = self._insert_participant()
        # Extra samples ordered now aren't considered missing or late.
        on_time_order = self._insert_order(
            p_on_time,
            "GoodOrder",
            BIOBANK_TESTS[:4],
            order_time,
            finalized_tests=BIOBANK_TESTS[:3],
            kit_id="kit1",
            tracking_number="t1",
            collected_note="\u2013foo",
            processed_note="bar",
            finalized_note="baz",
            order_origin="testOrigin"
        )
        # edited order with matching sample; show both in rx and modified
        self._modify_order("AMENDED", on_time_order)
        self._insert_samples(
            p_on_time,
            BIOBANK_TESTS[:2],
            ["GoodSample1", "GoodSample2"],
            "OGoodOrder",
            within_24_hours,
            within_24_hours - datetime.timedelta(hours=1),
        )

        # On time, recent order and samples not confirmed do not show up in reports.
        p_unconfirmed_samples = self._insert_participant()
        self._insert_order(
            p_unconfirmed_samples,
            "not_confirmed_order",
            BIOBANK_TESTS[:4],
            order_time,
            finalized_tests=BIOBANK_TESTS[:3],
            kit_id="kit3",
            tracking_number="t3",
            collected_note="\u2013foo",
            processed_note="bar",
            finalized_note="baz",
        )
        self._insert_samples(
            p_unconfirmed_samples,
            BIOBANK_TESTS[:3],
            ["Unconfirmed_sample", "Unconfirmed_sample2"],
            "Ounconfirmed_sample",
            None,
            within_24_hours,
        )

        # two day old order not confirmed shows up in missing.
        p_unconfirmed_missing = self._insert_participant()
        self._insert_order(
            p_unconfirmed_missing,
            "unconfirmed_missing",
            BIOBANK_TESTS[:1],
            two_days_ago,
            finalized_tests=BIOBANK_TESTS[:1],
            kit_id="kit4",
            tracking_number="t4",
            collected_note="\u2013foo",
            processed_note="baz",
            finalized_note="eggs",
        )
        self._insert_samples(
            p_unconfirmed_missing,
            BIOBANK_TESTS[:1],
            ["Unconfirmed_missing", "Unconfirmed_missing2"],
            "Ounconfirmed_missing_sample",
            None,
            two_days_ago,
        )

        # old order time and samples not confirmed does not exist;
        p_unconfirmed_samples_3 = self._insert_participant()
        self._insert_order(
            p_unconfirmed_samples_3,
            "not_confirmed_order_old",
            BIOBANK_TESTS[:4],
            order_time,
            finalized_tests=BIOBANK_TESTS[:3],
            kit_id="kit5",
            tracking_number="t5",
            collected_note="\u2013foo",
            processed_note="bar",
            finalized_note="baz",
        )
        self._insert_samples(
            p_unconfirmed_samples_3,
            BIOBANK_TESTS[:5],
            ["Unconfirmed_sample_2", "Unconfirmed_sample_3"],
            "Ounconfirmed_sample_2",
            None,
            old_order_time,
        )

        # insert a sample without an order, should not be in reports
        self._insert_samples(
            p_unconfirmed_samples_3,
            BIOBANK_TESTS[:5],
            ["Unconfirmed_sample_4", "Unconfirmed_sample_5"],
            "Ounconfirmed_sample_3",
            None,
            old_order_time,
        )
        # insert a sample without an order for two days ago, should be in received reports
        p_unconfirmed_samples_4 = self._insert_participant()
        self._insert_samples(
            p_unconfirmed_samples_4,
            BIOBANK_TESTS[:5],
            ["Unconfirmed_sample_6", "Unconfirmed_sample_7"],
            "Ounconfirmed_sample_4",
            two_days_ago,
            two_days_ago,
        )

        # On time order and samples from 10 days ago; should not show up in rx or modified
        p_old_on_time = self._insert_participant(race_codes=[RACE_AIAN_CODE])
        # Old missing samples from 10 days ago don't show up in missing or late.
        old_on_time_order = self._insert_order(
            p_old_on_time, "OldGoodOrder", BIOBANK_TESTS[:3], old_order_time, kit_id="kit2"
        )
        self._modify_order("AMENDED", old_on_time_order)
        self._insert_samples(
            p_old_on_time,
            BIOBANK_TESTS[:2],
            ["OldGoodSample1", "OldGoodSample2"],
            "OOldGoodOrder",
            old_within_24_hours,
            old_within_24_hours - datetime.timedelta(hours=1),
        )

        # Late, recent order and samples; shows up in rx and late. (But not missing, as it hasn't been
        # 36 hours since the order.)
        p_late_and_missing = self._insert_participant()
        # Extra missing sample doesn't show up as missing as it hasn't been 24 hours yet.
        self._insert_order(p_late_and_missing, "SlowOrder", BIOBANK_TESTS[:3], order_time)
        self._insert_samples(
            p_late_and_missing,
            [BIOBANK_TESTS[0]],
            ["LateSample"],
            "OSlowOrder",
            late_time,
            late_time - datetime.timedelta(minutes=59),
        )

        # ordered sample not finalized with stored sample should be in missing and received.
        p_not_finalized = self._insert_participant()
        self._insert_order(
            p_not_finalized, "UnfinalizedOrder", BIOBANK_TESTS[:2], order_time, finalized_tests=BIOBANK_TESTS[:1]
        )
        self._insert_samples(
            p_not_finalized,
            [BIOBANK_TESTS[1]],
            ["missing_order"],
            "OUnfinalizedOrder",
            order_time,
            order_time - datetime.timedelta(hours=1),
        )

        # Late order and samples from 10 days ago; should not show up in rx and missing, as it was too
        # long ago.
        p_old_late_and_missing = self._insert_participant()
        self._insert_order(p_old_late_and_missing, "OldSlowOrder", BIOBANK_TESTS[:2], old_order_time)
        self._insert_samples(
            p_old_late_and_missing,
            [BIOBANK_TESTS[0]],
            ["OldLateSample"],
            "OOldSlowOrder",
            old_late_time,
            old_late_time - datetime.timedelta(minutes=59),
        )

        # Order with missing sample from 2 days ago; shows up in missing.
        p_two_days_missing = self._insert_participant()
        # The third test doesn't wind up in missing, as it was never finalized.
        self._insert_order(
            p_two_days_missing,
            "TwoDaysMissingOrder",
            BIOBANK_TESTS[:3],
            two_days_ago,
            finalized_tests=BIOBANK_TESTS[:2],
        )

        # Order with missing sample from 2 days ago that was cancelled; does not show up in missing.
        p_modified_cancelled = self._insert_participant()
        modified_cancelled_order = self._insert_order(
            p_modified_cancelled, 'TwoDaysMissingCancelled',
            BIOBANK_TESTS[:1],
            two_days_ago, finalized_tests=BIOBANK_TESTS[:1]
        )
        self._modify_order('CANCELLED', modified_cancelled_order)

        # Recent samples with no matching order; shows up in missing and received.
        p_extra = self._insert_participant(race_codes=[RACE_WHITE_CODE])
        self._insert_samples(
            p_extra,
            [BIOBANK_TESTS[-1]],
            ["NobodyOrderedThisSample"],
            "OExtraOrderNotSent",
            order_time,
            order_time - datetime.timedelta(minutes=59),
        )

        # Old samples with no matching order; Does not show up.
        p_old_extra = self._insert_participant(race_codes=[RACE_AIAN_CODE])
        self._insert_samples(
            p_old_extra,
            [BIOBANK_TESTS[-1]],
            ["OldNobodyOrderedThisSample"],
            "OOldExtrOrderNotSent",
            old_order_time,
            old_order_time - datetime.timedelta(hours=1),
        )

        # cancelled/restored order with not matching sample; Does not show in rx, but should in modified
        p_modified_on_time = self._insert_participant()
        modified_order = self._insert_order(
            p_modified_on_time,
            "CancelledOrder",
            BIOBANK_TESTS[:1],
            order_time,
            finalized_tests=BIOBANK_TESTS[:1],
            kit_id="kit6",
            tracking_number="t6",
            collected_note="\u2013foo",
            processed_note="bar",
            finalized_note="baz",
        )
        self._modify_order("CANCELLED", modified_order)

        modified_order = self._insert_order(
            p_modified_on_time,
            "RestoredOrder",
            BIOBANK_TESTS[:1],
            order_time,
            finalized_tests=BIOBANK_TESTS[:1],
            kit_id="kit7",
            tracking_number="t7",
            collected_note="\u2013foo",
            processed_note="bar",
            finalized_note="baz",
        )
        self._modify_order("RESTORED", modified_order)

        # Withdrawn participants don't show up in any reports except withdrawal report.
        # only the participant who has sample collected will show in withdrawal report.
        p_withdrawn_old_on_time = self._insert_participant(race_codes=[RACE_AIAN_CODE])
        # This updates the version of the participant and its HPO ID.
        self._insert_order(p_withdrawn_old_on_time, "OldWithdrawnGoodOrder", BIOBANK_TESTS[:2], old_order_time)
        p_withdrawn_old_on_time = self.participant_dao.get(p_withdrawn_old_on_time.participantId)
        self._insert_samples(
            p_withdrawn_old_on_time,
            BIOBANK_TESTS[:2],
            ["OldWithdrawnGoodSample1", "OldWithdrawnGoodSample2"],
            "OOldWithdrawnGoodOrder",
            old_within_24_hours,
            old_within_24_hours - datetime.timedelta(hours=1),
        )
        self._withdraw(p_withdrawn_old_on_time, within_24_hours)

        p_withdrawn_late_and_missing = self._insert_participant()
        self._insert_order(p_withdrawn_late_and_missing, "WithdrawnSlowOrder", BIOBANK_TESTS[:2], order_time)
        self._insert_samples(
            p_withdrawn_late_and_missing,
            [BIOBANK_TESTS[0]],
            ["WithdrawnLateSample"],
            "OWithdrawnSlowOrder",
            late_time,
            late_time - datetime.timedelta(minutes=59),
        )
        p_withdrawn_late_and_missing = self.participant_dao.get(p_withdrawn_late_and_missing.participantId)
        self._withdraw(p_withdrawn_late_and_missing, within_24_hours)

        p_withdrawn_old_late_and_missing = self._insert_participant()
        self._insert_order(
            p_withdrawn_old_late_and_missing, "WithdrawnOldSlowOrder", BIOBANK_TESTS[:2], old_order_time
        )
        self._insert_samples(
            p_withdrawn_old_late_and_missing,
            [BIOBANK_TESTS[0]],
            ["WithdrawnOldLateSample"],
            "OWithdrawnOldSlowOrder",
            old_late_time,
            old_late_time - datetime.timedelta(minutes=59),
        )
        p_withdrawn_old_late_and_missing = self.participant_dao.get(p_withdrawn_old_late_and_missing.participantId)
        self._withdraw(p_withdrawn_old_late_and_missing, old_late_time)

        p_withdrawn_extra = self._insert_participant(race_codes=[RACE_WHITE_CODE])
        self._insert_samples(
            p_withdrawn_extra,
            [BIOBANK_TESTS[-1]],
            ["WithdrawnNobodyOrderedThisSample"],
            "OWithdrawnOldSlowOrder",
            order_time,
            order_time - datetime.timedelta(hours=1),
        )
        self._withdraw(p_withdrawn_extra, within_24_hours)

        p_withdrawn_old_extra = self._insert_participant(race_codes=[RACE_AIAN_CODE])
        self._insert_samples(
            p_withdrawn_old_extra,
            [BIOBANK_TESTS[-1]],
            ["WithdrawnOldNobodyOrderedThisSample"],
            "OwithdrawnOldSlowOrder",
            old_order_time,
            old_order_time - datetime.timedelta(hours=1),
        )
        self._withdraw(p_withdrawn_old_extra, within_24_hours)

        # this one will not show in the withdrawal report because no sample collected
        p_withdrawn_race_change = self._insert_participant(race_codes=[RACE_AIAN_CODE])
        p_withdrawn_race_change_id = to_client_participant_id(p_withdrawn_race_change.participantId)
        self._submit_race_questionnaire_response(p_withdrawn_race_change_id, [RACE_WHITE_CODE])
        self._withdraw(p_withdrawn_race_change, within_24_hours)

        # for the same participant/test, 3 orders sent and only 2 samples received. Shows up in both
        # missing (we are missing one sample) and late (the two samples that were received were after
        # 24 hours.)
        p_repeated = self._insert_participant()
        for repetition in range(3):
            self._insert_order(
                p_repeated,
                "RepeatedOrder%d" % repetition,
                [BIOBANK_TESTS[0]],
                two_days_ago + datetime.timedelta(hours=repetition),
            )
            if repetition != 2:
                self._insert_samples(
                    p_repeated,
                    [BIOBANK_TESTS[0]],
                    ["RepeatedSample%d" % repetition],
                    "ORepeatedOrder%d" % repetition,
                    within_24_hours + datetime.timedelta(hours=repetition),
                    within_24_hours + datetime.timedelta(hours=repetition - 1),
                )

        # Participants to test the salivary missing report
        p_missing_salivary = self._insert_participant(race_codes=[RACE_WHITE_CODE])
        self._create_dv_order(p_missing_salivary, missing=True)
        p_missing_salivary_test = self._insert_participant(race_codes=[RACE_WHITE_CODE])
        self._create_dv_order(p_missing_salivary_test, missing=True, is_test=True)

        p_missing_inside_timeframe = self._insert_participant(race_codes=[RACE_WHITE_CODE])
        self._create_dv_order(p_missing_inside_timeframe, missing=False)

        p_present_salivary = self._insert_participant(race_codes=[RACE_WHITE_CODE])
        self._create_dv_order(p_present_salivary, missing=False, received=True)

        received, missing, modified = "rx.csv", "missing.csv", "modified.csv"
        missing_salivary = "missing_salivary.csv"
        exporter = InMemorySqlExporter(self)
        biobank_samples_pipeline._query_and_write_reports(
            exporter, file_time, "daily", received, missing, modified, missing_salivary
        )

        exporter.assertFilesEqual((received, missing, modified, missing_salivary))

        # sent-and-received: 4 on-time, 2 late, none of the missing/extra/repeated ones;
        # not includes orders/samples from more than 10 days ago;
        # Includes 1 Salivary order
        exporter.assertRowCount(received, 11)
        exporter.assertColumnNamesEqual(received, _CSV_COLUMN_NAMES)
        row = exporter.assertHasRow(
            received,
            {
                "biobank_id": to_client_biobank_id(p_on_time.biobankId),
                "sent_test": BIOBANK_TESTS[0],
                "received_test": BIOBANK_TESTS[0],
            },
        )

        # sent count=0, received count=1 should in received report
        exporter.assertHasRow(
            received, {"sent_count": "0", "received_count": "1", "sent_test": "", "received_test": BIOBANK_TESTS[0]}
        )

        # p_repeated has 2 received and 2 late.
        exporter.assertHasRow(
            received,
            {
                "biobank_id": to_client_biobank_id(p_repeated.biobankId),
                "sent_test": BIOBANK_TESTS[0],
                "received_test": BIOBANK_TESTS[0],
                "sent_order_id": "ORepeatedOrder1",
            },
        )
        exporter.assertHasRow(
            received,
            {
                "biobank_id": to_client_biobank_id(p_repeated.biobankId),
                "sent_test": BIOBANK_TESTS[0],
                "received_test": BIOBANK_TESTS[0],
                "sent_order_id": "ORepeatedOrder0",
            },
        )
        exporter.assertHasRow(
            missing,
            {"biobank_id": to_client_biobank_id(p_not_finalized.biobankId), "sent_order_id": "OUnfinalizedOrder"},
        )

        # Also check the values of all remaining fields on one row.
        self.assertEqual(row["source_site_name"], "Monroeville Urgent Care Center")
        self.assertEqual(row["source_site_mayolink_client_number"], "7035769")
        self.assertEqual(row["source_site_hpo"], "PITT")
        self.assertEqual(row["source_site_hpo_type"], "HPO")
        self.assertEqual(row["finalized_site_name"], "Phoenix Urgent Care Center")
        self.assertEqual(row["finalized_site_mayolink_client_number"], "7035770")
        self.assertEqual(row["finalized_site_hpo"], "PITT")
        self.assertEqual(row["finalized_site_hpo_type"], "HPO")
        self.assertEqual(row["finalized_username"], "bob@pmi-ops.org")
        self.assertEqual(row["sent_finalized_time"], database_utils.format_datetime(order_time))
        self.assertEqual(row["sent_collection_time"], database_utils.format_datetime(order_time))
        self.assertEqual(row["sent_processed_time"], database_utils.format_datetime(order_time))
        self.assertEqual(row["received_time"], database_utils.format_datetime(within_24_hours))
        self.assertEqual(
            row["Sample Family Create Date"],
            database_utils.format_datetime(within_24_hours - datetime.timedelta(hours=1)),
        )
        self.assertEqual(row["sent_count"], "1")
        self.assertEqual(row["received_count"], "1")
        self.assertEqual(row["sent_order_id"], "OGoodOrder")
        self.assertEqual(row["received_sample_id"], "GoodSample1")
        self.assertEqual(row["biospecimen_kit_id"], "kit1")
        self.assertEqual(row["fedex_tracking_number"], "t1")
        self.assertEqual(row["is_native_american"], "N")
        self.assertEqual(row["notes_collected"], "\u2013foo")
        self.assertEqual(row["notes_processed"], "bar")
        self.assertEqual(row["notes_finalized"], "baz")
        self.assertEqual(row["sent_order_id"], "OGoodOrder")
        self.assertEqual(row["biobank_order_origin"], "testOrigin")

        # the other sent-and-received rows
        exporter.assertHasRow(
            received, {"biobank_id": to_client_biobank_id(p_on_time.biobankId), "sent_test": BIOBANK_TESTS[1]}
        )
        exporter.assertHasRow(
            received, {"biobank_id": to_client_biobank_id(p_late_and_missing.biobankId), "sent_test": BIOBANK_TESTS[0]}
        )
        exporter.assertHasRow(
            received,
            {
                "biobank_id": to_client_biobank_id(p_old_late_and_missing.biobankId),
                "sent_test": BIOBANK_TESTS[0],
                "is_native_american": "N",
            },
        )

        # orders/samples where something went wrong; don't include orders/samples from more than 10
        # days ago, or where 24 hours hasn't elapsed yet.
        exporter.assertRowCount(missing, 8)
        exporter.assertColumnNamesEqual(missing, _CSV_COLUMN_NAMES)
        exporter.assertHasRow(
            missing,
            {"biobank_id": to_client_biobank_id(p_not_finalized.biobankId), "sent_order_id": "OUnfinalizedOrder"},
        )
        # order not confirmed 2 days old
        exporter.assertHasRow(
            missing,
            {
                "biobank_id": to_client_biobank_id(p_unconfirmed_missing.biobankId),
                "sent_order_id": "Ounconfirmed_missing",
                "sent_test": "1ED10",
            },
        )
        # sample received, nothing ordered
        exporter.assertHasRow(
            missing, {"biobank_id": to_client_biobank_id(p_extra.biobankId), "sent_order_id": "OExtraOrderNotSent"}
        )
        # order received, no sample
        exporter.assertHasRow(
            missing,
            {
                "biobank_id": to_client_biobank_id(p_two_days_missing.biobankId),
                "sent_order_id": "OTwoDaysMissingOrder",
                "sent_test": BIOBANK_TESTS[0],
                "is_native_american": "N",
            },
        )
        exporter.assertHasRow(
            missing,
            {
                "biobank_id": to_client_biobank_id(p_two_days_missing.biobankId),
                "sent_order_id": "OTwoDaysMissingOrder",
                "sent_test": BIOBANK_TESTS[1],
            },
        )

        # 3 orders sent, only 2 received
        multi_sample_row = exporter.assertHasRow(
            missing,
            {"biobank_id": to_client_biobank_id(p_repeated.biobankId), "sent_count": "1", "received_count": "0"},
        )
        # Also verify the comma-joined fields of the row with multiple orders/samples.
        self.assertCountEqual(multi_sample_row["sent_order_id"].split(","), ["ORepeatedOrder2"])

        # modified biobank orders show in modified report
        exporter.assertRowCount(modified, 7)
        exporter.assertHasRow(
            modified,
            {
                "biobank_id": to_client_biobank_id(p_on_time.biobankId),
                "edited_cancelled_restored_status_flag": "edited",
                "edited_cancelled_restored_name": "mike@pmi-ops.org",
                "edited_cancelled_restored_site_reason": "I had to change something",
                "edited_cancelled_restored_site_name": "Monroeville Urgent Care Center",
            },
        )
        exporter.assertHasRow(
            modified,
            {
                "biobank_id": to_client_biobank_id(p_modified_on_time.biobankId),
                "edited_cancelled_restored_status_flag": "cancelled",
                "edited_cancelled_restored_name": "mike@pmi-ops.org",
                "edited_cancelled_restored_site_reason": "I messed something up :( ",
                "edited_cancelled_restored_site_name": "Monroeville Urgent Care Center",
            },
        )
        exporter.assertHasRow(
            modified,
            {
                "biobank_id": to_client_biobank_id(p_modified_on_time.biobankId),
                "edited_cancelled_restored_status_flag": "restored",
                "edited_cancelled_restored_name": "mike@pmi-ops.org",
                "edited_cancelled_restored_site_reason": 'I didn"t mess something up :( ',
                "edited_cancelled_restored_site_name": "Monroeville Urgent Care Center",
            },
        )

        # Test the missing DV order is in salivary_missing report
        exporter.assertRowCount(missing_salivary, 1)
        exporter.assertHasRow(
            missing_salivary,
            {
                "biobank_id": to_client_biobank_id(p_missing_salivary.biobankId)
            }
        )

        # Test that the DV received order is in the received report
        exporter.assertHasRow(
            received,
            {
                "biobank_id": to_client_biobank_id(p_present_salivary.biobankId)
            }
        )

        # Check that the reports have the participant_origin column
        participant_origin_data = {"participant_origin": "example"}
        exporter.assertHasRow(received, participant_origin_data)
        exporter.assertHasRow(missing, participant_origin_data)
        exporter.assertHasRow(modified, participant_origin_data)
        exporter.assertHasRow(missing_salivary, participant_origin_data)

    def test_monthly_reconciliation_report(self):
        self.setup_codes([RACE_WHITE_CODE], CodeType.ANSWER)
        self._questionnaire_id = self.create_questionnaire("questionnaire3.json")
        # MySQL and Python sub-second rounding differs, so trim micros from generated times.
        order_time = clock.CLOCK.now().replace(microsecond=0)
        edge_order_time = order_time - datetime.timedelta(days=60)
        old_order_time = order_time - datetime.timedelta(days=61)
        within_24_hours = order_time + datetime.timedelta(hours=23)
        old_within_24_hours = old_order_time + datetime.timedelta(hours=23)
        late_time = order_time + datetime.timedelta(hours=25)
        old_late_time = old_order_time + datetime.timedelta(hours=25)
        file_time = order_time + datetime.timedelta(hours=23) + datetime.timedelta(minutes=59)
        two_days_ago = file_time - datetime.timedelta(days=2)

        # On time, recent order and samples; shows up in rx
        p_on_time = self._insert_participant()
        # Extra samples ordered now aren't considered missing or late.
        on_time_order = self._insert_order(
            p_on_time,
            "GoodOrder",
            BIOBANK_TESTS[:4],
            order_time,
            finalized_tests=BIOBANK_TESTS[:3],
            kit_id="kit1",
            tracking_number="t1",
            collected_note="\u2013foo",
            processed_note="bar",
            finalized_note="baz",
            order_origin="testOrigin"
        )
        # edited order with matching sample; show both in rx and modified
        self._modify_order("AMENDED", on_time_order)
        self._insert_samples(
            p_on_time,
            BIOBANK_TESTS[:2],
            ["GoodSample1", "GoodSample2"],
            "OGoodOrder",
            within_24_hours,
            within_24_hours - datetime.timedelta(hours=1),
        )

        # edge_order should shows up in rx
        p_edge_time = self._insert_participant()
        edge_order = self._insert_order(
            p_edge_time,
            "EdgeOrder",
            BIOBANK_TESTS[:4],
            edge_order_time,
            finalized_tests=BIOBANK_TESTS[:3],
            kit_id="kit8",
            tracking_number="t8",
            collected_note="\u2013foo",
            processed_note="bar",
            finalized_note="baz",
        )
        # edited edge order with matching sample; show both in rx and modified
        self._modify_order("AMENDED", edge_order)
        self._insert_samples(
            p_edge_time,
            BIOBANK_TESTS[:2],
            ["EdgeSample1", "EdgeSample2"],
            "OEdgeOrder",
            within_24_hours,
            within_24_hours - datetime.timedelta(hours=1),
        )

        # On time, recent order and samples not confirmed do not show up in reports.
        p_unconfirmed_samples = self._insert_participant()
        self._insert_order(
            p_unconfirmed_samples,
            "not_confirmed_order",
            BIOBANK_TESTS[:4],
            order_time,
            finalized_tests=BIOBANK_TESTS[:3],
            kit_id="kit3",
            tracking_number="t3",
            collected_note="\u2013foo",
            processed_note="bar",
            finalized_note="baz",
        )
        self._insert_samples(
            p_unconfirmed_samples,
            BIOBANK_TESTS[:3],
            ["Unconfirmed_sample", "Unconfirmed_sample2"],
            "Ounconfirmed_sample",
            None,
            within_24_hours,
        )

        # two day old order not confirmed shows up in missing.
        p_unconfirmed_missing = self._insert_participant()
        self._insert_order(
            p_unconfirmed_missing,
            "unconfirmed_missing",
            BIOBANK_TESTS[:1],
            two_days_ago,
            finalized_tests=BIOBANK_TESTS[:1],
            kit_id="kit4",
            tracking_number="t4",
            collected_note="\u2013foo",
            processed_note="baz",
            finalized_note="eggs",
        )
        self._insert_samples(
            p_unconfirmed_missing,
            BIOBANK_TESTS[:1],
            ["Unconfirmed_missing", "Unconfirmed_missing2"],
            "Ounconfirmed_missing_sample",
            None,
            two_days_ago,
        )

        # old order time and samples not confirmed does not exist;
        p_unconfirmed_samples_3 = self._insert_participant()
        self._insert_order(
            p_unconfirmed_samples_3,
            "not_confirmed_order_old",
            BIOBANK_TESTS[:4],
            order_time,
            finalized_tests=BIOBANK_TESTS[:3],
            kit_id="kit5",
            tracking_number="t5",
            collected_note="\u2013foo",
            processed_note="bar",
            finalized_note="baz",
        )
        self._insert_samples(
            p_unconfirmed_samples_3,
            BIOBANK_TESTS[:5],
            ["Unconfirmed_sample_2", "Unconfirmed_sample_3"],
            "Ounconfirmed_sample_2",
            None,
            old_order_time,
        )

        # insert a sample without an order, should not be in reports
        self._insert_samples(
            p_unconfirmed_samples_3,
            BIOBANK_TESTS[:5],
            ["Unconfirmed_sample_4", "Unconfirmed_sample_5"],
            "Ounconfirmed_sample_3",
            None,
            old_order_time,
        )
        # insert a sample without an order for two days ago, should be in received reports
        p_unconfirmed_samples_4 = self._insert_participant()
        self._insert_samples(
            p_unconfirmed_samples_4,
            BIOBANK_TESTS[:5],
            ["Unconfirmed_sample_6", "Unconfirmed_sample_7"],
            "Ounconfirmed_sample_4",
            two_days_ago,
            two_days_ago,
        )

        # On time order and samples from 60 days ago; should not show up in monthly rx or modified
        p_old_on_time = self._insert_participant(race_codes=[RACE_AIAN_CODE])
        # Old missing samples from 60 days ago don't show up in missing or late.
        old_on_time_order = self._insert_order(
            p_old_on_time, "OldGoodOrder", BIOBANK_TESTS[:3], old_order_time, kit_id="kit2"
        )
        self._modify_order("AMENDED", old_on_time_order)
        self._insert_samples(
            p_old_on_time,
            BIOBANK_TESTS[:2],
            ["OldGoodSample1", "OldGoodSample2"],
            "OOldGoodOrder",
            old_within_24_hours,
            old_within_24_hours - datetime.timedelta(hours=1),
        )

        # Late, recent order and samples; shows up in rx and late. (But not missing, as it hasn't been
        # 36 hours since the order.)
        p_late_and_missing = self._insert_participant()
        # Extra missing sample doesn't show up as missing as it hasn't been 24 hours yet.
        self._insert_order(p_late_and_missing, "SlowOrder", BIOBANK_TESTS[:3], order_time)
        self._insert_samples(
            p_late_and_missing,
            [BIOBANK_TESTS[0]],
            ["LateSample"],
            "OSlowOrder",
            late_time,
            late_time - datetime.timedelta(minutes=59),
        )

        # ordered sample not finalized with stored sample should be in missing and received.
        p_not_finalized = self._insert_participant()
        self._insert_order(
            p_not_finalized, "UnfinalizedOrder", BIOBANK_TESTS[:2], order_time, finalized_tests=BIOBANK_TESTS[:1]
        )
        self._insert_samples(
            p_not_finalized,
            [BIOBANK_TESTS[1]],
            ["missing_order"],
            "OUnfinalizedOrder",
            order_time,
            order_time - datetime.timedelta(hours=1),
        )

        # Late order and samples from 60 days ago; should not show up in monthly rx and missing, as it
        # was too long ago.
        p_old_late_and_missing = self._insert_participant()
        self._insert_order(p_old_late_and_missing, "OldSlowOrder", BIOBANK_TESTS[:2], old_order_time)
        self._insert_samples(
            p_old_late_and_missing,
            [BIOBANK_TESTS[0]],
            ["OldLateSample"],
            "OOldSlowOrder",
            old_late_time,
            old_late_time - datetime.timedelta(minutes=59),
        )

        # Order with missing sample from 2 days ago; shows up in missing.
        p_two_days_missing = self._insert_participant()
        # The third test doesn't wind up in missing, as it was never finalized.
        self._insert_order(
            p_two_days_missing,
            "TwoDaysMissingOrder",
            BIOBANK_TESTS[:3],
            two_days_ago,
            finalized_tests=BIOBANK_TESTS[:2],
        )

        # Recent samples with no matching order; shows up in missing and received.
        p_extra = self._insert_participant(race_codes=[RACE_WHITE_CODE])
        self._insert_samples(
            p_extra,
            [BIOBANK_TESTS[-1]],
            ["NobodyOrderedThisSample"],
            "OExtraOrderNotSent",
            order_time,
            order_time - datetime.timedelta(minutes=59),
        )

        # Old samples with no matching order; Does not show up.
        p_old_extra = self._insert_participant(race_codes=[RACE_AIAN_CODE])
        self._insert_samples(
            p_old_extra,
            [BIOBANK_TESTS[-1]],
            ["OldNobodyOrderedThisSample"],
            "OOldExtrOrderNotSent",
            old_order_time,
            old_order_time - datetime.timedelta(hours=1),
        )

        # cancelled/restored order with not matching sample; Does not show in rx, but should in modified
        p_modified_on_time = self._insert_participant()
        modified_order = self._insert_order(
            p_modified_on_time,
            "CancelledOrder",
            BIOBANK_TESTS[:1],
            order_time,
            finalized_tests=BIOBANK_TESTS[:1],
            kit_id="kit6",
            tracking_number="t6",
            collected_note="\u2013foo",
            processed_note="bar",
            finalized_note="baz",
        )
        self._modify_order("CANCELLED", modified_order)

        modified_order = self._insert_order(
            p_modified_on_time,
            "RestoredOrder",
            BIOBANK_TESTS[:1],
            order_time,
            finalized_tests=BIOBANK_TESTS[:1],
            kit_id="kit7",
            tracking_number="t7",
            collected_note="\u2013foo",
            processed_note="bar",
            finalized_note="baz",
        )
        self._modify_order("RESTORED", modified_order)

        # Withdrawn participants don't show up in any reports except withdrawal report.

        p_withdrawn_old_on_time = self._insert_participant(race_codes=[RACE_AIAN_CODE])
        # This updates the version of the participant and its HPO ID.
        self._insert_order(p_withdrawn_old_on_time, "OldWithdrawnGoodOrder", BIOBANK_TESTS[:2], old_order_time)
        p_withdrawn_old_on_time = self.participant_dao.get(p_withdrawn_old_on_time.participantId)
        self._insert_samples(
            p_withdrawn_old_on_time,
            BIOBANK_TESTS[:2],
            ["OldWithdrawnGoodSample1", "OldWithdrawnGoodSample2"],
            "OOldWithdrawnGoodOrder",
            old_within_24_hours,
            old_within_24_hours - datetime.timedelta(hours=1),
        )
        self._withdraw(p_withdrawn_old_on_time, within_24_hours)

        p_withdrawn_late_and_missing = self._insert_participant()
        self._insert_order(p_withdrawn_late_and_missing, "WithdrawnSlowOrder", BIOBANK_TESTS[:2], order_time)
        self._insert_samples(
            p_withdrawn_late_and_missing,
            [BIOBANK_TESTS[0]],
            ["WithdrawnLateSample"],
            "OWithdrawnSlowOrder",
            late_time,
            late_time - datetime.timedelta(minutes=59),
        )
        p_withdrawn_late_and_missing = self.participant_dao.get(p_withdrawn_late_and_missing.participantId)
        self._withdraw(p_withdrawn_late_and_missing, within_24_hours)

        p_withdrawn_old_late_and_missing = self._insert_participant()
        self._insert_order(
            p_withdrawn_old_late_and_missing, "WithdrawnOldSlowOrder", BIOBANK_TESTS[:2], old_order_time
        )
        self._insert_samples(
            p_withdrawn_old_late_and_missing,
            [BIOBANK_TESTS[0]],
            ["WithdrawnOldLateSample"],
            "OWithdrawnOldSlowOrder",
            old_late_time,
            old_late_time - datetime.timedelta(minutes=59),
        )
        p_withdrawn_old_late_and_missing = self.participant_dao.get(p_withdrawn_old_late_and_missing.participantId)
        self._withdraw(p_withdrawn_old_late_and_missing, old_late_time)

        p_withdrawn_extra = self._insert_participant(race_codes=[RACE_WHITE_CODE])
        self._insert_samples(
            p_withdrawn_extra,
            [BIOBANK_TESTS[-1]],
            ["WithdrawnNobodyOrderedThisSample"],
            "OWithdrawnOldSlowOrder",
            order_time,
            order_time - datetime.timedelta(hours=1),
        )
        self._withdraw(p_withdrawn_extra, within_24_hours)

        p_withdrawn_old_extra = self._insert_participant(race_codes=[RACE_AIAN_CODE])
        self._insert_samples(
            p_withdrawn_old_extra,
            [BIOBANK_TESTS[-1]],
            ["WithdrawnOldNobodyOrderedThisSample"],
            "OwithdrawnOldSlowOrder",
            old_order_time,
            old_order_time - datetime.timedelta(hours=1),
        )
        self._withdraw(p_withdrawn_old_extra, within_24_hours)

        # this one will not show in the withdrawal report because no sample collected
        p_withdrawn_race_change = self._insert_participant(race_codes=[RACE_AIAN_CODE])
        p_withdrawn_race_change_id = to_client_participant_id(p_withdrawn_race_change.participantId)
        self._submit_race_questionnaire_response(p_withdrawn_race_change_id, [RACE_WHITE_CODE])
        self._withdraw(p_withdrawn_race_change, within_24_hours)

        # for the same participant/test, 3 orders sent and only 2 samples received. Shows up in both
        # missing (we are missing one sample) and late (the two samples that were received were after
        # 24 hours.)
        p_repeated = self._insert_participant()
        for repetition in range(3):
            self._insert_order(
                p_repeated,
                "RepeatedOrder%d" % repetition,
                [BIOBANK_TESTS[0]],
                two_days_ago + datetime.timedelta(hours=repetition),
            )
            if repetition != 2:
                self._insert_samples(
                    p_repeated,
                    [BIOBANK_TESTS[0]],
                    ["RepeatedSample%d" % repetition],
                    "ORepeatedOrder%d" % repetition,
                    within_24_hours + datetime.timedelta(hours=repetition),
                    within_24_hours + datetime.timedelta(hours=repetition - 1),
                )

        received, missing, modified = (
            "rx_monthly.csv",
            "missing_monthly.csv",
            "modified_monthly.csv",
        )
        exporter = InMemorySqlExporter(self)
        biobank_samples_pipeline._query_and_write_reports(
            exporter, file_time, "monthly", received, missing, modified
        )

        exporter.assertFilesEqual((received, missing, modified))

        # sent-and-received: 4 on-time, 2 late, 2 edge, none of the missing/extra/repeated ones;
        # not includes orders/samples from more than 60 days ago
        exporter.assertRowCount(received, 12)
        exporter.assertColumnNamesEqual(received, _CSV_COLUMN_NAMES)
        row = exporter.assertHasRow(
            received,
            {
                "biobank_id": to_client_biobank_id(p_on_time.biobankId),
                "sent_test": BIOBANK_TESTS[0],
                "received_test": BIOBANK_TESTS[0],
            },
        )

        # sent count=0, received count=1 should in received report
        exporter.assertHasRow(
            received, {"sent_count": "0", "received_count": "1", "sent_test": "", "received_test": BIOBANK_TESTS[0]}
        )

        # p_repeated has 2 received and 2 late.
        exporter.assertHasRow(
            received,
            {
                "biobank_id": to_client_biobank_id(p_repeated.biobankId),
                "sent_test": BIOBANK_TESTS[0],
                "received_test": BIOBANK_TESTS[0],
                "sent_order_id": "ORepeatedOrder1",
            },
        )
        exporter.assertHasRow(
            received,
            {
                "biobank_id": to_client_biobank_id(p_repeated.biobankId),
                "sent_test": BIOBANK_TESTS[0],
                "received_test": BIOBANK_TESTS[0],
                "sent_order_id": "ORepeatedOrder0",
            },
        )
        exporter.assertHasRow(
            missing,
            {"biobank_id": to_client_biobank_id(p_not_finalized.biobankId), "sent_order_id": "OUnfinalizedOrder"},
        )

        # Also check the values of all remaining fields on one row.
        self.assertEqual(row["source_site_name"], "Monroeville Urgent Care Center")
        self.assertEqual(row["source_site_mayolink_client_number"], "7035769")
        self.assertEqual(row["source_site_hpo"], "PITT")
        self.assertEqual(row["source_site_hpo_type"], "HPO")
        self.assertEqual(row["finalized_site_name"], "Phoenix Urgent Care Center")
        self.assertEqual(row["finalized_site_mayolink_client_number"], "7035770")
        self.assertEqual(row["finalized_site_hpo"], "PITT")
        self.assertEqual(row["finalized_site_hpo_type"], "HPO")
        self.assertEqual(row["finalized_username"], "bob@pmi-ops.org")
        self.assertEqual(row["sent_finalized_time"], database_utils.format_datetime(order_time))
        self.assertEqual(row["sent_collection_time"], database_utils.format_datetime(order_time))
        self.assertEqual(row["sent_processed_time"], database_utils.format_datetime(order_time))
        self.assertEqual(row["received_time"], database_utils.format_datetime(within_24_hours))
        self.assertEqual(
            row["Sample Family Create Date"],
            database_utils.format_datetime(within_24_hours - datetime.timedelta(hours=1)),
        )
        self.assertEqual(row["sent_count"], "1")
        self.assertEqual(row["received_count"], "1")
        self.assertEqual(row["sent_order_id"], "OGoodOrder")
        self.assertEqual(row["received_sample_id"], "GoodSample1")
        self.assertEqual(row["biospecimen_kit_id"], "kit1")
        self.assertEqual(row["fedex_tracking_number"], "t1")
        self.assertEqual(row["is_native_american"], "N")
        self.assertEqual(row["notes_collected"], "\u2013foo")
        self.assertEqual(row["notes_processed"], "bar")
        self.assertEqual(row["notes_finalized"], "baz")
        self.assertEqual(row["sent_order_id"], "OGoodOrder")
        self.assertEqual(row["biobank_order_origin"], "testOrigin")

        # the other sent-and-received rows
        exporter.assertHasRow(
            received, {"biobank_id": to_client_biobank_id(p_on_time.biobankId), "sent_test": BIOBANK_TESTS[1]}
        )
        exporter.assertHasRow(
            received, {"biobank_id": to_client_biobank_id(p_late_and_missing.biobankId), "sent_test": BIOBANK_TESTS[0]}
        )
        exporter.assertHasRow(
            received,
            {
                "biobank_id": to_client_biobank_id(p_old_late_and_missing.biobankId),
                "sent_test": BIOBANK_TESTS[0],
                "is_native_american": "N",
            },
        )

        # orders/samples where something went wrong; don't include orders/samples from more than 60
        # days ago, or where 24 hours hasn't elapsed yet.
        exporter.assertRowCount(missing, 9)
        exporter.assertColumnNamesEqual(missing, _CSV_COLUMN_NAMES)
        exporter.assertHasRow(
            missing,
            {"biobank_id": to_client_biobank_id(p_not_finalized.biobankId), "sent_order_id": "OUnfinalizedOrder"},
        )
        # order not confirmed 2 days old
        exporter.assertHasRow(
            missing,
            {
                "biobank_id": to_client_biobank_id(p_unconfirmed_missing.biobankId),
                "sent_order_id": "Ounconfirmed_missing",
                "sent_test": "1ED10",
            },
        )
        # sample received, nothing ordered
        exporter.assertHasRow(
            missing, {"biobank_id": to_client_biobank_id(p_extra.biobankId), "sent_order_id": "OExtraOrderNotSent"}
        )
        # order received, no sample
        exporter.assertHasRow(
            missing,
            {
                "biobank_id": to_client_biobank_id(p_two_days_missing.biobankId),
                "sent_order_id": "OTwoDaysMissingOrder",
                "sent_test": BIOBANK_TESTS[0],
                "is_native_american": "N",
            },
        )
        exporter.assertHasRow(
            missing,
            {
                "biobank_id": to_client_biobank_id(p_two_days_missing.biobankId),
                "sent_order_id": "OTwoDaysMissingOrder",
                "sent_test": BIOBANK_TESTS[1],
            },
        )

        # 3 orders sent, only 2 received
        multi_sample_row = exporter.assertHasRow(
            missing,
            {"biobank_id": to_client_biobank_id(p_repeated.biobankId), "sent_count": "1", "received_count": "0"},
        )
        # Also verify the comma-joined fields of the row with multiple orders/samples.
        self.assertCountEqual(multi_sample_row["sent_order_id"].split(","), ["ORepeatedOrder2"])

        # modified biobank orders show in modified report
        exporter.assertRowCount(modified, 10)
        exporter.assertHasRow(
            modified,
            {
                "biobank_id": to_client_biobank_id(p_on_time.biobankId),
                "edited_cancelled_restored_status_flag": "edited",
                "edited_cancelled_restored_name": "mike@pmi-ops.org",
                "edited_cancelled_restored_site_reason": "I had to change something",
                "edited_cancelled_restored_site_name": "Monroeville Urgent Care Center",
            },
        )
        exporter.assertHasRow(
            modified,
            {
                "biobank_id": to_client_biobank_id(p_modified_on_time.biobankId),
                "edited_cancelled_restored_status_flag": "cancelled",
                "edited_cancelled_restored_name": "mike@pmi-ops.org",
                "edited_cancelled_restored_site_reason": "I messed something up :( ",
                "edited_cancelled_restored_site_name": "Monroeville Urgent Care Center",
            },
        )
        exporter.assertHasRow(
            modified,
            {
                "biobank_id": to_client_biobank_id(p_modified_on_time.biobankId),
                "edited_cancelled_restored_status_flag": "restored",
                "edited_cancelled_restored_name": "mike@pmi-ops.org",
                "edited_cancelled_restored_site_reason": 'I didn"t mess something up :( ',
                "edited_cancelled_restored_site_name": "Monroeville Urgent Care Center",
            },
        )


def _add_code_answer(code_answers, link_id, code):
    if code:
        code_answers.append((link_id, Concept(PPI_SYSTEM, code)))
