from collections import namedtuple
import csv
from decimal import Decimal
import io
from datetime import datetime, timedelta
import mock
import random
import os
import pytz

from rdr_service import clock, config
from rdr_service.api_util import open_cloud_file
from rdr_service.code_constants import BIOBANK_TESTS, RACE_QUESTION_CODE, RACE_AIAN_CODE,\
    WITHDRAWAL_CEREMONY_QUESTION_CODE, WITHDRAWAL_CEREMONY_YES, WITHDRAWAL_CEREMONY_NO
from rdr_service.config import BIOBANK_SAMPLES_DAILY_INVENTORY_FILE_PATTERN,\
    BIOBANK_SAMPLES_MONTHLY_INVENTORY_FILE_PATTERN
from rdr_service.dao.biobank_order_dao import BiobankOrderDao
from rdr_service.dao.biobank_stored_sample_dao import BiobankStoredSampleDao
from rdr_service.dao.participant_dao import ParticipantDao
from rdr_service.dao.participant_summary_dao import ParticipantSummaryDao
from rdr_service.model.biobank_mail_kit_order import BiobankMailKitOrder
from rdr_service.model.biobank_order import BiobankOrder, BiobankOrderIdentifier, BiobankOrderedSample
from rdr_service.model.biobank_stored_sample import BiobankStoredSample
from rdr_service.model.config_utils import get_biobank_id_prefix, to_client_biobank_id
from rdr_service.model.participant import Participant
from rdr_service.model.participant_summary import ParticipantSummary
from rdr_service.model.questionnaire_response import QuestionnaireResponseAnswer
from rdr_service.offline import biobank_samples_pipeline
from rdr_service.offline.sql_exporter import SqlExporter
from rdr_service.participant_enums import EnrollmentStatus, SampleStatus, get_sample_status_enum_value,\
    SampleCollectionMethod, WithdrawalAIANCeremonyStatus, WithdrawalStatus
from tests import test_data
from tests.helpers.unittest_base import BaseTestCase, PDRGeneratorTestMixin

_BASELINE_TESTS = list(BIOBANK_TESTS)
_FAKE_BUCKET = "rdr_fake_bucket"

FakeBlob = namedtuple('FakeBlob', ['name', 'updated'])


class BiobankSamplesPipelineTest(BaseTestCase, PDRGeneratorTestMixin):
    def setUp(self):
        super(BiobankSamplesPipelineTest, self).setUp()
        config.override_setting(config.BASELINE_SAMPLE_TEST_CODES, _BASELINE_TESTS)
        # Everything is stored as a list, so override bucket name as a 1-element list.
        config.override_setting(config.BIOBANK_SAMPLES_BUCKET_NAME, [_FAKE_BUCKET])
        self.participant_dao = ParticipantDao()
        self.summary_dao = ParticipantSummaryDao()

        config.override_setting(BIOBANK_SAMPLES_DAILY_INVENTORY_FILE_PATTERN, 'Sample Inventory Report v1')
        config.override_setting(BIOBANK_SAMPLES_MONTHLY_INVENTORY_FILE_PATTERN, 'Sample Inventory Report 60d')

        self.withdrawal_questionnaire = None

    mock_bucket_paths = [_FAKE_BUCKET, _FAKE_BUCKET + os.sep + biobank_samples_pipeline._REPORT_SUBDIR]

    def _write_cloud_csv(self, file_name, contents_str):
        with open_cloud_file("/%s/%s" % (_FAKE_BUCKET, file_name), mode='wb') as cloud_file:
            cloud_file.write(contents_str.encode("utf-8"))

    def _make_biobank_order(self, **kwargs):
        """Makes a new BiobankOrder (same values every time) with valid/complete defaults.

    Kwargs pass through to BiobankOrder constructor, overriding defaults.
    """
        participantId = kwargs["participantId"]
        modified = datetime(2019, 0o3, 25, 15, 59, 30)

        for k, default_value in (
            ("biobankOrderId", "1"),
            ("created", clock.CLOCK.now()),
            # ('participantId', self.participant.participantId),
            ("sourceSiteId", 1),
            ("sourceUsername", "fred@pmi-ops.org"),
            ("collectedSiteId", 1),
            ("collectedUsername", "joe@pmi-ops.org"),
            ("processedSiteId", 1),
            ("processedUsername", "sue@pmi-ops.org"),
            ("finalizedSiteId", 2),
            ("finalizedUsername", "bob@pmi-ops.org"),
            ("version", 1),
            ("identifiers", [BiobankOrderIdentifier(system="a", value="c")]),
            ("samples", [BiobankOrderedSample(test="1SAL2", description="description", processingRequired=True)]),
            ("mailKitOrders", [BiobankMailKitOrder(participantId=participantId, modified=modified, version=1)]),
        ):
            if k not in kwargs:
                kwargs[k] = default_value
        return BiobankOrder(**kwargs)

    def test_dv_order_sample_update(self):
        """
        Test Biobank Direct Volunteer order
        """
        self.clear_default_storage()
        self.create_mock_buckets(self.mock_bucket_paths)
        participant = self.participant_dao.insert(Participant())
        self.summary_dao.insert(self.participant_summary(participant))

        created_ts = datetime(2019, 0o3, 22, 18, 30, 45)
        confirmed_ts = datetime(2019, 0o3, 23, 12, 13, 00)

        bo = self._make_biobank_order(participantId=participant.participantId)
        BiobankOrderDao().insert(bo)

        boi = bo.identifiers[0]

        bss = BiobankStoredSample(
            biobankStoredSampleId="23523523",
            biobankId=participant.biobankId,
            test="1SAL2",
            created=created_ts,
            biobankOrderIdentifier=boi.value,
            confirmed=confirmed_ts,
        )

        with self.participant_dao.session() as session:
            session.add(bss)

        ps = self.summary_dao.get(participant.participantId)
        self.assertIsNone(ps.sampleStatusDV1SAL2)
        self.assertIsNone(ps.sampleStatusDV1SAL2Time)

        self.summary_dao.update_from_biobank_stored_samples()
        ps = self.summary_dao.get(participant.participantId)
        self.assertEqual(ps.sampleStatus1SAL2, SampleStatus.RECEIVED)
        self.assertEqual(ps.sampleStatus1SAL2Time, confirmed_ts)

    def test_core_participants_stay_core(self):
        self.clear_default_storage()
        self.create_mock_buckets(self.mock_bucket_paths)
        participant = self.participant_dao.insert(Participant())
        self.summary_dao.insert(self.participant_summary(participant))

        with self.summary_dao.session() as session:
            participant_summary = session.query(ParticipantSummary).filter(
                ParticipantSummary.participantId == participant.participantId
            ).one()
            participant_summary.enrollmentStatus = EnrollmentStatus.FULL_PARTICIPANT

        # This updates all participants, regardless of whether they have samples imported or not
        self.summary_dao.update_from_biobank_stored_samples()

        ps = self.summary_dao.get(participant.participantId)
        self.assertEqual(EnrollmentStatus.FULL_PARTICIPANT, ps.enrollmentStatus)

    def test_end_to_end(self):
        config.override_setting(BIOBANK_SAMPLES_DAILY_INVENTORY_FILE_PATTERN, 'cloud')

        self.clear_default_storage()
        self.create_mock_buckets(self.mock_bucket_paths)
        dao = BiobankStoredSampleDao()
        self.assertEqual(dao.count(), 0)

        # Create 3 participants and pass their (random) IDs into sample rows.
        summary_dao = ParticipantSummaryDao()
        biobank_ids = []
        participant_ids = []
        nids = 16  # equal to the number of parent rows in 'biobank_samples_1.csv'
        cids = 1  # equal to the number of child rows in 'biobank_samples_1.csv'

        for _ in range(nids):
            participant = self.participant_dao.insert(Participant())
            summary_dao.insert(self.participant_summary(participant))
            participant_ids.append(participant.participantId)
            biobank_ids.append(participant.biobankId)
            self.assertEqual(summary_dao.get(participant.participantId).numBaselineSamplesArrived, 0)

        test_codes = random.sample(_BASELINE_TESTS, nids)

        # Arbitrarily pick samples to be used for testing 1SAL2 collection method checking
        mail_kit_1sal2_participant_id = participant_ids[6]
        on_site_1sal2_participant_id = participant_ids[11]
        no_order_1sal2_participant_id = participant_ids[14]
        test_codes[6] = test_codes[11] = test_codes[14] = '1SAL2'

        mailed_biobank_order = self.data_generator.create_database_biobank_order(
            participantId=mail_kit_1sal2_participant_id
        )
        self.data_generator.create_database_biobank_order_identifier(
            biobankOrderId=mailed_biobank_order.biobankOrderId,
            value='KIT-6'  # from the 7th record in biobank_samples_1.csv
        )
        self.data_generator.create_database_biobank_mail_kit_order(
            biobankOrderId=mailed_biobank_order.biobankOrderId,
            participantId=mail_kit_1sal2_participant_id
        )

        on_site_biobank_order = self.data_generator.create_database_biobank_order(
            participantId=on_site_1sal2_participant_id
        )
        self.data_generator.create_database_biobank_order_identifier(
            biobankOrderId=on_site_biobank_order.biobankOrderId,
            value='KIT-11'  # from the 12th record in the biobank_samples_1.csv
        )

        samples_file = test_data.open_biobank_samples(biobank_ids=biobank_ids, tests=test_codes)
        lines = samples_file.split("\n")[1:]  # remove field name line

        input_filename = "cloud%s.csv" % self._naive_utc_to_naive_central(clock.CLOCK.now()).strftime(
            biobank_samples_pipeline.INPUT_CSV_TIME_FORMAT
        )
        self._write_cloud_csv(input_filename, samples_file)
        biobank_samples_pipeline.upsert_from_latest_csv()

        self.assertEqual(dao.count(), nids - cids)

        for x in range(0, nids):
            cols = lines[x].split("\t")

            if cols[10].strip():  # skip child sample
                continue

            # If status is 'In Prep', then sample confirmed timestamp should be empty
            if cols[2] == "In Prep":
                self.assertEqual(len(cols[11]), 0)
            else:
                status = SampleStatus.RECEIVED
                ts_str = cols[11]
                # DA-814 - Participant Summary test status should be: Unset, Received or Disposed only.
                # If sample is disposed, then check disposed timestamp, otherwise check confirmed timestamp.
                # DA-871 - Only check status is disposed when reason code is a bad disposal.
                if cols[2] == "Disposed" and get_sample_status_enum_value(cols[8]) > SampleStatus.UNKNOWN:
                    status = SampleStatus.DISPOSED
                    ts_str = cols[9]

                ts = datetime.strptime(ts_str, "%Y/%m/%d %H:%M:%S")
                self._check_summary(participant_ids[x], test_codes[x], ts, status)

        # Check that the 1SAL2 collection methods were set correctly
        on_site_summary: ParticipantSummary = self.session.query(ParticipantSummary).filter(
            ParticipantSummary.participantId == on_site_1sal2_participant_id
        ).one()
        self.assertEqual(SampleCollectionMethod.ON_SITE, on_site_summary.sample1SAL2CollectionMethod)

        mail_kit_summary: ParticipantSummary = self.session.query(ParticipantSummary).filter(
            ParticipantSummary.participantId == mail_kit_1sal2_participant_id
        ).one()
        self.assertEqual(SampleCollectionMethod.MAIL_KIT, mail_kit_summary.sample1SAL2CollectionMethod)

        no_order_summary: ParticipantSummary = self.session.query(ParticipantSummary).filter(
            ParticipantSummary.participantId == no_order_1sal2_participant_id
        ).one()
        self.assertIsNone(no_order_summary.sample1SAL2CollectionMethod)

    def test_old_csv_not_imported(self):
        self.clear_default_storage()
        self.create_mock_buckets(self.mock_bucket_paths)
        now = clock.CLOCK.now()
        too_old_time = now - timedelta(hours=25)
        input_filename = "cloud%s.csv" % self._naive_utc_to_naive_central(too_old_time).strftime(
            biobank_samples_pipeline.INPUT_CSV_TIME_FORMAT
        )
        self._write_cloud_csv(input_filename, "")
        with self.assertRaises(biobank_samples_pipeline.DataError):
            biobank_samples_pipeline.upsert_from_latest_csv()

    def _naive_utc_to_naive_central(self, naive_utc_date):
        utc_date = pytz.utc.localize(naive_utc_date)
        central_date = utc_date.astimezone(pytz.timezone("US/Central"))
        return central_date.replace(tzinfo=None)

    def _check_summary(self, participant_id, test, date_formatted, status):
        summary = ParticipantSummaryDao().get(participant_id)
        self.assertEqual(summary.numBaselineSamplesArrived, 1)
        # DA-614 - All specific disposal statuses in biobank_stored_samples are changed to DISPOSED
        # in the participant summary.
        self.assertEqual(status, getattr(summary, "sampleStatus" + test))
        sample_time = self._naive_utc_to_naive_central(getattr(summary, "sampleStatus" + test + "Time"))
        self.assertEqual(date_formatted, sample_time)

    @mock.patch('rdr_service.offline.biobank_samples_pipeline.list_blobs')
    def test_find_latest_csv(self, mock_list_blobs):
        mock_list_blobs.return_value = [
            FakeBlob(name='Sample Inventory Report v12020-06-01.csv',  # older file
                     updated=datetime(2020, 6, 1)),
            FakeBlob(name='Sample Inventory Report v12020-07-14.csv',  # last inventory file (should use this one)
                     updated=datetime(2020, 7, 14)),
            FakeBlob(name='not an inventory file v12020-08-01.csv',  # not the correct name pattern
                     updated=datetime(2020, 8, 1)),
            FakeBlob(name='60_day_manifests/Sample Inventory Report 60d2020-08-02-04-00-21.csv',  # 60 day manifest
                     updated=datetime(2020, 8, 2)),
            FakeBlob(name='genomic_samples_manifests/Genomic-Manifest-AoU-4-2020-06-30-08-22-52_C2.CSV',  # genomic file
                     updated=datetime(2020, 8, 3)),
            FakeBlob(name='Sample Inventory Report v12020-08-04',  # not a csv
                     updated=datetime(2020, 8, 4)),
        ]  # todo: make sure file names that get listed by google actually look like this

        latest_filename = biobank_samples_pipeline._find_latest_samples_csv(_FAKE_BUCKET)
        self.assertEqual('Sample Inventory Report v12020-07-14.csv', latest_filename)

    @mock.patch('rdr_service.offline.biobank_samples_pipeline.list_blobs')
    def test_find_latest_csv(self, mock_list_blobs):
        mock_list_blobs.return_value = [
            FakeBlob(name='60_day_manifests/Sample Inventory Report 60d2020-06-01-04-00-21.csv',  # older file
                     updated=datetime(2020, 6, 1)),
            FakeBlob(name='60_day_manifests/Sample Inventory Report 60d2020-07-14-04-00-21.csv',  # current manifest
                     updated=datetime(2020, 7, 14)),
            FakeBlob(name='not an inventory file 60d2020-08-01.csv',  # not the correct name pattern
                     updated=datetime(2020, 8, 1)),
            FakeBlob(name='Sample Inventory Report v12020-08-02.csv',  # single day inventory file
                     updated=datetime(2020, 8, 2)),
            FakeBlob(name='genomic_samples_manifests/Genomic-Manifest-AoU-4-2020-06-30-08-22-52_C2.CSV',  # genomic file
                     updated=datetime(2020, 8, 3)),
            FakeBlob(name='60_day_manifests/Sample Inventory Report 60d2020-08-04',  # not a csv
                     updated=datetime(2020, 8, 4)),
        ]  # todo: make sure file names that get listed by google actually look like this

        latest_filename = biobank_samples_pipeline._find_latest_samples_csv(_FAKE_BUCKET, monthly=True)
        self.assertEqual('60_day_manifests/Sample Inventory Report 60d2020-07-14-04-00-21.csv', latest_filename)

    def test_sample_from_row(self):
        self.clear_default_storage()
        self.create_mock_buckets(self.mock_bucket_paths)
        samples_file = test_data.open_biobank_samples([112, 222, 333], [])
        reader = csv.DictReader(io.StringIO(samples_file), delimiter="\t")
        row = next(reader)
        sample = biobank_samples_pipeline._create_sample_from_row(row, get_biobank_id_prefix())
        self.assertIsNotNone(sample)

        cols = biobank_samples_pipeline.CsvColumns
        self.assertEqual(sample.biobankStoredSampleId, row[cols.SAMPLE_ID])
        self.assertEqual(to_client_biobank_id(sample.biobankId), row[cols.EXTERNAL_PARTICIPANT_ID])
        self.assertEqual(sample.test, row[cols.TEST_CODE])
        confirmed_date = self._naive_utc_to_naive_central(sample.confirmed)
        self.assertEqual(
            confirmed_date.strftime(biobank_samples_pipeline._INPUT_TIMESTAMP_FORMAT), row[cols.CONFIRMED_DATE]
        )
        received_date = self._naive_utc_to_naive_central(sample.created)
        self.assertEqual(
            received_date.strftime(biobank_samples_pipeline._INPUT_TIMESTAMP_FORMAT), row[cols.CREATE_DATE]
        )

    def test_sample_from_row_wrong_prefix(self):
        self.clear_default_storage()
        self.create_mock_buckets(self.mock_bucket_paths)
        samples_file = test_data.open_biobank_samples([111, 222, 333], [])
        reader = csv.DictReader(io.StringIO(samples_file), delimiter="\t")
        row = next(reader)
        row[biobank_samples_pipeline.CsvColumns.CONFIRMED_DATE] = "2016 11 19"
        self.assertIsNone(biobank_samples_pipeline._create_sample_from_row(row, "Q"))

    def test_sample_from_row_invalid(self):
        self.clear_default_storage()
        self.create_mock_buckets(self.mock_bucket_paths)
        samples_file = test_data.open_biobank_samples([111, 222, 333], [])
        reader = csv.DictReader(io.StringIO(samples_file), delimiter="\t")
        row = next(reader)
        row[biobank_samples_pipeline.CsvColumns.CONFIRMED_DATE] = "2016 11 19"
        with self.assertRaises(biobank_samples_pipeline.DataError):
            biobank_samples_pipeline._create_sample_from_row(row, get_biobank_id_prefix())

    def test_sample_from_row_old_test(self):
        self.clear_default_storage()
        self.create_mock_buckets(self.mock_bucket_paths)
        samples_file = test_data.open_biobank_samples([111, 222, 333], [])
        reader = csv.DictReader(io.StringIO(samples_file), delimiter="\t")
        row = next(reader)
        row[biobank_samples_pipeline.CsvColumns.TEST_CODE] = "2PST8"
        sample = biobank_samples_pipeline._create_sample_from_row(row, get_biobank_id_prefix())
        self.assertIsNotNone(sample)
        cols = biobank_samples_pipeline.CsvColumns
        self.assertEqual(sample.biobankStoredSampleId, row[cols.SAMPLE_ID])
        self.assertEqual(sample.test, row[cols.TEST_CODE])

    def test_column_missing(self):
        self.clear_default_storage()
        self.create_mock_buckets(self.mock_bucket_paths)
        with open(test_data.data_path("biobank_samples_missing_field.csv")) as samples_file:
            reader = csv.DictReader(samples_file, delimiter="\t")
            with self.assertRaises(biobank_samples_pipeline.DataError):
                biobank_samples_pipeline._upsert_samples_from_csv(reader)

    def test_wrong_csv_delimiter(self):
        self.clear_default_storage()
        self.create_mock_buckets(self.mock_bucket_paths)
        # Use a valid file containing commas as separators
        with open(test_data.data_path("biobank_samples_wrong_delimiter.csv")) as samples_file:
            reader = csv.DictReader(samples_file, delimiter="\t")
            with self.assertRaises(biobank_samples_pipeline.DataError):
                biobank_samples_pipeline._upsert_samples_from_csv(reader)

    def test_get_reconciliation_report_paths(self):
        self.clear_default_storage()
        self.create_mock_buckets(self.mock_bucket_paths)
        dt = datetime(2016, 12, 22, 18, 30, 45)
        expected_prefix = "reconciliation/report_2016-12-22"
        paths = biobank_samples_pipeline._get_report_paths(dt)
        self.assertEqual(len(paths), 5)
        for path in paths:
            self.assertTrue(
                path.startswith(expected_prefix), "Report path %r must start with %r." % (expected_prefix, path)
            )
            self.assertTrue(path.endswith(".csv"))

    def _init_report_codes(self):
        self.race_question_code = self.data_generator.create_database_code(value=RACE_QUESTION_CODE)
        self.native_answer_code = self.data_generator.create_database_code(value=RACE_AIAN_CODE)

        self.ceremony_question_code = self.data_generator.create_database_code(value=WITHDRAWAL_CEREMONY_QUESTION_CODE)
        self.ceremony_yes_answer_code = self.data_generator.create_database_code(value=WITHDRAWAL_CEREMONY_YES)
        self.ceremony_no_answer_code = self.data_generator.create_database_code(value=WITHDRAWAL_CEREMONY_NO)

    @staticmethod
    def _datetime_days_ago(num_days_ago):
        today = datetime.today().replace(hour=0, minute=0, second=0, microsecond=0)
        return today - timedelta(days=num_days_ago)

    @staticmethod
    def _format_datetime(timestamp):
        return timestamp.strftime('%Y-%m-%dT%H:%M:%SZ')

    def test_quest_samples_in_report(self):
        self._init_report_codes()

        # Generate data for a Quest sample to be in the report
        participant = self.data_generator.create_database_participant()
        order = self.data_generator.create_database_biobank_order(participantId=participant.participantId)

        # Setup order identifiers that CE would send for Quest order
        self.data_generator.create_database_biobank_order_identifier(
            biobankOrderId=order.biobankOrderId,
            value='aoeu-1234',  # CE CareTask system doesn't use the KIT numbers for identifiers
            system=biobank_samples_pipeline._CE_QUEST_SYSTEM
        )
        kit_order_identifier = self.data_generator.create_database_biobank_order_identifier(
            biobankOrderId=order.biobankOrderId,
            value='KIT-001',
            system=biobank_samples_pipeline._KIT_ID_SYSTEM
        )

        ordered_sample = self.data_generator.create_database_biobank_ordered_sample(
            biobankOrderId=order.biobankOrderId,
            collected=datetime(2020, 6, 5),
            processed=datetime(2020, 6, 6),
            finalized=datetime(2020, 6, 7)
        )
        stored_sample = self.data_generator.create_database_biobank_stored_sample(
            test=ordered_sample.test,
            biobankId=participant.biobankId,
            biobankOrderIdentifier=kit_order_identifier.value,
            confirmed=self._datetime_days_ago(7)
        )

        # Mocking the file writer to catch what gets exported and mocking the upload method because
        # that isn't what this test is meant to cover
        with mock.patch('rdr_service.offline.sql_exporter.csv.writer') as mock_writer_class,\
                mock.patch('rdr_service.offline.sql_exporter.SqlExporter.upload_export_file'):
            biobank_samples_pipeline.write_reconciliation_report(datetime.now())

            mock_write_rows = mock_writer_class.return_value.writerows
            mock_write_rows.assert_called_once_with([(
                f'Z{participant.biobankId}',
                ordered_sample.test,
                Decimal('1'),  # sent count
                kit_order_identifier.value,
                self._format_datetime(ordered_sample.collected),
                self._format_datetime(ordered_sample.processed),
                self._format_datetime(ordered_sample.finalized),
                None, None, None, 'UNKNOWN',  # site info: name, client_number, hpo, hpo_type
                None, None, None, 'UNKNOWN',  # finalized site info: name, client_number, hpo, hpo_type
                None,  # finalized username
                ordered_sample.test,
                1,  # received count
                str(stored_sample.biobankStoredSampleId),
                self._format_datetime(stored_sample.confirmed),  # received time
                None,  # created family date
                None,  # elapsed hours
                'KIT-001',  # kit identifier
                None,  # fedex tracking number
                'N',  # is Native American
                None, None, None,  # notes info: collected, processed, finalized
                None, None, None, None, None,  # cancelled_restored info: status_flag, name, name, time, reason
                None,  # order origin
                'example'  # Participant origin
            )])

    def _get_questionnaire(self):
        if self.withdrawal_questionnaire is None:
            race_question = self.data_generator.create_database_questionnaire_question(
                codeId=self.race_question_code.codeId
            )
            ceremony_question = self.data_generator.create_database_questionnaire_question(
                codeId=self.ceremony_question_code.codeId
            )
            self.withdrawal_questionnaire = self.data_generator.create_database_questionnaire_history(
                # As of writing this, the pipeline only checks for the answers, regardless of questionnaire
                # so putting them in the same questionnaire for convenience of the test code
                questions=[race_question, ceremony_question]
            )

        return self.withdrawal_questionnaire

    def _create_participant(self, is_native_american=False, requests_ceremony=None, withdrawal_time=datetime.utcnow()):
        participant = self.data_generator.create_database_participant(
            withdrawalTime=withdrawal_time
        )

        # Withdrawal report only includes participants that have stored samples
        self.data_generator.create_database_biobank_stored_sample(biobankId=participant.biobankId, test='test')

        # Create a questionnaire response that satisfies the parameters for the test participant
        questionnaire = self._get_questionnaire()
        answers = []
        for question in questionnaire.questions:
            answer_code_id = None
            if question.codeId == self.race_question_code.codeId and is_native_american:
                answer_code_id = self.native_answer_code.codeId
            elif question.codeId == self.ceremony_question_code.codeId and requests_ceremony:
                if requests_ceremony == WithdrawalAIANCeremonyStatus.REQUESTED:
                    answer_code_id = self.ceremony_yes_answer_code.codeId
                elif requests_ceremony == WithdrawalAIANCeremonyStatus.DECLINED:
                    answer_code_id = self.ceremony_no_answer_code.codeId

            if answer_code_id:
                answers.append(QuestionnaireResponseAnswer(
                    questionId=question.questionnaireQuestionId,
                    valueCodeId=answer_code_id
                ))
        self.data_generator.create_database_questionnaire_response(
            questionnaireId=questionnaire.questionnaireId,
            questionnaireVersion=questionnaire.version,
            answers=answers,
            participantId=participant.participantId
        )

        return participant

    def assert_participant_in_report_rows(self, participant: Participant, rows, withdrawal_date_str,
                                          as_native_american: bool = False, needs_ceremony_indicator: str = 'NA'):
        self.assertIn((
            f'Z{participant.biobankId}',
            withdrawal_date_str,
            'Y' if as_native_american else 'N',
            needs_ceremony_indicator,
            participant.participantOrigin
        ), rows)

    def _generate_withdrawal_report(self):
        with mock.patch('rdr_service.offline.sql_exporter.csv.writer') as mock_writer_class,\
                mock.patch('rdr_service.offline.sql_exporter.SqlExporter.upload_export_file'):

            # Generate the withdrawal report
            day_range_of_report = 10
            biobank_samples_pipeline._query_and_write_withdrawal_report(
                SqlExporter(''), '', day_range_of_report, datetime.now()
            )

            # Check the header values written
            mock_writer_class.return_value.writerow.assert_called_with(
                ['biobank_id', 'withdrawal_time', 'is_native_american', 'needs_disposal_ceremony', 'participant_origin']
            )

            mock_write_rows = mock_writer_class.return_value.writerows
            return mock_write_rows.call_args[0][0] if mock_write_rows.called else []

    def test_native_american_data_in_withdrawal_report(self):
        # Set up data for withdrawal report
        self._init_report_codes()
        two_days_ago = self._datetime_days_ago(2)
        no_ceremony_native_american_participant = self._create_participant(
            is_native_american=True,
            requests_ceremony=WithdrawalAIANCeremonyStatus.DECLINED,
            withdrawal_time=two_days_ago
        )
        ceremony_native_american_participant = self._create_participant(
            is_native_american=True,
            requests_ceremony=WithdrawalAIANCeremonyStatus.REQUESTED,
            withdrawal_time=two_days_ago
        )
        native_american_participant_without_answer = self._create_participant(
            is_native_american=True,
            requests_ceremony=None,
            withdrawal_time=two_days_ago
        )
        # Non-AIAN should not have been presented with a ceremony choice
        non_native_american_participant = self._create_participant(
            is_native_american=False,
            requests_ceremony=None,
            withdrawal_time=two_days_ago
        )


        # Check that the participants are written to the export with the expected values
        withdrawal_iso_str = two_days_ago.strftime('%Y-%m-%dT%H:%M:%SZ')
        rows_written = self._generate_withdrawal_report()
        self.assert_participant_in_report_rows(
            non_native_american_participant,
            rows_written,
            withdrawal_iso_str,
            as_native_american=False,
            needs_ceremony_indicator='NA'
        )
        self.assert_participant_in_report_rows(
            ceremony_native_american_participant,
            rows_written,
            withdrawal_iso_str,
            as_native_american=True,
            needs_ceremony_indicator='Y'
        )
        self.assert_participant_in_report_rows(
            native_american_participant_without_answer,
            rows_written,
            withdrawal_iso_str,
            as_native_american=True,
            needs_ceremony_indicator='U'
        )
        self.assert_participant_in_report_rows(
            no_ceremony_native_american_participant,
            rows_written,
            withdrawal_iso_str,
            as_native_american=True,
            needs_ceremony_indicator='N'
        )

        p_id = no_ceremony_native_american_participant.participantId
        ps_bqs_data = self.make_bq_participant_summary(p_id)
        self.assertEqual(ps_bqs_data.get('withdrawal_aian_ceremony_status'),
                         str(WithdrawalAIANCeremonyStatus.DECLINED))
        self.assertEqual(ps_bqs_data.get('withdrawal_aian_ceremony_status_id'),
                         int(WithdrawalAIANCeremonyStatus.DECLINED))

        ps_rsrc_data = self.make_participant_resource(p_id)
        self.assertEqual(ps_rsrc_data.get('withdrawal_aian_ceremony_status'),
                         str(WithdrawalAIANCeremonyStatus.DECLINED))
        self.assertEqual(ps_rsrc_data.get('withdrawal_aian_ceremony_status_id'),
                         int(WithdrawalAIANCeremonyStatus.DECLINED))

        p_id = ceremony_native_american_participant.participantId
        ps_bqs_data = self.make_bq_participant_summary(p_id)
        self.assertEqual(ps_bqs_data.get('withdrawal_aian_ceremony_status'),
                         str(WithdrawalAIANCeremonyStatus.REQUESTED))
        self.assertEqual(ps_bqs_data.get('withdrawal_aian_ceremony_status_id'),
                         int(WithdrawalAIANCeremonyStatus.REQUESTED))

        ps_rsrc_data = self.make_participant_resource(p_id)
        self.assertEqual(ps_rsrc_data.get('withdrawal_aian_ceremony_status'),
                         str(WithdrawalAIANCeremonyStatus.REQUESTED))
        self.assertEqual(ps_rsrc_data.get('withdrawal_aian_ceremony_status_id'),
                         int(WithdrawalAIANCeremonyStatus.REQUESTED))

        p_id = non_native_american_participant.participantId
        ps_bqs_data = self.make_bq_participant_summary(p_id)
        self.assertEqual(ps_bqs_data.get('withdrawal_aian_ceremony_status'),
                         str(WithdrawalAIANCeremonyStatus.UNSET))
        self.assertEqual(ps_bqs_data.get('withdrawal_aian_ceremony_status_id'),
                         int(WithdrawalAIANCeremonyStatus.UNSET))

        ps_rsrc_data = self.make_participant_resource(p_id)
        self.assertEqual(ps_rsrc_data.get('withdrawal_aian_ceremony_status'),
                         str(WithdrawalAIANCeremonyStatus.UNSET))
        self.assertEqual(ps_rsrc_data.get('withdrawal_aian_ceremony_status_id'),
                         int(WithdrawalAIANCeremonyStatus.UNSET))

    def test_withdrawal_report_includes_participants_with_recent_samples(self):
        """
        Occasionally a participant will send a saliva kit and then immediately withdraw. In this scenario
        they would never be on a withdrawal manifest because they didn't have any samples until after the
        10-day window.
        """
        self._init_report_codes()
        twenty_days_ago = self._datetime_days_ago(20)
        five_days_ago = self._datetime_days_ago(5)

        # Create a participant that has a withdrawal time outside of the report range, but recently had a sample created
        withdrawn_participant = self.data_generator.create_database_participant(
            withdrawalTime=twenty_days_ago,
            withdrawalStatus=WithdrawalStatus.NO_USE
        )
        self.data_generator.create_database_biobank_stored_sample(
            biobankId=withdrawn_participant.biobankId,
            created=five_days_ago
        )

        from tests.helpers.diagnostics import LoggingDatabaseActivity
        with LoggingDatabaseActivity():
            rows_written = self._generate_withdrawal_report()
        self.assert_participant_in_report_rows(
            withdrawn_participant,
            rows=rows_written,
            withdrawal_date_str=twenty_days_ago.strftime('%Y-%m-%dT%H:%M:%SZ')
        )
