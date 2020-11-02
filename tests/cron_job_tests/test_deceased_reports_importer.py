from datetime import date, datetime, timedelta
import mock

from rdr_service.clock import FakeClock
from rdr_service.offline.import_deceased_reports import DeceasedReportImporter, PROJECT_TOKEN_CONFIG_KEY
from rdr_service.model.deceased_report import DeceasedReport
from rdr_service.model.deceased_report_import_record import DeceasedReportImportRecord
from rdr_service.model.utils import to_client_participant_id
from rdr_service.participant_enums import DeceasedNotification, DeceasedReportStatus
from tests.helpers.unittest_base import BaseTestCase


@mock.patch('rdr_service.offline.import_deceased_reports.RedcapClient')
class DeceasedReportImporterTest(BaseTestCase):
    def setUp(self, **kwargs):
        super(DeceasedReportImporterTest, self).setUp(**kwargs)

        self.test_api_key = '123ABC'
        self.importer = DeceasedReportImporter({
            PROJECT_TOKEN_CONFIG_KEY: self.test_api_key
        })

    @staticmethod
    def _redcap_deceased_report_record(participant_id, notification, identity_confirmed=True, date_of_death=None,
                                       cause_of_death=None, reporter_first_name=None, reporter_last_name=None,
                                       reporter_relationship=None, reporter_email=None, reporter_phone=None,
                                       report_death_date=None):
        """Build out a record that matches the codes we're expecting from redcap"""
        return {
            'recordid': to_client_participant_id(participant_id),
            'death_date': date_of_death,
            'death_cause': cause_of_death,
            'reportperson_type': notification,
            'reportperson_firstname': reporter_first_name,
            'reportperson_lastname': reporter_last_name,
            'reportperson_relationship': reporter_relationship,
            'reportperson_email': reporter_email,
            'reportperson_phone': reporter_phone,
            'reportdeath_date': report_death_date,
            'reportdeath_identityconfirm': identity_confirmed
        }

    def test_report_import_uses_key_and_default_date_range(self, redcap_class):
        """Test the parameters that the importer uses for Redcap"""

        # The importer will import everything from the start of yesterday by default
        now_yesterday = datetime.now() - timedelta(days=1)
        start_of_yesterday = datetime(now_yesterday.year, now_yesterday.month, now_yesterday.day)

        self.importer.import_reports()

        redcap_instance = redcap_class.return_value
        redcap_instance.get_records.assert_called_with(self.test_api_key, start_of_yesterday)

    def test_deceased_reports_created(self, redcap_class):
        unpaired_participant = self.data_generator.create_database_participant()
        paired_participant = self.data_generator.create_database_participant(
            hpoId=self.data_generator.create_database_hpo().hpoId
        )

        redcap_class.return_value.get_records.return_value = [
            self._redcap_deceased_report_record(
                participant_id=unpaired_participant.participantId,
                date_of_death='2020-01-01',
                cause_of_death='test',
                notification='1',                       # OTHER notification
                reporter_email='reportauthor@test.com',
                report_death_date='2020-01-03 12:31'
            ),
            self._redcap_deceased_report_record(
                participant_id=paired_participant.participantId,
                notification='2',                       # NEXT_KIN_SUPPORT notification
                reporter_first_name='Jane',
                reporter_last_name='Doe',
                reporter_relationship='2',              # CHILD
                reporter_email='jdoe@test.com',
                reporter_phone='1234567890'
            )
        ]

        import_datetime = datetime(2020, 10, 24, 1, 27, 45)
        with FakeClock(import_datetime):
            self.importer.import_reports()

        auto_approved_report: DeceasedReport = self.session.query(DeceasedReport).filter(
            DeceasedReport.participantId == unpaired_participant.participantId
        ).one()
        self.assertEqual(DeceasedReportStatus.APPROVED, auto_approved_report.status)
        self.assertEqual(date(2020, 1, 1), auto_approved_report.dateOfDeath)
        self.assertEqual(DeceasedNotification.OTHER, auto_approved_report.notification)
        self.assertEqual('HPO contacted support center before Sept. 2020', auto_approved_report.notificationOther)
        self.assertEqual('reportauthor@test.com', auto_approved_report.author.username)
        self.assertEqual(datetime(2020, 1, 3, 12, 31), auto_approved_report.authored)
        self.assertEqual(auto_approved_report.author.username, auto_approved_report.reviewer.username)
        self.assertEqual(auto_approved_report.authored, auto_approved_report.reviewed)
        self.assertEqual('test', auto_approved_report.causeOfDeath)

        pending_report: DeceasedReport = self.session.query(DeceasedReport).filter(
            DeceasedReport.participantId == paired_participant.participantId
        ).one()
        self.assertEqual(DeceasedReportStatus.PENDING, pending_report.status)
        self.assertIsNone(pending_report.dateOfDeath)
        self.assertEqual(DeceasedNotification.NEXT_KIN_SUPPORT, pending_report.notification)
        self.assertIsNone(pending_report.notificationOther)
        self.assertEqual('scstaff@pmi-ops.org', pending_report.author.username)
        self.assertEqual(import_datetime, pending_report.authored)
        self.assertIsNone(pending_report.reviewer)
        self.assertIsNone(pending_report.reviewed)
        self.assertEqual('Jane Doe', pending_report.reporterName)
        self.assertEqual('CHILD', pending_report.reporterRelationship)
        self.assertEqual('jdoe@test.com', pending_report.reporterEmail)
        self.assertEqual('1234567890', pending_report.reporterPhone)

    def test_unidentified_report_not_generated(self, redcap_class):
        """Make sure reports that don't have the identity confirmed are not created"""

        participant = self.data_generator.create_database_participant()
        unidentified_participant = self.data_generator.create_database_participant()

        redcap_class.return_value.get_records.return_value = [
            self._redcap_deceased_report_record(
                participant_id=unidentified_participant.participantId,
                notification='1',                       # OTHER notification
                reporter_email='reportauthor@test.com',
                identity_confirmed=False
            ),
            self._redcap_deceased_report_record(
                participant_id=participant.participantId,
                notification='1',                       # OTHER notification
                reporter_email='reportauthor@test.com',
                identity_confirmed=True
            )
        ]

        self.importer.import_reports()

        unidentified_report = self.session.query(DeceasedReport).filter(
            DeceasedReport.participantId == unidentified_participant.participantId
        ).one_or_none()
        self.assertIsNone(unidentified_report)

        # Making sure that the report was only ignored because the identity wasn't confirmed
        other_report = self.session.query(DeceasedReport).filter(
            DeceasedReport.participantId == participant.participantId
        ).one_or_none()
        self.assertIsNotNone(other_report)

    @mock.patch('rdr_service.offline.import_deceased_reports.logging')
    def test_that_validation_happens(self, mock_logging, redcap_class):
        participant = self.data_generator.create_database_participant()

        redcap_class.return_value.get_records.return_value = [
            self._redcap_deceased_report_record(                        # Unknown participant
                participant_id=1234,
                notification='1',                       # OTHER notification
                reporter_email='reportauthor@test.com'
            ),
            self._redcap_deceased_report_record(                        # No reporter name
                participant_id=participant.participantId,
                notification='2',                       # NEXT_KIN_SUPPORT notification
            ),
            self._redcap_deceased_report_record(                        # Missing reporter relationship
                participant_id=participant.participantId,
                notification='2',                       # NEXT_KIN_SUPPORT notification
                reporter_first_name='Jane',
                reporter_last_name='Doe',
            )
        ]

        self.importer.import_reports()
        print(mock_logging)
        mock_logging.error.assert_has_calls([
            mock.call('Record for 1234 encountered a database error', exc_info=True),
            mock.call(f'Record for {participant.participantId} encountered an error', exc_info=True),
            mock.call(f'Record for {participant.participantId} encountered an error', exc_info=True)
        ])

    @mock.patch('rdr_service.offline.import_deceased_reports.logging')
    def test_unknown_notification(self, mock_logging, redcap_class):
        participant = self.data_generator.create_database_participant()

        redcap_class.return_value.get_records.return_value = [
            self._redcap_deceased_report_record(
                participant_id=participant.participantId,
                notification='19'
            )
        ]

        self.importer.import_reports()
        mock_logging.error.assert_called_with(
            f'Record for {participant.participantId} has an unrecognized notification value: "19"'
        )

    def test_blank_email_with_other_notification(self, redcap_class):
        participant = self.data_generator.create_database_participant()

        redcap_class.return_value.get_records.return_value = [
            self._redcap_deceased_report_record(
                participant_id=participant.participantId,
                notification='1'                        # OTHER notification
            )
        ]

        self.importer.import_reports()

        pending_report: DeceasedReport = self.session.query(DeceasedReport).filter(
            DeceasedReport.participantId == participant.participantId
        ).one()
        self.assertEqual('scstaff@pmi-ops.org', pending_report.author.username)

    @mock.patch('rdr_service.offline.import_deceased_reports.logging')
    def test_bad_entry_for_date(self, mock_logging, redcap_class):
        participant = self.data_generator.create_database_participant()

        redcap_class.return_value.get_records.return_value = [
            self._redcap_deceased_report_record(
                participant_id=participant.participantId,
                notification='1',                       # OTHER notification
                report_death_date='2020-01-03 90:31'    # Datetime that can't be parsed
            ),
        ]

        self.importer.import_reports()
        mock_logging.error.assert_called_with(
            f'Record for {participant.participantId} encountered an error', exc_info=True
        )

    def test_report_records_are_created(self, redcap_class):
        """Checking that import records are created and have deceased reports attached when appropriate"""
        identified_participant = self.data_generator.create_database_participant()
        unidentified_participant = self.data_generator.create_database_participant()

        redcap_class.return_value.get_records.return_value = [
            self._redcap_deceased_report_record(
                participant_id=identified_participant.participantId,
                notification='1'                        # OTHER notification
            ),
            self._redcap_deceased_report_record(
                participant_id=unidentified_participant.participantId,
                notification='1',                       # OTHER notification,
                identity_confirmed=False
            )
        ]

        self.importer.import_reports()

        import_records = self.session.query(DeceasedReportImportRecord).all()

        # Make sure there's an import record for each record from redcap
        recorded_participant_ids = [import_record.participantId for import_record in import_records]
        self.assertIn(identified_participant.participantId, recorded_participant_ids)
        self.assertIn(unidentified_participant.participantId, recorded_participant_ids)

        # Check the linked reports
        for record in import_records:
            if record.participantId == identified_participant.participantId:
                # Make sure that the report was created
                self.assertIsNotNone(record.deceasedReport)
            elif record.participantId == unidentified_participant.participantId:
                # Make sure the report wasn't created
                self.assertIsNone(record.deceasedReport)

    def test_reports_not_imported_again(self, redcap_class):
        """Checking that redcap records are not imported twice, and that 'lastSeen' is updated"""
        identified_participant = self.data_generator.create_database_participant()
        unidentified_participant = self.data_generator.create_database_participant()

        redcap_class.return_value.get_records.return_value = [
            self._redcap_deceased_report_record(
                participant_id=identified_participant.participantId,
                notification='1'                        # OTHER notification
            ),
            self._redcap_deceased_report_record(
                participant_id=unidentified_participant.participantId,
                notification='1',                       # OTHER notification,
                identity_confirmed=False
            )
        ]

        first_import_time = datetime(2020, 10, 8)
        with FakeClock(first_import_time):
            self.importer.import_reports()

        # Setting deceased report as DENIED so that there are no active reports for the participant
        new_deceased_report = self.session.query(DeceasedReport).filter(
            DeceasedReport.participantId == identified_participant.participantId
        ).one()
        new_deceased_report.status = DeceasedReportStatus.DENIED
        self.session.commit()

        # Import from redcap again
        second_import_time = datetime(2020, 10, 10)
        with FakeClock(second_import_time):
            self.importer.import_reports()

        # Check that the valid redcap record doesn't create a new deceased report
        self.assertEqual(1, self.session.query(DeceasedReport).count(), 'There should only be one deceased report')

        import_records = self.session.query(DeceasedReportImportRecord).all()
        # Check the linked reports times
        for record in import_records:
            self.assertEqual(first_import_time, record.created)
            self.assertEqual(second_import_time, record.lastSeen)
