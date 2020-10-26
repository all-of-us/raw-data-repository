from datetime import date, datetime, timedelta
import mock

from rdr_service.offline.import_deceased_reports import DeceasedReportImporter, PROJECT_TOKEN_CONFIG_KEY
from rdr_service.model.deceased_report import DeceasedReport
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
                                       report_death_date=None, survey_completion_datetime='2020-08-12 10:32:21',
                                       redcap_record_id=1):
        """Build out a record that matches the codes we're expecting from redcap"""
        return {
            'record_id': redcap_record_id,
            'recordid': participant_id,
            'death_date': date_of_death,
            'death_cause': cause_of_death,
            'reportperson_type': notification,
            'reportperson_firstname': reporter_first_name,
            'reportperson_lastname': reporter_last_name,
            'reportperson_relationship': reporter_relationship,
            'reportperson_email': reporter_email,
            'reportperson_phone': reporter_phone,
            'reportdeath_date': report_death_date,
            'reportdeath_identityconfirm': identity_confirmed,
            'reported_death_timestamp': survey_completion_datetime
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
                notification=1,  # OTHER notification
                reporter_email='reportauthor@test.com',
                report_death_date='2020-01-03 12:31'
            ),
            self._redcap_deceased_report_record(
                participant_id=paired_participant.participantId,
                notification=2,  # NEXT_KIN_SUPPORT notification
                reporter_first_name='Jane',
                reporter_last_name='Doe',
                reporter_relationship=2,  # CHILD
                reporter_email='jdoe@test.com',
                reporter_phone='1234567890',
                survey_completion_datetime='2020-10-31 01:27:45'
            )
        ]

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
        self.assertEqual(datetime(2020, 10, 31, 1, 27, 45), pending_report.authored)
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
                notification=1,  # OTHER notification
                reporter_email='reportauthor@test.com',
                identity_confirmed=False
            ),
            self._redcap_deceased_report_record(
                participant_id=participant.participantId,
                notification=1,  # OTHER notification
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
                redcap_record_id=1,
                participant_id=1234,
                notification=1,  # OTHER notification
                reporter_email='reportauthor@test.com'
            ),
            self._redcap_deceased_report_record(                        # No reporter name
                redcap_record_id=2,
                participant_id=participant.participantId,
                notification=2,  # NEXT_KIN_SUPPORT notification
            ),
            self._redcap_deceased_report_record(                        # Missing reporter relationship
                redcap_record_id=3,
                participant_id=participant.participantId,
                notification=2,  # NEXT_KIN_SUPPORT notification
                reporter_first_name='Jane',
                reporter_last_name='Doe',
            )
        ]

        self.importer.import_reports()
        print(mock_logging)
        mock_logging.error.assert_has_calls([
            mock.call('Record 1 encountered an error', exc_info=True),
            mock.call('Record 2 encountered an error', exc_info=True),
            mock.call('Record 3 encountered an error', exc_info=True)
        ])

    @mock.patch('rdr_service.offline.import_deceased_reports.logging')
    def test_unknown_notification(self, mock_logging, redcap_class):
        participant = self.data_generator.create_database_participant()

        redcap_class.return_value.get_records.return_value = [
            self._redcap_deceased_report_record(
                redcap_record_id=4,
                participant_id=participant.participantId,
                notification=19
            )
        ]

        self.importer.import_reports()
        mock_logging.error.assert_called_with('Record 4 has an unrecognized notification: "19"')

    def test_blank_email_with_other_notification(self, redcap_class):
        participant = self.data_generator.create_database_participant()

        redcap_class.return_value.get_records.return_value = [
            self._redcap_deceased_report_record(
                participant_id=participant.participantId,
                notification=1  # OTHER notification
            )
        ]

        self.importer.import_reports()

        pending_report: DeceasedReport = self.session.query(DeceasedReport).filter(
            DeceasedReport.participantId == participant.participantId
        ).one()
        self.assertEqual('scstaff@pmi-ops.org', pending_report.author.username)

    def test_report_records_are_created(self):
        pass

    def test_reports_not_imported_again(self):
        pass
