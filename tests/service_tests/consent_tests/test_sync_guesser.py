from datetime import datetime

from rdr_service.model.consent_file import ConsentFile
from rdr_service.model.participant_summary import ParticipantSummary
from rdr_service.offline.sync_consent_files import ConsentSyncGuesser, PairingHistoryRecord
from tests.helpers.unittest_base import BaseTestCase


class SyncGuesserTest(BaseTestCase):
    def __init__(self, *args, **kwargs):
        super(SyncGuesserTest, self).__init__(*args, **kwargs)
        self.uses_database = False

        self.older_pairing_record = PairingHistoryRecord(org_name='test', start_date=datetime(2020, 7, 8))
        self._mid_march_date = datetime(2021, 3, 14)
        self._late_march_date = datetime(2021, 3, 27)
        self._start_of_apr = datetime(2021, 4, 1)
        self._mid_apr_date = datetime(2021, 4, 16)
        self._late_apr_date = datetime(2021, 4, 25)
        self._start_of_may = datetime(2021, 5, 1)
        self._early_may_date = datetime(2021, 5, 4)
        self._mid_may_date = datetime(2021, 5, 14)
        self._late_may_date = datetime(2021, 5, 27)
        self._start_of_june = datetime(2021, 6, 1)
        self._mid_june_date = datetime(2021, 6, 16)

    def test_sync_from_primary_time(self):
        # Same day as consent payload time would have synced
        self.assertEqual(
            self._start_of_apr,
            ConsentSyncGuesser.get_sync_date(
                file=ConsentFile(file_upload_time=self._mid_march_date),
                summary=ParticipantSummary(consentForStudyEnrollmentTime=self._mid_march_date),
                latest_pairing_info=self.older_pairing_record
            )
        )
        # Later in the same month of consent payload time
        self.assertEqual(
            self._start_of_apr,
            ConsentSyncGuesser.get_sync_date(
                file=ConsentFile(file_upload_time=self._late_march_date),
                summary=ParticipantSummary(consentForStudyEnrollmentTime=self._mid_march_date),
                latest_pairing_info=self.older_pairing_record
            )
        )
        # File uploaded after month of consent payload would not have synced
        self.assertIsNone(
            ConsentSyncGuesser.get_sync_date(
                file=ConsentFile(file_upload_time=self._mid_apr_date),
                summary=ParticipantSummary(consentForStudyEnrollmentTime=self._mid_march_date),
                latest_pairing_info=self.older_pairing_record
            )
        )

    def test_sync_from_ehr_time(self):
        self.assertEqual(
            self._start_of_apr,
            ConsentSyncGuesser.get_sync_date(
                file=ConsentFile(file_upload_time=self._late_march_date),
                summary=ParticipantSummary(consentForElectronicHealthRecordsTime=self._mid_march_date),
                latest_pairing_info=self.older_pairing_record
            )
        )

    def test_sync_from_gror_time(self):
        # A check for GROR consent time was added for the June 2021 sync.
        # Any time before that would have not triggered a sync.
        self.assertIsNone(
            ConsentSyncGuesser.get_sync_date(
                file=ConsentFile(file_upload_time=self._mid_march_date),
                summary=ParticipantSummary(consentForGenomicsRORTime=self._mid_march_date),
                latest_pairing_info=self.older_pairing_record
            )
        )

        # Same day as consent payload time would have synced
        self.assertEqual(
            self._start_of_june,
            ConsentSyncGuesser.get_sync_date(
                file=ConsentFile(file_upload_time=self._mid_may_date),
                summary=ParticipantSummary(consentForGenomicsRORTime=self._mid_may_date),
                latest_pairing_info=self.older_pairing_record
            )
        )
        # Later in the same month of consent payload time
        self.assertEqual(
            self._start_of_june,
            ConsentSyncGuesser.get_sync_date(
                file=ConsentFile(file_upload_time=self._late_may_date),
                summary=ParticipantSummary(consentForGenomicsRORTime=self._mid_may_date),
                latest_pairing_info=self.older_pairing_record
            )
        )
        # File uploaded after month of consent payload would not have synced
        self.assertIsNone(
            ConsentSyncGuesser.get_sync_date(
                file=ConsentFile(file_upload_time=self._mid_june_date),
                summary=ParticipantSummary(consentForGenomicsRORTime=self._mid_may_date),
                latest_pairing_info=self.older_pairing_record
            )
        )

    def test_org_pairing_check(self):
        # A participant paired the same month as the file would have it copied
        self.assertEqual(
            self._start_of_june,
            ConsentSyncGuesser.get_sync_date(
                file=ConsentFile(file_upload_time=self._mid_may_date),
                summary=ParticipantSummary(consentForStudyEnrollmentTime=self._mid_may_date),
                latest_pairing_info=PairingHistoryRecord(org_name='test', start_date=self._late_may_date)
            )
        )
        # A file at the end of the previous month would have been picked up in the time window
        # Note: The sync date would have been at the start of June, but this is considered close enough for the
        #       purposes of this code.
        self.assertEqual(
            self._start_of_may,
            ConsentSyncGuesser.get_sync_date(
                file=ConsentFile(file_upload_time=self._late_apr_date),
                summary=ParticipantSummary(consentForStudyEnrollmentTime=self._late_apr_date),
                latest_pairing_info=PairingHistoryRecord(org_name='test', start_date=self._late_may_date)
            )
        )
        # A consent after the pairing should be ok
        self.assertEqual(
            self._start_of_may,
            ConsentSyncGuesser.get_sync_date(
                file=ConsentFile(file_upload_time=self._late_apr_date),
                summary=ParticipantSummary(consentForStudyEnrollmentTime=self._late_apr_date),
                latest_pairing_info=PairingHistoryRecord(org_name='test', start_date=self._mid_march_date)
            )
        )
        # A consent earlier than the time window would not have copied
        self.assertIsNone(
            ConsentSyncGuesser.get_sync_date(
                file=ConsentFile(file_upload_time=self._mid_apr_date),
                summary=ParticipantSummary(consentForStudyEnrollmentTime=self._mid_apr_date),
                latest_pairing_info=PairingHistoryRecord(org_name='test', start_date=self._early_may_date)
            )
        )

    def test_sync_from_time_overlap(self):
        # Files at the end of the previous month still get picked up by triggering dates in the next month
        self.assertEqual(
            self._start_of_may,
            ConsentSyncGuesser.get_sync_date(
                file=ConsentFile(file_upload_time=self._late_march_date),
                summary=ParticipantSummary(consentForElectronicHealthRecordsTime=self._mid_apr_date),
                latest_pairing_info=self.older_pairing_record
            )
        )
